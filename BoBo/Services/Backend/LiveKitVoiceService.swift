import AVFoundation
import Combine
import Foundation
import LiveKit

/// Owns the LiveKit Room for voice mode. Connects, publishes the mic, and
/// surfaces the agent state so `ChatVoiceModeController` can drive UI without
/// understanding WebRTC.
@MainActor
final class LiveKitVoiceService: NSObject, ObservableObject {

    enum Phase: Equatable {
        case idle
        case connecting
        case listening   // user can speak; agent is silent
        case speaking    // agent is speaking
        case error(String)
    }

    enum ConnectError: LocalizedError {
        case microphonePermissionDenied
        case invalidServerURL
        case missingAccessToken

        var errorDescription: String? {
            switch self {
            case .microphonePermissionDenied: return "Microphone access is required for voice mode."
            case .invalidServerURL: return "Voice service returned an invalid URL."
            case .missingAccessToken: return "Sign in to use voice mode."
            }
        }
    }

    @Published private(set) var phase: Phase = .idle
    @Published private(set) var isConnected: Bool = false
    @Published private(set) var micLevel: Float = 0
    @Published private(set) var speakerLevel: Float = 0
    @Published private(set) var isMuted: Bool = false

    private var room: Room?
    private var agentParticipant: RemoteParticipant?
    private var levelTimer: Timer?
    /// Set while `disconnect()` is running so the `RoomDelegate` callbacks
    /// don't clobber the post-teardown `.idle` phase with a stale `.error`.
    private var isTearingDown: Bool = false

    private static let customPromptMaxCharacters = 8_000

    deinit {
        levelTimer?.invalidate()
    }

    func connect(voiceAgent: String, ghostName: String?, customPrompt: String?) async throws {
        // Allow recovery from a previous error without a manual disconnect.
        switch phase {
        case .idle, .error: break
        default: return
        }

        phase = .connecting
        isTearingDown = false

        guard await Self.ensureMicrophonePermission() else {
            phase = .error(ConnectError.microphonePermissionDenied.localizedDescription)
            throw ConnectError.microphonePermissionDenied
        }

        guard let accessToken = await AuthService.shared.getAccessToken() else {
            phase = .error(ConnectError.missingAccessToken.localizedDescription)
            throw ConnectError.missingAccessToken
        }

        let cappedPrompt = customPrompt.map { String($0.prefix(Self.customPromptMaxCharacters)) }

        let token = try await BackendService.shared.fetchLiveKitToken(
            voiceAgent: voiceAgent,
            ghostName: ghostName,
            customPrompt: cappedPrompt,
            accessToken: accessToken
        )

        guard let url = URL(string: token.url), url.scheme == "wss" || url.scheme == "ws" else {
            phase = .error(ConnectError.invalidServerURL.localizedDescription)
            throw ConnectError.invalidServerURL
        }

        let room = Room()
        room.add(delegate: self)
        self.room = room

        let connectOptions = ConnectOptions(autoSubscribe: true)
        let roomOptions = RoomOptions(defaultAudioCaptureOptions: AudioCaptureOptions())

        do {
            try await room.connect(url: url.absoluteString, token: token.token, connectOptions: connectOptions, roomOptions: roomOptions)
            try await room.localParticipant.setMicrophone(enabled: true)
            isConnected = true
            phase = .listening
            startLevelPolling()
        } catch {
            phase = .error(error.localizedDescription)
            self.room = nil
            levelTimer?.invalidate()
            levelTimer = nil
            throw error
        }
    }

    func disconnect() async {
        isTearingDown = true
        levelTimer?.invalidate()
        levelTimer = nil

        let roomToTearDown = room
        // Clear state synchronously so re-entrant delegate callbacks early-return.
        room = nil
        agentParticipant = nil
        isConnected = false
        micLevel = 0
        speakerLevel = 0
        phase = .idle

        if let roomToTearDown {
            await roomToTearDown.disconnect()
        }
        // Note: `isTearingDown` stays true until the next `connect()` resets it,
        // so any late `didDisconnect` callback queued during teardown is still
        // ignored after the await returns.
    }

    func setMuted(_ muted: Bool) async {
        guard let room else { return }
        do {
            try await room.localParticipant.setMicrophone(enabled: !muted)
            isMuted = muted
        } catch {
            // Surface but don't crash — mute is best-effort.
            phase = .error(error.localizedDescription)
        }
    }

    private func startLevelPolling() {
        levelTimer?.invalidate()
        levelTimer = Timer.scheduledTimer(withTimeInterval: 0.1, repeats: true) { [weak self] _ in
            Task { @MainActor in
                guard let self else { return }
                let newMic = Float(self.room?.localParticipant.audioLevel ?? 0)
                let newSpeaker = Float(self.agentParticipant?.audioLevel ?? 0)
                // Threshold-dedup so SwiftUI doesn't re-render 10×/sec on noise.
                if abs(newMic - self.micLevel) > 0.02 { self.micLevel = newMic }
                if abs(newSpeaker - self.speakerLevel) > 0.02 { self.speakerLevel = newSpeaker }
            }
        }
    }

    private static func ensureMicrophonePermission() async -> Bool {
        if #available(iOS 17, *) {
            switch AVAudioApplication.shared.recordPermission {
            case .granted: return true
            case .denied: return false
            case .undetermined:
                return await AVAudioApplication.requestRecordPermission()
            @unknown default: return false
            }
        } else {
            switch AVAudioSession.sharedInstance().recordPermission {
            case .granted: return true
            case .denied: return false
            case .undetermined:
                return await withCheckedContinuation { cont in
                    AVAudioSession.sharedInstance().requestRecordPermission { granted in
                        cont.resume(returning: granted)
                    }
                }
            @unknown default: return false
            }
        }
    }
}

extension LiveKitVoiceService: RoomDelegate {

    nonisolated func room(_ room: Room, didConnect isReconnect: Bool) {
        Task { @MainActor in
            guard !self.isTearingDown else { return }
            self.isConnected = true
            if case .connecting = self.phase { self.phase = .listening }
        }
    }

    nonisolated func room(_ room: Room, didDisconnect error: LiveKitError?) {
        Task { @MainActor in
            // If we initiated the teardown, `disconnect()` already set state to .idle.
            guard !self.isTearingDown else { return }
            self.isConnected = false
            if let error {
                self.phase = .error(error.localizedDescription)
            } else {
                self.phase = .idle
            }
        }
    }

    nonisolated func room(_ room: Room, participantDidConnect participant: RemoteParticipant) {
        Task { @MainActor in
            guard !self.isTearingDown else { return }
            // The agent worker is the only other participant in our voice rooms.
            self.agentParticipant = participant
        }
    }

    nonisolated func room(_ room: Room, participantDidDisconnect participant: RemoteParticipant) {
        Task { @MainActor in
            if self.agentParticipant?.identity == participant.identity {
                self.agentParticipant = nil
            }
        }
    }

    nonisolated func room(_ room: Room, participant: RemoteParticipant, didSubscribePublication publication: RemoteTrackPublication) {
        // Track auto-attaches its audio to the system output via LiveKit's audio engine.
    }

    nonisolated func room(_ room: Room, participant: Participant, didUpdateIsSpeaking speaking: Bool) {
        Task { @MainActor in
            guard !self.isTearingDown else { return }
            // Tolerate races where speaking arrives before participantDidConnect:
            // adopt the first non-local participant we hear from.
            if self.agentParticipant == nil, !(participant is LocalParticipant), let remote = participant as? RemoteParticipant {
                self.agentParticipant = remote
            }
            guard let agent = self.agentParticipant, participant.identity == agent.identity else { return }
            if speaking {
                self.phase = .speaking
            } else if self.isConnected {
                self.phase = .listening
            }
        }
    }
}
