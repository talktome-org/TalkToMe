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

    struct CommittedVoiceMessage {
        let id: UUID?
        let sessionId: UUID
        let role: String
        let content: String
    }

    @Published private(set) var phase: Phase = .idle
    @Published private(set) var isConnected: Bool = false
    @Published private(set) var micLevel: Float = 0
    @Published private(set) var speakerLevel: Float = 0
    @Published private(set) var isMuted: Bool = false

    /// Live (interim or final) transcript of what the user is currently saying.
    /// Cleared once the agent commits the turn (see `onMessageCommitted`).
    @Published private(set) var liveUserTranscript: String = ""
    /// Live transcript of the agent's current utterance, word-by-word as it speaks.
    @Published private(set) var liveAgentTranscript: String = ""

    /// Fired when the worker reports a turn has been persisted to the chat DB.
    /// The receiver should insert the message into the chat and dedup against
    /// the eventual GET /messages refresh.
    var onMessageCommitted: ((CommittedVoiceMessage) -> Void)?

    private var room: Room?
    private var agentParticipant: RemoteParticipant?
    private var levelTimer: Timer?
    /// Set while `disconnect()` is running so the `RoomDelegate` callbacks
    /// don't clobber the post-teardown `.idle` phase with a stale `.error`.
    private var isTearingDown: Bool = false
    private var localIdentity: String?
    private var liveTranscriptSegments: [String: String] = [:]

    private static let customPromptMaxCharacters = 8_000
    private static let transcriptionTopic = "lk.transcription"
    private static let messageCommittedTopic = "bobo.message_committed"

    deinit {
        levelTimer?.invalidate()
    }

    func connect(voiceAgent: String, ghostName: String?, customPrompt: String?, sessionId: UUID?) async throws {
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
            sessionId: sessionId,
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
            self.localIdentity = room.localParticipant.identity?.stringValue
            try await registerStreamHandlers(on: room)
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

    /// Wire up text-stream handlers for live transcripts and committed-message
    /// notifications. Called once on connect; LiveKit unregisters them on
    /// disconnect automatically.
    private func registerStreamHandlers(on room: Room) async throws {
        try await room.registerTextStreamHandler(for: Self.transcriptionTopic) { [weak self] reader, participantIdentity in
            guard let self else { return }
            let identityString = participantIdentity.stringValue
            let info = reader.info
            let isFinal = (info.attributes["lk.transcription_final"] ?? "false") == "true"
            let segmentId = info.attributes["lk.segment_id"] ?? identityString
            let text: String
            do {
                text = try await reader.readAll()
            } catch {
                return
            }
            await self.applyTranscriptUpdate(
                participantIdentity: identityString,
                segmentId: segmentId,
                text: text,
                isFinal: isFinal
            )
        }

        try await room.registerTextStreamHandler(for: Self.messageCommittedTopic) { [weak self] reader, _ in
            guard let self else { return }
            let payload: String
            do {
                payload = try await reader.readAll()
            } catch {
                return
            }
            await self.handleCommittedMessage(payload: payload)
        }
    }

    @MainActor
    private func applyTranscriptUpdate(
        participantIdentity: String,
        segmentId: String,
        text: String,
        isFinal: Bool
    ) {
        let isLocal = (participantIdentity == localIdentity)
        if isFinal {
            liveTranscriptSegments.removeValue(forKey: segmentId)
            if isLocal { liveUserTranscript = "" } else { liveAgentTranscript = "" }
        } else {
            liveTranscriptSegments[segmentId] = text
            if isLocal {
                liveUserTranscript = text
            } else {
                liveAgentTranscript = text
            }
        }
    }

    @MainActor
    private func handleCommittedMessage(payload: String) {
        guard let data = payload.data(using: .utf8),
              let obj = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
              let role = obj["role"] as? String,
              let content = obj["content"] as? String,
              let sessionIdStr = obj["session_id"] as? String,
              let sessionId = UUID(uuidString: sessionIdStr)
        else { return }

        let id = (obj["id"] as? String).flatMap(UUID.init(uuidString:))
        let msg = CommittedVoiceMessage(id: id, sessionId: sessionId, role: role, content: content)

        if role == "user" { liveUserTranscript = "" }
        if role == "assistant" { liveAgentTranscript = "" }

        onMessageCommitted?(msg)
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
        liveUserTranscript = ""
        liveAgentTranscript = ""
        liveTranscriptSegments.removeAll()
        localIdentity = nil

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

    nonisolated func roomDidConnect(_ room: Room) {
        Task { @MainActor in
            guard !self.isTearingDown else { return }
            self.isConnected = true
            if case .connecting = self.phase { self.phase = .listening }
        }
    }

    nonisolated func room(_ room: Room, didDisconnectWithError error: LiveKitError?) {
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

    nonisolated func room(_ room: Room, participant: RemoteParticipant, didSubscribeTrack publication: RemoteTrackPublication) {
        // Track auto-attaches its audio to the system output via LiveKit's audio engine.
    }

    nonisolated func room(_ room: Room, didUpdateSpeakingParticipants participants: [Participant]) {
        Task { @MainActor in
            guard !self.isTearingDown else { return }
            // Tolerate races where the speakers update arrives before
            // participantDidConnect: adopt the first non-local speaker we hear from.
            if self.agentParticipant == nil {
                if let remote = participants.first(where: { !($0 is LocalParticipant) }) as? RemoteParticipant {
                    self.agentParticipant = remote
                }
            }
            guard let agent = self.agentParticipant else { return }
            let agentIsSpeaking = participants.contains(where: { $0.identity == agent.identity })
            if agentIsSpeaking {
                self.phase = .speaking
            } else if self.isConnected {
                self.phase = .listening
            }
        }
    }
}
