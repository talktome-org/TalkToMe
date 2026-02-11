import SwiftUI
import AVKit

struct TransparentVideoPlayerView: UIViewRepresentable {
    let videoName: String
    let videoExtension: String
    var loop: Bool = true

    func makeUIView(context: Context) -> TransparentPlayerUIView {
        let view = TransparentPlayerUIView()
        view.configure(videoName: videoName, extension: videoExtension, loop: loop)
        return view
    }

    func updateUIView(_ uiView: TransparentPlayerUIView, context: Context) {}

    static func dismantleUIView(_ uiView: TransparentPlayerUIView, coordinator: ()) {
        uiView.cleanup()
    }
}

final class TransparentPlayerUIView: UIView {
    private var queuePlayer: AVQueuePlayer?
    private var playerLooper: AVPlayerLooper?
    private var playerLayer: AVPlayerLayer?
    private var statusObservation: NSKeyValueObservation?
    private var rateObservation: NSKeyValueObservation?
    private var heartbeatTimer: Timer?
    private var shouldLoop = true

    override init(frame: CGRect) {
        super.init(frame: frame)
        backgroundColor = .clear
    }

    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    func configure(videoName: String, extension ext: String, loop: Bool) {
        shouldLoop = loop

        guard let url = Bundle.main.url(forResource: videoName, withExtension: ext) else {
            print("TransparentVideoPlayer: Could not find \(videoName).\(ext)")
            return
        }

        // Decorative ghost playback should never claim exclusive audio focus.
        SharedAudioEngine.shared.ensureIdleAudioSession()

        let asset = AVURLAsset(url: url)
        let playerItem = AVPlayerItem(asset: asset)
        // Disable embedded audio tracks so decorative ghost loops are strictly visual.
        for track in playerItem.tracks where track.assetTrack?.mediaType == .audio {
            track.isEnabled = false
        }
        let queuePlayer = AVQueuePlayer(playerItem: playerItem)
        queuePlayer.volume = 0
        queuePlayer.isMuted = true
        // Local file – never wait for network buffering
        queuePlayer.automaticallyWaitsToMinimizeStalling = false
        self.queuePlayer = queuePlayer

        // Use AVPlayerLooper for seamless gapless looping
        if loop {
            playerLooper = AVPlayerLooper(player: queuePlayer, templateItem: playerItem)
        }

        let layer = AVPlayerLayer(player: queuePlayer)
        layer.videoGravity = .resizeAspect
        layer.backgroundColor = UIColor.clear.cgColor
        // Enable alpha channel rendering for HEVC with alpha
        layer.pixelBufferAttributes = [
            kCVPixelBufferPixelFormatTypeKey as String: kCVPixelFormatType_32BGRA
        ]
        self.layer.addSublayer(layer)
        playerLayer = layer

        queuePlayer.play()

        // Resume playback when app returns to foreground.
        NotificationCenter.default.addObserver(
            self,
            selector: #selector(appDidBecomeActive),
            name: UIApplication.didBecomeActiveNotification,
            object: nil
        )
        NotificationCenter.default.addObserver(
            self,
            selector: #selector(handleAudioInterruption(_:)),
            name: AVAudioSession.interruptionNotification,
            object: nil
        )
        NotificationCenter.default.addObserver(
            self,
            selector: #selector(handleRouteChange),
            name: AVAudioSession.routeChangeNotification,
            object: nil
        )

        // Keep the ghost moving if voice mode audio/session changes pause it.
        statusObservation = queuePlayer.observe(\.timeControlStatus, options: [.new]) { [weak self] _, _ in
            self?.resumeIfNeeded()
        }
        rateObservation = queuePlayer.observe(\.rate, options: [.new]) { [weak self] _, _ in
            self?.resumeIfNeeded()
        }

        // Lightweight safety net: if AVPlayer silently stalls after session switches,
        // this keeps the loop alive without the old high-frequency timer overhead.
        let timer = Timer(timeInterval: 0.8, repeats: true) { [weak self] _ in
            self?.resumeIfNeeded()
        }
        RunLoop.main.add(timer, forMode: .common)
        heartbeatTimer = timer
    }

    @objc private func appDidBecomeActive() {
        resumeIfNeeded()
    }

    @objc private func handleAudioInterruption(_ notification: Notification) {
        guard let info = notification.userInfo,
              let typeValue = info[AVAudioSessionInterruptionTypeKey] as? UInt,
              let type = AVAudioSession.InterruptionType(rawValue: typeValue) else { return }
        if type == .ended {
            resumeIfNeeded()
        }
    }

    @objc private func handleRouteChange() {
        resumeIfNeeded()
    }

    private func resumeIfNeeded() {
        guard shouldLoop else { return }
        guard UIApplication.shared.applicationState == .active else { return }
        guard let player = queuePlayer else { return }

        // After route/session changes AVPlayer may stick in waiting state until nudged.
        if player.timeControlStatus == .waitingToPlayAtSpecifiedRate {
            let t = player.currentTime()
            player.seek(to: t, toleranceBefore: .zero, toleranceAfter: .zero)
        }
        if player.rate == 0 || player.timeControlStatus != .playing {
            player.play()
        }
    }

    override func layoutSubviews() {
        super.layoutSubviews()
        playerLayer?.frame = bounds
    }

    func cleanup() {
        shouldLoop = false
        heartbeatTimer?.invalidate()
        heartbeatTimer = nil
        statusObservation?.invalidate()
        statusObservation = nil
        rateObservation?.invalidate()
        rateObservation = nil
        NotificationCenter.default.removeObserver(self)
        playerLooper?.disableLooping()
        playerLooper = nil
        queuePlayer?.pause()
        queuePlayer = nil
        playerLayer?.removeFromSuperlayer()
        playerLayer = nil
    }

    deinit {
        cleanup()
    }
}

#Preview {
    TransparentVideoPlayerView(videoName: "1vmake", videoExtension: "mp4")
        .frame(width: 100, height: 100)
        .background(Color.gray.opacity(0.3))
}
