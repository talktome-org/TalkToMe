import SwiftUI
import AVKit

struct TransparentVideoPlayerView: UIViewRepresentable {
    let videoName: String
    let videoExtension: String
    var loop: Bool = true
    var startTime: Double = 0

    func makeUIView(context: Context) -> TransparentPlayerUIView {
        // Reuse a recycled view that is already playing this video.
        if let recycled = TransparentPlayerUIView.takeRecycled(for: videoName) {
            return recycled
        }
        let view = TransparentPlayerUIView()
        view.configure(videoName: videoName, extension: videoExtension, loop: loop, startTime: startTime)
        return view
    }

    func updateUIView(_ uiView: TransparentPlayerUIView, context: Context) {
        uiView.swapVideoIfNeeded(videoName: videoName, extension: videoExtension, loop: loop, startTime: startTime)
    }

    static func dismantleUIView(_ uiView: TransparentPlayerUIView, coordinator: ()) {
        // Don't destroy — recycle so the player keeps running across sheet presentations.
        TransparentPlayerUIView.recycle(uiView)
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
    private(set) var currentVideoName: String = ""

    // MARK: - Recycling pool

    private static let recycleLock = NSLock()
    private static var recycledViews: [String: TransparentPlayerUIView] = [:]

    /// Return a view that is already playing `videoName`, or nil.
    static func takeRecycled(for videoName: String) -> TransparentPlayerUIView? {
        recycleLock.lock()
        defer { recycleLock.unlock() }
        return recycledViews.removeValue(forKey: videoName)
    }

    /// Stash a view for reuse instead of destroying it.
    static func recycle(_ view: TransparentPlayerUIView) {
        let name = view.currentVideoName
        guard !name.isEmpty else {
            view.cleanup()
            return
        }
        // Detach from old superview but keep the player + layer alive.
        view.removeFromSuperview()
        recycleLock.lock()
        recycledViews[name] = view
        recycleLock.unlock()
    }

    // MARK: - Preload cache

    private static let preloadLock = NSLock()
    private static var preloadedPlayers: [String: (player: AVQueuePlayer, looper: AVPlayerLooper?)] = [:]

    /// Pre-build an AVQueuePlayer on a background thread so it's ready instantly
    /// when `configure` is called later.
    static func preload(videoName: String, extension ext: String, loop: Bool, startTime: Double = 0) {
        preloadLock.lock()
        let exists = preloadedPlayers[videoName] != nil
        preloadLock.unlock()
        if exists { return }

        guard let url = Bundle.main.url(forResource: videoName, withExtension: ext) else { return }

        DispatchQueue.global(qos: .userInitiated).async {
            let asset = AVURLAsset(url: url)
            let playerItem = AVPlayerItem(asset: asset)
            for track in playerItem.tracks where track.assetTrack?.mediaType == .audio {
                track.isEnabled = false
            }
            let player = AVQueuePlayer(playerItem: playerItem)
            player.volume = 0
            player.isMuted = true
            player.automaticallyWaitsToMinimizeStalling = false

            let looper: AVPlayerLooper? = {
                guard loop else { return nil }
                if startTime > 0 {
                    let start = CMTime(seconds: startTime, preferredTimescale: 600)
                    let range = CMTimeRange(start: start, end: CMTime.positiveInfinity)
                    return AVPlayerLooper(player: player, templateItem: playerItem, timeRange: range)
                }
                return AVPlayerLooper(player: player, templateItem: playerItem)
            }()

            preloadLock.lock()
            preloadedPlayers[videoName] = (player: player, looper: looper)
            preloadLock.unlock()
        }
    }

    private static func takePreloaded(for videoName: String) -> (player: AVQueuePlayer, looper: AVPlayerLooper?)? {
        preloadLock.lock()
        defer { preloadLock.unlock() }
        return preloadedPlayers.removeValue(forKey: videoName)
    }

    // MARK: - Init

    override init(frame: CGRect) {
        super.init(frame: frame)
        backgroundColor = .clear
    }

    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    func swapVideoIfNeeded(videoName: String, extension ext: String, loop: Bool, startTime: Double = 0) {
        guard videoName != currentVideoName else { return }
        cleanup()
        configure(videoName: videoName, extension: ext, loop: loop, startTime: startTime)
    }

    func configure(videoName: String, extension ext: String, loop: Bool, startTime: Double = 0) {
        currentVideoName = videoName
        shouldLoop = loop

        // Decorative ghost playback should never claim exclusive audio focus.
        SharedAudioEngine.shared.ensureIdleAudioSession()

        // Fast path: use a pre-warmed player if available.
        if let preloaded = Self.takePreloaded(for: videoName) {
            attachPlayer(preloaded.player, looper: preloaded.looper)
            return
        }

        guard let url = Bundle.main.url(forResource: videoName, withExtension: ext) else {
            print("TransparentVideoPlayer: Could not find \(videoName).\(ext)")
            return
        }

        // Build AVPlayer off the main thread to avoid blocking UI.
        let capturedName = videoName
        DispatchQueue.global(qos: .userInitiated).async { [weak self] in
            let asset = AVURLAsset(url: url)
            let playerItem = AVPlayerItem(asset: asset)
            // Disable embedded audio tracks so decorative ghost loops are strictly visual.
            for track in playerItem.tracks where track.assetTrack?.mediaType == .audio {
                track.isEnabled = false
            }
            let player = AVQueuePlayer(playerItem: playerItem)
            player.volume = 0
            player.isMuted = true
            player.automaticallyWaitsToMinimizeStalling = false

            let looper: AVPlayerLooper? = {
                guard loop else { return nil }
                if startTime > 0 {
                    let start = CMTime(seconds: startTime, preferredTimescale: 600)
                    let range = CMTimeRange(start: start, end: CMTime.positiveInfinity)
                    return AVPlayerLooper(player: player, templateItem: playerItem, timeRange: range)
                }
                return AVPlayerLooper(player: player, templateItem: playerItem)
            }()

            DispatchQueue.main.async { [weak self] in
                guard let self else {
                    player.pause()
                    looper?.disableLooping()
                    return
                }
                guard self.currentVideoName == capturedName else {
                    player.pause()
                    looper?.disableLooping()
                    return
                }
                self.attachPlayer(player, looper: looper)
            }
        }
    }

    /// Attach a freshly-built player to this view and start playback.
    private func attachPlayer(_ player: AVQueuePlayer, looper: AVPlayerLooper?) {
        self.queuePlayer = player
        self.playerLooper = looper

        let layer = AVPlayerLayer(player: player)
        layer.videoGravity = .resizeAspect
        layer.backgroundColor = UIColor.clear.cgColor
        layer.pixelBufferAttributes = [
            kCVPixelBufferPixelFormatTypeKey as String: kCVPixelFormatType_32BGRA
        ]
        self.layer.addSublayer(layer)
        layer.frame = self.bounds
        self.playerLayer = layer

        player.play()
        setupObservers(for: player)
    }

    private func setupObservers(for player: AVQueuePlayer) {
        NotificationCenter.default.addObserver(
            self,
            selector: #selector(self.appDidBecomeActive),
            name: UIApplication.didBecomeActiveNotification,
            object: nil
        )
        NotificationCenter.default.addObserver(
            self,
            selector: #selector(self.handleAudioInterruption(_:)),
            name: AVAudioSession.interruptionNotification,
            object: nil
        )
        NotificationCenter.default.addObserver(
            self,
            selector: #selector(self.handleRouteChange),
            name: AVAudioSession.routeChangeNotification,
            object: nil
        )

        self.statusObservation = player.observe(\.timeControlStatus, options: [.new]) { [weak self] _, _ in
            self?.resumeIfNeeded()
        }
        self.rateObservation = player.observe(\.rate, options: [.new]) { [weak self] _, _ in
            self?.resumeIfNeeded()
        }

        let timer = Timer(timeInterval: 0.8, repeats: true) { [weak self] _ in
            self?.resumeIfNeeded()
        }
        RunLoop.main.add(timer, forMode: .common)
        self.heartbeatTimer = timer
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
