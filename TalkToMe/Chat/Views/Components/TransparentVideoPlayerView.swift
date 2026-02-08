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

    override init(frame: CGRect) {
        super.init(frame: frame)
        backgroundColor = .clear
    }

    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    func configure(videoName: String, extension ext: String, loop: Bool) {
        guard let url = Bundle.main.url(forResource: videoName, withExtension: ext) else {
            print("TransparentVideoPlayer: Could not find \(videoName).\(ext)")
            return
        }

        let asset = AVURLAsset(url: url)
        let playerItem = AVPlayerItem(asset: asset)
        let queuePlayer = AVQueuePlayer(playerItem: playerItem)
        queuePlayer.isMuted = true
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
    }

    override func layoutSubviews() {
        super.layoutSubviews()
        playerLayer?.frame = bounds
    }

    func cleanup() {
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
