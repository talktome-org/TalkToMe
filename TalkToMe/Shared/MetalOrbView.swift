import Metal
import MetalKit
import SwiftUI

/// SwiftUI wrapper around a Metal-rendered “Siri-like” orb.
struct MetalOrbView: UIViewRepresentable {
    var level: Float
    var isSpeaking: Bool

    func makeUIView(context: Context) -> MTKView {
        let view = MTKView(frame: .zero, device: MTLCreateSystemDefaultDevice())
        view.isOpaque = false
        view.backgroundColor = .clear
        view.clearColor = MTLClearColorMake(0, 0, 0, 0)
        view.framebufferOnly = true
        view.enableSetNeedsDisplay = false
        view.isPaused = false
        view.preferredFramesPerSecond = 60
        view.colorPixelFormat = .bgra8Unorm

        context.coordinator.attach(to: view)
        context.coordinator.set(level: level, isSpeaking: isSpeaking)
        return view
    }

    func updateUIView(_ uiView: MTKView, context: Context) {
        context.coordinator.set(level: level, isSpeaking: isSpeaking)
    }

    func makeCoordinator() -> Coordinator {
        Coordinator()
    }

    final class Coordinator: NSObject, MTKViewDelegate {
        private var renderer: MetalOrbRenderer?

        func attach(to view: MTKView) {
            guard renderer == nil else { return }
            renderer = MetalOrbRenderer(mtkView: view)
            view.delegate = self
        }

        func set(level: Float, isSpeaking: Bool) {
            renderer?.level = max(0, min(1, level))
            renderer?.isSpeaking = isSpeaking
        }

        func mtkView(_ view: MTKView, drawableSizeWillChange size: CGSize) {
            renderer?.drawableSize = size
        }

        func draw(in view: MTKView) {
            renderer?.draw(in: view)
        }
    }
}

private final class MetalOrbRenderer {
    struct Uniforms {
        var resolution: SIMD2<Float>
        var time: Float
        var level: Float
        var isSpeaking: Float
    }

    private let device: MTLDevice
    private let commandQueue: MTLCommandQueue
    private let pipeline: MTLRenderPipelineState

    private let start = CFAbsoluteTimeGetCurrent()
    private var uniformBuffer: MTLBuffer?

    var drawableSize: CGSize = .zero
    var level: Float = 0
    var isSpeaking: Bool = false

    init?(mtkView: MTKView) {
        guard let device = mtkView.device ?? MTLCreateSystemDefaultDevice() else { return nil }
        self.device = device
        guard let q = device.makeCommandQueue() else { return nil }
        self.commandQueue = q

        // Load shader functions from the default library (OrbShader.metal).
        guard let library = device.makeDefaultLibrary() else { return nil }
        guard let v = library.makeFunction(name: "orbVertex"),
              let f = library.makeFunction(name: "orbFragment") else { return nil }

        let desc = MTLRenderPipelineDescriptor()
        desc.vertexFunction = v
        desc.fragmentFunction = f
        desc.colorAttachments[0].pixelFormat = mtkView.colorPixelFormat
        // Enable alpha blending so orb can sit on top of SwiftUI materials.
        let a0 = desc.colorAttachments[0]
        a0?.isBlendingEnabled = true
        a0?.rgbBlendOperation = .add
        a0?.alphaBlendOperation = .add
        a0?.sourceRGBBlendFactor = .sourceAlpha
        a0?.destinationRGBBlendFactor = .oneMinusSourceAlpha
        a0?.sourceAlphaBlendFactor = .one
        a0?.destinationAlphaBlendFactor = .oneMinusSourceAlpha

        do {
            self.pipeline = try device.makeRenderPipelineState(descriptor: desc)
        } catch {
            return nil
        }

        self.uniformBuffer = device.makeBuffer(length: MemoryLayout<Uniforms>.stride, options: .storageModeShared)
    }

    func draw(in view: MTKView) {
        guard let drawable = view.currentDrawable,
              let rpd = view.currentRenderPassDescriptor,
              let cmd = commandQueue.makeCommandBuffer(),
              let enc = cmd.makeRenderCommandEncoder(descriptor: rpd)
        else { return }

        let now = CFAbsoluteTimeGetCurrent()
        let t = Float(now - start)
        let u = Uniforms(
            resolution: SIMD2(Float(drawableSize.width), Float(drawableSize.height)),
            time: t,
            level: level,
            isSpeaking: isSpeaking ? 1 : 0
        )
        if let buf = uniformBuffer {
            memcpy(buf.contents(), [u], MemoryLayout<Uniforms>.stride)
            enc.setVertexBuffer(nil, offset: 0, index: 0)
            enc.setFragmentBuffer(buf, offset: 0, index: 0)
        }

        enc.setRenderPipelineState(pipeline)
        enc.drawPrimitives(type: .triangleStrip, vertexStart: 0, vertexCount: 4)
        enc.endEncoding()

        cmd.present(drawable)
        cmd.commit()
    }
}

#Preview("Metal Orb") {
    VStack(spacing: 18) {
        TimelineView(.animation) { context in
            let t = context.date.timeIntervalSinceReferenceDate
            let level = Float((sin(t * 2.2) * 0.5 + 0.5)) // 0..1
            MetalOrbView(level: level, isSpeaking: true)
                .frame(width: 140, height: 140)
                .background(.thinMaterial, in: RoundedRectangle(cornerRadius: 24, style: .continuous))
                .overlay(
                    RoundedRectangle(cornerRadius: 24, style: .continuous)
                        .strokeBorder(Color.white.opacity(0.12), lineWidth: 1)
                )
        }

        HStack(spacing: 14) {
            MetalOrbView(level: 0.08, isSpeaking: false)
                .frame(width: 64, height: 64)
                .background(.thinMaterial, in: RoundedRectangle(cornerRadius: 18, style: .continuous))

            MetalOrbView(level: 0.45, isSpeaking: true)
                .frame(width: 64, height: 64)
                .background(.thinMaterial, in: RoundedRectangle(cornerRadius: 18, style: .continuous))

            MetalOrbView(level: 0.95, isSpeaking: true)
                .frame(width: 64, height: 64)
                .background(.thinMaterial, in: RoundedRectangle(cornerRadius: 18, style: .continuous))
        }
    }
    .padding(24)
    .background(Color.black.opacity(0.92))
}

