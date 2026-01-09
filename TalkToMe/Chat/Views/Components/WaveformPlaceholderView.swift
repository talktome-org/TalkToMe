import SwiftUI

struct WaveformPlaceholderView: View {

    let currentLevel: CGFloat

    var barWidth: CGFloat = 2.5
    var barSpacing: CGFloat = 1.5
    var minHeight: CGFloat = 2
    var maxHeight: CGFloat = 30
    var color: Color = Color(red: 0.54, green: 0.32, blue: 0.78)
    var widthGain: CGFloat = 1.2
    var scrollSpeed: CGFloat = 40
    private let maxBars: Int = 200

    @State private var bars: [CGFloat] = []
    @State private var phaseOffset: CGFloat = 0
    @State private var distanceAccumulator: CGFloat = 0
    @State private var lastTick: TimeInterval = 0
    private let ticker = Timer.publish(every: 1.0 / 60.0, on: .main, in: .common).autoconnect()

    var body: some View {
        Canvas(rendersAsynchronously: true) { context, size in
            let unit = barWidth + barSpacing
            let offset = phaseOffset.truncatingRemainder(dividingBy: unit)
            let startIndex = max(0, bars.count - Int(ceil((size.width + unit) / unit)) - 2)
            var x = size.width - offset - unit * CGFloat(bars.count - 1 - startIndex)
            for i in startIndex..<bars.count {
                let v = bars[i]
                let w = barWidth + (widthGain * (0.5 + 0.5 * v))
                let h = minHeight + (maxHeight - minHeight) * v
                let rect = CGRect(x: x, y: (size.height - h) / 2, width: w, height: h)
                context.fill(Path(roundedRect: rect, cornerRadius: 1.5), with: .color(color))
                x += unit
            }
        }
        .frame(height: maxHeight)
        .onReceive(ticker) { now in
            let t = now.timeIntervalSinceReferenceDate
            if lastTick == 0 { lastTick = t; return }
            let dt = CGFloat(t - lastTick)
            lastTick = t
            let unit = barWidth + barSpacing
            let advance = scrollSpeed * dt
            phaseOffset += advance
            distanceAccumulator += advance
            let clamped = max(0, min(1, currentLevel))
            while distanceAccumulator >= unit {
                distanceAccumulator -= unit
                bars.append(clamped)
                if bars.count > maxBars { bars.removeFirst(bars.count - maxBars) }
            }
        }
        .accessibilityHidden(true)
        .onAppear {
            bars = []
            phaseOffset = 0
            distanceAccumulator = 0
            lastTick = 0
        }
    }
}

#Preview {
    WaveformPlaceholderView(currentLevel: 0.5)
        .padding()
}


