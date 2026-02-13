import SwiftUI

struct SpeechInputDrawerView: View {
    let phase: SpeakModePhase
    let micLevel: CGFloat
    let speakerLevel: CGFloat
    var onCancel: (() -> Void)? = nil

    @State private var snapBackRawOffset: CGFloat = 0
    @GestureState private var gestureRawOffset: CGFloat = 0
    @GestureState private var gestureIsActive: Bool = false

    private let cancelThreshold: CGFloat = -100

    private var rawDragOffset: CGFloat {
        gestureIsActive ? gestureRawOffset : snapBackRawOffset
    }

    private var statusText: String {
        if gestureIsActive && rawDragOffset < cancelThreshold {
            return "Release to cancel"
        }
        switch phase {
        case .idle: return "Ready"
        case .connecting: return "Connecting..."
        case .listening: return "Listening..."
        case .processing: return "Processing..."
        case .answering: return "Speaking..."
        }
    }

    private var barColor: Color {
        if gestureIsActive && rawDragOffset < cancelThreshold {
            return .red.opacity(0.9)
        }
        switch phase {
        case .idle: return .gray.opacity(0.5)
        case .connecting: return .gray.opacity(0.6)
        case .listening: return .red.opacity(0.8)
        case .processing: return .orange.opacity(0.8)
        case .answering: return .blue.opacity(0.8)
        }
    }

    private var activeLevel: CGFloat {
        switch phase {
        case .listening: return micLevel
        case .answering: return speakerLevel
        default: return 0
        }
    }

    private var effectiveOffset: CGFloat {
        // Apply resistance when dragging right (positive direction)
        if rawDragOffset > 0 {
            return rawDragOffset * 0.3
        }
        return rawDragOffset
    }

    private var cancelProgress: CGFloat {
        guard rawDragOffset < 0 else { return 0 }
        return min(1.0, abs(rawDragOffset) / abs(cancelThreshold))
    }

    var body: some View {
        if #available(iOS 26.0, *) {
            ZStack(alignment: .leading) {
                // Cancel indicator that appears when dragging left
                if gestureIsActive && rawDragOffset < -20 {
                    HStack(spacing: 6) {
                        Image(systemName: "xmark.circle.fill")
                            .font(.system(size: 18, weight: .semibold))
                        Text("Cancel")
                            .font(.system(size: 14, weight: .semibold))
                    }
                    .foregroundColor(.red.opacity(0.8 + cancelProgress * 0.2))
                    .padding(.leading, 24)
                    .opacity(cancelProgress)
                    .scaleEffect(0.8 + cancelProgress * 0.2)
                }

                // Main drawer content
                HStack(spacing: 10) {
                    AnimatedBarsView(
                        level: activeLevel,
                        color: barColor,
                        isAnimating: phase == .listening || phase == .answering,
                        isPulsing: phase == .processing || phase == .connecting
                    )

                    Text(statusText)
                        .font(.system(size: 15, weight: .medium))
                        .foregroundColor(gestureIsActive && rawDragOffset < cancelThreshold ? .red : .primary)
                        .animation(.smooth(duration: 0.25), value: statusText)

                    Spacer()

                    // Drag hint indicator
                    if !gestureIsActive {
                        Image(systemName: "chevron.left")
                            .font(.system(size: 12, weight: .medium))
                            .foregroundColor(.secondary.opacity(0.5))
                            .padding(.trailing, 4)
                    }
                }
                .padding(.horizontal, 12)
                .padding(.vertical, 14)
                .frame(maxWidth: .infinity)
                .glassEffect(.regular.interactive(), in: RoundedRectangle(cornerRadius: 16, style: .continuous))
                .contentShape(Rectangle())
                .offset(x: effectiveOffset)
                .scaleEffect(gestureIsActive ? 0.98 : 1.0, anchor: .center)
                .highPriorityGesture(
                    DragGesture(minimumDistance: 10)
                        .updating($gestureIsActive) { _, state, _ in
                            state = true
                        }
                        .updating($gestureRawOffset) { value, state, _ in
                            state = value.translation.width
                        }
                        .onChanged { _ in
                            // If we were snapping back, take control immediately.
                            snapBackRawOffset = 0
                        }
                        .onEnded { value in
                            let final = value.translation.width
                            if final < cancelThreshold {
                                Haptics.impact(.medium)
                                snapBackRawOffset = 0
                                onCancel?()
                                return
                            }

                            snapBackRawOffset = final
                            withAnimation(.spring(response: 0.35, dampingFraction: 0.78)) {
                                snapBackRawOffset = 0
                            }
                        }
                )
            }
            .padding(.horizontal, 16)
        }
    }
}

struct AnimatedBarsView: View {
    let level: CGFloat
    let color: Color
    let isAnimating: Bool
    let isPulsing: Bool

    @State private var pulsePhase: CGFloat = 0
    @State private var pulseTimer: Timer?

    private let baseHeights: [CGFloat] = [0.3, 0.5, 0.7, 0.5, 0.3]
    private let minHeight: CGFloat = 8
    private let maxHeight: CGFloat = 28

    var body: some View {
        HStack(alignment: .center, spacing: 4) {
            ForEach(0..<5, id: \.self) { index in
                Capsule()
                    .fill(color)
                    .frame(width: 4, height: barHeight(for: index))
            }
        }
        .frame(height: maxHeight)
        .animation(.smooth(duration: 0.12), value: level)
        .animation(.linear(duration: 0.08), value: pulsePhase)
        .onAppear {
            if isPulsing {
                startPulseTimer()
            }
        }
        .onDisappear {
            stopPulseTimer()
        }
        .onChange(of: isPulsing) { _, pulsing in
            if pulsing {
                startPulseTimer()
            } else {
                stopPulseTimer()
            }
        }
    }

    private func barHeight(for index: Int) -> CGFloat {
        let base = baseHeights[index]
        let normalized: CGFloat

        if isPulsing {
            let wave = sin(pulsePhase + CGFloat(index) * 0.8) * 0.3
            normalized = min(1.0, max(0.2, base + wave + 0.1))
        } else if isAnimating {
            let levelBoost = level * 0.7
            normalized = min(1.0, max(0.2, base + levelBoost))
        } else {
            normalized = base
        }

        return minHeight + (maxHeight - minHeight) * normalized
    }

    private func startPulseTimer() {
        guard pulseTimer == nil else { return }
        pulseTimer = Timer.scheduledTimer(withTimeInterval: 0.05, repeats: true) { _ in
            Task { @MainActor in
                pulsePhase += 0.12
            }
        }
    }

    private func stopPulseTimer() {
        pulseTimer?.invalidate()
        pulseTimer = nil
        pulsePhase = 0
    }
}

#Preview("Idle") {
    SpeechInputDrawerView(phase: .idle, micLevel: 0, speakerLevel: 0)
}

#Preview("Listening") {
    SpeechInputDrawerView(phase: .listening, micLevel: 0.6, speakerLevel: 0)
}

#Preview("Processing") {
    SpeechInputDrawerView(phase: .processing, micLevel: 0, speakerLevel: 0)
}

#Preview("Answering") {
    SpeechInputDrawerView(phase: .answering, micLevel: 0, speakerLevel: 0.7)
}
