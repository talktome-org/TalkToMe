import SwiftUI

struct TypingIndicatorView: View {

    @State private var animate: Bool = false
    @State private var startTask: Task<Void, Never>?

    let showAfter: Double

    var body: some View {
        HStack(spacing: 6) {
            ForEach(0..<3) { i in
                Circle()
                    .fill(Color.secondary.opacity(0.7))
                    .frame(width: 8, height: 8)
                    .scaleEffect(animate ? 1.0 : 0.6)
                    .opacity(0.85)
                    .animation(
                        .easeInOut(duration: 0.85)
                            .repeatForever(autoreverses: true)
                            .delay(Double(i) * 0.2),
                        value: animate
                    )
            }
        }
        .onAppear {
            startTask?.cancel()
            animate = false

            let delay = max(0, showAfter)
            if delay == 0 {
                animate = true
                return
            }

            startTask = Task {
                try? await Task.sleep(nanoseconds: UInt64(delay * 1_000_000_000))
                guard !Task.isCancelled else { return }
                await MainActor.run { animate = true }
            }
        }
        .onDisappear {
            startTask?.cancel()
            startTask = nil
            animate = false
        }
        .opacity(animate ? 1 : 0)
    }
}

#Preview {
    VStack(alignment: .leading) {
        TypingIndicatorView(showAfter: 0)
    }
    .padding()
}


