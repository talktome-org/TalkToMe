import SwiftUI

struct ThinkingIndicatorView: View {

    let thinkingText: String
    let thinkingTextDone: Bool

    private var hasSummaryTokens: Bool {
        !thinkingText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
    }

    private var summaryText: String {
        thinkingText.trimmingCharacters(in: .whitespacesAndNewlines)
    }

    private var statusFont: Font {
        .system(size: 17, weight: .regular)
    }

    var body: some View {
        Group {
            if hasSummaryTokens {
                VStack(alignment: .leading, spacing: 6) {
                    ShimmeringStatusText(
                        text: "Thinking",
                        font: statusFont,
                        color: .secondary
                    )
                        .padding(.vertical, 6)

                    Text(summaryText)
                        .font(.system(size: 14))
                        .foregroundColor(.secondary.opacity(0.82))
                        .lineSpacing(2)
                        .frame(maxWidth: .infinity, alignment: .leading)
                }
                .padding(.horizontal, 4)
            } else {
                HStack(spacing: 6) {
                    ShimmeringStatusText(
                        text: "Generating",
                        font: statusFont,
                        color: .secondary
                    )
                }
                .padding(.horizontal, 4)
                .padding(.vertical, 6)
            }
        }
    }
}

private struct ShimmeringStatusText: View {
    let text: String
    let font: Font
    let color: Color

    @State private var phase: CGFloat = -1.0

    var body: some View {
        Text(text)
            .font(font)
            .foregroundColor(color)
            .overlay(
                GeometryReader { geo in
                    let width = max(geo.size.width, 1)
                    LinearGradient(
                        stops: [
                            .init(color: .white.opacity(0.0), location: 0.0),
                            .init(color: .white.opacity(0.08), location: 0.30),
                            .init(color: .white.opacity(0.28), location: 0.50),
                            .init(color: .white.opacity(0.08), location: 0.70),
                            .init(color: .white.opacity(0.0), location: 1.0),
                        ],
                        startPoint: .top,
                        endPoint: .bottom
                    )
                    .frame(width: width * 0.95)
                    .blur(radius: 0.6)
                    .offset(x: phase * width * 2.2)
                    .blendMode(.screen)
                }
                .mask(
                    Text(text)
                        .font(font)
                )
                .allowsHitTesting(false)
            )
            .onAppear {
                phase = -1.0
                withAnimation(.linear(duration: 1.2).repeatForever(autoreverses: false)) {
                    phase = 1.0
                }
            }
            .onDisappear {
                phase = -1.0
            }
    }
}

#Preview {
    VStack(alignment: .leading, spacing: 20) {
        ThinkingIndicatorView(thinkingText: "", thinkingTextDone: false)
        ThinkingIndicatorView(thinkingText: "Analyzing the user's request and considering how to respond...", thinkingTextDone: true)
        ThinkingIndicatorView(thinkingText: "", thinkingTextDone: false)
    }
    .padding()
}
