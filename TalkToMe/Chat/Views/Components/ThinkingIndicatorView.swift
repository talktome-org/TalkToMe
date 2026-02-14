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

    private var plainSummaryText: String {
        if let parsed = try? AttributedString(markdown: summaryText) {
            return String(parsed.characters)
        }
        return summaryText
    }

    private var statusFont: Font {
        .body
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
                        .padding(.bottom, 4)

                    Text(plainSummaryText)
                        .font(.system(size: 14))
                        .foregroundStyle(.secondary)
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
                .padding(.bottom, 4)
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
        ZStack(alignment: .leading) {
            // Base text in secondary color
            Text(text)
                .font(font)
                .foregroundColor(color)

            // Bright sweep layer masked to the text shape
            Text(text)
                .font(font)
                .foregroundColor(.primary)
                .mask(
                    GeometryReader { geo in
                        let width = max(geo.size.width, 1)
                        LinearGradient(
                            stops: [
                                .init(color: .clear, location: 0.0),
                                .init(color: .white.opacity(0.45), location: 0.20),
                                .init(color: .white.opacity(0.75), location: 0.50),
                                .init(color: .white.opacity(0.45), location: 0.80),
                                .init(color: .clear, location: 1.0),
                            ],
                            startPoint: .leading,
                            endPoint: .trailing
                        )
                        .frame(width: width * 1.6)
                        .blur(radius: 2)
                        .offset(x: phase * width * 2.2)
                    }
                )
                .allowsHitTesting(false)
        }
        .onAppear {
            phase = -1.0
            withAnimation(.linear(duration: 1.3).repeatForever(autoreverses: false)) {
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
