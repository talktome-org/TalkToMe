import SwiftUI

struct AssistantMessageActionsView: View {
    let messageText: String
    let regenerationCount: Int
    let onRegenerate: (() -> Void)?
    var onToast: ((String) -> Void)? = nil

    @State private var showCopyCheck: Bool = false
    @State private var feedbackGiven: FeedbackType? = nil

    private enum FeedbackType {
        case positive
        case negative
    }

    init(
        messageText: String,
        regenerationCount: Int = 0,
        onRegenerate: (() -> Void)? = nil,
        onToast: ((String) -> Void)? = nil
    ) {
        self.messageText = messageText
        self.regenerationCount = regenerationCount
        self.onRegenerate = onRegenerate
        self.onToast = onToast
    }

    var body: some View {
        HStack(spacing: 10) {
            // Copy button
            Button(action: copyToClipboard) {
                Image(systemName: showCopyCheck ? "checkmark" : "square.on.square")
                    .font(.system(size: 14, weight: .medium))
                    .foregroundColor(showCopyCheck ? .primary : .secondary)
            }
            .buttonStyle(.plain)
            .animation(.easeInOut(duration: 0.15), value: showCopyCheck)

            // Regenerate button
            if let onRegenerate {
                Button(action: {
                    Haptics.impact(.light)
                    onRegenerate()
                }) {
                    HStack(spacing: 4) {
                        Image(systemName: "arrow.clockwise")
                            .font(.system(size: 14, weight: .medium))

                        if regenerationCount > 0 {
                            Text("x\(regenerationCount)")
                                .font(.system(size: 12, weight: .semibold, design: .rounded))
                                .foregroundColor(.secondary)
                        }
                    }
                    .foregroundColor(.secondary)
                }
                .buttonStyle(.plain)
            }

            // Thumbs up
            Button(action: { giveFeedback(.positive) }) {
                Image(systemName: feedbackGiven == .positive ? "hand.thumbsup.fill" : "hand.thumbsup")
                    .font(.system(size: 14, weight: .medium))
                    .foregroundColor(.secondary)
            }
            .buttonStyle(.plain)

            // Thumbs down
            Button(action: { giveFeedback(.negative) }) {
                Image(systemName: feedbackGiven == .negative ? "hand.thumbsdown.fill" : "hand.thumbsdown")
                    .font(.system(size: 14, weight: .medium))
                    .foregroundColor(.secondary)
            }
            .buttonStyle(.plain)

            Spacer()
        }
        .padding(.top, 8)
        .padding(.leading, 4)
    }

    private func copyToClipboard() {
        guard !showCopyCheck else { return }
        UIPasteboard.general.string = messageText
        Haptics.impact(.light)
        showCopyCheck = true
        onToast?("Copied response to clipboard.")
        DispatchQueue.main.asyncAfter(deadline: .now() + 2) {
            showCopyCheck = false
        }
    }

    private func giveFeedback(_ type: FeedbackType) {
        Haptics.impact(.light)
        withAnimation(.easeInOut(duration: 0.2)) {
            if feedbackGiven == type {
                feedbackGiven = nil
            } else {
                feedbackGiven = type
                onToast?(type == .positive
                    ? "We're glad you liked the response. We'll make sure to provide responses of similar quality."
                    : "We're sad to hear you didn't like the response. We'll work on providing better responses.")
            }
        }
        // TODO: Send feedback to backend
    }
}

#Preview {
    VStack(spacing: 20) {
        AssistantMessageActionsView(
            messageText: "Hello! This is a test message.",
            regenerationCount: 0,
            onRegenerate: { print("Regenerate tapped") }
        )

        AssistantMessageActionsView(
            messageText: "Regenerated message.",
            regenerationCount: 2,
            onRegenerate: { print("Regenerate tapped") }
        )

        AssistantMessageActionsView(
            messageText: "Message without regenerate option.",
            onRegenerate: nil
        )
    }
    .padding()
}
