import SwiftUI

struct AssistantMessageActionsView: View {
    let messageText: String
    let regenerationCount: Int
    let onRegenerate: (() -> Void)?

    @State private var feedbackGiven: FeedbackType? = nil

    private enum FeedbackType {
        case positive
        case negative
    }

    init(
        messageText: String,
        regenerationCount: Int = 0,
        onRegenerate: (() -> Void)? = nil
    ) {
        self.messageText = messageText
        self.regenerationCount = regenerationCount
        self.onRegenerate = onRegenerate
    }

    var body: some View {
        HStack(spacing: 12) {
            // Regenerate button
            if let onRegenerate {
                Button(action: {
                    Haptics.impact(.light)
                    onRegenerate()
                }) {
                    HStack(spacing: 4) {
                        Image(systemName: "arrow.clockwise")
                            .font(.system(size: 14, weight: .medium))
                            .foregroundColor(.secondary)

                        if regenerationCount > 0 {
                            Text("x\(regenerationCount)")
                                .font(.system(size: 12, weight: .semibold, design: .rounded))
                                .foregroundColor(.secondary)
                        }
                    }
                }
                .buttonStyle(.plain)
            }

            // Thumbs up
            Button(action: { giveFeedback(.positive) }) {
                Image(systemName: feedbackGiven == .positive ? "hand.thumbsup.fill" : "hand.thumbsup")
                    .font(.system(size: 14, weight: .medium))
                    .foregroundColor(feedbackGiven == .positive ? .green : .secondary)
            }
            .buttonStyle(.plain)
            .disabled(feedbackGiven != nil)

            // Thumbs down
            Button(action: { giveFeedback(.negative) }) {
                Image(systemName: feedbackGiven == .negative ? "hand.thumbsdown.fill" : "hand.thumbsdown")
                    .font(.system(size: 14, weight: .medium))
                    .foregroundColor(feedbackGiven == .negative ? .red : .secondary)
            }
            .buttonStyle(.plain)
            .disabled(feedbackGiven != nil)

            Spacer()
        }
        .padding(.top, 8)
        .padding(.leading, 4)
    }

    private func giveFeedback(_ type: FeedbackType) {
        Haptics.impact(.light)
        withAnimation(.easeInOut(duration: 0.2)) {
            feedbackGiven = type
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
