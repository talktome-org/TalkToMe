import SwiftUI

struct AssistantMessageActionsView: View {
    let messageText: String
    let onRegenerate: (() -> Void)?
    
    @State private var showCopyCheck: Bool = false
    @State private var feedbackGiven: FeedbackType? = nil
    
    private enum FeedbackType {
        case positive
        case negative
    }
    
    var body: some View {
        HStack(spacing: 16) {
            // Copy button
            Button(action: copyToClipboard) {
                Image(systemName: showCopyCheck ? "checkmark" : "doc.on.doc")
                    .font(.system(size: 14, weight: .medium))
                    .foregroundColor(showCopyCheck ? .green : .secondary)
            }
            .buttonStyle(.plain)
            .animation(.easeInOut(duration: 0.15), value: showCopyCheck)
            
            // Regenerate button
            if let onRegenerate {
                Button(action: {
                    Haptics.impact(.light)
                    onRegenerate()
                }) {
                    Image(systemName: "arrow.clockwise")
                        .font(.system(size: 14, weight: .medium))
                        .foregroundColor(.secondary)
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
    
    private func copyToClipboard() {
        guard !showCopyCheck else { return }
        UIPasteboard.general.string = messageText
        Haptics.impact(.light)
        showCopyCheck = true
        DispatchQueue.main.asyncAfter(deadline: .now() + 2) {
            showCopyCheck = false
        }
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
            onRegenerate: { print("Regenerate tapped") }
        )
        
        AssistantMessageActionsView(
            messageText: "Message without regenerate option.",
            onRegenerate: nil
        )
    }
    .padding()
}
