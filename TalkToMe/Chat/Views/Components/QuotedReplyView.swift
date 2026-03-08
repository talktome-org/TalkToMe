import SwiftUI

/// Renders quoted/reply text inside a user message bubble.
struct QuotedReplyView: View {
    let text: String

    var body: some View {
        HStack(alignment: .top, spacing: 8) {
            RoundedRectangle(cornerRadius: 1.5)
                .fill(Color.white.opacity(0.5))
                .frame(width: 3)

            Text(text)
                .font(.system(size: 14))
                .foregroundColor(.white.opacity(0.7))
                .italic()
                .lineLimit(4)
                .fixedSize(horizontal: false, vertical: true)
        }
        .padding(.bottom, 4)
    }
}
