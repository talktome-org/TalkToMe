import SwiftUI

struct SidebarConversationRowView: View {
    let title: String
    let dateText: String
    let previewText: String
    let showUnreadDot: Bool

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack {
                Text(title)
                    .font(.system(size: 18, weight: .regular))
                    .foregroundColor(.primary)
                Spacer()
                Text(dateText)
                    .font(.system(size: 12))
                    .foregroundColor(.secondary)
            }

            HStack(spacing: 6) {
                Text(previewText)
                    .font(.system(size: 14))
                    .foregroundColor(.secondary)
                    .lineLimit(1)
                    .truncationMode(.tail)
                Spacer()
                if showUnreadDot {
                    Circle()
                        .fill(Color(red: 0.4, green: 0.2, blue: 0.6))
                        .frame(width: 14, height: 14)
                }
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .padding(.horizontal, 2)
        .padding(.vertical, 12)
    }
}


