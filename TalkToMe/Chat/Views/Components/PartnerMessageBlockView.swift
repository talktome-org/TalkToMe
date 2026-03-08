import SwiftUI

struct PartnerMessageBlockView: View {

    @Environment(\.colorScheme) private var colorScheme
    @EnvironmentObject private var friendsViewModel: FriendsViewModel
    @AppStorage(PreferenceKeys.fontSizePreference) private var fontSizeScale: Double = 1.0

    let text: String
    let senderUserId: UUID?
    var timestamp: Date = Date()
    var onReply: ((String) -> Void)? = nil

    private var bubbleColor: Color {
        colorScheme == .light
            ? Color.talkToMePartnerBubbleLightGray
            : AppTheme.talkToMeBubbleAI
    }

    private var resolvedFriend: FriendSummary? {
        guard let senderUserId else { return nil }
        return friendsViewModel.friends.first(where: { $0.id == senderUserId })
    }

    private var resolvedName: String {
        let fallbackName = PreferenceKeys.getPartnerDisplayName()
        let name = (resolvedFriend?.fullName ?? fallbackName)
            .trimmingCharacters(in: .whitespacesAndNewlines)
        return name.isEmpty ? "Partner" : name
    }

    private var resolvedAvatarURL: String {
        let fallback = (UserDefaults.standard.string(forKey: PreferenceKeys.partnerAvatarURL) ?? "")
            .trimmingCharacters(in: .whitespacesAndNewlines)
        return (resolvedFriend?.avatarURL ?? fallback)
            .trimmingCharacters(in: .whitespacesAndNewlines)
    }

    private var timeAgo: String {
        let interval = Date().timeIntervalSince(timestamp)
        if interval < 60 { return "now" }
        let minutes = Int(interval / 60)
        if minutes < 60 { return "\(minutes)m" }
        let hours = minutes / 60
        if hours < 24 { return "\(hours)h" }
        let days = hours / 24
        return "\(days)d"
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 4) {
            // Name · time above the bubble
            HStack(spacing: 4) {
                Text(resolvedName)
                    .font(.system(size: 15, weight: .semibold))
                    .foregroundColor(.primary)
                Text("·")
                    .font(.system(size: 13, weight: .bold))
                    .foregroundColor(Color(.tertiaryLabel))
                Text(timeAgo)
                    .font(.system(size: 13, weight: .medium))
                    .foregroundColor(AppTheme.brand)
            }
            .padding(.leading, 61)

            HStack(alignment: .bottom, spacing: 6) {
                AvatarCacheManager.shared.cachedAsyncImage(
                    urlString: resolvedAvatarURL.isEmpty ? nil : resolvedAvatarURL,
                    placeholder: avatarPlaceholder,
                    fallback: avatarPlaceholder
                )
                .frame(width: 43, height: 43)
                .clipShape(Circle())

                Group {
                    if let onReply, !text.isEmpty {
                        SelectableTextView(
                            attributedText: partnerNSAttributedString(text),
                            replyToName: resolvedName,
                            onReply: onReply
                        )
                        .padding(.horizontal, 2)
                        .padding(.vertical, 2)
                    } else {
                        Text(text.isEmpty ? " " : text)
                            .font(.system(size: 17 * fontSizeScale))
                            .lineSpacing(2)
                            .foregroundColor(.primary)
                            .padding(.horizontal, 2)
                            .padding(.vertical, 2)
                    }
                }
                .padding(10)
                .background {
                    PartnerBubbleShape()
                        .fill(bubbleColor)
                }
            }
        }
        .padding(.bottom, 2)
    }

    private func partnerNSAttributedString(_ text: String) -> NSAttributedString {
        let fontSize: CGFloat = 17 * fontSizeScale
        let textColor = UIColor.label
        let paragraphStyle = NSMutableParagraphStyle()
        paragraphStyle.lineSpacing = 2
        return NSAttributedString(string: text, attributes: [
            .font: UIFont.systemFont(ofSize: fontSize),
            .foregroundColor: textColor,
            .paragraphStyle: paragraphStyle,
        ])
    }

    private func avatarPlaceholder() -> AnyView {
        AnyView(
            Circle()
                .fill(Color.gray.opacity(0.2))
                .overlay(
                    Image(systemName: "person.fill")
                        .font(.system(size: 15, weight: .medium))
                        .foregroundColor(.gray)
                )
        )
    }
}

private struct PartnerBubbleShape: Shape {
    func path(in rect: CGRect) -> Path {
        let cr: CGFloat = 18
        let tailExtent: CGFloat = 5

        return Path { p in
            // Start top-left after corner
            p.move(to: CGPoint(x: cr, y: 0))

            // Top edge
            p.addLine(to: CGPoint(x: rect.width - cr, y: 0))
            // Top-right corner
            p.addArc(center: CGPoint(x: rect.width - cr, y: cr),
                     radius: cr, startAngle: .degrees(-90), endAngle: .degrees(0), clockwise: false)
            // Right edge
            p.addLine(to: CGPoint(x: rect.width, y: rect.height - cr))
            // Bottom-right corner
            p.addArc(center: CGPoint(x: rect.width - cr, y: rect.height - cr),
                     radius: cr, startAngle: .degrees(0), endAngle: .degrees(90), clockwise: false)
            // Bottom edge to tail area
            p.addLine(to: CGPoint(x: cr, y: rect.height))

            // Bottom of raindrop: curved outward (control below the line)
            p.addQuadCurve(
                to: CGPoint(x: -tailExtent, y: rect.height - 1),
                control: CGPoint(x: 4, y: rect.height + 3)
            )
            // Top of raindrop: slightly curved inward
            p.addQuadCurve(
                to: CGPoint(x: 0, y: rect.height - cr * 1.2),
                control: CGPoint(x: 0, y: rect.height - cr * 0.5)
            )

            // Left edge
            p.addLine(to: CGPoint(x: 0, y: cr))
            // Top-left corner
            p.addArc(center: CGPoint(x: cr, y: cr),
                     radius: cr, startAngle: .degrees(180), endAngle: .degrees(270), clockwise: false)
        }
    }
}
