import SwiftUI

struct PartnerMessageBlockView: View {

    @EnvironmentObject private var friendsViewModel: FriendsViewModel

    let text: String
    let senderUserId: UUID?

    private let bubbleColor = Color(.secondarySystemBackground)

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

    private var resolvedVoiceName: String {
        if let name = resolvedFriend?.voiceName?.trimmingCharacters(in: .whitespacesAndNewlines),
           !name.isEmpty {
            return name
        }
        let fallback = (UserDefaults.standard.string(forKey: PreferenceKeys.partnerVoiceName) ?? "")
            .trimmingCharacters(in: .whitespacesAndNewlines)
        if !fallback.isEmpty { return fallback }
        // Default ghost when partner's voice is unknown
        return "mira"
    }

    private var ghostImage: UIImage? {
        ElevenLabsVoiceSuggestionsView.ghostUIImage(for: resolvedVoiceName)
    }

    var body: some View {
        HStack(alignment: .bottom, spacing: 6) {
            // Domino: ghost on left (shifted up), profile pic overlaps from right
            HStack(alignment: .bottom, spacing: -14) {
                if let ghostImage {
                    Image(uiImage: ghostImage)
                        .resizable()
                        .scaledToFit()
                        .frame(width: 48, height: 48)
                        .offset(y: 1)
                }

                AvatarCacheManager.shared.cachedAsyncImage(
                    urlString: resolvedAvatarURL.isEmpty ? nil : resolvedAvatarURL,
                    placeholder: avatarPlaceholder,
                    fallback: avatarPlaceholder
                )
                .frame(width: 43, height: 43)
                .clipShape(Circle())
                .zIndex(1)
            }

            VStack(alignment: .leading, spacing: 6) {
                Text(resolvedName)
                    .font(.system(size: 14, weight: .medium))
                    .foregroundColor(.teal)

                Text(text.isEmpty ? " " : text)
                    .font(.system(size: 17))
                    .lineSpacing(2)
                    .foregroundColor(.primary)
            }
            .padding(.horizontal, 12)
            .padding(.vertical, 8)
            .background {
                PartnerBubbleShape()
                    .fill(bubbleColor)
            }

            Spacer(minLength: 0)
        }
        .padding(.bottom, 2)
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
