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

    private var resolvedFirstName: String {
        let fallbackName = PreferenceKeys.getPartnerDisplayName()
        let resolvedName = (resolvedFriend?.fullName ?? fallbackName)
            .trimmingCharacters(in: .whitespacesAndNewlines)
        let nameToShow = resolvedName.isEmpty ? "Partner" : resolvedName
        return nameToShow.split(separator: " ").first.map(String.init) ?? "Partner"
    }

    private var resolvedAvatarURL: String {
        let fallback = (UserDefaults.standard.string(forKey: PreferenceKeys.partnerAvatarURL) ?? "")
            .trimmingCharacters(in: .whitespacesAndNewlines)
        return (resolvedFriend?.avatarURL ?? fallback)
            .trimmingCharacters(in: .whitespacesAndNewlines)
    }

    var body: some View {
        HStack(alignment: .bottom, spacing: 4) {
            AvatarCacheManager.shared.cachedAsyncImage(
                urlString: resolvedAvatarURL.isEmpty ? nil : resolvedAvatarURL,
                placeholder: avatarPlaceholder,
                fallback: avatarPlaceholder
            )
            .frame(width: 28, height: 28)
            .clipShape(Circle())

            VStack(alignment: .leading, spacing: 2) {
                Text(resolvedFirstName)
                    .font(.caption)
                    .foregroundColor(.secondary)
                    .padding(.leading, 10)

                Text(text.isEmpty ? " " : text)
                    .font(.system(size: 17))
                    .lineSpacing(2)
                    .foregroundColor(.primary)
                    .padding(.horizontal, 12)
                    .padding(.vertical, 8)
                    .background(bubbleColor, in: RoundedRectangle(cornerRadius: 18, style: .continuous))
                    .overlay(alignment: .bottomLeading) {
                        BubbleTailShape()
                            .fill(bubbleColor)
                            .frame(width: 12, height: 8)
                            .offset(x: -3, y: 1)
                    }
            }

            Spacer(minLength: 40)
        }
        .padding(.bottom, 2)
    }

    private func avatarPlaceholder() -> AnyView {
        AnyView(
            Circle()
                .fill(Color.gray.opacity(0.2))
                .overlay(
                    Image(systemName: "person.fill")
                        .font(.system(size: 12, weight: .medium))
                        .foregroundColor(.gray)
                )
        )
    }
}

private struct BubbleTailShape: Shape {
    func path(in rect: CGRect) -> Path {
        Path { p in
            p.move(to: CGPoint(x: rect.width, y: 0))
            p.addCurve(
                to: CGPoint(x: 0, y: rect.height),
                control1: CGPoint(x: rect.width, y: rect.height * 0.6),
                control2: CGPoint(x: rect.width * 0.1, y: rect.height)
            )
            p.addCurve(
                to: CGPoint(x: rect.width, y: rect.height * 0.4),
                control1: CGPoint(x: rect.width * 0.3, y: rect.height * 0.9),
                control2: CGPoint(x: rect.width * 0.7, y: rect.height * 0.5)
            )
        }
    }
}
