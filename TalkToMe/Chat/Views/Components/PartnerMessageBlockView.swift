import SwiftUI

struct PartnerMessageBlockView: View {

    @EnvironmentObject private var friendsViewModel: FriendsViewModel

    let text: String
    let senderUserId: UUID?

    var body: some View {
        VStack(alignment: .leading) {
            HStack {
                HStack(spacing: 6) {
                    let resolvedFriend: FriendSummary? = {
                        guard let senderUserId else { return nil }
                        return friendsViewModel.friends.first(where: { $0.id == senderUserId })
                    }()

                    // If we can't resolve the sender from the friends list yet, fall back to the user's linked partner prefs.
                    // (This is correct for direct partner messages, and avoids showing a random last-picked "send to" friend.)
                    let fallbackName = PreferenceKeys.getPartnerDisplayName()
                    let resolvedName = (resolvedFriend?.fullName ?? fallbackName).trimmingCharacters(in: .whitespacesAndNewlines)
                    let nameToShow = resolvedName.isEmpty ? "Partner" : resolvedName
                    let firstName = nameToShow.split(separator: " ").first.map(String.init)

                    let fallbackAvatarURL = (UserDefaults.standard.string(forKey: PreferenceKeys.partnerAvatarURL) ?? "")
                        .trimmingCharacters(in: .whitespacesAndNewlines)
                    let avatarURL = (resolvedFriend?.avatarURL ?? fallbackAvatarURL).trimmingCharacters(in: .whitespacesAndNewlines)

                    AvatarCacheManager.shared.cachedAsyncImage(
                        urlString: avatarURL.isEmpty ? nil : avatarURL,
                        placeholder: avatarPlaceholder,
                        fallback: avatarPlaceholder
                    )
                    .frame(width: 16, height: 16)
                    .clipShape(Circle())

                    Text(firstName ?? "Partner")
                        .font(.footnote)
                        .foregroundColor(Color.secondary)
                }
                .offset(y: -4)

                Spacer()

                MessageActionsView(text: text)
                    .offset(y: -4)
            }

            Divider()
                .padding(.horizontal, -12)
                .offset(y: -4)

            Text(text.isEmpty ? " " : text)
                .font(.callout)
                .foregroundColor(.primary)
                .padding(.vertical, 8)
                .padding(.horizontal, 4)
                .frame(maxWidth: .infinity, alignment: .leading)
        }
        .padding(12)
        .background(
            RoundedRectangle(cornerRadius: 16)
                .strokeBorder(Color(.separator), lineWidth: 1)
                .background(
                    RoundedRectangle(cornerRadius: 16).fill(Color(.secondarySystemBackground))
                )
        )
    }

    private func avatarPlaceholder() -> AnyView {
        AnyView(
            Circle()
                .fill(Color.gray.opacity(0.2))
                .overlay(
                    Image(systemName: "person.fill")
                        .font(.system(size: 10, weight: .medium))
                        .foregroundColor(.gray)
                )
        )
    }
}

