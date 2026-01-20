import SwiftUI

struct PartnerMessageBlockView: View {

    @EnvironmentObject private var friendsViewModel: FriendsViewModel

    let text: String

    @AppStorage(PreferenceKeys.partnerUserId) private var cachedPartnerUserId: String = ""
    @AppStorage(PreferenceKeys.partnerName) private var cachedPartnerName: String = ""
    @AppStorage(PreferenceKeys.partnerAvatarURL) private var cachedPartnerAvatarURL: String = ""

    var body: some View {
        VStack(alignment: .leading) {
            HStack {
                HStack(spacing: 6) {
                    let resolvedFriend: FriendSummary? = {
                        let friends = friendsViewModel.friends
                        if let id = UUID(uuidString: cachedPartnerUserId),
                           let match = friends.first(where: { $0.id == id }) {
                            return match
                        }
                        if friends.count == 1 {
                            return friends.first
                        }
                        return nil
                    }()

                    let resolvedName = (resolvedFriend?.fullName ?? cachedPartnerName).trimmingCharacters(in: .whitespacesAndNewlines)
                    let nameToShow = resolvedName.isEmpty ? "Partner" : resolvedName
                    let firstName = nameToShow.split(separator: " ").first.map(String.init)

                    let avatarURL = (resolvedFriend?.avatarURL ?? cachedPartnerAvatarURL).trimmingCharacters(in: .whitespacesAndNewlines)

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

