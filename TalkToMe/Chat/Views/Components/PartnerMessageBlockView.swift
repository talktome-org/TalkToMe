import SwiftUI

struct PartnerMessageBlockView: View {

    let text: String

    var body: some View {
        VStack(alignment: .leading) {
            HStack {
                HStack(spacing: 6) {
                    let name = UserDefaults.standard.string(forKey: PreferenceKeys.partnerName)
                    let firstName = name?.split(separator: " ").first.map(String.init)
                    let avatarURL = UserDefaults.standard.string(forKey: PreferenceKeys.partnerAvatarURL)

                    AvatarCacheManager.shared.cachedAsyncImage(
                        urlString: avatarURL,
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


