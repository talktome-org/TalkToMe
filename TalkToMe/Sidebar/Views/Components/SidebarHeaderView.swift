import SwiftUI

struct SidebarHeaderView: View {

    @Environment(\.colorScheme) private var colorScheme

    let avatarURL: String?
    let profileNamespace: Namespace.ID

    @Binding var searchText: String
    let isSearchActive: Bool
    let isSearchFieldFocused: FocusState<Bool>.Binding

    let onTapProfile: () -> Void
    let onCloseSearch: () -> Void
    let onOpenSettings: () -> Void
    let onNewChat: () -> Void

    var body: some View {
        let pillShape = RoundedRectangle(cornerRadius: 999, style: .continuous)

        HStack(spacing: 12) {
            Button(action: onTapProfile) {
                SidebarAvatarView(avatarURL: avatarURL)
                    .frame(width: 40, height: 40)
                    .clipShape(Circle())
                    .overlay(
                        Circle()
                            .stroke(Color.white.opacity(0.18), lineWidth: 1)
                    )
            }
            .buttonStyle(.plain)

            HStack(spacing: 10) {
                Image(systemName: "magnifyingglass")
                    .font(.system(size: 16, weight: .semibold))
                    .foregroundStyle(.secondary)

                TextField("Search conversations", text: $searchText)
                    .focused(isSearchFieldFocused)
                    .submitLabel(.search)
                    .textInputAutocapitalization(.never)
                    .disableAutocorrection(true)
                    .font(.system(size: 16, weight: .regular))
                    .foregroundStyle(.primary)
            }
            .padding(.vertical, 10)
            .padding(.horizontal, 14)
            .frame(maxWidth: .infinity)
            .background {
                if #available(iOS 26.0, *) {
                    pillShape
                        .fill(.clear)
                        .glassEffect()
                } else {
                    pillShape
                        .fill(.ultraThinMaterial)
                }
            }
            .clipShape(pillShape)
            .overlay(
                pillShape
                    .stroke(Color.white.opacity(colorScheme == .dark ? 0.14 : 0.18), lineWidth: 1)
            )

            if isSearchActive {
                GlassCircleButton(systemName: "xmark", action: onCloseSearch)
                    .accessibilityLabel("Close search")
            } else {
                GlassCircleButton(systemName: "gearshape", action: onOpenSettings)
                    .matchedGeometryEffect(id: "settingsGearIcon", in: profileNamespace)
                    .accessibilityLabel("Settings")

                GlassCircleButton(systemName: "square.and.pencil", action: onNewChat)
                    .accessibilityLabel("New chat")
            }
        }
        .padding(.horizontal, 16)
        .padding(.top, 10)
        .padding(.bottom, 8)
        .background(Color(.systemBackground).opacity(0.96))
    }
}


