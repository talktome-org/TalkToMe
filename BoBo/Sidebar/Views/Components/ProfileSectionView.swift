import SwiftUI

struct ProfileSectionView: View {
    @EnvironmentObject private var navigationViewModel: SidebarNavigationViewModel
    @EnvironmentObject private var sessionsViewModel: ChatSessionsViewModel
    @Environment(\.colorScheme) private var colorScheme

    // Settings gear moved to sidebar toolbar; keep local state for compatibility but unused
    @State private var showSettingsSheet = false
    @State private var avatarRefreshKey = UUID()
    @Namespace private var profileNamespace

    private var userName: String {
        // Prefer loaded profile info full name if available via SettingsViewModel cache on NotificationCenter
        if let cached = UserDefaults.standard.string(forKey: "bobo_profile_full_name"), !cached.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            return cached
        }
        if let user = AuthService.shared.currentUser {
            let metadata = user.userMetadata
            if let fullName = metadata["full_name"]?.stringValue, !fullName.isEmpty {
                return fullName
            } else if let name = metadata["name"]?.stringValue, !name.isEmpty {
                return name
            } else if let displayName = metadata["display_name"]?.stringValue, !displayName.isEmpty {
                return displayName
            }
            return user.email ?? "User"
        }
        return "User"
    }

    var body: some View {
        HStack(spacing: 8) {
            // Profile Picture (non-interactive)
            AvatarCacheManager.shared.cachedAsyncImage(
                urlString: sessionsViewModel.myAvatarURL,
                placeholder: {
                    AnyView(
                        Circle()
                            .fill(
                                LinearGradient(
                                    colors: [
                                        Color(red: 0.26, green: 0.58, blue: 1.00),
                                        Color(red: 0.63, green: 0.32, blue: 0.98)
                                    ],
                                    startPoint: .topLeading,
                                    endPoint: .bottomTrailing
                                )
                            )
                            .overlay(
                                Image(systemName: "person.fill")
                                    .font(.system(size: 16, weight: .medium))
                                    .foregroundColor(.white)
                            )
                    )
                },
                fallback: {
                    AnyView(
                        Circle()
                            .fill(
                                LinearGradient(
                                    colors: [
                                        Color(red: 0.26, green: 0.58, blue: 1.00),
                                        Color(red: 0.63, green: 0.32, blue: 0.98)
                                    ],
                                    startPoint: .topLeading,
                                    endPoint: .bottomTrailing
                                )
                            )
                            .overlay(
                                Image(systemName: "person.fill")
                                    .font(.system(size: 20, weight: .medium))
                                    .foregroundColor(.white)
                            )
                    )
                }
            )
            .frame(width: 44, height: 44)
            .clipShape(Circle())
            .overlay(
                Circle()
                    .stroke(Color.white.opacity(0.2), lineWidth: 1)
            )
            .id(avatarRefreshKey)
            .offset(x: -4, y: -12)
            Text(userName)
                .font(.system(size: 19, weight: .semibold))
                .foregroundColor(colorScheme == .dark ? .white : .black)
                .lineLimit(1)
                .offset(y: -10)

            Spacer(minLength: 20)

            // Settings icon removed from profile row; now lives in sidebar top bar
        }
        .padding(.horizontal, 12)
        .padding(.vertical, 12)
        // Settings sheet presentation now handled by `SidebarView`
        .onReceive(NotificationCenter.default.publisher(for: .profileChanged)) { _ in
            avatarRefreshKey = UUID()
        }
        .onReceive(NotificationCenter.default.publisher(for: .avatarChanged)) { _ in
            avatarRefreshKey = UUID()
        }
    }
}

#Preview {
    ProfileSectionView()
        .environmentObject(SidebarNavigationViewModel())
        .environmentObject(ChatSessionsViewModel())
        .background(Color.gray.opacity(0.1))
        .padding()
}
