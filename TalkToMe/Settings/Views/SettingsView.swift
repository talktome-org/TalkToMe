import SwiftUI
import PhotosUI

struct SettingsView: View {

    let profileNamespace: Namespace.ID

    @EnvironmentObject private var friendsVM: FriendsViewModel
    @EnvironmentObject private var sessionsVM: ChatSessionsViewModel
    @EnvironmentObject private var viewModel: SettingsViewModel
    @Environment(\.colorScheme) private var colorScheme
    @AppStorage(PreferenceKeys.appearancePreference) private var appearance: String = "System"

    @Binding var isPresented: Bool

    @State private var avatarRefreshKey = UUID()

    private var avatarPlaceholder: AnyView { AnyView(Color.clear) }

    private var avatarFallback: AnyView {
        AnyView(
            Circle()
                .fill(Color(.tertiarySystemFill))
                .frame(width: 72, height: 72)
                .overlay(
                    Text(preferredName.prefix(1).uppercased())
                        .font(.system(size: 28, weight: .semibold, design: .rounded))
                        .foregroundColor(.secondary)
                )
        )
    }

    private var preferredName: String {
        let name = viewModel.fullName.trimmingCharacters(in: .whitespacesAndNewlines)
        if !name.isEmpty { return name }
        if let user = AuthService.shared.currentUser {
            if let n = user.userMetadata["full_name"]?.stringValue, !n.isEmpty { return n }
            if let n = user.userMetadata["name"]?.stringValue, !n.isEmpty { return n }
            if let email = user.email { return email.components(separatedBy: "@").first ?? "User" }
        }
        return "User"
    }

    private var avatarView: some View {
        AvatarCacheManager.shared.cachedAsyncImage(
            urlString: sessionsVM.myAvatarURL,
            placeholder: { avatarPlaceholder },
            fallback: { avatarFallback }
        )
        .frame(width: 72, height: 72)
        .clipShape(Circle())
        .overlay(Circle().stroke(Color(.separator).opacity(0.3), lineWidth: 0.5))
        .id(avatarRefreshKey)
    }

    private var profileHeader: some View {
        VStack(spacing: 6) {
            avatarView
                .padding(.bottom, 6)

            Text(preferredName)
                .font(.system(size: 20, weight: .semibold))
                .foregroundColor(.primary)

            if let email = AuthService.shared.currentUser?.email, !email.isEmpty {
                Text(email)
                    .font(.system(size: 15, weight: .regular))
                    .foregroundColor(.secondary)
            }
        }
        .padding(.top, 12)
        .padding(.bottom, 24)
    }

    @ViewBuilder
    private var sectionsListView: some View {
        VStack(spacing: 36) {
            ForEach(Array(viewModel.settingsSections.enumerated()), id: \.offset) { sectionIndex, section in
                SettingsCardView(
                    section: section,
                    onToggle: { settingIndex in
                        viewModel.toggleSetting(for: sectionIndex, settingIndex: settingIndex)
                    },
                    onAction: { settingIndex in
                        viewModel.handleSettingAction(for: sectionIndex, settingIndex: settingIndex)
                    }
                )
            }
        }
        .padding(.horizontal, 16)
        .padding(.bottom, 40)
    }

    var body: some View {
        NavigationStack {
            ZStack {
                Color(.systemGroupedBackground).ignoresSafeArea()

                ScrollView {
                    VStack(spacing: 0) {
                        profileHeader
                        sectionsListView
                    }
                }
                .scrollIndicators(.hidden)
            }
            .toolbar {
                ToolbarItem(placement: .topBarLeading) {
                    Button {
                        Haptics.impact(.light)
                        viewModel.showPersonalizationEdit = true
                    } label: {
                        Text("Edit")
                    }
                }
            }
        }
        .sheet(isPresented: $viewModel.showPersonalizationEdit) {
            PersonalizationEditView(
                isPresented: $viewModel.showPersonalizationEdit,
                profileNamespace: profileNamespace,
                viewModel: viewModel
            )
            .environmentObject(sessionsVM)
            .presentationDetents([.large])
            .presentationDragIndicator(.visible)
        }
        .preferredColorScheme(
            appearance == "Light" ? .light : appearance == "Dark" ? .dark : nil
        )
        .onAppear {
            viewModel.preloadAvatar()

            Task { @MainActor in
                await sessionsVM.ensureProfilePictureCached()
                avatarRefreshKey = UUID()
            }

            if !viewModel.isProfileLoaded {
                viewModel.loadProfileInfo()
            }

            Task { await friendsVM.refreshMyCode() }
        }
        .onReceive(NotificationCenter.default.publisher(for: .profileChanged)) { _ in
            viewModel.loadProfileInfo()
        }
        .onReceive(NotificationCenter.default.publisher(for: .avatarChanged)) { _ in
            avatarRefreshKey = UUID()
        }
    }

}

#Preview {
    @Previewable @State var isPresented = true
    @Previewable @Namespace var namespace

    SettingsView(
        profileNamespace: namespace,
        isPresented: $isPresented
    )
    .environmentObject(FriendsViewModel(accessTokenProvider: { "" }))
    .environmentObject(ChatSessionsViewModel())
    .environmentObject(SettingsViewModel())
}
