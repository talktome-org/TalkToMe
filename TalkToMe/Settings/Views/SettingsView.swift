import PhotosUI
import SwiftUI

struct SettingsView: View {

  let profileNamespace: Namespace.ID

  @EnvironmentObject private var friendsVM: FriendsViewModel
  @EnvironmentObject private var sessionsVM: ChatSessionsViewModel
  @EnvironmentObject private var viewModel: SettingsViewModel
  @Environment(\.colorScheme) private var colorScheme
  @AppStorage(PreferenceKeys.appearancePreference) private var appearance: String = "System"

  @Binding var isPresented: Bool

<<<<<<< Updated upstream
    @State private var avatarRefreshKey = UUID()
    @State private var toastMessage: String? = nil
    @State private var toastWorkItem: DispatchWorkItem? = nil
=======
  @State private var avatarRefreshKey = UUID()
>>>>>>> Stashed changes

  private var avatarPlaceholder: AnyView { AnyView(Color.clear) }

  private var avatarFallback: AnyView {
    AnyView(
      Circle()
        .fill(Color(.tertiarySystemFill))
        .frame(width: 100, height: 100)
<<<<<<< Updated upstream
        .clipShape(Circle())
        .overlay(Circle().stroke(Color(.separator).opacity(0.3), lineWidth: 0.5))
        .id(avatarRefreshKey)
    }

    private var profileHeader: some View {
        VStack(spacing: 16) {
            HStack(alignment: .top, spacing: 0) {
                // You
                VStack(spacing: 10) {
                    avatarView

                    Text(preferredName.components(separatedBy: " ").first ?? preferredName)
                        .font(.system(size: 18, weight: .semibold))
                        .foregroundColor(.primary)
                        .lineLimit(1)

                    Text("You")
                        .font(.system(size: 13, weight: .medium))
                        .foregroundColor(.secondary)
                        .padding(.horizontal, 14)
                        .padding(.vertical, 6)
                        .background(Color(.secondarySystemGroupedBackground))
                        .clipShape(RoundedRectangle(cornerRadius: 8, style: .continuous))
                }
                .frame(maxWidth: .infinity)

                // Buddy
                SettingsBuddyDisplayView()
                    .frame(maxWidth: .infinity)
            }
            .padding(.horizontal, 16)

            if let email = AuthService.shared.currentUser?.email, !email.isEmpty {
                Text(email)
                    .font(.system(size: 14, weight: .regular))
                    .foregroundColor(.secondary)
            }
        }
        .padding(.top, 20)
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
            .overlay(alignment: .top) {
                if let toastMessage {
                    ToastOverlayView(message: toastMessage, onDismiss: dismissToast)
                        .padding(.top, 8)
                }
            }
            .animation(.spring(response: 0.3, dampingFraction: 0.8), value: toastMessage)
            .toolbar {
                ToolbarItem(placement: .topBarLeading) {
                    Button {
                        Haptics.impact(.light)
                        viewModel.showPersonalizationEdit = true
                    } label: {
                        Text("Edit")
                    }
                }

                ToolbarItem(placement: .topBarTrailing) {
                    Menu {
                        if let code = friendsVM.myCode {
                            Button {
                                UIPasteboard.general.string = code
                                Haptics.impact(.light)
                                showToast("Add a Friend code copied to clipboard!")
                            } label: {
                                Label(code, systemImage: "square.on.square")
                            }
                        } else {
                            Button {
                                Task { await friendsVM.refreshMyCode(force: true) }
                            } label: {
                                Label("Load Code", systemImage: "arrow.clockwise")
                            }
                        }
                    } label: {
                        Image(systemName: "textformat.123")
                            .font(.system(size: 16))
                            .foregroundStyle(.primary)
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
=======
        .overlay(
          Text(preferredName.prefix(1).uppercased())
            .font(.system(size: 36, weight: .semibold, design: .rounded))
            .foregroundColor(.secondary)
>>>>>>> Stashed changes
        )
    )
  }

<<<<<<< Updated upstream
            Task { @MainActor in
                await sessionsVM.ensureProfilePictureCached()
                avatarRefreshKey = UUID()
            }

            if !viewModel.isProfileLoaded {
                viewModel.loadProfileInfo()
            }

            Task { await friendsVM.refreshMyCode() }

            // Pre-warm ghost video players for Customize Buddies
            ElevenLabsVoiceSuggestionsView.preloadGhostVideos()
        }
        .onReceive(NotificationCenter.default.publisher(for: .profileChanged)) { _ in
            viewModel.loadProfileInfo()
        }
        .onReceive(NotificationCenter.default.publisher(for: .avatarChanged)) { _ in
            avatarRefreshKey = UUID()
        }
=======
  private var preferredName: String {
    let name = viewModel.fullName.trimmingCharacters(in: .whitespacesAndNewlines)
    if !name.isEmpty { return name }
    if let user = AuthService.shared.currentUser {
      if let n = user.userMetadata["full_name"]?.stringValue, !n.isEmpty { return n }
      if let n = user.userMetadata["name"]?.stringValue, !n.isEmpty { return n }
      if let email = user.email { return email.components(separatedBy: "@").first ?? "User" }
>>>>>>> Stashed changes
    }
    return "User"
  }

  private var avatarView: some View {
    AvatarCacheManager.shared.cachedAsyncImage(
      urlString: sessionsVM.myAvatarURL,
      placeholder: { avatarPlaceholder },
      fallback: { avatarFallback }
    )
    .frame(width: 100, height: 100)
    .clipShape(Circle())
    .overlay(Circle().stroke(Color(.separator).opacity(0.3), lineWidth: 0.5))
    .id(avatarRefreshKey)
  }

  private var profileHeader: some View {
    VStack(spacing: 16) {
      HStack(alignment: .top, spacing: 0) {
        // You
        VStack(spacing: 10) {
          avatarView

          Text(preferredName)
            .font(.system(size: 18, weight: .semibold))
            .foregroundColor(.primary)
            .lineLimit(1)

          Text("You")
            .font(.system(size: 13, weight: .medium))
            .foregroundColor(.secondary)
            .padding(.horizontal, 14)
            .padding(.vertical, 6)
            .background(AppTheme.surface)
            .clipShape(RoundedRectangle(cornerRadius: 8, style: .continuous))
        }
        .frame(maxWidth: .infinity)

        // Buddy
        SettingsBuddyDisplayView()
          .frame(maxWidth: .infinity)
      }
      .padding(.horizontal, 16)

      if let email = AuthService.shared.currentUser?.email, !email.isEmpty {
        Text(email)
          .font(.system(size: 14, weight: .regular))
          .foregroundColor(.secondary)
      }
    }
    .padding(.top, 20)
    .padding(.bottom, 24)
  }

  @ViewBuilder
  private var sectionsListView: some View {
    VStack(spacing: 36) {
      ForEach(Array(viewModel.settingsSections.enumerated()), id: \.offset) {
        sectionIndex, section in
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
        AppTheme.background.ignoresSafeArea()

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

    private func showToast(_ message: String) {
        toastWorkItem?.cancel()
        toastMessage = message
        let item = DispatchWorkItem { dismissToast() }
        toastWorkItem = item
        DispatchQueue.main.asyncAfter(deadline: .now() + 2.5, execute: item)
    }

    private func dismissToast() {
        toastWorkItem?.cancel()
        toastWorkItem = nil
        toastMessage = nil
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
