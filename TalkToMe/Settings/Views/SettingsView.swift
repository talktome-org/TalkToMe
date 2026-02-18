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

  @AppStorage("get_started_dismissed") private var getStartedDismissed: Bool = false
  @AppStorage("get_started_say_hi") private var sayHiDone: Bool = false
  @AppStorage("get_started_connect_friend") private var connectFriendDone: Bool = false
  @AppStorage("get_started_write_diary") private var writeDiaryDone: Bool = false
  @AppStorage("get_started_customize_buddy") private var customizeBuddyDone: Bool = false

  @State private var avatarRefreshKey = UUID()
  @State private var toastMessage: String? = nil
  @State private var toastWorkItem: DispatchWorkItem? = nil
  @State private var customizationHighlightOpacity: CGFloat = 0

  private var getStartedHidden: Bool {
    getStartedDismissed || [sayHiDone, connectFriendDone, writeDiaryDone, customizeBuddyDone].allSatisfy { $0 }
  }

  private var avatarPlaceholder: AnyView { AnyView(Color.clear) }

  private var avatarFallback: AnyView {
    AnyView(
      Circle()
        .fill(Color(.tertiarySystemFill))
        .frame(width: 100, height: 100)
        .overlay(
          Text(preferredName.prefix(1).uppercased())
            .font(.system(size: 36, weight: .semibold, design: .rounded))
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
    .frame(width: 100, height: 100)
    .clipShape(Circle())
    .overlay(Circle().stroke(Color(.separator).opacity(0.5), lineWidth: 1))
    .id(avatarRefreshKey)
  }

  private var profileHeader: some View {
    let buddyDisplay = SettingsBuddyDisplayView()

    return HStack(alignment: .top, spacing: 0) {
      // Left column: User
      VStack(spacing: 6) {
        avatarView
          .frame(height: 120)
        Text(preferredName.components(separatedBy: " ").first ?? preferredName)
          .font(.system(size: 18, weight: .semibold))
          .foregroundColor(.primary)
          .lineLimit(1)
          .frame(height: 22)
        Text("You")
          .font(.system(size: 13, weight: .medium))
          .foregroundColor(.secondary)
          .lineLimit(1)
          .padding(.horizontal, 14)
          .padding(.vertical, 6)
          .background(AppTheme.surface)
          .clipShape(RoundedRectangle(cornerRadius: 8, style: .continuous))
      }
      .frame(maxWidth: .infinity)

      // Right column: Buddy
      VStack(spacing: 6) {
        buddyDisplay.ghostVideoView
        Text(buddyDisplay.buddyDisplayName)
          .font(.system(size: 18, weight: .semibold))
          .foregroundColor(.primary)
          .lineLimit(1)
          .frame(height: 22)
        Text("Buddy")
          .font(.system(size: 13, weight: .medium))
          .foregroundColor(.secondary)
          .lineLimit(1)
          .padding(.horizontal, 14)
          .padding(.vertical, 6)
          .background(AppTheme.surface)
          .clipShape(RoundedRectangle(cornerRadius: 8, style: .continuous))
      }
      .frame(maxWidth: .infinity)
    }
    .padding(.horizontal, 16)
    .padding(.top, 10)
    .padding(.bottom, 48)
  }

  @ViewBuilder
  private var sectionsListView: some View {
    VStack(spacing: 36) {
      if getStartedHidden {
        Button {
          Haptics.impact(.light)
          withAnimation(.spring(response: 0.4, dampingFraction: 0.8)) {
            sayHiDone = false
            connectFriendDone = false
            writeDiaryDone = false
            customizeBuddyDone = false
            getStartedDismissed = false
          }
          showToast("Get Started has been reset!")
        } label: {
          HStack(spacing: 14) {
            Image(systemName: "arrow.counterclockwise")
              .font(.system(size: 18))
              .foregroundColor(.secondary)
              .frame(width: 30, height: 30)

            Text("Reset Roadmap")
              .font(.system(size: 17, weight: .regular))
              .foregroundColor(.primary)

            Spacer()
          }
          .padding(.horizontal, 16)
          .frame(minHeight: 44)
        }
        .buttonStyle(PlainButtonStyle())
        .padding(.vertical, 6)
        .background(AppTheme.surface)
        .clipShape(RoundedRectangle(cornerRadius: 26, style: .continuous))
      }

      ForEach(Array(viewModel.settingsSections.enumerated()), id: \.element.id) { sectionIndex, section in
        if section.id == "account" {
          accountSectionView(sectionIndex: sectionIndex, section: section)
        } else {
          SettingsCardView(
            section: section,
            onToggle: { settingIndex in
              viewModel.toggleSetting(for: sectionIndex, settingIndex: settingIndex)
            },
            onAction: { settingIndex in
              viewModel.handleSettingAction(for: sectionIndex, settingIndex: settingIndex)
            }
          )
          .id(section.id)
          .overlay {
            if section.id == "customization" {
              RoundedRectangle(cornerRadius: 26, style: .continuous)
                .strokeBorder(AppTheme.accent, lineWidth: 2)
                .opacity(customizationHighlightOpacity)
            }
          }
        }
      }
    }
    .padding(.horizontal, 16)
    .padding(.bottom, 40)
  }

  @ViewBuilder
  private func accountSectionView(sectionIndex: Int, section: SettingsSection) -> some View {
    VStack(spacing: 0) {
      if let user = AuthService.shared.currentUser,
         let email = user.email, !email.isEmpty {
        let provider = user.appMetadata["provider"]?.stringValue ?? "email"

        HStack(spacing: 14) {
          Image(systemName: "envelope")
            .font(.system(size: 18))
            .foregroundColor(.secondary)
            .frame(width: 30, height: 30)

          Text(email)
            .font(.system(size: 17, weight: .regular))
            .foregroundColor(.primary)
            .lineLimit(1)

          Spacer()

          if provider == "apple" {
            Image(systemName: "applelogo")
              .font(.system(size: 16))
              .foregroundColor(.primary)
          } else if provider == "google" {
            Image("icons8-google-48 copy")
              .renderingMode(.original)
              .resizable()
              .aspectRatio(contentMode: .fit)
              .frame(width: 20, height: 20)
          }
        }
        .padding(.horizontal, 16)
        .frame(minHeight: 44)

        Divider()
          .padding(.leading, 60)
      }

      ForEach(Array(section.settings.enumerated()), id: \.element.id) { settingIndex, setting in
        if settingIndex > 0 {
          Divider()
            .padding(.leading, 60)
        }

        Button(action: { viewModel.handleSettingAction(for: sectionIndex, settingIndex: settingIndex) }) {
          HStack(spacing: 14) {
            Image(systemName: setting.icon)
              .font(.system(size: 18))
              .foregroundColor(.secondary)
              .frame(width: 30, height: 30)

            Text(setting.title)
              .font(.system(size: 17, weight: .regular))
              .foregroundColor(.red)

            Spacer()
          }
          .padding(.horizontal, 16)
          .frame(minHeight: 44)
        }
        .buttonStyle(PlainButtonStyle())
        .disabled(setting.title == "Delete Account" && viewModel.isDeletingAccount)
      }
    }
    .padding(.vertical, 6)
    .background(AppTheme.surface)
    .clipShape(RoundedRectangle(cornerRadius: 26, style: .continuous))
  }

  var body: some View {
    NavigationStack {
      ZStack {
        AppTheme.background.ignoresSafeArea()

        ScrollViewReader { proxy in
          ScrollView {
            VStack(spacing: 0) {
              profileHeader
              sectionsListView
            }
          }
          .scrollIndicators(.hidden)
          .onChange(of: viewModel.shouldHighlightCustomization) { _, shouldHighlight in
            guard shouldHighlight else { return }
            viewModel.shouldHighlightCustomization = false
            withAnimation(.easeOut(duration: 0.3)) {
              proxy.scrollTo("customization", anchor: .center)
            }
            DispatchQueue.main.asyncAfter(deadline: .now() + 0.25) {
              animateCustomizationHighlight()
            }
          }
        }
      }
      .overlay(alignment: .top) {
        if let toastMessage {
          ToastOverlayView(message: toastMessage, onDismiss: dismissToast)
            .padding(.top, 8)
        }
      }
      .animation(.spring(response: 0.3, dampingFraction: 0.8), value: toastMessage)
      .navigationDestination(isPresented: $viewModel.shouldNavigateToContacts) {
        FriendsAndContactsSectionView()
      }
      .navigationDestination(isPresented: $viewModel.shouldNavigateToAppearance) {
        AppearanceSettingsView()
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
    .alert("Delete Account", isPresented: $viewModel.showDeleteAccountConfirmation) {
      Button("Delete", role: .destructive) {
        viewModel.deleteAccount()
      }
      Button("Cancel", role: .cancel) {}
    } message: {
      Text("This will permanently delete your account and all your data. This action cannot be undone.")
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

      // Pre-warm ghost video players for Customize Buddies
      ElevenLabsVoiceSuggestionsView.preloadGhostVideos()
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

  private func animateCustomizationHighlight() {
    withAnimation(.easeIn(duration: 0.2)) {
      customizationHighlightOpacity = 1.0
    }
    DispatchQueue.main.asyncAfter(deadline: .now() + 0.7) {
      withAnimation(.easeOut(duration: 0.3)) {
        customizationHighlightOpacity = 0
      }
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
