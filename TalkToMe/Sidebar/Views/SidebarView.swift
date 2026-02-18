import SwiftUI

struct SidebarView: View {

  @EnvironmentObject private var navigationViewModel: SidebarNavigationViewModel
  @EnvironmentObject private var sessionsViewModel: ChatSessionsViewModel
  @EnvironmentObject private var friendsViewModel: FriendsViewModel

  let onOpenChat: (UUID) -> Void
  let onStartNewChat: () -> Void
  var hideHeader: Bool = false

  @State private var searchText: String = ""

  private struct SessionSection: Identifiable {
    let id: String
    let title: String
    let sessions: [ChatSession]
  }

  private var filteredSessions: [ChatSession] {
    guard !searchText.isEmpty else { return sessionsViewModel.sessions }
    let query = searchText.lowercased()
    return sessionsViewModel.sessions.filter { session in
      session.title.lowercased().contains(query)
        || (session.lastMessageContent?.lowercased().contains(query) ?? false)
    }
  }

  private var isSearching: Bool {
    !searchText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
  }

  private var groupedSessions: [SessionSection] {
    let sessions = filteredSessions
    guard !sessions.isEmpty else { return [] }

    if isSearching {
      return [SessionSection(id: "results", title: "Matching Chats", sessions: sessions)]
    }

    var today: [ChatSession] = []
    var yesterday: [ChatSession] = []
    var thisWeek: [ChatSession] = []
    var earlier: [ChatSession] = []

    let calendar = Calendar.current
    let weekAgo = calendar.date(byAdding: .day, value: -7, to: Date()) ?? .distantPast

    for session in sessions {
      guard let date = parseISO8601Date(session.lastUsedISO8601) else {
        earlier.append(session)
        continue
      }
      if calendar.isDateInToday(date) {
        today.append(session)
      } else if calendar.isDateInYesterday(date) {
        yesterday.append(session)
      } else if date >= weekAgo {
        thisWeek.append(session)
      } else {
        earlier.append(session)
      }
    }

    var sections: [SessionSection] = []
    if !today.isEmpty {
      sections.append(SessionSection(id: "today", title: "Today", sessions: today))
    }
    if !yesterday.isEmpty {
      sections.append(SessionSection(id: "yesterday", title: "Yesterday", sessions: yesterday))
    }
    if !thisWeek.isEmpty {
      sections.append(SessionSection(id: "thisWeek", title: "This Week", sessions: thisWeek))
    }
    if !earlier.isEmpty {
      sections.append(SessionSection(id: "earlier", title: "Earlier", sessions: earlier))
    }
    return sections
  }

  private var mascotKeys: [String] {
    let keys = ElevenLabsVoiceSuggestionsView.requiredBuddies.map(\.key)
    return keys.isEmpty ? ["mira", "pax", "luma", "snow", "jay", "hex"] : keys
  }

  @State private var renameText: String = ""
  @State private var renameTargetId: UUID? = nil

  private enum ActiveSheet: String, Identifiable {
    case renameConversation

    var id: String { self.rawValue }
  }

  @State private var activeSheet: ActiveSheet? = nil

  // MARK: - Body

  @MainActor
  var body: some View {
    GeometryReader { geometry in
      ScrollView {
        let availableWidth = geometry.size.width

        VStack(alignment: .leading, spacing: 12) {
          if let error = sessionsViewModel.sessionsLoadError,
            !error.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
          {
            statusCard(
              title: "Couldn't refresh chats",
              subtitle: error,
              icon: "wifi.exclamationmark",
              tint: .orange
            )
          }

          if filteredSessions.isEmpty {
            emptyState
          } else {
            ForEach(groupedSessions) { section in
              Text(section.title)
                .font(.system(size: 12, weight: .semibold))
                .textCase(.uppercase)
                .tracking(0.55)
                .foregroundStyle(.secondary)
                .padding(.horizontal, 4)
                .padding(.top, 2)

              LazyVStack(spacing: 10) {
                ForEach(section.sessions, id: \.id) { session in
                  sessionRow(session, availableWidth: availableWidth)
                }
              }
            }
          }
        }
        .padding(.horizontal, 18)
        .padding(.top, 6)
        .padding(.bottom, 24)
      }
      .scrollIndicators(.hidden)
      .refreshable { await sessionsViewModel.refreshSessions() }
    }
    .frame(maxWidth: .infinity, maxHeight: .infinity)
    .background {
      sidebarBackground
        .ignoresSafeArea()
    }
    .navigationBarTitleDisplayMode(.inline)
    .toolbar {
      if !sessionsViewModel.sessions.isEmpty {
        ToolbarItem(placement: .topBarLeading) {
          Text(" \(sessionsViewModel.sessions.count) Chats ")
            .font(.system(size: 15, weight: .semibold))
            .foregroundStyle(Color(.label))
            .fixedSize()
        }
      }

      ToolbarItem(placement: .principal) {
        ConnectionStatusPillView()
      }

      ToolbarItemGroup(placement: .topBarTrailing) {
        headerNewChatButton
        headerTalkNowButton
      }
    }
    .onAppear {
      Task {
        await sessionsViewModel.ensureProfilePictureCached()
        await friendsViewModel.refreshMyCode()
        try? await friendsViewModel.loadFriends()
      }
    }
    .sheet(item: $activeSheet) { sheet in
      switch sheet {
      case .renameConversation:
        renameSheet
      }
    }
  }

  @AppStorage(PreferenceKeys.elevenLabsVoiceName) private var selectedVoiceName: String = ""

  private var headerNewChatButton: some View {
    Button(action: {
      Haptics.impact(.light)
      onStartNewChat()
    }) {
      Image(systemName: "plus")
        .font(.system(size: 18, weight: .semibold))
        .frame(width: 38, height: 38)
        .contentShape(Circle())
    }
    .buttonStyle(.plain)
    .accessibilityLabel("New Chat")
  }

  private var headerTalkNowButton: some View {
    Button(action: {
      Haptics.impact(.light)
      startVoiceChat()
    }) {
      ghostToolbarImage
        .frame(width: 38, height: 38)
        .contentShape(Circle())
    }
    .buttonStyle(.plain)
    .accessibilityLabel("Talk Now")
    .accessibilityHint("Start a new voice chat")
  }

  private var effectiveVoiceName: String {
    selectedVoiceName.isEmpty ? "luma" : selectedVoiceName
  }

  @ViewBuilder
  private var ghostToolbarImage: some View {
    if let videoName = ElevenLabsVoiceSuggestionsView.ghostVideoName(for: effectiveVoiceName),
       Bundle.main.url(forResource: videoName, withExtension: "mp4") != nil {
      TransparentVideoPlayerView(
        videoName: videoName,
        videoExtension: "mp4",
        startTime: ElevenLabsVoiceSuggestionsView.ghostStartTimes[videoName] ?? 0
      )
    } else if let uiImage = ElevenLabsVoiceSuggestionsView.ghostUIImage(for: effectiveVoiceName) {
      Image(uiImage: uiImage)
        .resizable()
        .scaledToFit()
    } else {
      Image(systemName: "waveform")
        .font(.system(size: 18, weight: .semibold))
    }
  }

  // MARK: - Session Row

  private func sessionRow(_ session: ChatSession, availableWidth: CGFloat) -> some View {
    let title = session.title
    let dateText = sessionsViewModel.formatLastUsed(session.lastUsedISO8601)
    let previewText = previewText(for: session, availableWidth: availableWidth)
    let isActive = sessionsViewModel.activeSessionId == session.id
    let shape = RoundedRectangle(cornerRadius: 18, style: .continuous)

    return Button(action: {
      Haptics.impact(.light)
      onOpenChat(session.id)
    }) {
      HStack(alignment: .center, spacing: 12) {
        mascotAvatar(
          name: mascotName(for: session),
          size: 38,
          cornerRadius: 11,
          borderColor: session.unreadCount > 0 ? AppTheme.accent.opacity(0.35) : Color(.separator).opacity(0.16)
        )
        .frame(width: 36, height: 36)

        VStack(alignment: .leading, spacing: 8) {
          HStack(alignment: .center, spacing: 8) {
            Text(title)
              .font(.system(size: 17, weight: session.unreadCount > 0 ? .semibold : .medium))
              .foregroundColor(.primary)
              .lineLimit(1)

            if isActive {
              Text("Open")
                .font(.system(size: 10, weight: .semibold))
                .textCase(.uppercase)
                .tracking(0.5)
                .foregroundStyle(AppTheme.accent)
                .padding(.horizontal, 7)
                .padding(.vertical, 3)
                .background(AppTheme.accent.opacity(0.12), in: Capsule())
            }
          }

          Text(previewText)
            .font(.system(size: 14))
            .foregroundColor(session.unreadCount > 0 ? .primary : .secondary)
            .lineLimit(2)
            .truncationMode(.tail)

          if session.unreadCount > 0 {
            Text(session.unreadCount == 1 ? "1 unread message" : "\(session.unreadCount) unread messages")
              .font(.system(size: 12, weight: .medium))
              .foregroundStyle(AppTheme.accent)
              .lineLimit(1)
          }
        }

        Spacer(minLength: 10)

        VStack(alignment: .trailing, spacing: 8) {
          if !dateText.isEmpty {
            Text(dateText)
              .font(.system(size: 11, weight: .medium))
              .foregroundColor(.secondary)
              .padding(.horizontal, 8)
              .padding(.vertical, 4)
              .background(Color(.secondarySystemBackground), in: Capsule())
          }

          if session.unreadCount > 0 {
            Text("\(session.unreadCount)")
              .font(.system(size: 12, weight: .semibold))
              .foregroundColor(.white)
              .padding(.horizontal, 7)
              .padding(.vertical, 4)
              .background(AppTheme.accent)
              .clipShape(Capsule())
          } else {
            Image(systemName: "chevron.right")
              .font(.system(size: 12, weight: .semibold))
              .foregroundStyle(.tertiary)
              .padding(.top, 2)
          }
        }
      }
      .frame(maxWidth: .infinity, alignment: .leading)
      .padding(.horizontal, 14)
      .padding(.vertical, 13)
      .background {
        if #available(iOS 26.0, *) {
          Color.clear.glassEffect(isActive ? .regular.interactive() : .regular, in: shape)
        } else {
          shape.fill(AppTheme.surface.opacity(0.72))
        }
      }
      .overlay {
        shape.stroke(
          isActive ? AppTheme.accent.opacity(0.42) : Color(.separator).opacity(0.18),
          lineWidth: isActive ? 1.0 : 0.5
        )
      }
      .shadow(
        color: isActive ? AppTheme.accent.opacity(0.16) : .black.opacity(0.04),
        radius: isActive ? 10 : 6,
        x: 0,
        y: isActive ? 4 : 2
      )
    }
    .buttonStyle(.plain)
    .contentShape(shape)
    .contextMenu {
      Button("Rename", systemImage: "pencil") {
        renameTargetId = session.id
        renameText = (session.title == ChatSession.defaultTitle) ? "" : session.title
        presentSheet(.renameConversation)
      }
      Button(role: .destructive) {
        Task { await sessionsViewModel.deleteSession(session.id) }
      } label: {
        Label("Delete", systemImage: "trash")
      }
    }
  }

  private var sidebarBackground: some View {
    AppTheme.background
  }

  @ViewBuilder
  private func statusCard(title: String, subtitle: String, icon: String, tint: Color) -> some View {
    let shape = RoundedRectangle(cornerRadius: 16, style: .continuous)
    HStack(alignment: .top, spacing: 10) {
      Image(systemName: icon)
        .font(.system(size: 16, weight: .semibold))
        .foregroundStyle(tint)
        .frame(width: 26, height: 26)
        .background(tint.opacity(0.16), in: RoundedRectangle(cornerRadius: 8, style: .continuous))

      VStack(alignment: .leading, spacing: 4) {
        Text(title)
          .font(.system(size: 14, weight: .semibold))
          .foregroundStyle(.primary)
        Text(subtitle)
          .font(.system(size: 13))
          .foregroundStyle(.secondary)
          .lineLimit(3)
      }
      Spacer(minLength: 0)
    }
    .padding(12)
    .background {
      if #available(iOS 26.0, *) {
        Color.clear.glassEffect(.regular, in: shape)
      } else {
        shape.fill(AppTheme.surface.opacity(0.78))
      }
    }
    .overlay {
      shape.stroke(Color(.separator).opacity(0.2), lineWidth: 0.5)
    }
  }

  private var emptyState: some View {
    VStack(spacing: 24) {
      ZStack {
        ghostCard(name: "hex", size: 140)
          .rotationEffect(.degrees(-8))
          .offset(x: -60, y: 6)

        ghostCard(name: "jay", size: 110)
          .rotationEffect(.degrees(5))
          .offset(x: 0, y: -4)

        ghostCard(name: "luma", size: 110)
          .rotationEffect(.degrees(10))
          .offset(x: 60, y: 6)
      }
      .frame(height: 140)

      VStack(spacing: 8) {
        Text("Your buddies are waiting!")
          .font(.system(size: 17, weight: .bold))
          .foregroundStyle(.primary)

        let buddyName = ElevenLabsVoiceSuggestionsView.requiredBuddies.first(where: { $0.key == effectiveVoiceName })?.name ?? "Luma"
        Text("Tap \(buddyName) to open a voice chat or tap + to start a regular chat.")
          .font(.system(size: 14))
          .foregroundStyle(.secondary)
          .multilineTextAlignment(.center)
          .padding(.horizontal, 20)
      }
    }
    .frame(maxWidth: .infinity)
    .padding(.top, 160)
  }

  private func ghostCard(name: String, size: CGFloat = 110) -> some View {
    Group {
      if let uiImage = ElevenLabsVoiceSuggestionsView.ghostUIImage(for: name) {
        Image(uiImage: uiImage)
          .resizable()
          .scaledToFit()
          .frame(width: size, height: size)
      } else {
        Image(systemName: "sparkles")
          .font(.system(size: size * 0.33, weight: .semibold))
          .foregroundStyle(.secondary)
          .frame(width: size, height: size)
      }
    }
  }

  // MARK: - Rename Sheet

  private var renameSheet: some View {
    VStack(spacing: 16) {
      Text("Rename Conversation")
        .font(.system(size: 20, weight: .semibold))

      TextField("Title", text: $renameText)
        .textInputAutocapitalization(.sentences)
        .disableAutocorrection(true)
        .padding(12)
        .background(Color(.secondarySystemBackground))
        .clipShape(RoundedRectangle(cornerRadius: 10, style: .continuous))

      HStack {
        Button("Cancel") { activeSheet = nil }
        Spacer()
        Button("Save") {
          let text = renameText
          activeSheet = nil
          if let id = renameTargetId {
            Task { await sessionsViewModel.renameSession(id, to: text) }
          }
        }
        .disabled(renameText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
      }
      .padding(.top, 6)
    }
    .padding(20)
    .presentationDetents([.medium])
  }

  // MARK: - Helpers

  @MainActor
  private func presentSheet(_ sheet: ActiveSheet) {
    if activeSheet == sheet {
      activeSheet = nil
      DispatchQueue.main.async {
        activeSheet = sheet
      }
    } else {
      DispatchQueue.main.async {
        activeSheet = sheet
      }
    }
  }

  private func startVoiceChat() {
    sessionsViewModel.shouldStartInVoiceMode = true
    onStartNewChat()
  }

  private func mascotName(for session: ChatSession) -> String {
    let keys = mascotKeys
    guard !keys.isEmpty else { return "mira" }
    let raw = session.id.uuidString.replacingOccurrences(of: "-", with: "")
    let bucket = Int(raw.prefix(8), radix: 16) ?? 0
    return keys[bucket % keys.count]
  }

  @ViewBuilder
  private func mascotAvatar(
    name: String,
    size: CGFloat,
    cornerRadius: CGFloat,
    borderColor: Color
  ) -> some View {
    ZStack {
      RoundedRectangle(cornerRadius: cornerRadius, style: .continuous)
        .fill(
          LinearGradient(
            colors: [AppTheme.brand.opacity(0.35), AppTheme.accent.opacity(0.20)],
            startPoint: .topLeading,
            endPoint: .bottomTrailing
          )
        )

      if let image = ElevenLabsVoiceSuggestionsView.ghostUIImage(for: name) {
        Image(uiImage: image)
          .resizable()
          .scaledToFill()
          .padding(2)
      } else {
        Image(systemName: "sparkles")
          .font(.system(size: size * 0.35, weight: .semibold))
          .foregroundStyle(AppTheme.accent)
      }
    }
    .frame(width: size, height: size)
    .clipShape(RoundedRectangle(cornerRadius: cornerRadius, style: .continuous))
    .overlay(
      RoundedRectangle(cornerRadius: cornerRadius, style: .continuous)
        .stroke(borderColor, lineWidth: 0.9)
    )
  }

  private func previewText(for session: ChatSession, availableWidth: CGFloat) -> String {
    let _ = availableWidth
    let raw =
      shouldShowLastMessage(session.lastMessageContent)
      ? (session.lastMessageContent ?? "") : "No messages yet"

    let singleLine = raw
      .replacingOccurrences(of: "\n", with: " ")
      .trimmingCharacters(in: .whitespacesAndNewlines)
    return singleLine.isEmpty ? "No messages yet" : singleLine
  }

  private func shouldShowLastMessage(_ content: String?) -> Bool {
    guard let content = content else { return false }
    let trimmed = content.trimmingCharacters(in: .whitespacesAndNewlines)
    return !trimmed.isEmpty && trimmed.uppercased() != "NULL"
  }

  private func parseISO8601Date(_ iso: String?) -> Date? {
    guard let raw = iso?.trimmingCharacters(in: .whitespacesAndNewlines), !raw.isEmpty else {
      return nil
    }
    let iso1 = ISO8601DateFormatter()
    iso1.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
    let iso2 = ISO8601DateFormatter()
    iso2.formatOptions = [.withInternetDateTime]
    return iso1.date(from: raw) ?? iso2.date(from: raw)
  }
}

// MARK: - Friends Sheet

struct FriendsSheetView: View {
  @Binding var isPresented: Bool
  @EnvironmentObject private var friendsViewModel: FriendsViewModel

  @State private var codeToAdd: String = ""
  @State private var showInviteMessageComposer: Bool = false
  @State private var inviteRecipients: [String] = []
  @State private var inviteErrorMessage: String? = nil

  private var cleanedCodeToAdd: String {
    codeToAdd.trimmingCharacters(in: .whitespacesAndNewlines)
  }
  private var isCodeComplete: Bool { cleanedCodeToAdd.count == 4 }

  var body: some View {
    ScrollView {
      VStack(spacing: 0) {
        // Title
        Text("Friends and Contacts")
          .font(.system(size: 20, weight: .bold))
          .frame(maxWidth: .infinity, alignment: .leading)
          .padding(.horizontal, 20)
          .padding(.top, 20)
          .padding(.bottom, 16)

        // Your code + Add friend cards
        HStack(spacing: 12) {
          // Your code card
          VStack(spacing: 8) {
            Text("Your Code")
              .font(.system(size: 12, weight: .medium))
              .foregroundStyle(.secondary)
              .textCase(.uppercase)
              .tracking(0.5)

            Text(friendsViewModel.myCode ?? "----")
              .font(.system(size: 28, weight: .bold, design: .rounded))
              .monospacedDigit()
              .foregroundStyle(.primary)
          }
          .frame(maxWidth: .infinity)
          .padding(.vertical, 18)
          .background(AppTheme.surface)
          .clipShape(RoundedRectangle(cornerRadius: 16, style: .continuous))
          .foregroundColor(AppTheme.textPrimary)

          // Add friend card
          VStack(spacing: 8) {
            Text("Add Friend")
              .font(.system(size: 12, weight: .medium))
              .foregroundStyle(.secondary)
              .textCase(.uppercase)
              .tracking(0.5)

            HStack(spacing: 8) {
              TextField("Code", text: $codeToAdd)
                .keyboardType(.numberPad)
                .textContentType(.oneTimeCode)
                .multilineTextAlignment(.center)
                .font(.system(size: 20, weight: .bold, design: .rounded))
                .monospacedDigit()
                .frame(maxWidth: .infinity)
                .onChange(of: codeToAdd, initial: false) { _, newValue in
                  let filtered = newValue.filter { $0.isNumber }
                  let clipped = String(filtered.prefix(4))
                  if clipped != newValue {
                    codeToAdd = clipped
                  }
                }

              Button {
                Task { await friendsViewModel.addFriendByCode(cleanedCodeToAdd) }
              } label: {
                if friendsViewModel.isAddingFriend {
                  ProgressView()
                    .controlSize(.small)
                } else {
                  Image(systemName: "arrow.right.circle.fill")
                    .font(.system(size: 26))
                    .foregroundStyle(isCodeComplete ? Color.blue : Color(.tertiaryLabel))
                }
              }
              .disabled(!isCodeComplete || friendsViewModel.isAddingFriend)
            }
            .padding(.horizontal, 8)
          }
          .frame(maxWidth: .infinity)
          .padding(.vertical, 18)
          .background(AppTheme.surface)
          .clipShape(RoundedRectangle(cornerRadius: 16, style: .continuous))
          .foregroundColor(AppTheme.textPrimary)
        }
        .padding(.horizontal, 20)

        // Action message
        if let msg = friendsViewModel.lastActionMessage, !msg.isEmpty {
          Text(msg)
            .font(.system(size: 13, weight: .medium))
            .foregroundStyle(.secondary)
            .multilineTextAlignment(.center)
            .padding(.top, 10)
            .padding(.horizontal, 20)
        }

        // Friends list
        VStack(alignment: .leading, spacing: 0) {
          Text("Your Buddies")
            .font(.system(size: 13, weight: .semibold))
            .foregroundStyle(.secondary)
            .textCase(.uppercase)
            .tracking(0.5)
            .padding(.horizontal, 20)
            .padding(.top, 24)
            .padding(.bottom, 12)

          if friendsViewModel.isLoadingFriends && friendsViewModel.friends.isEmpty {
            HStack(spacing: 10) {
              ProgressView()
              Text("Loading...")
                .font(.system(size: 14))
                .foregroundStyle(.secondary)
            }
            .frame(maxWidth: .infinity)
            .padding(.vertical, 24)
          } else if friendsViewModel.friends.isEmpty {
            VStack(spacing: 10) {
              Image(systemName: "person.2.slash")
                .font(.system(size: 28, weight: .light))
                .foregroundStyle(.tertiary)
              Text("No buddies yet")
                .font(.system(size: 14, weight: .medium))
                .foregroundStyle(.secondary)
              Text("Share your code or enter a friend's code above")
                .font(.system(size: 13))
                .foregroundStyle(.tertiary)
                .multilineTextAlignment(.center)
            }
            .frame(maxWidth: .infinity)
            .padding(.vertical, 24)
            .padding(.horizontal, 20)
          } else {
            VStack(spacing: 0) {
              ForEach(friendsViewModel.friends) { friend in
                friendRow(friend)
                if friend.id != friendsViewModel.friends.last?.id {
                  Divider()
                    .padding(.leading, 72)
                }
              }
            }
            .padding(.horizontal, 20)
          }
        }

        // Invite from contacts
        VStack(alignment: .leading, spacing: 0) {
          Text("Invite")
            .font(.system(size: 13, weight: .semibold))
            .foregroundStyle(.secondary)
            .textCase(.uppercase)
            .tracking(0.5)
            .padding(.horizontal, 20)
            .padding(.top, 24)
            .padding(.bottom, 12)

          InviteContactsInlineListView(
            onInvite: { phone in
              let cleaned = phone.trimmingCharacters(in: .whitespacesAndNewlines)
              guard !cleaned.isEmpty else {
                inviteErrorMessage = "That contact doesn't have a phone number."
                return
              }
              guard InviteMessageComposerView.canSendText else {
                inviteErrorMessage = "This device can't send texts."
                return
              }
              inviteRecipients = [cleaned]
              showInviteMessageComposer = true
            }
          )
          .padding(.horizontal, 20)
        }
        .padding(.bottom, 24)
      }
    }
    .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .top)
    .task {
      await friendsViewModel.refreshMyCode()
      try? await friendsViewModel.loadFriends()
    }
    .sheet(isPresented: $showInviteMessageComposer) {
      InviteMessageComposerView(
        recipients: inviteRecipients,
        body: "",
        isPresented: $showInviteMessageComposer
      )
    }
    .alert(
      "Invite",
      isPresented: Binding(
        get: {
          inviteErrorMessage != nil
        },
        set: { newValue in
          if !newValue { inviteErrorMessage = nil }
        })
    ) {
      Button("OK", role: .cancel) { inviteErrorMessage = nil }
    } message: {
      Text(inviteErrorMessage ?? "")
    }
  }

  // MARK: - Friend Row

  private func friendRow(_ friend: FriendSummary) -> some View {
    HStack(spacing: 14) {
      ZStack(alignment: .bottomTrailing) {
        SidebarAvatarView(avatarURL: friend.avatarURL)
          .frame(width: 44, height: 44)
          .clipShape(Circle())

        if friend.isOnline {
          Circle()
            .fill(Color.green)
            .frame(width: 12, height: 12)
            .overlay(
              Circle()
                .strokeBorder(Color(.systemBackground), lineWidth: 2)
            )
            .offset(x: 2, y: 2)
        }
      }

      VStack(alignment: .leading, spacing: 2) {
        Text(friend.fullName)
          .font(.system(size: 16, weight: .semibold))
          .foregroundStyle(.primary)
          .lineLimit(1)

        if let presence = friend.presenceText {
          Text(presence)
            .font(.system(size: 13))
            .foregroundColor(friend.isOnline ? .green : .gray)
            .lineLimit(1)
        } else if let voice = friend.voiceName, !voice.isEmpty {
          Text(voice)
            .font(.system(size: 13))
            .foregroundStyle(.tertiary)
            .lineLimit(1)
        }
      }

      Spacer()
    }
    .padding(.vertical, 10)
  }
}

// MARK: - Connection Status Pill

private struct ConnectionStatusPillView: View {
  @EnvironmentObject private var sessionsViewModel: ChatSessionsViewModel
  @ObservedObject private var authService = AuthService.shared
  @ObservedObject private var networkMonitor = NetworkMonitor.shared

  @State private var lastPathSatisfied: Bool = (NetworkMonitor.shared.pathStatus == .satisfied)
  @State private var reconnectTask: Task<Void, Never>? = nil
  @State private var isReconnectUpdating: Bool = false

  private enum PillStatus: Hashable {
    case waitingForNetwork
    case connecting
    case updating
  }

  private enum ConnectivityPolicy {
    static let coreTimeoutSeconds: TimeInterval = BackendService.coreRequestTimeoutSeconds
    static let maxRetryAttemptsWithCachedUI: Int = 3
    static let maxRetryWindowSecondsWithCachedUI: TimeInterval = coreTimeoutSeconds
    static let connectingFailureCooldownSeconds: TimeInterval = coreTimeoutSeconds
    static let backoffCapSeconds: TimeInterval = 16
  }

  @MainActor
  var body: some View {
    let status = pillStatus
    ZStack {
      if let status {
        pillView(status)
          .id(status)
          .transition(
            .asymmetric(
              insertion: .scale(scale: 0.96).combined(with: .opacity),
              removal: .scale(scale: 1.06).combined(with: .opacity)
            )
          )
      }
    }
    .animation(.spring(response: 0.30, dampingFraction: 0.70, blendDuration: 0), value: status)
    .onChange(of: networkMonitor.pathStatus, initial: false) { _, newStatus in
      let isSatisfied = (newStatus == .satisfied)
      let wasSatisfied = lastPathSatisfied
      lastPathSatisfied = isSatisfied

      if !isSatisfied {
        reconnectTask?.cancel()
        reconnectTask = nil
        isReconnectUpdating = false
        return
      }

      guard !wasSatisfied else { return }

      sessionsViewModel.lastSessionsSyncSucceeded = nil
      sessionsViewModel.lastSessionsSyncAt = nil
      startCatchUpSync()
    }
  }

  private var pillStatus: PillStatus? {
    // During bootstrap, drive the flow: Connecting → Updating → done
    if sessionsViewModel.isBootstrapping {
      if sessionsViewModel.isLoadingSessions { return .updating }
      return .connecting
    }

    // After bootstrap, surface network / sync issues
    if networkMonitor.pathStatus != .satisfied { return .waitingForNetwork }
    if isReconnectUpdating { return .updating }

    let now = Date()
    let recentFailureWindowSeconds: TimeInterval = ConnectivityPolicy
      .connectingFailureCooldownSeconds
    let sessionsFailedRecently =
      (sessionsViewModel.lastSessionsSyncSucceeded == false)
      && ((sessionsViewModel.lastSessionsSyncAt.map { now.timeIntervalSince($0) } ?? .infinity)
        <= recentFailureWindowSeconds)
    if sessionsFailedRecently { return .connecting }

    if sessionsViewModel.isLoadingSessions && sessionsViewModel.sessions.isEmpty {
      return .updating
    }
    return nil
  }

  @ViewBuilder
  private func pillView(_ status: PillStatus) -> some View {
    let capsule = Capsule(style: .continuous)
    let content = HStack(spacing: 6) {
      ProgressView()
        .controlSize(.mini)
      switch status {
      case .waitingForNetwork:
        Text("Waiting for network...")
          .font(.system(size: 12, weight: .regular))
      case .connecting:
        Text("Connecting...")
          .font(.system(size: 12, weight: .regular))
      case .updating:
        Text("Updating...")
          .font(.system(size: 12, weight: .regular))
      }
    }
    .padding(.horizontal, 10)
    .padding(.vertical, 6)

    if #available(iOS 26.0, *) {
      content.glassEffect(.regular.interactive(), in: capsule)
    } else {
      content.background(.ultraThinMaterial, in: capsule)
    }
  }

  private func startCatchUpSync() {
    reconnectTask?.cancel()
    reconnectTask = Task { @MainActor in
      await sessionsViewModel.preloadCachedSessionsIfNeeded()

      let hasCachedUIAtStart = !sessionsViewModel.sessions.isEmpty
      var didAttemptOnce = false
      var failureCount = 0
      let startedAt = Date()

      while Task.isCancelled == false {
        if networkMonitor.pathStatus != .satisfied { break }

        if hasCachedUIAtStart && didAttemptOnce {
          isReconnectUpdating = false
        } else {
          isReconnectUpdating = true
        }
        didAttemptOnce = true

        sessionsViewModel.lastSessionsSyncSucceeded = nil

        await sessionsViewModel.loadSessions()

        isReconnectUpdating = false

        let sessionsOK = (sessionsViewModel.lastSessionsSyncSucceeded == true)
        if sessionsOK {
          return
        }

        failureCount += 1

        if hasCachedUIAtStart {
          let elapsed = Date().timeIntervalSince(startedAt)
          if failureCount >= ConnectivityPolicy.maxRetryAttemptsWithCachedUI
            || elapsed >= ConnectivityPolicy.maxRetryWindowSecondsWithCachedUI
          {
            break
          }
        }

        let capped = min(failureCount, 5)
        let delaySeconds = min(pow(2.0, Double(capped - 1)), ConnectivityPolicy.backoffCapSeconds)
        try? await Task.sleep(nanoseconds: UInt64(delaySeconds * 1_000_000_000))
      }

      isReconnectUpdating = false
    }
  }
}

#if DEBUG
  #Preview("SidebarView") {
    SidebarView(onOpenChat: { _ in }, onStartNewChat: {})
      .environmentObject(SidebarNavigationViewModel())
      .environmentObject(ChatSessionsViewModel())
      .environmentObject(FriendsViewModel(accessTokenProvider: { "" }))
  }
#endif
