import SwiftUI

struct SidebarView: View {

    @EnvironmentObject private var navigationViewModel: SidebarNavigationViewModel
    @EnvironmentObject private var sessionsViewModel: ChatSessionsViewModel
    @EnvironmentObject private var friendsViewModel: FriendsViewModel

    let onOpenChat: (UUID) -> Void
    let onStartNewChat: () -> Void
    var searchText: String = ""
    var hideHeader: Bool = false

    private var filteredSessions: [ChatSession] {
        guard !searchText.isEmpty else { return sessionsViewModel.sessions }
        let query = searchText.lowercased()
        return sessionsViewModel.sessions.filter { session in
            session.title.lowercased().contains(query) ||
            (session.lastMessageContent?.lowercased().contains(query) ?? false)
        }
    }

    @State private var renameText: String = ""
    @State private var renameTargetId: UUID? = nil

    private enum ActiveSheet: String, Identifiable {
        case renameConversation
        case friends

        var id: String { self.rawValue }
    }

    @State private var activeSheet: ActiveSheet? = nil

    @MainActor
    var body: some View {
        GeometryReader { geometry in
                let pinnedHeaderBar = HStack {
                    Button(action: {
                        Haptics.impact(.light)
                        Task { @MainActor in
                            await sessionsViewModel.ensureProfilePictureCached()
                            navigationViewModel.showSettingsSheet = true
                        }
                    }) {
                        SidebarAvatarView(avatarURL: sessionsViewModel.myAvatarURL)
                            .frame(width: 36, height: 36)
                            .clipShape(Circle())
                    }
                    .frame(width: 44, height: 44)
                    .buttonStyle(.plain)
                    .modifier(HeaderCircleStyle())

                    Button(action: {
                        Haptics.impact(.light)
                        presentSheet(.friends)
                    }) {
                        Image(systemName: "person.2")
                            .font(.system(size: 18, weight: .semibold))
                            .foregroundStyle(.primary)
                            .frame(width: 44, height: 44)
                    }
                    .buttonStyle(.plain)
                    .accessibilityLabel("Friends")
                    .contentShape(Circle())
                    .modifier(HeaderCircleStyle())

                    Spacer()
                    ConnectionStatusPillView()
                    Spacer()

                    Button(action: {
                        Haptics.impact(.light)
                        onStartNewChat()
                    }) {
                        Image(systemName: "plus.bubble.fill")
                            .font(.system(size: 18, weight: .semibold))
                            .foregroundStyle(.primary)
                            .frame(width: 44, height: 44)
                    }
                    .buttonStyle(.plain)
                    .accessibilityLabel("New Chat")
                    .contentShape(Circle())
                    .modifier(HeaderCircleStyle())
                }
                .padding(.horizontal, 16)
                .padding(.top, 6)

                ScrollView {
                    let availableWidth = geometry.size.width

                    VStack(spacing: 10) {
                        LazyVStack(spacing: 6) {
                            ForEach(filteredSessions, id: \.id) { session in
                                Button(action: {
                                    onOpenChat(session.id)
                                }) {
                                    let title = session.title
                                    let dateText = sessionsViewModel.formatLastUsed(session.lastUsedISO8601)
                                    let previewText = previewText(for: session, availableWidth: availableWidth)

                                    VStack(alignment: .leading, spacing: 12) {
                                        HStack {
                                            Text(title)
                                                .font(.system(size: 18, weight: .regular))
                                                .foregroundColor(.primary)
                                            Spacer()
                                            Text(dateText)
                                                .font(.system(size: 12))
                                                .foregroundColor(.secondary)
                                        }

                                        HStack(spacing: 6) {
                                            Text(previewText)
                                                .font(.system(size: 14))
                                                .foregroundColor(.secondary)
                                                .lineLimit(1)
                                                .truncationMode(.tail)
                                            Spacer()
                                        }
                                    }
                                    .frame(maxWidth: .infinity, alignment: .leading)
                                    .padding(.horizontal, 2)
                                    .padding(.vertical, 12)
                                }
                                .buttonStyle(.plain)
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
                        }
                        .padding(.horizontal, 20)
                    }
                }
                .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .top)
                .refreshable { await sessionsViewModel.refreshSessions() }
                .safeAreaInset(edge: .top) {
                    if !hideHeader {
                        pinnedHeaderBar
                    }
                }
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity)
            .background(Color(.systemBackground))
            .onAppear {
                Task {
                    await sessionsViewModel.ensureProfilePictureCached()
                    // Prefetch the friend code so it's ready before the user opens the sheet.
                    await friendsViewModel.refreshMyCode()
                    // Prefetch friends + avatars so the Friends list sheet is instant.
                    try? await friendsViewModel.loadFriends()
                }
            }
            .sheet(item: $activeSheet) { sheet in
                switch sheet {
                case .renameConversation:
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

                case .friends:
                    FriendsSheetView(isPresented: sheetPresentedBinding(for: .friends))
                        .environmentObject(friendsViewModel)
                        .presentationDetents([.medium, .large])
                        .presentationDragIndicator(.visible)
                }
            }
    }

    @MainActor
    private func presentSheet(_ sheet: ActiveSheet) {
        // SwiftUI can drop/ignore sheet presentations if multiple updates happen in one run loop,
        // or if the same sheet is requested while it's already presented.
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

    private func sheetPresentedBinding(for sheet: ActiveSheet) -> Binding<Bool> {
        Binding(
            get: { activeSheet == sheet },
            set: { newValue in
                if !newValue {
                    activeSheet = nil
                }
            }
        )
    }

    private func previewText(for session: ChatSession, availableWidth: CGFloat) -> String {
        let previewTargetWidth = availableWidth * 0.88
        let rawPreview = shouldShowLastMessage(session.lastMessageContent) ? (session.lastMessageContent ?? "") : "No messages yet"
        let clipped = wordBoundaryTruncated(rawPreview, previewTargetWidth)
        return clipped + (clipped.count < rawPreview.count ? "…" : "")
    }

    private func shouldShowLastMessage(_ content: String?) -> Bool {
        guard let content = content else { return false }
        let trimmed = content.trimmingCharacters(in: .whitespacesAndNewlines)
        return !trimmed.isEmpty && trimmed.uppercased() != "NULL"
    }

    private func wordBoundaryTruncated(_ text: String, _ targetWidth: CGFloat) -> String {
        let trimmed = text.trimmingCharacters(in: .whitespacesAndNewlines)
        if trimmed.isEmpty { return "" }
        let avgCharWidth: CGFloat = 7.0
        let maxChars = max(8, Int((targetWidth / avgCharWidth).rounded(.down)))
        if trimmed.count <= maxChars { return trimmed }
        var result: String = ""
        for word in trimmed.split(separator: " ") {
            if result.isEmpty {
                if word.count > maxChars {
                    return String(word.prefix(maxChars))
                } else {
                    result = String(word)
                }
            } else {
                if result.count + 1 + word.count > maxChars { break }
                result += " " + word
            }
        }
        return result
    }
}

private struct HeaderPillStyle: ViewModifier {
    func body(content: Content) -> some View {
        if #available(iOS 26.0, *) {
            content
                .glassEffect(.regular.interactive(), in: RoundedRectangle(cornerRadius: 14, style: .continuous))
        } else {
            content
                .background(Color(.secondarySystemBackground))
                .clipShape(RoundedRectangle(cornerRadius: 14, style: .continuous))
        }
    }
}

private struct HeaderCircleStyle: ViewModifier {
    func body(content: Content) -> some View {
        if #available(iOS 26.0, *) {
            content
                .glassEffect(.regular.interactive(), in: Circle())
        } else {
            content
                .background(Color(.secondarySystemBackground))
                .clipShape(Circle())
        }
    }
}

private struct FriendsSheetView: View {
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

    private let avatarSize: CGFloat = 64

    var body: some View {
        VStack(spacing: 0) {
            // Friends list section
            VStack(alignment: .leading, spacing: 16) {
                if friendsViewModel.isLoadingFriends && friendsViewModel.friends.isEmpty {
                    HStack {
                        Spacer()
                        VStack(spacing: 8) {
                            ProgressView()
                            Text("Loading…")
                                .font(.system(size: 13))
                                .foregroundStyle(.secondary)
                        }
                        Spacer()
                    }
                    .padding(.vertical, 20)
                } else if friendsViewModel.friends.isEmpty {
                    HStack {
                        Spacer()
                        Text("No friends yet")
                            .font(.system(size: 14))
                            .foregroundStyle(.secondary)
                        Spacer()
                    }
                    .padding(.vertical, 20)
                } else {
                    ScrollView(.horizontal, showsIndicators: false) {
                        HStack(spacing: 16) {
                            ForEach(friendsViewModel.friends) { friend in
                                VStack(spacing: 6) {
                                    SidebarAvatarView(avatarURL: friend.avatarURL)
                                        .frame(width: avatarSize, height: avatarSize)
                                        .clipShape(Circle())

                                    Text(friend.fullName.components(separatedBy: " ").first ?? friend.fullName)
                                        .font(.system(size: 12))
                                        .foregroundStyle(.primary)
                                        .lineLimit(1)
                                }
                                .frame(width: avatarSize + 8)
                            }
                        }
                        .padding(.horizontal, 16)
                    }
                }
            }
            .padding(.top, 24)
            .padding(.bottom, 16)

            Divider()
                .padding(.horizontal, 16)

            // Your code & Add friend section
            VStack(spacing: 16) {
                HStack(spacing: 24) {
                    VStack(spacing: 4) {
                        Text("Your code")
                            .font(.system(size: 12, weight: .medium))
                            .foregroundStyle(.secondary)
                        Text(friendsViewModel.myCode ?? "----")
                            .font(.system(size: 24, weight: .bold, design: .rounded))
                            .monospacedDigit()
                    }

                    VStack(spacing: 4) {
                        Text("Add a friend")
                            .font(.system(size: 12, weight: .medium))
                            .foregroundStyle(.secondary)

                        HStack(spacing: 8) {
                            TextField("Code", text: $codeToAdd)
                                .keyboardType(.numberPad)
                                .textContentType(.oneTimeCode)
                                .multilineTextAlignment(.center)
                                .font(.system(size: 18, weight: .semibold, design: .monospaced))
                                .frame(width: 80)
                                .padding(.vertical, 8)
                                .padding(.horizontal, 8)
                                .background(Color(.secondarySystemBackground))
                                .clipShape(RoundedRectangle(cornerRadius: 10, style: .continuous))
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
                                    Image(systemName: "plus.circle.fill")
                                        .font(.system(size: 28))
                                }
                            }
                            .disabled(!isCodeComplete || friendsViewModel.isAddingFriend)
                        }
                    }
                }

                if let msg = friendsViewModel.lastActionMessage, !msg.isEmpty {
                    Text(msg)
                        .font(.system(size: 12))
                        .foregroundStyle(.secondary)
                        .multilineTextAlignment(.center)
                }
            }
            .padding(.horizontal, 16)
            .padding(.top, 16)
            .padding(.bottom, 16)

            Divider()
                .padding(.horizontal, 16)

            // Contacts section
            VStack(alignment: .leading, spacing: 10) {
                Text("Invite from Contacts")
                    .font(.system(size: 14, weight: .semibold))
                    .foregroundStyle(.secondary)
                    .padding(.horizontal, 16)

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
                .padding(.horizontal, 16)
            }
            .padding(.top, 14)
            .padding(.bottom, 16)

            Spacer(minLength: 0)
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
        .alert("Invite", isPresented: Binding(get: {
            inviteErrorMessage != nil
        }, set: { newValue in
            if !newValue { inviteErrorMessage = nil }
        })) {
            Button("OK", role: .cancel) { inviteErrorMessage = nil }
        } message: {
            Text(inviteErrorMessage ?? "")
        }
    }
}

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
        if networkMonitor.pathStatus != .satisfied { return .waitingForNetwork }
        if authService.isCheckingAuth { return .connecting }
        if isReconnectUpdating { return .updating }

        let now = Date()
        let recentFailureWindowSeconds: TimeInterval = ConnectivityPolicy.connectingFailureCooldownSeconds
        let sessionsFailedRecently =
            (sessionsViewModel.lastSessionsSyncSucceeded == false) &&
            ((sessionsViewModel.lastSessionsSyncAt.map { now.timeIntervalSince($0) } ?? .infinity) <= recentFailureWindowSeconds)
        if sessionsFailedRecently { return .connecting }

        if sessionsViewModel.isBootstrapping && sessionsViewModel.sessions.isEmpty { return .updating }
        if sessionsViewModel.isLoadingSessions && sessionsViewModel.sessions.isEmpty { return .updating }
        return nil
    }

    @ViewBuilder
    private func pillView(_ status: PillStatus) -> some View {
        if #available(iOS 26.0, *) {
        let capsule = Capsule(style: .continuous)
        HStack(spacing: 6) {
            switch status {
            case .waitingForNetwork:
                Image(systemName: "wifi.slash")
                    .font(.system(size: 11, weight: .regular))
                Text("Waiting for network…")
                    .font(.system(size: 12, weight: .regular))
            case .connecting:
                ProgressView()
                    .controlSize(.mini)
                Text("Connecting…")
                    .font(.system(size: 12, weight: .regular))
            case .updating:
                ProgressView()
                Text("Updating…")
                    .font(.system(size: 12, weight: .regular))
            }
        }
        .padding(.horizontal, 10)
        .padding(.vertical, 6)
        .glassEffect(.regular, in: capsule)
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
                    if failureCount >= ConnectivityPolicy.maxRetryAttemptsWithCachedUI ||
                        elapsed >= ConnectivityPolicy.maxRetryWindowSecondsWithCachedUI {
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
    SidebarView(onOpenChat: { _ in }, onStartNewChat: { })
        .environmentObject(SidebarNavigationViewModel())
        .environmentObject(ChatSessionsViewModel())
        .environmentObject(FriendsViewModel(accessTokenProvider: { "" }))
}
#endif
