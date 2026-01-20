import SwiftUI
import UIKit

struct SidebarView: View {

    @EnvironmentObject private var navigationViewModel: SidebarNavigationViewModel
    @EnvironmentObject private var sessionsViewModel: ChatSessionsViewModel
    @EnvironmentObject private var friendsViewModel: FriendsViewModel

    @ObservedObject private var authService = AuthService.shared
    @ObservedObject private var networkMonitor = NetworkMonitor.shared

    @Binding var isOpen: Bool
    let profileNamespace: Namespace.ID

    @FocusState private var isSearchFieldFocused: Bool

    @State private var searchText: String = ""
    @State private var showRenameSheet: Bool = false
    @State private var renameText: String = ""
    @State private var renameTargetId: UUID? = nil
    @State private var inviteErrorMessage: String? = nil
    @State private var showFriendsSheet: Bool = false

    private var isSearchActive: Bool {
        if isSearchFieldFocused { return true }
        return !searchText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
    }

    private var filteredSessions: [ChatSession] {
        let term = searchText.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        if term.isEmpty { return sessionsViewModel.sessions }
        return sessionsViewModel.sessions.filter { $0.title.lowercased().contains(term) }
    }

    @MainActor
    var body: some View {
        NavigationStack {
            GeometryReader { geometry in
                let pinnedHeaderBar = HStack {
                    if #available(iOS 26.0, *) {
                        Button(action: {
                            Haptics.impact(.light)
                            presentFriendsSheet()
                        }) {
                            HStack(spacing: 8) {
                                Text("Friends")
                                    .font(.system(size: 15, weight: .semibold))
                                Image(systemName: "plus")
                                    .font(.system(size: 16, weight: .semibold))
                            }
                            .foregroundColor(.primary)
                            .padding(.horizontal, 12)
                            .frame(height: 44)
                        }
                        .buttonStyle(.plain)
                        .contentShape(RoundedRectangle(cornerRadius: 14, style: .continuous))
                        .glassEffect(.regular.interactive(), in: RoundedRectangle(cornerRadius: 14, style: .continuous))
                    }

                    Spacer()
                    ConnectionStatusPillView()
                    Spacer()
                    // Keep the right side the same width as the left so the status pill stays centered.
                    HStack {
                        Spacer(minLength: 0)
                        if #available(iOS 26.0, *) {
                            Button(action: {
                                Haptics.impact(.light)
                                // Select new chat immediately (no animation) so the main content updates
                                // while the sidebar is still visible.
                                withAnimation(nil) {
                                    sessionsViewModel.startNewChat()
                                    navigationViewModel.selectedTab = .chat
                                }
                                // Only animate the sidebar closing.
                                withAnimation(.spring(response: 0.3, dampingFraction: 0.8, blendDuration: 0)) {
                                    isOpen = false
                                }
                            }) {
                                Image(systemName: "square.and.pencil")
                                    .font(.system(size: 18, weight: .semibold))
                                    .foregroundStyle(.primary)
                                    .frame(width: 44, height: 44)
                            }
                            .buttonStyle(.plain)
                            .accessibilityLabel("New chat")
                            .glassEffect(.regular.interactive(), in: Circle())
                        }
                    }
                    .frame(width: 110, height: 44)
                }
                .padding(.horizontal, 16)
                .padding(.top, 6)

                ScrollView {
                    let availableWidth = geometry.size.width

                    VStack(spacing: 10) {
                        LazyVStack(spacing: 6) {
                            ForEach(filteredSessions, id: \.id) { session in
                                Button(action: {
                                    Haptics.impact(.light)
                                    // Switch the active chat immediately (no animation) to avoid briefly showing
                                    // the previous chat while the sidebar is sliding away.
                                    withAnimation(nil) {
                                        sessionsViewModel.openSession(session.id)
                                        navigationViewModel.selectedTab = .chat
                                    }
                                    // Only animate the sidebar closing.
                                    withAnimation(.spring(response: 0.3, dampingFraction: 0.8, blendDuration: 0)) {
                                        isOpen = false
                                    }
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
                                        showRenameSheet = true
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
                    pinnedHeaderBar
                }
                .safeAreaInset(edge: .bottom) {
                    controlsBar(bottomInset: 0, availableWidth: geometry.size.width)
                }
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity)
            .background(Color(.systemBackground))
            .navigationBarTitleDisplayMode(.inline)
            .toolbar(.hidden, for: .navigationBar)
            .onChange(of: isOpen) { _, open in
                if !open {
                    searchText = ""
                    isSearchFieldFocused = false
                }
            }
            .onAppear {
                Task {
                    await sessionsViewModel.ensureProfilePictureCached()
                    // Prefetch the friend code so it's ready before the user opens the sheet.
                    await friendsViewModel.refreshMyCode()
                }
            }
            .sheet(isPresented: $showRenameSheet) {
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
                        Button("Cancel") { showRenameSheet = false }
                        Spacer()
                        Button("Save") {
                            let text = renameText
                            showRenameSheet = false
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
            .sheet(isPresented: $showFriendsSheet) {
                FriendsSheetView(isPresented: $showFriendsSheet)
                    .environmentObject(friendsViewModel)
                    .presentationDetents([.large])
                    .presentationDragIndicator(.visible)
            }
        }
    }

    @MainActor
    private func presentFriendsSheet() {
        // SwiftUI sometimes drops sheet presentations if `isPresented` is already true
        // or if multiple state updates happen in the same run loop.
        if showFriendsSheet {
            showFriendsSheet = false
            DispatchQueue.main.async {
                showFriendsSheet = true
            }
        } else {
            DispatchQueue.main.async {
                showFriendsSheet = true
            }
        }
    }

    private func controlsBar(bottomInset: CGFloat, availableWidth: CGFloat) -> some View {
        let showLeftButtons: Bool = !isSearchActive
        let isSearchCollapsed: Bool = !isSearchActive

        let horizontalPadding: CGFloat = 40 // matches `.padding(.horizontal, 20)` below
        let contentWidth: CGFloat = max(0, availableWidth - horizontalPadding)
        let sideButtonWidth: CGFloat = 50
        let buttonSpacing: CGFloat = 8

        let fullSearchWidthBetweenButtons: CGFloat = max(0, contentWidth - sideButtonWidth - sideButtonWidth - (2 * buttonSpacing))
        let collapsedSearchWidthFallback: CGFloat = 220
        let collapsedSearchWidth: CGFloat = {
            guard fullSearchWidthBetweenButtons > 0 else { return collapsedSearchWidthFallback }
            return max(200, fullSearchWidthBetweenButtons * 0.90)
        }()

        return HStack(spacing: 8) {
            if #available(iOS 26.0, *) {
                if showLeftButtons {
                    Button(action: {
                        Haptics.impact(.light)
                        Task { @MainActor in
                            // Warm avatar before presenting Settings so it shows instantly.
                            await sessionsViewModel.ensureProfilePictureCached()
                            navigationViewModel.showSettingsSheet = true
                        }
                    }) {
                        SidebarAvatarView(avatarURL: sessionsViewModel.myAvatarURL)
                            .frame(width: 40, height: 40)
                            .clipShape(Circle())
                    }
                    .frame(width: 50, height: 50)
                    .buttonStyle(.plain)
                    .glassEffect(.regular, in: Circle())
                }

                HStack(spacing: 8) {
                    Image(systemName: "magnifyingglass")
                        .font(.system(size: 17, weight: .regular))
                        .foregroundStyle(.secondary)

                    TextField("Search", text: $searchText)
                        .focused($isSearchFieldFocused)
                        .submitLabel(.search)
                        .textInputAutocapitalization(.never)
                        .disableAutocorrection(true)
                        .font(.system(size: 17, weight: .regular))
                        .padding(.vertical, 4)
                }
                .padding(.vertical, 10)
                .padding(.horizontal, 14)
                .frame(maxWidth: isSearchCollapsed ? collapsedSearchWidth : .infinity)
                .clipShape(RoundedRectangle(cornerRadius: 999, style: .continuous))
                .glassEffect(.regular.interactive())
            }

            if showLeftButtons {
                if #available(iOS 26.0, *) {
                    Button(action: {
                        // This button is for closing the sidebar (NOT for creating a new chat).
                        // New chat lives in the top bar (pencil icon) to avoid accidental resets.
                        Haptics.impact(.light)
                        withAnimation(.spring(response: 0.28, dampingFraction: 0.92, blendDuration: 0)) {
                            navigationViewModel.selectedTab = .chat
                            isOpen = false
                        }
                    }) {
                        Image(systemName: "chevron.right.2")
                            .font(.system(size: 20, weight: .semibold))
                            .foregroundStyle(.primary)
                            .offset(y: -1)
                    }
                    .frame(width: 50, height: 50)
                    .buttonStyle(.plain)
                    .accessibilityLabel("Close sidebar")
                    .glassEffect(.regular.interactive(), in: Circle())
                }
            }

            if isSearchActive {
                if #available(iOS 26.0, *) {
                    Button(action: {
                        Haptics.impact(.light)
                        searchText = ""
                        isSearchFieldFocused = false
                    }) {
                        Image(systemName: "xmark")
                            .font(.system(size: 20, weight: .semibold))
                            .foregroundStyle(.primary)
                            .frame(width: 50, height: 50)
                    }
                    .buttonStyle(.plain)
                    .glassEffect(.regular.interactive(), in: Circle())
                    .accessibilityLabel("Close search")
                }
            }
        }
        .padding(.horizontal, 20)
        .padding(.bottom, bottomInset + 2 + (isSearchFieldFocused ? 12 : 0))
        .contentShape(Rectangle())
        .animation(.spring(response: 0.28, dampingFraction: 0.92, blendDuration: 0), value: isSearchActive)
        .animation(.spring(response: 0.28, dampingFraction: 0.92, blendDuration: 0), value: isSearchFieldFocused)
        .zIndex(10)
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

    var body: some View {
        NavigationStack {
            ScrollView {
                VStack(spacing: 16) {
                    VStack(spacing: 6) {
                        Text("Your code")
                            .font(.system(size: 13, weight: .semibold))
                            .foregroundStyle(.secondary)
                        Text(friendsViewModel.myCode ?? "— — — —")
                            .font(.system(size: 34, weight: .bold, design: .rounded))
                            .monospacedDigit()
                    }
                    .padding(.top, 8)

                    VStack(spacing: 12) {
                        VStack(alignment: .center, spacing: 8) {
                            Text("Add a friend")
                                .font(.system(size: 13, weight: .semibold))
                                .foregroundStyle(.secondary)

                            TextField("Enter 4-digit code", text: $codeToAdd)
                                .keyboardType(.numberPad)
                                .textContentType(.oneTimeCode)
                                .textInputAutocapitalization(.never)
                                .disableAutocorrection(true)
                                .multilineTextAlignment(.center)
                                .font(.system(size: 20, weight: .semibold, design: .monospaced))
                                .padding(.vertical, 12)
                                .padding(.horizontal, 12)
                                .background(Color(.secondarySystemBackground))
                                .clipShape(RoundedRectangle(cornerRadius: 12, style: .continuous))
                                .onChange(of: codeToAdd, initial: false) { _, newValue in
                                    let filtered = newValue.filter { $0.isNumber }
                                    let clipped = String(filtered.prefix(4))
                                    if clipped != newValue {
                                        codeToAdd = clipped
                                    }
                                }
                        }
                        .frame(maxWidth: .infinity)

                        Button {
                            Task { await friendsViewModel.addFriendByCode(cleanedCodeToAdd) }
                        } label: {
                            HStack(spacing: 10) {
                                if friendsViewModel.isAddingFriend {
                                    ProgressView()
                                        .controlSize(.small)
                                }
                                Text(friendsViewModel.isAddingFriend ? "Adding…" : "Add")
                            }
                            .frame(maxWidth: .infinity)
                        }
                        .buttonStyle(.borderedProminent)
                        .disabled(!isCodeComplete || friendsViewModel.isAddingFriend)
                    }

                    if let msg = friendsViewModel.lastActionMessage, !msg.isEmpty {
                        Text(msg)
                            .font(.system(size: 13))
                            .foregroundStyle(.secondary)
                            .multilineTextAlignment(.center)
                            .padding(.top, 4)
                    }

                    Divider()
                        .padding(.top, 6)

                    VStack(alignment: .leading, spacing: 10) {
                        Text("Contacts")
                            .font(.system(size: 16, weight: .semibold))
                            .foregroundStyle(.primary)

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
                    }
                }
                .padding(16)
            }
            .navigationTitle("Friends")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .topBarTrailing) {
                    Button("Done") { isPresented = false }
                }
            }
            .task { await friendsViewModel.refreshMyCode() }
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
}

private struct ConnectionStatusPillView: View {
    @EnvironmentObject private var sessionsViewModel: ChatSessionsViewModel
    @ObservedObject private var authService = AuthService.shared
    @ObservedObject private var networkMonitor = NetworkMonitor.shared

    @State private var lastPathSatisfied: Bool? = nil
    @State private var reconnectTask: Task<Void, Never>? = nil
    @State private var isReconnectUpdating: Bool = false
    @State private var displayedStatus: PillStatus? = nil

    private enum PillStatus: Equatable {
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
        ZStack {
            if let status = displayedStatus {
                pillView(status)
                    .transition(
                        .asymmetric(
                            insertion: .scale(scale: 0.96).combined(with: .opacity),
                            removal: .scale(scale: 1.06).combined(with: .opacity)
                        )
                    )
            }
        }
        .onAppear {
            lastPathSatisfied = (networkMonitor.pathStatus == .satisfied)
            displayedStatus = pillStatus
        }
        .onChange(of: pillStatus, initial: false) { _, newValue in
            withAnimation(.spring(response: 0.30, dampingFraction: 0.70, blendDuration: 0)) {
                displayedStatus = newValue
            }
        }
        .onChange(of: networkMonitor.pathStatus, initial: false) { _, newStatus in
            let isSatisfied = (newStatus == .satisfied)
            let wasSatisfied = lastPathSatisfied
            lastPathSatisfied = isSatisfied

            if isSatisfied == false {
                reconnectTask?.cancel()
                reconnectTask = nil
                isReconnectUpdating = false
                return
            }

            guard wasSatisfied == false else { return }

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
    @Previewable @State var isOpen: Bool = true
    @Previewable @Namespace var ns
    SidebarView(isOpen: $isOpen, profileNamespace: ns)
        .environmentObject(SidebarNavigationViewModel())
        .environmentObject(ChatSessionsViewModel())
        .environmentObject(FriendsViewModel(accessTokenProvider: { "" }))
}
#endif
