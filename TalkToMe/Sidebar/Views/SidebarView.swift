import SwiftUI
import Contacts
import MessageUI
import UIKit

struct SidebarView: View {

    @EnvironmentObject private var navigationViewModel: SidebarNavigationViewModel
    @EnvironmentObject private var sessionsViewModel: ChatSessionsViewModel
    @EnvironmentObject private var linkVM: LinkViewModel
    @ObservedObject private var authService = AuthService.shared
    @ObservedObject private var networkMonitor = NetworkMonitor.shared

    @Environment(\.colorScheme) private var colorScheme

    @Binding var isOpen: Bool

    @FocusState private var isSearchFieldFocused: Bool

    @State private var searchText: String = ""
    @State private var showRenameSheet: Bool = false
    @State private var renameText: String = ""
    @State private var renameTargetId: UUID? = nil
    @State private var showAddFriendSheet: Bool = false

    let profileNamespace: Namespace.ID

    private var isSearchActive: Bool {
        if isSearchFieldFocused { return true }
        return !searchText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
    }

    private func controlsBar(bottomInset: CGFloat, availableWidth: CGFloat) -> some View {
        let showLeftButtons: Bool = !isSearchActive
        let isSearchCollapsed: Bool = !isSearchActive

        // Make the search pill ~1/3 shorter when not active.
        // Use the parent GeometryReader width (not a self-measured width) to avoid feedback loops.
        let horizontalPadding: CGFloat = 40 // matches `.padding(.horizontal, 20)` below
        let contentWidth: CGFloat = max(0, availableWidth - horizontalPadding)
        let sideButtonWidth: CGFloat = 50
        let buttonSpacing: CGFloat = 8

        // Base "full width" (inactive layout is [profile][search][newChat]) has 2 gaps (2 * 8).
        let fullSearchWidthBetweenButtons: CGFloat = max(0, contentWidth - sideButtonWidth - sideButtonWidth - (2 * buttonSpacing))
        let collapsedSearchWidthFallback: CGFloat = 220
        let collapsedSearchWidth: CGFloat = {
            guard fullSearchWidthBetweenButtons > 0 else { return collapsedSearchWidthFallback }
            return max(200, fullSearchWidthBetweenButtons * 0.90)
        }()

        return HStack(spacing: 8) {
            if #available(iOS 26.0, *) {
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
                        withAnimation(.spring(response: 0.28, dampingFraction: 0.92, blendDuration: 0)) {
                            sessionsViewModel.startNewChat()
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
                    .accessibilityLabel("New chat")
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

    var body: some View {
        NavigationStack {
            GeometryReader { geometry in
                let pinnedHeaderBar = HStack {
                    if #available(iOS 26.0, *) {
                    Button(action: {
                        Haptics.impact(.light)
                        navigationViewModel.showSettingsSheet = true
                    }) {
                        SidebarAvatarView(avatarURL: sessionsViewModel.myAvatarURL)
                            .frame(width: 40, height: 40)
                            .clipShape(Circle())
                    }
                    .frame(width: 50, height: 50)
                    .buttonStyle(.plain)
                    .glassEffect(.regular, in: Circle())
                    }

                    Spacer()
                    ConnectionStatusPillView()
                    Spacer()

                    if #available(iOS 26.0, *) {
                        Button(action: {
                            showAddFriendSheet = true
                            Task { await linkVM.ensureInviteReady() }
                        }) {
                            Image(systemName: "person.fill.badge.plus")
                                .font(.system(size: 18, weight: .semibold))
                                .foregroundColor(.primary)
                        }
                        .frame(width: 50, height: 50)
                        .contentShape(Circle())
                        .buttonStyle(.plain)
                        .glassEffect(.regular.interactive(), in: Circle())
                    }

                }
                .padding(.horizontal, 16)
                .padding(.top, 6)

                ScrollView {
                    let availableWidth = geometry.size.width
                    let isLinked = (linkVM.state == .linked) || (sessionsViewModel.partnerInfo?.linked == true)

                    VStack(spacing: 10) {
                        if !sessionsViewModel.pendingRequests.isEmpty {
                            VStack(alignment: .leading, spacing: 12) {
                                ForEach(sessionsViewModel.pendingRequests, id: \.id) { request in
                                    Button(action: {
                                        withAnimation(.spring(response: 0.3, dampingFraction: 0.8, blendDuration: 0)) {
                                            sessionsViewModel.openPendingRequest(request)
                                            isOpen = false
                                        }
                                    }) {
                                        PendingRequestRowView(request: request)
                                    }
                                    .buttonStyle(.plain)
                                }
                            }
                            .padding(12)
                            .background(
                                RoundedRectangle(cornerRadius: 16, style: .continuous)
                                    .fill(Color(.systemGray6))
                            )
                            .padding(.horizontal, 16)
                            .padding(.top, 10)
                        }


                        let term = searchText.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
                        let filteredSessions: [ChatSession] = sessionsViewModel.sessions.filter { session in
                            term.isEmpty || session.title.lowercased().contains(term)
                        }

                        LazyVStack(spacing: 6) {
                            if term.isEmpty,
                               let err = sessionsViewModel.sessionsLoadError,
                               !err.isEmpty,
                               !sessionsViewModel.sessions.isEmpty,
                               !sessionsViewModel.isLoadingSessions {
                                HStack(spacing: 10) {
                                    Image(systemName: "exclamationmark.triangle.fill")
                                        .font(.system(size: 12, weight: .semibold))
                                        .foregroundStyle(.secondary)
                                    Text(err)
                                        .font(.system(size: 13))
                                        .foregroundStyle(.secondary)
                                        .lineLimit(2)
                                    Spacer()
                                    Button(action: {
                                        Haptics.impact(.light)
                                        Task { await sessionsViewModel.refreshSessions() }
                                    }) {
                                        Text("Retry")
                                            .font(.system(size: 13, weight: .semibold))
                                    }
                                    .buttonStyle(.bordered)
                                }
                                .padding(.vertical, 6)
                            }

                            if term.isEmpty && sessionsViewModel.sessions.isEmpty {
                                if sessionsViewModel.isLoadingSessions {
                                    VStack(spacing: 10) {
                                        ProgressView()
                                        Text("Loading conversations…")
                                            .font(.system(size: 14))
                                            .foregroundStyle(.secondary)
                                    }
                                    .frame(maxWidth: .infinity)
                                    .padding(.vertical, 24)
                                } else if let err = sessionsViewModel.sessionsLoadError, !err.isEmpty {
                                    VStack(spacing: 10) {
                                        Text(err)
                                            .font(.system(size: 14))
                                            .foregroundStyle(.secondary)
                                            .multilineTextAlignment(.center)
                                        Button(action: {
                                            Haptics.impact(.light)
                                            Task { await sessionsViewModel.refreshSessions() }
                                        }) {
                                            Text("Retry")
                                                .font(.system(size: 14, weight: .semibold))
                                        }
                                        .buttonStyle(.bordered)
                                    }
                                    .frame(maxWidth: .infinity)
                                    .padding(.vertical, 24)
                                } else {
                                    VStack(spacing: 10) {
                                        Text("No conversations yet")
                                            .font(.system(size: 14))
                                            .foregroundStyle(.secondary)
                                    }
                                    .frame(maxWidth: .infinity)
                                    .padding(.vertical, 24)
                                }
                            }

                            ForEach(filteredSessions, id: \.id) { session in
                                Button(action: {
                                    withAnimation(.spring(response: 0.3, dampingFraction: 0.8, blendDuration: 0)) {
                                        sessionsViewModel.openSession(session.id)
                                        navigationViewModel.selectedTab = .chat
                                        isOpen = false
                                    }
                                }) {
                                    let title = session.title
                                    let dateText = sessionsViewModel.formatLastUsed(session.lastUsedISO8601)
                                    let previewText = previewText(for: session, availableWidth: availableWidth)
                                    let showUnreadDot = isLinked && sessionsViewModel.unreadPartnerSessionIds.contains(session.id)

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
                                            if showUnreadDot {
                                                Circle()
                                                    .fill(Color(red: 0.4, green: 0.2, blue: 0.6))
                                                    .frame(width: 14, height: 14)
                                            }
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
                // Match the top: the bottom controls bar is now a real safe-area inset, not an overlay.
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
                Task { await sessionsViewModel.ensureProfilePictureCached() }
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
            .sheet(isPresented: $showAddFriendSheet) {
                AddFriendContactsSheetView(isPresented: $showAddFriendSheet)
                    .environmentObject(linkVM)
            }
        }
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

private struct InviteContact: Identifiable, Hashable {
    let id: String
    let givenName: String
    let familyName: String
    let thumbnailImageData: Data?
    let phoneNumbers: [String]

    var displayName: String {
        let full = "\(givenName) \(familyName)".trimmingCharacters(in: .whitespacesAndNewlines)
        return full.isEmpty ? "Unknown" : full
    }

    var primaryPhoneNumber: String? {
        phoneNumbers.first
    }
}

private struct AddFriendContactsSheetView: View {
    @Binding var isPresented: Bool

    @EnvironmentObject private var linkVM: LinkViewModel

    @State private var authorizationStatus: CNAuthorizationStatus = CNContactStore.authorizationStatus(for: .contacts)
    @State private var isLoadingContacts: Bool = false
    @State private var contacts: [InviteContact] = []
    @State private var loadError: String? = nil

    @State private var activeMessageDraft: MessageDraft? = nil
    @State private var showMessagesUnavailableAlert: Bool = false
    @State private var showInviteLinkUnavailableAlert: Bool = false

    private struct MessageDraft: Identifiable, Hashable {
        let id = UUID()
        let recipients: [String]
        let body: String
    }

    private var inviteLinkString: String? {
        if case .shareReady(let url) = linkVM.state {
            return url.absoluteString
        }
        return nil
    }

    var body: some View {
        NavigationStack {
            Group {
                switch authorizationStatus {
                case .authorized:
                    contentList
                case .limited:
                    contentList
                case .notDetermined:
                    loadingView(title: "Requesting Contacts…")
                case .denied, .restricted:
                    deniedView
                @unknown default:
                    deniedView
                }
            }
            .navigationTitle("Add Friend")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .topBarTrailing) {
                    Button("Done") { isPresented = false }
                        .font(.system(size: 16, weight: .semibold))
                }
            }
        }
        .task {
            // Kick link generation (this is the same link shown in Settings)
            await linkVM.ensureInviteReady()

            await refreshAuthorizationStatus()
            if authorizationStatus == .notDetermined {
                await requestContactsAccess()
            }
            if authorizationStatus == .authorized {
                await loadContacts()
            }
        }
        .sheet(item: $activeMessageDraft) { draft in
            MessageComposeView(
                recipients: draft.recipients,
                body: draft.body,
                onFinish: { activeMessageDraft = nil }
            )
        }
        .alert("Messages not available", isPresented: $showMessagesUnavailableAlert) {
            Button("OK", role: .cancel) {}
        } message: {
            Text("This device can’t send text messages.")
        }
        .alert("Invite link not ready", isPresented: $showInviteLinkUnavailableAlert) {
            Button("OK", role: .cancel) {}
        } message: {
            Text("Open Settings → Link Partner to generate your invite link, then try again.")
        }
    }

    private var contentList: some View {
        List {
            Section {
                switch linkVM.state {
                case .creating, .accepting, .unlinking:
                    HStack {
                        ProgressView()
                        Text("Preparing your link…")
                            .foregroundStyle(.secondary)
                    }
                case .linked:
                    Text("You’re already connected. Unlink in Settings to generate a new invite link.")
                        .font(.footnote)
                        .foregroundStyle(.secondary)
                case .shareReady:
                    EmptyView()
                case .idle, .unlinked, .error:
                    HStack {
                        ProgressView()
                        Text("Preparing your link…")
                            .foregroundStyle(.secondary)
                    }
                }
            }

            if isLoadingContacts {
                Section {
                    HStack {
                        Spacer()
                        ProgressView("Loading contacts…")
                        Spacer()
                    }
                }
            } else if let loadError {
                Section {
                    Text(loadError)
                        .font(.footnote)
                        .foregroundStyle(.secondary)
                }
            } else {
                Section {
                    ForEach(contacts) { contact in
                        HStack(spacing: 12) {
                            avatar(for: contact)

                            VStack(alignment: .leading, spacing: 2) {
                                Text(contact.displayName)
                                    .font(.system(size: 16, weight: .regular))
                                    .foregroundStyle(.primary)
                                    .lineLimit(1)
                                if let phone = contact.primaryPhoneNumber {
                                    Text(phone)
                                        .font(.system(size: 13, weight: .regular))
                                        .foregroundStyle(.secondary)
                                        .lineLimit(1)
                                }
                            }

                            Spacer()

                            Button(action: { invite(contact) }) {
                                HStack(spacing: 8) {
                                    Text("Invite")
                                        .font(.system(size: 14, weight: .semibold))
                                    Spacer(minLength: 0)
                                }
                                .padding(.horizontal, 12)
                                .padding(.vertical, 8)
                                .frame(width: 92)
                                .background(
                                    RoundedRectangle(cornerRadius: 12, style: .continuous)
                                        .fill(Color(red: 0.4, green: 0.2, blue: 0.6).opacity(0.12))
                                )
                            }
                            .buttonStyle(.plain)
                            .disabled(contact.primaryPhoneNumber == nil || inviteLinkString == nil)
                        }
                        .padding(.vertical, 4)
                    }
                }
            }
        }
        .listStyle(.insetGrouped)
    }

    private func avatar(for contact: InviteContact) -> some View {
        Group {
            if let data = contact.thumbnailImageData, let image = UIImage(data: data) {
                Image(uiImage: image)
                    .resizable()
                    .scaledToFill()
            } else {
                ZStack {
                    Circle()
                        .fill(Color(.systemGray5))
                    Image(systemName: "person.fill")
                        .font(.system(size: 14, weight: .semibold))
                        .foregroundStyle(.secondary)
                }
            }
        }
        .frame(width: 40, height: 40)
        .clipShape(Circle())
    }

    private var deniedView: some View {
        VStack(spacing: 12) {
            Text("Contacts access is off.")
                .font(.system(size: 18, weight: .semibold))
            Text("Enable Contacts in Settings to invite friends.")
                .font(.system(size: 14))
                .foregroundStyle(.secondary)
                .multilineTextAlignment(.center)

            Button("Open Settings") {
                if let url = URL(string: UIApplication.openSettingsURLString) {
                    UIApplication.shared.open(url)
                }
            }
            .font(.system(size: 16, weight: .semibold))
            .padding(.top, 6)
        }
        .padding(20)
    }

    private func loadingView(title: String) -> some View {
        VStack(spacing: 12) {
            ProgressView()
            Text(title)
                .font(.system(size: 14))
                .foregroundStyle(.secondary)
        }
        .padding(20)
    }

    private func invite(_ contact: InviteContact) {
        guard MFMessageComposeViewController.canSendText() else {
            showMessagesUnavailableAlert = true
            return
        }
        guard let phone = contact.primaryPhoneNumber else { return }
        guard let link = inviteLinkString else {
            showInviteLinkUnavailableAlert = true
            return
        }

        let body = """
Hey! I just discovered TalkToMe and I want to connect. It's a new app to foster good-quality communication, one meaningful conversation at a time.

Here's the link to connect with my profile: \(link)
"""

        activeMessageDraft = MessageDraft(recipients: [phone], body: body)
    }

    private func refreshAuthorizationStatus() async {
        let status = CNContactStore.authorizationStatus(for: .contacts)
        await MainActor.run { authorizationStatus = status }
    }

    private func requestContactsAccess() async {
        let store = CNContactStore()
        let granted: Bool = await withCheckedContinuation { continuation in
            store.requestAccess(for: .contacts) { ok, _ in
                continuation.resume(returning: ok)
            }
        }
        await refreshAuthorizationStatus()
        if !granted {
            await MainActor.run {
                loadError = "Contacts permission was not granted."
            }
        }
    }

    private func loadContacts() async {
        await MainActor.run {
            isLoadingContacts = true
            loadError = nil
        }

        let store = CNContactStore()
        let keys: [CNKeyDescriptor] = [
            CNContactIdentifierKey as CNKeyDescriptor,
            CNContactGivenNameKey as CNKeyDescriptor,
            CNContactFamilyNameKey as CNKeyDescriptor,
            CNContactThumbnailImageDataKey as CNKeyDescriptor,
            CNContactPhoneNumbersKey as CNKeyDescriptor
        ]

        let request = CNContactFetchRequest(keysToFetch: keys)
        request.unifyResults = true
        request.sortOrder = .userDefault

        var results: [InviteContact] = []
        do {
            try store.enumerateContacts(with: request) { contact, _ in
                let phones = contact.phoneNumbers
                    .map { $0.value.stringValue }
                    .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
                    .filter { !$0.isEmpty }

                // Only show contacts that can actually be invited via SMS
                if phones.isEmpty { return }

                results.append(
                    InviteContact(
                        id: contact.identifier,
                        givenName: contact.givenName,
                        familyName: contact.familyName,
                        thumbnailImageData: contact.thumbnailImageData,
                        phoneNumbers: phones
                    )
                )
            }

            await MainActor.run {
                contacts = results
                isLoadingContacts = false
            }
        } catch {
            await MainActor.run {
                isLoadingContacts = false
                loadError = error.localizedDescription
            }
        }
    }
}

private struct MessageComposeView: UIViewControllerRepresentable {
    let recipients: [String]
    let body: String
    let onFinish: () -> Void

    final class Coordinator: NSObject, MFMessageComposeViewControllerDelegate {
        let onFinish: () -> Void

        init(onFinish: @escaping () -> Void) {
            self.onFinish = onFinish
        }

        func messageComposeViewController(_ controller: MFMessageComposeViewController, didFinishWith result: MessageComposeResult) {
            onFinish()
        }
    }

    func makeCoordinator() -> Coordinator {
        Coordinator(onFinish: onFinish)
    }

    func makeUIViewController(context: Context) -> MFMessageComposeViewController {
        let vc = MFMessageComposeViewController()
        vc.messageComposeDelegate = context.coordinator
        vc.recipients = recipients
        vc.body = body
        return vc
    }

    func updateUIViewController(_ uiViewController: MFMessageComposeViewController, context: Context) {
        // Ensure the message is always prefilled even if SwiftUI updates state around presentation timing.
        if uiViewController.recipients != recipients {
            uiViewController.recipients = recipients
        }
        if uiViewController.body != body {
            uiViewController.body = body
        }
    }
}

#if DEBUG
private struct SidebarView_PreviewHost: View {
    @StateObject private var navigationVM = SidebarNavigationViewModel()
    @StateObject private var sessionsVM = ChatSessionsViewModel()
    @StateObject private var linkVM = LinkViewModel.preview(state: .linked)

    @Namespace private var profileNamespace
    @State private var isOpen: Bool = true

    var body: some View {
        SidebarView(isOpen: $isOpen, profileNamespace: profileNamespace)
            .environmentObject(navigationVM)
            .environmentObject(sessionsVM)
            .environmentObject(linkVM)
            .onAppear {
                sessionsVM.setNavigationViewModel(navigationVM)
                sessionsVM.setLinkViewModel(linkVM)
                navigationVM.isOpen = true
            }
            .task {
                if sessionsVM.sessions.isEmpty {
                    sessionsVM.sessions = sampleSessions
                    sessionsVM.unreadPartnerSessionIds = [sampleSessions[0].id]
                }
            }
    }

    private var sampleSessions: [ChatSession] {
        [
            ChatSession(
                id: UUID(),
                title: "Casual conversation",
                lastUsedISO8601: "2025-11-27T12:34:56Z",
                lastMessageContent: "Hey"
            ),
            ChatSession(
                id: UUID(),
                title: "Therapy notes",
                lastUsedISO8601: "2025-11-23T09:15:02Z",
                lastMessageContent: "Can we talk about what happened this week?"
            ),
            ChatSession(
                id: UUID(),
                title: "New Chat",
                lastUsedISO8601: nil,
                lastMessageContent: nil
            )
        ]
    }
}

#Preview("SidebarView") {
    SidebarView_PreviewHost()
}
#endif


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

    var body: some View {
        ZStack {
            if let status = displayedStatus {
                pillView(status)
                    .transition(
                        .asymmetric(
                            insertion: .scale(scale: 0.96).combined(with: .opacity),
                            // “Escape” animation: a tiny pop before fading out.
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

            // Only trigger catch-up when we transition offline -> online (path unsatisfied -> satisfied).
            guard wasSatisfied == false else { return }

            // Reset reachability markers so the reconnect loop can decide between Connecting/Updating.
            sessionsViewModel.lastSessionsSyncSucceeded = nil
            sessionsViewModel.lastPendingRequestsSyncSucceeded = nil

            startCatchUpSync()
        }
    }

    private var pillStatus: PillStatus? {
        if networkMonitor.pathStatus != .satisfied { return .waitingForNetwork }
        if authService.isCheckingAuth { return .connecting }
        if isReconnectUpdating { return .updating }

        // If we're online but our last core sync failed *recently*, show Connecting… (cool-down prevents infinite pill).
        let now = Date()
        let recentFailureWindowSeconds: TimeInterval = ConnectivityPolicy.connectingFailureCooldownSeconds
        let sessionsFailedRecently =
            (sessionsViewModel.lastSessionsSyncSucceeded == false) &&
            ((sessionsViewModel.lastSessionsSyncAt.map { now.timeIntervalSince($0) } ?? .infinity) <= recentFailureWindowSeconds)
        let pendingFailedRecently =
            (sessionsViewModel.lastPendingRequestsSyncSucceeded == false) &&
            ((sessionsViewModel.lastPendingRequestsSyncAt.map { now.timeIntervalSince($0) } ?? .infinity) <= recentFailureWindowSeconds)
        if sessionsFailedRecently || pendingFailedRecently {
            return .connecting
        }

        // For first-run empty UI, keep a minimal updating indicator.
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
            // Show cached UI immediately. Sync status should never block usability.
            await sessionsViewModel.preloadCachedSessionsIfNeeded()

            let hasCachedUIAtStart = !sessionsViewModel.sessions.isEmpty
            var didAttemptOnce = false
            var failureCount = 0
            let startedAt = Date()

            while Task.isCancelled == false {
                if networkMonitor.pathStatus != .satisfied { break }

                await AppSyncGate.shared.setSyncing(true)

                // Avoid UI flip-flopping between Updating/Connecting when the backend is timing out:
                // - If we already have cached UI, show Updating only on the first attempt.
                // - If we have no cached UI, keep showing Updating for every attempt (user needs feedback).
                if hasCachedUIAtStart && didAttemptOnce {
                    isReconnectUpdating = false
                } else {
                    isReconnectUpdating = true
                }
                didAttemptOnce = true

                // Reset reachability markers for this attempt.
                sessionsViewModel.lastSessionsSyncSucceeded = nil
                sessionsViewModel.lastPendingRequestsSyncSucceeded = nil

                async let sessionsTask: Void = sessionsViewModel.loadSessions(ensurePartnerInfo: false)
                async let pendingTask: Void = sessionsViewModel.loadPendingRequests()
                _ = await (sessionsTask, pendingTask)

                await AppSyncGate.shared.setSyncing(false)
                isReconnectUpdating = false

                let sessionsOK = (sessionsViewModel.lastSessionsSyncSucceeded == true)
                let pendingOK = (sessionsViewModel.lastPendingRequestsSyncSucceeded == true)
                if sessionsOK && pendingOK {
                    // Core sync succeeded: kick off non-critical work in the background.
                    Task { @MainActor in
                        await sessionsViewModel.loadPartnerInfo(prefetchAvatars: false)
                        await sessionsViewModel.loadPairedAvatars()
                        await sessionsViewModel.preloadAvatars()
                        await sessionsViewModel.ensureProfilePictureCached()
                    }
                    return
                }

                failureCount += 1

                // If we already have cached UI, don't retry forever (prevents infinite Connecting… loops).
                if hasCachedUIAtStart {
                    let elapsed = Date().timeIntervalSince(startedAt)
                    if failureCount >= ConnectivityPolicy.maxRetryAttemptsWithCachedUI ||
                        elapsed >= ConnectivityPolicy.maxRetryWindowSecondsWithCachedUI {
                        break
                    }
                }

                let capped = min(failureCount, 5) // 1,2,4,8,16s cap
                let delaySeconds = min(pow(2.0, Double(capped - 1)), ConnectivityPolicy.backoffCapSeconds)
                try? await Task.sleep(nanoseconds: UInt64(delaySeconds * 1_000_000_000))
            }

            await AppSyncGate.shared.setSyncing(false)
            isReconnectUpdating = false
        }
    }
}
