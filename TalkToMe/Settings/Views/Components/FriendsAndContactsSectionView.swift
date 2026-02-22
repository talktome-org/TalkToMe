import SwiftUI
import Contacts

struct FriendsAndContactsSectionView: View {
    @EnvironmentObject private var friendsViewModel: FriendsViewModel

    @State private var codeToAdd: String = ""
    @State private var searchText: String = ""
    @State private var showInviteMessageComposer: Bool = false
    @State private var inviteRecipients: [String] = []
    @State private var showShareSheet: Bool = false
    @State private var toastMessage: String? = nil
    @State private var toastWorkItem: DispatchWorkItem? = nil
    @FocusState private var isCodeFieldFocused: Bool
    @EnvironmentObject private var contactsVM: ContactsViewModel
    @State private var shakeOffset: CGFloat = 0

    private var cleanedCodeToAdd: String {
        codeToAdd.trimmingCharacters(in: .whitespacesAndNewlines)
    }
    private var isCodeComplete: Bool { cleanedCodeToAdd.count == 4 }

    private var inviteMessage: String {
        let code = friendsViewModel.myCode ?? "----"
        return "Hey, I'm using TalkToMe to chat with friends - join us here! Add me to your friends list with the code: \(code)."
    }

    private var filteredContacts: [ContactRow] {
        guard !searchText.isEmpty else { return contactsVM.contacts }
        let query = searchText.lowercased()
        return contactsVM.contacts.filter { $0.name.lowercased().contains(query) }
    }

    var body: some View {
        ZStack(alignment: .top) {
            listContent
            if let toastMessage {
                ToastOverlayView(message: toastMessage, onDismiss: dismissToast)
                    .padding(.top, 8)
                    .zIndex(1)
            }
        }
    }

    private var listContent: some View {
        List {
            if searchText.isEmpty {
                shareSection
                codeSection
                friendsSection
                contactsSection
            } else {
                contactsSection
            }
        }
        .listSectionSpacing(.compact)
        .contentMargins(.top, 0)
        .listStyle(.insetGrouped)
        .scrollContentBackground(.hidden)
        .background(AppTheme.background)
        .scrollDismissesKeyboard(.immediately)
        .searchable(text: $searchText, placement: .navigationBarDrawer(displayMode: .always), prompt: "Search")
        .navigationTitle("Contacts")
        .navigationBarTitleDisplayMode(.inline)
        .onAppear {
            Task {
                await friendsViewModel.refreshMyCode()
                try? await friendsViewModel.loadFriends()
                if contactsVM.contacts.isEmpty {
                    await contactsVM.load()
                }
            }
        }
        .sheet(isPresented: $showInviteMessageComposer) {
            InviteMessageComposerView(
                recipients: inviteRecipients,
                body: inviteMessage,
                isPresented: $showInviteMessageComposer
            )
        }
        .sheet(isPresented: $showShareSheet) {
            ShareSheet(activityItems: [inviteMessage])
                .presentationDetents([.large])
        }
        .onChange(of: friendsViewModel.lastActionMessage) { _, newValue in
            guard let msg = newValue, !msg.isEmpty else { return }
            showToast(msg)
        }
    }

    @State private var shimmerPhase: CGFloat = -1.0
    @State private var shimmerEnabled: Bool = true

    private var shareSection: some View {
        Section {
            Button {
                shimmerEnabled = false
                shimmerPhase = -1.0
                showShareSheet = true
            } label: {
                HStack(spacing: 6) {
                    Image(systemName: "heart")
                        .font(.system(size: 18))
                    Text("Share TalkToMe")
                        .font(.system(size: 17, weight: .regular))
                }
                .foregroundStyle(.blue)
                .overlay {
                    if shimmerEnabled {
                        HStack(spacing: 6) {
                            Image(systemName: "heart")
                                .font(.system(size: 18))
                            Text("Share TalkToMe")
                                .font(.system(size: 17, weight: .regular))
                        }
                        .foregroundStyle(.white)
                        .mask(
                            GeometryReader { geo in
                                let width = max(geo.size.width, 1)
                                LinearGradient(
                                    stops: [
                                        .init(color: .clear, location: 0.0),
                                        .init(color: .white.opacity(0.45), location: 0.20),
                                        .init(color: .white.opacity(0.75), location: 0.50),
                                        .init(color: .white.opacity(0.45), location: 0.80),
                                        .init(color: .clear, location: 1.0),
                                    ],
                                    startPoint: .leading,
                                    endPoint: .trailing
                                )
                                .frame(width: width * 1.6)
                                .blur(radius: 2)
                                .offset(x: shimmerPhase * width * 2.2)
                            }
                        )
                        .allowsHitTesting(false)
                    }
                }
            }
            .listRowBackground(Color.clear)
            .listRowInsets(EdgeInsets(top: 2, leading: 0, bottom: 2, trailing: 0))
            .onAppear {
                shimmerEnabled = true
                // Reset before starting to avoid stacking repeat-forever drivers
                shimmerPhase = -1.0
                DispatchQueue.main.async {
                    withAnimation(.linear(duration: 1.8).repeatForever(autoreverses: false)) {
                        shimmerPhase = 1.0
                    }
                }
            }
            .onDisappear {
                // Cancel the repeating animation to free the main thread
                withAnimation(nil) {
                    shimmerPhase = -1.0
                }
                shimmerEnabled = false
            }
        }
    }

    private var codeSection: some View {
        Section {
            HStack(spacing: 0) {
                VStack(alignment: .leading, spacing: 8) {
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
                .frame(maxWidth: .infinity, alignment: .leading)
                .padding(.leading, 20)
                .padding(.trailing, 12)
                .padding(.vertical, 14)

                Divider()
                    .frame(height: 50)

                HStack(spacing: 0) {
                    VStack(alignment: .leading, spacing: 8) {
                        Text("Add Friend")
                            .font(.system(size: 12, weight: .medium))
                            .foregroundStyle(.secondary)
                            .textCase(.uppercase)
                            .tracking(0.5)

                        TextField("Enter code", text: $codeToAdd)
                            .autocorrectionDisabled()
                            .textInputAutocapitalization(.never)
                            .multilineTextAlignment(.leading)
                            .font(.system(size: 20, weight: .bold, design: .rounded))
                            .monospacedDigit()
                            .focused($isCodeFieldFocused)
                            .frame(height: 28)
                            .onChange(of: codeToAdd, initial: false) { _, newValue in
                                let filtered = newValue.filter { $0.isNumber }
                                let clipped = String(filtered.prefix(4))
                                if clipped != newValue {
                                    codeToAdd = clipped
                                    triggerShake()
                                }
                            }
                    }

                    Spacer()

                    Button {
                        Task { await friendsViewModel.addFriendByCode(cleanedCodeToAdd) }
                    } label: {
                        if friendsViewModel.isAddingFriend {
                            ProgressView()
                                .controlSize(.small)
                                .frame(width: 44, height: 44)
                        } else {
                            Image(systemName: "arrow.right.circle.fill")
                                .font(.system(size: 32))
                                .foregroundStyle(isCodeComplete ? Color.blue : Color(.tertiaryLabel))
                                .frame(width: 44, height: 44)
                        }
                    }
                    .contentShape(Rectangle())
                    .buttonStyle(.plain)
                    .disabled(!isCodeComplete || friendsViewModel.isAddingFriend)
                    .offset(y: 6)
                }
                .padding(.leading, 12)
                .padding(.trailing, 8)
                .padding(.vertical, 14)
                .offset(x: shakeOffset)
            }
            .listRowInsets(EdgeInsets(top: 0, leading: 0, bottom: 0, trailing: 8))
        }
    }

    private func triggerShake() {
        let generator = UINotificationFeedbackGenerator()
        generator.notificationOccurred(.error)

        withAnimation(.spring(response: 0.08, dampingFraction: 0.2)) {
            shakeOffset = 8
        }
        DispatchQueue.main.asyncAfter(deadline: .now() + 0.08) {
            withAnimation(.spring(response: 0.08, dampingFraction: 0.2)) {
                shakeOffset = -6
            }
        }
        DispatchQueue.main.asyncAfter(deadline: .now() + 0.16) {
            withAnimation(.spring(response: 0.08, dampingFraction: 0.2)) {
                shakeOffset = 4
            }
        }
        DispatchQueue.main.asyncAfter(deadline: .now() + 0.24) {
            withAnimation(.spring(response: 0.1, dampingFraction: 0.5)) {
                shakeOffset = 0
            }
        }
    }

    @ViewBuilder
    private var friendsSection: some View {
        if !friendsViewModel.friends.isEmpty {
            Section(header: Text("Friends")) {
                ForEach(friendsViewModel.friends) { friend in
                    HStack(spacing: 14) {
                        ZStack(alignment: .bottomTrailing) {
                            SidebarAvatarView(avatarURL: friend.avatarURL)
                                .frame(width: 44, height: 44)
                                .clipShape(Circle())

                            Circle()
                                .fill(friend.isOnline ? Color.green : Color.gray)
                                .frame(width: 12, height: 12)
                                .overlay(Circle().strokeBorder(Color(.systemBackground), lineWidth: 2))
                                .offset(x: 2, y: 2)
                        }

                        VStack(alignment: .leading, spacing: 2) {
                            Text(friend.fullName)
                                .font(.system(size: 16, weight: .semibold))
                                .foregroundStyle(.primary)
                                .lineLimit(1)

                            if let presence = friend.presenceText {
                                Text(presence)
                                    .font(.system(size: 13))
                                    .foregroundStyle(friend.isOnline ? .green : .gray)
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
                    .padding(.vertical, 2)
                }
            }
        }
    }

    private var contactsSection: some View {
        Section(header: Text("Contacts")) {
            if contactsVM.isLoading {
                HStack(spacing: 10) {
                    ProgressView()
                    Text("Loading contacts...")
                        .font(.system(size: 14))
                        .foregroundStyle(.secondary)
                }
            } else if let error = contactsVM.errorMessage {
                VStack(spacing: 10) {
                    Text(error)
                        .font(.system(size: 13))
                        .foregroundStyle(.secondary)
                        .multilineTextAlignment(.center)
                    Button("Try Again") {
                        Task { await contactsVM.load() }
                    }
                    .buttonStyle(.bordered)
                }
                .frame(maxWidth: .infinity)
                .padding(.vertical, 8)
            } else if filteredContacts.isEmpty {
                Text(searchText.isEmpty ? "No contacts found" : "No contacts matching \"\(searchText)\"")
                    .font(.system(size: 14))
                    .foregroundStyle(.secondary)
                    .frame(maxWidth: .infinity)
                    .padding(.vertical, 8)
            } else {
                ForEach(filteredContacts) { contact in
                    HStack(spacing: 14) {
                        contactAvatar(contact)
                            .frame(width: 40, height: 40)
                            .clipShape(Circle())

                        Text(contact.name)
                            .font(.system(size: 16, weight: .regular))
                            .foregroundStyle(.primary)
                            .lineLimit(1)

                        Spacer()

                        Button("Invite") {
                            inviteRecipients = [contact.phone]
                            showInviteMessageComposer = true
                        }
                        .font(.system(size: 14, weight: .semibold))
                        .buttonStyle(.borderedProminent)
                        .buttonBorderShape(.capsule)
                    }
                }
            }
        }
    }

    private func showToast(_ message: String) {
        toastWorkItem?.cancel()
        withAnimation(.spring(response: 0.3, dampingFraction: 0.8)) {
            toastMessage = message
        }
        let item = DispatchWorkItem { dismissToast() }
        toastWorkItem = item
        DispatchQueue.main.asyncAfter(deadline: .now() + 4, execute: item)
    }

    private func dismissToast() {
        toastWorkItem?.cancel()
        toastWorkItem = nil
        withAnimation(.spring(response: 0.3, dampingFraction: 0.8)) {
            toastMessage = nil
        }
    }

    @ViewBuilder
    private func contactAvatar(_ contact: ContactRow) -> some View {
        if let imageData = contact.thumbnailData, let uiImage = UIImage(data: imageData) {
            Image(uiImage: uiImage)
                .resizable()
                .scaledToFill()
        } else {
            Circle()
                .fill(Color(.tertiarySystemFill))
                .overlay(
                    Text(contact.name.prefix(1).uppercased())
                        .font(.system(size: 16, weight: .semibold, design: .rounded))
                        .foregroundStyle(.secondary)
                )
        }
    }
}

// MARK: - Contacts ViewModel

struct ContactRow: Identifiable {
    let id: String
    let name: String
    let phone: String
    let thumbnailData: Data?
}

@MainActor
final class ContactsViewModel: ObservableObject {
    @Published var contacts: [ContactRow] = []
    @Published var isLoading: Bool = false
    @Published var errorMessage: String? = nil

    /// Preload contacts silently if permission is already granted.
    /// Does NOT prompt the user for permission — safe to call on app launch.
    func preloadIfAuthorized() async {
        let status = CNContactStore.authorizationStatus(for: .contacts)
        guard status == .authorized else { return }
        await fetchContacts()
    }

    func load() async {
        isLoading = true
        errorMessage = nil

        let store = CNContactStore()
        let granted: Bool
        do {
            granted = try await store.requestAccess(for: .contacts)
        } catch {
            isLoading = false
            errorMessage = "Couldn't access contacts."
            return
        }

        guard granted else {
            isLoading = false
            errorMessage = "Contacts permission is off. Enable it in Settings to invite people."
            return
        }

        await fetchContacts()
    }

    private func fetchContacts() async {
        isLoading = true
        errorMessage = nil

        do {
            let result = try await Task.detached(priority: .userInitiated) { () throws -> [ContactRow] in
                let store = CNContactStore()
                let keys: [CNKeyDescriptor] = [
                    CNContactGivenNameKey as CNKeyDescriptor,
                    CNContactFamilyNameKey as CNKeyDescriptor,
                    CNContactPhoneNumbersKey as CNKeyDescriptor,
                    CNContactThumbnailImageDataKey as CNKeyDescriptor
                ]

                let request = CNContactFetchRequest(keysToFetch: keys)
                request.unifyResults = true
                request.sortOrder = .userDefault

                var rows: [ContactRow] = []
                try store.enumerateContacts(with: request) { contact, _ in
                    let name = [contact.givenName, contact.familyName]
                        .joined(separator: " ")
                        .trimmingCharacters(in: .whitespacesAndNewlines)
                    let displayName = name.isEmpty ? "Unknown" : name

                    let phone = contact.phoneNumbers.first?.value.stringValue
                        .trimmingCharacters(in: .whitespacesAndNewlines) ?? ""

                    // Include all contacts, even those without phone numbers
                    rows.append(ContactRow(
                        id: contact.identifier,
                        name: displayName,
                        phone: phone,
                        thumbnailData: contact.thumbnailImageData
                    ))
                }
                return rows
            }.value

            contacts = result
            isLoading = false
        } catch {
            isLoading = false
            errorMessage = "Failed to load contacts."
        }
    }
}

// MARK: - Share Sheet

struct ShareSheet: UIViewControllerRepresentable {
    let activityItems: [Any]

    func makeUIViewController(context: Context) -> UIActivityViewController {
        UIActivityViewController(activityItems: activityItems, applicationActivities: nil)
    }

    func updateUIViewController(_ uiViewController: UIActivityViewController, context: Context) {}
}
