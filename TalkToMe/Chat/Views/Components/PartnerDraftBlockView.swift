import SwiftUI

struct PartnerDraftBlockView: View {

    enum Action { case send(String) }

    @EnvironmentObject private var friendsViewModel: FriendsViewModel
    @AppStorage(PreferenceKeys.elevenLabsVoiceName) private var currentBuddyName: String = ""
    @AppStorage(PreferenceKeys.fontSizePreference) private var fontSizeScale: Double = 1.0

    @State private var text: String
    @State private var isConfirmingNormalSend: Bool = false
    @State private var showSentLocally: Bool = false
    @State private var showFriendMenu: Bool = false
    @State private var showFullFriendSheet: Bool = false
    @State private var selectedFriend: FriendSummary? = nil
    @State private var isUnsending: Bool = false
    @State private var chevronNudge: Bool = false

    let initialText: String
    let isSent: Bool
    let isLinked: Bool
    let recipientUserId: UUID?
    let ghostName: String?
    let onAction: (Action) -> Void

    init(
        initialText: String,
        isSent: Bool = false,
        isLinked: Bool = true,
        recipientUserId: UUID?,
        ghostName: String? = nil,
        onAction: @escaping (Action) -> Void
    ) {
        self.initialText = initialText
        self.isSent = isSent
        self.isLinked = isLinked
        self.recipientUserId = recipientUserId
        self.ghostName = ghostName
        self._text = State(initialValue: initialText)
        self.onAction = onAction
    }

    /// Use the persisted ghost name if available, otherwise fall back to the currently selected buddy.
    private var effectiveBuddyName: String {
        if let gn = ghostName, !gn.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            return gn
        }
        return currentBuddyName
    }

    private var resolvedBuddyName: String {
        let name = effectiveBuddyName.trimmingCharacters(in: .whitespacesAndNewlines)
        return name.isEmpty ? "Your buddy" : name
    }

    private var buddyImage: UIImage? {
        ElevenLabsVoiceSuggestionsView.ghostUIImage(for: effectiveBuddyName)
    }

    private var recipientFirstName: String {
        guard let recipientUserId else { return "Friend" }
        let friend = friendsViewModel.friends.first(where: { $0.id == recipientUserId })
        let fullName = (friend?.fullName ?? "Friend").trimmingCharacters(in: .whitespacesAndNewlines)
        return fullName.split(separator: " ").first.map(String.init) ?? "Friend"
    }

    private var alreadySent: Bool {
        isSent || showSentLocally
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            // Buddy header
            HStack(spacing: 5) {
                Group {
                    if let uiImage = buddyImage {
                        Image(uiImage: uiImage)
                            .resizable()
                            .scaledToFill()
                            .frame(width: 26, height: 26)
                            .clipShape(Circle())
                    } else {
                        Circle()
                            .fill(Color(.systemGray5))
                            .frame(width: 26, height: 26)
                    }
                }
                .offset(y: -1)

                HStack(spacing: 4) {
                    Text(resolvedBuddyName)
                        .font(.system(size: 13, weight: .semibold))
                        .foregroundStyle(.primary)

                    Text("drafted this")
                        .font(.system(size: 13, weight: .medium))
                        .foregroundStyle(.secondary)
                }

                Spacer()
            }
            .padding(.bottom, 10)

            // Message body
            Text(text.isEmpty ? " " : text)
                .font(.system(size: 16.5 * fontSizeScale))
                .foregroundColor(.primary)
                .multilineTextAlignment(.leading)
                .lineSpacing(3)
                .frame(maxWidth: .infinity, alignment: .leading)
                .textSelection(.enabled)
                .padding(.bottom, 14)

            // Send / unsend button row
            HStack(spacing: 10) {
                if isConfirmingNormalSend {
                    Button(action: {
                        Haptics.impact(.light)
                        withAnimation(.spring(response: 0.22, dampingFraction: 0.9)) {
                            isConfirmingNormalSend = false
                        }
                    }) {
                        Text("Cancel")
                            .font(.system(size: 14, weight: .medium))
                            .foregroundColor(.secondary)
                    }
                    .buttonStyle(.plain)
                    .transition(.scale.combined(with: .opacity))
                }

                if alreadySent && !isUnsending {
                    Button {
                        Haptics.impact(.light)
                        handleUnsend()
                    } label: {
                        HStack(spacing: 4) {
                            Image(systemName: "arrow.uturn.backward")
                                .font(.system(size: 11, weight: .semibold))
                            Text("Unsend")
                                .font(.system(size: 13, weight: .medium))
                        }
                        .foregroundColor(.secondary)
                    }
                    .buttonStyle(.plain)
                    .transition(.scale.combined(with: .opacity))
                }

                Spacer()

                Button(action: handleButtonTap) {
                    sendButtonContent
                }
                .buttonStyle(.plain)
                .disabled(isUnsending || alreadySent)
            }

            // Inline friend picker menu
            if showFriendMenu {
                friendAvatarPicker
                    .transition(.opacity)
            }
        }
        .padding(16)
        .background(
            RoundedRectangle(cornerRadius: 20, style: .continuous)
                .fill(.ultraThinMaterial)
        )
        .onAppear {
            isConfirmingNormalSend = false
            showSentLocally = false
            showFriendMenu = false
            showFullFriendSheet = false
            selectedFriend = nil
            isUnsending = false
        }
        .onChange(of: initialText) { _, newValue in
            self.text = newValue
            isConfirmingNormalSend = false
            showSentLocally = false
            showFriendMenu = false
            showFullFriendSheet = false
            selectedFriend = nil
            isUnsending = false
        }
        .onChange(of: isSent) { _, _ in
            withAnimation(.spring(response: 0.22, dampingFraction: 0.9)) {
                isConfirmingNormalSend = false
                showSentLocally = false
                // Don't reset isUnsending here — let the result notification handle it
            }
        }
        .onReceive(NotificationCenter.default.publisher(for: .unsendPartnerMessageResult)) { note in
            let content = (note.userInfo?["content"] as? String) ?? ""
            let trimmed = text.trimmingCharacters(in: .whitespacesAndNewlines)
            guard content == trimmed else { return }
            let success = (note.userInfo?["success"] as? Bool) ?? false
            withAnimation(.spring(response: 0.22, dampingFraction: 0.9)) {
                isUnsending = false
                if success {
                    showSentLocally = false
                }
            }
        }
        .sheet(isPresented: $showFullFriendSheet) {
            SendToFriendSheetView(
                onPick: { friend in
                    showFullFriendSheet = false
                    handleFriendPicked(friend)
                },
                onDismiss: {
                    showFullFriendSheet = false
                }
            )
            .environmentObject(friendsViewModel)
            .presentationDetents([.medium, .large])
            .presentationDragIndicator(.visible)
        }
        .task {
            if friendsViewModel.friends.isEmpty {
                try? await friendsViewModel.loadFriends()
            }
        }
    }

    // MARK: - Inline Friend Avatar Picker

    @ViewBuilder
    private var friendAvatarPicker: some View {
        VStack(spacing: 0) {
            Rectangle()
                .fill(Color(.separator).opacity(0.3))
                .frame(height: 0.5)
                .padding(.top, 14)
                .padding(.bottom, 16)

            HStack(spacing: 20) {
                ForEach(friendsViewModel.friends.prefix(3)) { friend in
                    let isSelected = selectedFriend?.id == friend.id
                    Button {
                        Haptics.impact(.light)
                        withAnimation(.spring(response: 0.22, dampingFraction: 0.85)) {
                            selectedFriend = isSelected ? nil : friend
                        }
                    } label: {
                        VStack(spacing: 6) {
                            ZStack {
                                SidebarAvatarView(avatarURL: friend.avatarURL)
                                    .frame(width: 44, height: 44)
                                    .clipShape(Circle())
                                    .overlay(
                                        Circle()
                                            .strokeBorder(
                                                isSelected ? Color.accentColor : Color.white.opacity(0.12),
                                                lineWidth: isSelected ? 2 : 1
                                            )
                                    )

                                if isSelected {
                                    Circle()
                                        .fill(Color.accentColor)
                                        .frame(width: 20, height: 20)
                                        .overlay(
                                            Image(systemName: "checkmark")
                                                .font(.system(size: 10, weight: .bold))
                                                .foregroundColor(.white)
                                        )
                                        .offset(x: 16, y: -16)
                                        .transition(.scale.combined(with: .opacity))
                                }
                            }

                            Text(firstName(of: friend))
                                .font(.system(size: 11, weight: isSelected ? .semibold : .medium))
                                .foregroundStyle(isSelected ? .primary : .secondary)
                                .lineLimit(1)
                        }
                        .contentShape(Rectangle())
                    }
                    .buttonStyle(.plain)
                }

                Spacer()

                Button {
                    Haptics.impact(.light)
                    withAnimation(.easeInOut(duration: 0.2)) {
                        showFriendMenu = false
                        selectedFriend = nil
                    }
                    showFullFriendSheet = true
                } label: {
                    VStack(spacing: 6) {
                        Circle()
                            .fill(Color(.systemGray5))
                            .frame(width: 44, height: 44)
                            .overlay(
                                Image(systemName: "ellipsis")
                                    .font(.system(size: 15, weight: .bold))
                                    .foregroundStyle(.secondary)
                            )

                        Text("More")
                            .font(.system(size: 11, weight: .medium))
                            .foregroundStyle(.secondary)
                    }
                    .contentShape(Rectangle())
                }
                .buttonStyle(.plain)
            }

        }
        .clipped()
    }

    // MARK: - Send Button Content

    /// Unique key for the current button state so SwiftUI can identity-transition between them.
    private var sendButtonStateId: String {
        if isUnsending { return "unsending" }
        if alreadySent { return "sent" }
        if isConfirmingNormalSend { return "confirm" }
        if selectedFriend != nil { return "friend" }
        if isLinked { return "linked" }
        return "default"
    }

    @ViewBuilder
    private var sendButtonContent: some View {
        ZStack {
            if isUnsending {
                sendCapsule(label: "Unsending", icon: nil, color: .red.opacity(0.7), showSpinner: true)
                    .transition(.opacity)
            } else if alreadySent {
                sendCapsule(label: "Sent", icon: "checkmark", color: .green)
                    .transition(.opacity)
            } else if isConfirmingNormalSend {
                sendCapsule(label: "Confirm", icon: "checkmark", color: .accentColor.opacity(0.85))
                    .transition(.opacity)
            } else if let friend = selectedFriend {
                sendCapsule(label: "Send to \(firstName(of: friend))", icon: "arrow.up.right", color: .accentColor)
                    .transition(.opacity)
            } else if isLinked {
                sendCapsule(label: "Send to \(recipientFirstName)", icon: "arrow.up.right", color: .accentColor)
                    .transition(.opacity)
            } else {
                HStack(spacing: 5) {
                    Text("Send")
                        .font(.system(size: 14, weight: .semibold))
                    Image(systemName: "chevron.right")
                        .font(.system(size: 11, weight: .bold))
                        .offset(x: chevronNudge ? 3 : 0)
                        .animation(.spring(response: 0.2, dampingFraction: 0.5), value: chevronNudge)
                }
                .foregroundColor(.white)
                .padding(.horizontal, 18)
                .padding(.vertical, 10)
                .background(
                    Capsule(style: .continuous)
                        .fill(Color.accentColor)
                )
                .transition(.opacity)
            }
        }
        .animation(.easeInOut(duration: 0.18), value: sendButtonStateId)
    }

    private func sendCapsule(label: String, icon: String?, color: Color, showSpinner: Bool = false) -> some View {
        HStack(spacing: 5) {
            if showSpinner {
                ProgressView()
                    .controlSize(.small)
                    .tint(.white)
            }
            Text(label)
                .font(.system(size: 14, weight: .semibold))
            if let icon {
                Image(systemName: icon)
                    .font(.system(size: 11, weight: .bold))
            }
        }
        .foregroundColor(.white)
        .padding(.horizontal, 18)
        .padding(.vertical, 10)
        .background(
            Capsule(style: .continuous)
                .fill(color)
        )
    }

    // MARK: - Actions

    private func handleButtonTap() {
        let trimmed = text.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return }

        Haptics.impact(.light)

        if let friend = selectedFriend {
            // Friend is selected in the inline picker — send to them
            confirmSendToFriend(friend)
        } else if isLinked {
            if isConfirmingNormalSend {
                withAnimation(.spring(response: 0.22, dampingFraction: 0.9)) {
                    showSentLocally = true
                    isConfirmingNormalSend = false
                }
                onAction(.send(trimmed))
            } else {
                withAnimation(.spring(response: 0.22, dampingFraction: 0.9)) {
                    isConfirmingNormalSend = true
                }
            }
        } else {
            // Nudge chevron on tap
            chevronNudge = true
            DispatchQueue.main.asyncAfter(deadline: .now() + 0.15) {
                chevronNudge = false
            }
            withAnimation(.easeInOut(duration: 0.2)) {
                showFriendMenu.toggle()
            }
            if friendsViewModel.friends.isEmpty {
                Task { try? await friendsViewModel.loadFriends() }
            }
        }
    }

    private func handleUnsend() {
        let trimmed = text.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return }

        withAnimation(.spring(response: 0.22, dampingFraction: 0.9)) {
            isUnsending = true
        }
        NotificationCenter.default.post(
            name: .unsendPartnerMessageFromBubble,
            object: nil,
            userInfo: ["content": trimmed]
        )
    }

    private func handleFriendPicked(_ friend: FriendSummary) {
        // From the sheet — select and confirm in one step
        Haptics.impact(.light)
        withAnimation(.spring(response: 0.22, dampingFraction: 0.9)) {
            showFriendMenu = false
            showFullFriendSheet = false
            selectedFriend = friend
        }
        // Brief delay so user sees the selection, then confirm
        DispatchQueue.main.asyncAfter(deadline: .now() + 0.15) {
            confirmSendToFriend(friend)
        }
    }

    private func confirmSendToFriend(_ friend: FriendSummary) {
        let trimmed = text.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return }

        Haptics.impact(.medium)
        withAnimation(.spring(response: 0.22, dampingFraction: 0.9)) {
            selectedFriend = nil
            showFriendMenu = false
            showSentLocally = true
        }

        NotificationCenter.default.post(
            name: .sendPartnerMessageFromBubble,
            object: nil,
            userInfo: ["content": trimmed, "friend_user_id": friend.id]
        )
    }

    private func firstName(of friend: FriendSummary) -> String {
        let full = friend.fullName.trimmingCharacters(in: .whitespacesAndNewlines)
        return full.split(separator: " ").first.map(String.init) ?? "Friend"
    }
}

// MARK: - Send to Friend Sheet

private struct SendToFriendSheetView: View {
    @EnvironmentObject private var friendsViewModel: FriendsViewModel
    let onPick: (FriendSummary) -> Void
    let onDismiss: () -> Void

    @State private var showAddFriend: Bool = false

    var body: some View {
        NavigationStack {
            Group {
                if friendsViewModel.friends.isEmpty && friendsViewModel.isLoadingFriends {
                    VStack(spacing: 10) {
                        ProgressView()
                        Text("Loading friends…")
                            .font(.system(size: 14))
                            .foregroundStyle(.secondary)
                    }
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
                } else {
                    List {
                        ForEach(friendsViewModel.friends) { friend in
                            Button {
                                onPick(friend)
                            } label: {
                                HStack(spacing: 12) {
                                    SidebarAvatarView(avatarURL: friend.avatarURL)
                                        .frame(width: 34, height: 34)
                                        .clipShape(Circle())

                                    Text(friend.fullName.isEmpty ? "Friend" : friend.fullName)
                                        .font(.system(size: 16, weight: .semibold))
                                        .foregroundStyle(.primary)

                                    Spacer()

                                    Image(systemName: "arrow.up.right")
                                        .font(.system(size: 11, weight: .semibold))
                                        .foregroundStyle(.secondary)
                                }
                                .padding(.vertical, 4)
                            }
                            .buttonStyle(.plain)
                        }
                    }
                    .listStyle(.plain)
                }
            }
            .navigationTitle("Send to…")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .topBarLeading) {
                    Button {
                        onDismiss()
                    } label: {
                        Image(systemName: "xmark")
                            .font(.system(size: 14, weight: .semibold))
                            .foregroundStyle(.secondary)
                    }
                }
                ToolbarItem(placement: .topBarTrailing) {
                    Button {
                        showAddFriend = true
                    } label: {
                        HStack(spacing: 4) {
                            Image(systemName: "person.badge.plus")
                                .font(.system(size: 13, weight: .medium))
                            Text("Add")
                                .font(.system(size: 14, weight: .semibold))
                        }
                        .foregroundColor(.accentColor)
                    }
                }
            }
            .navigationDestination(isPresented: $showAddFriend) {
                AddFriendInlineView()
                    .environmentObject(friendsViewModel)
            }
        }
        .task {
            if friendsViewModel.friends.isEmpty {
                try? await friendsViewModel.loadFriends()
            }
        }
    }
}

// MARK: - Add Friend View (inside sheet navigation)

private struct AddFriendInlineView: View {
    @EnvironmentObject private var friendsViewModel: FriendsViewModel
    @State private var codeToAdd: String = ""

    private var cleanedCodeToAdd: String {
        codeToAdd.trimmingCharacters(in: .whitespacesAndNewlines)
    }
    private var isCodeComplete: Bool { cleanedCodeToAdd.count == 4 }

    var body: some View {
        VStack(spacing: 24) {
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
            .background(Color(.secondarySystemBackground))
            .clipShape(RoundedRectangle(cornerRadius: 16, style: .continuous))

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
            .background(Color(.secondarySystemBackground))
            .clipShape(RoundedRectangle(cornerRadius: 16, style: .continuous))

            if let msg = friendsViewModel.lastActionMessage, !msg.isEmpty {
                Text(msg)
                    .font(.system(size: 13, weight: .medium))
                    .foregroundStyle(.secondary)
                    .multilineTextAlignment(.center)
            }

            Spacer()
        }
        .padding(.horizontal, 20)
        .padding(.top, 20)
        .navigationTitle("Add a Friend")
        .navigationBarTitleDisplayMode(.inline)
        .task {
            await friendsViewModel.refreshMyCode()
        }
    }
}

#Preview {
    PartnerDraftBlockView(
        initialText: "Hey love — I've been feeling a bit overwhelmed lately and could use a little extra help this week.",
        isSent: false,
        recipientUserId: nil
    ) { _ in }
        .padding()
}
