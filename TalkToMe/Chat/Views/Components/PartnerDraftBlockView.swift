import SwiftUI

struct PartnerDraftBlockView: View {

    enum Action { case send(String) }

    @EnvironmentObject private var friendsViewModel: FriendsViewModel
    @AppStorage(PreferenceKeys.elevenLabsVoiceName) private var currentGlobalBuddyName: String = ""

    @State private var text: String
    @State private var isConfirmingNormalSend: Bool = false
    @State private var showSentLocally: Bool = false
    @State private var showFriendMenu: Bool = false
    @State private var isConfirmingUnsend: Bool = false
    @State private var isUnsending: Bool = false

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

    /// Use the persisted ghost name if available, otherwise fall back to the current global setting.
    private var effectiveBuddyName: String {
        let persisted = ghostName?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        if !persisted.isEmpty { return persisted }
        return currentGlobalBuddyName
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
        VStack(alignment: .leading, spacing: 12) {
            // Buddy header
            HStack(spacing: 8) {
                if let uiImage = buddyImage {
                    Image(uiImage: uiImage)
                        .resizable()
                        .scaledToFill()
                        .frame(width: 28, height: 28)
                        .clipShape(Circle())
                } else {
                    Circle()
                        .fill(Color(.systemGray5))
                        .frame(width: 28, height: 28)
                        .overlay(
                            Image(systemName: "sparkles")
                                .font(.system(size: 12, weight: .semibold))
                                .foregroundColor(.secondary)
                        )
                }

                HStack(spacing: 4) {
                    Text(resolvedBuddyName)
                        .font(.system(size: 14, weight: .semibold))
                        .foregroundColor(.primary)

                    Text("drafted this")
                        .font(.system(size: 14, weight: .regular))
                        .foregroundColor(.secondary)
                }

                Spacer()

                Image(systemName: "sparkles")
                    .font(.system(size: 12, weight: .medium))
                    .foregroundColor(.secondary.opacity(0.6))
            }

            // Message body
            Text(text.isEmpty ? " " : text)
                .font(.callout)
                .foregroundColor(.primary)
                .multilineTextAlignment(.leading)
                .lineSpacing(2)
                .frame(maxWidth: .infinity, alignment: .leading)
                .textSelection(.enabled)
                .padding(.horizontal, 4)

            // Send / unsend button row
            HStack {
                if isConfirmingNormalSend || isConfirmingUnsend {
                    Button(action: {
                        Haptics.impact(.light)
                        withAnimation(.spring(response: 0.22, dampingFraction: 0.9)) {
                            isConfirmingNormalSend = false
                            isConfirmingUnsend = false
                        }
                    }) {
                        Text("Cancel")
                            .font(.system(size: 14, weight: .medium))
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
                .disabled(isUnsending)
            }

            // Inline friend picker menu
            if showFriendMenu {
                friendPickerMenu
                    .transition(.opacity.combined(with: .move(edge: .bottom)))
            }
        }
        .padding(14)
        .background(
            RoundedRectangle(cornerRadius: 16, style: .continuous)
                .fill(.ultraThinMaterial)
        )
        .onAppear {
            isConfirmingNormalSend = false
            showSentLocally = false
            showFriendMenu = false
            isConfirmingUnsend = false
            isUnsending = false
        }
        .onChange(of: initialText) { _, newValue in
            self.text = newValue
            isConfirmingNormalSend = false
            showSentLocally = false
            showFriendMenu = false
            isConfirmingUnsend = false
            isUnsending = false
        }
        .onChange(of: isSent) { _, _ in
            isConfirmingNormalSend = false
            isConfirmingUnsend = false
            isUnsending = false
            if isSent { showSentLocally = false }
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
    }

    @ViewBuilder
    private var sendButtonContent: some View {
        ZStack {
            if isUnsending {
                HStack(spacing: 6) {
                    ProgressView()
                        .controlSize(.small)
                        .tint(.white)
                    Text("Unsending")
                        .font(.system(size: 14, weight: .semibold))
                }
                .foregroundColor(.white)
                .padding(.horizontal, 16)
                .padding(.vertical, 9)
                .background(
                    Capsule(style: .continuous)
                        .fill(Color.red.opacity(0.7))
                )
                .transition(.scale.combined(with: .opacity))
            } else if isConfirmingUnsend {
                HStack(spacing: 6) {
                    Image(systemName: "arrow.uturn.backward")
                        .font(.system(size: 13, weight: .semibold))
                    Text("Unsend?")
                        .font(.system(size: 14, weight: .semibold))
                }
                .foregroundColor(.white)
                .padding(.horizontal, 16)
                .padding(.vertical, 9)
                .background(
                    Capsule(style: .continuous)
                        .fill(Color.red.opacity(0.85))
                )
                .transition(.scale.combined(with: .opacity))
            } else if alreadySent {
                HStack(spacing: 6) {
                    Image(systemName: "checkmark")
                        .font(.system(size: 13, weight: .semibold))
                    Text("Sent")
                        .font(.system(size: 14, weight: .semibold))
                }
                .foregroundColor(.white)
                .padding(.horizontal, 16)
                .padding(.vertical, 9)
                .background(
                    Capsule(style: .continuous)
                        .fill(Color.green)
                )
                .transition(.scale.combined(with: .opacity))
            } else if isConfirmingNormalSend {
                HStack(spacing: 6) {
                    Image(systemName: "checkmark")
                        .font(.system(size: 13, weight: .semibold))
                    Text("Confirm")
                        .font(.system(size: 14, weight: .semibold))
                }
                .foregroundColor(.white)
                .padding(.horizontal, 16)
                .padding(.vertical, 9)
                .background(
                    Capsule(style: .continuous)
                        .fill(Color.accentColor.opacity(0.85))
                )
                .transition(.scale.combined(with: .opacity))
            } else {
                HStack(spacing: 6) {
                    Text(isLinked ? "Send to \(recipientFirstName)" : "Send to Friend")
                        .font(.system(size: 14, weight: .semibold))
                    Image(systemName: isLinked ? "arrow.up.right" : (showFriendMenu ? "chevron.down" : "chevron.up"))
                        .font(.system(size: 12, weight: .semibold))
                }
                .foregroundColor(.white)
                .padding(.horizontal, 16)
                .padding(.vertical, 9)
                .background(
                    Capsule(style: .continuous)
                        .fill(Color.accentColor)
                )
                .transition(.scale.combined(with: .opacity))
            }
        }
    }

    @ViewBuilder
    private var friendPickerMenu: some View {
        VStack(alignment: .leading, spacing: 0) {
            if !friendsViewModel.friends.isEmpty {
                Divider()
                    .padding(.bottom, 4)

                ForEach(friendsViewModel.friends) { friend in
                    Button {
                        handleFriendPicked(friend)
                    } label: {
                        HStack(spacing: 10) {
                            SidebarAvatarView(avatarURL: friend.avatarURL)
                                .frame(width: 30, height: 30)
                                .clipShape(Circle())

                            Text(firstName(of: friend))
                                .font(.system(size: 15, weight: .medium))
                                .foregroundColor(.primary)

                            Spacer()

                            Image(systemName: "arrow.up.right")
                                .font(.system(size: 11, weight: .semibold))
                                .foregroundColor(.secondary)
                        }
                        .padding(.vertical, 8)
                        .contentShape(Rectangle())
                    }
                    .buttonStyle(.plain)
                }
            }

            Divider()
                .padding(.vertical, 4)

            Button {
                Haptics.impact(.light)
                withAnimation(.spring(response: 0.25)) {
                    showFriendMenu = false
                }
                NotificationCenter.default.post(name: .openAddFriendSheet, object: nil)
            } label: {
                HStack(spacing: 10) {
                    Image(systemName: "person.badge.plus")
                        .font(.system(size: 14, weight: .medium))
                        .foregroundColor(.accentColor)
                        .frame(width: 30, height: 30)

                    Text("Add a friend")
                        .font(.system(size: 15, weight: .medium))
                        .foregroundColor(.accentColor)

                    Spacer()
                }
                .padding(.vertical, 6)
                .contentShape(Rectangle())
            }
            .buttonStyle(.plain)
        }
        .padding(.top, 4)
    }

    private func handleButtonTap() {
        let trimmed = text.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return }

        Haptics.impact(.light)

        if isConfirmingUnsend {
            // Execute unsend
            withAnimation(.spring(response: 0.22, dampingFraction: 0.9)) {
                isConfirmingUnsend = false
                isUnsending = true
            }
            NotificationCenter.default.post(
                name: .unsendPartnerMessageFromBubble,
                object: nil,
                userInfo: ["content": trimmed]
            )
        } else if alreadySent {
            // Show unsend confirmation
            withAnimation(.spring(response: 0.22, dampingFraction: 0.9)) {
                isConfirmingUnsend = true
            }
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
            withAnimation(.spring(response: 0.25)) {
                showFriendMenu.toggle()
            }
            if friendsViewModel.friends.isEmpty {
                Task { try? await friendsViewModel.loadFriends() }
            }
        }
    }

    private func handleFriendPicked(_ friend: FriendSummary) {
        let trimmed = text.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return }

        Haptics.impact(.light)
        withAnimation(.spring(response: 0.22, dampingFraction: 0.9)) {
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

#Preview {
    PartnerDraftBlockView(
        initialText: "Hey love — I've been feeling a bit overwhelmed lately and could use a little extra help this week.",
        isSent: false,
        recipientUserId: nil
    ) { _ in }
        .padding()
}
