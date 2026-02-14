import SwiftUI

struct PartnerDraftBlockView: View {

    enum Action { case send(String) }

    @EnvironmentObject private var friendsViewModel: FriendsViewModel
    @AppStorage(PreferenceKeys.elevenLabsVoiceName) private var currentBuddyName: String = ""

    @State private var text: String
    @State private var isConfirmingNormalSend: Bool = false
    @State private var showSentLocally: Bool = false

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
        isLinked && (isSent || showSentLocally)
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

            // Send button
            HStack {
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

                Spacer()

                Button(action: handleSendTap) {
                    sendButtonContent
                }
                .buttonStyle(.plain)
                .disabled(alreadySent)
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
        }
        .onChange(of: initialText) { _, newValue in
            self.text = newValue
            isConfirmingNormalSend = false
            showSentLocally = false
        }
        .onChange(of: isSent) { _, _ in
            isConfirmingNormalSend = false
            if isSent { showSentLocally = false }
        }
    }

    @ViewBuilder
    private var sendButtonContent: some View {
        ZStack {
            if alreadySent {
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
                    Text("Send to \(recipientFirstName)")
                        .font(.system(size: 14, weight: .semibold))
                    Image(systemName: "arrow.up.right")
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

    private func handleSendTap() {
        guard !alreadySent else { return }
        let trimmed = text.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return }

        Haptics.impact(.light)

        if isConfirmingNormalSend {
            withAnimation(.spring(response: 0.22, dampingFraction: 0.9)) {
                if isLinked {
                    showSentLocally = true
                }
                isConfirmingNormalSend = false
            }
            onAction(.send(trimmed))
        } else {
            withAnimation(.spring(response: 0.22, dampingFraction: 0.9)) {
                isConfirmingNormalSend = true
            }
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
