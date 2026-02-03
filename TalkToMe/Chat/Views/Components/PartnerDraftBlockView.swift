import SwiftUI

struct PartnerDraftBlockView: View {

    enum Action { case send(String) }

    @EnvironmentObject private var friendsViewModel: FriendsViewModel

    @State private var text: String
    @State private var isConfirmingNormalSend: Bool = false
    @State private var showSentLocally: Bool = false

    let initialText: String
    let isSent: Bool
    let isLinked: Bool
    let recipientUserId: UUID?
    let onAction: (Action) -> Void

    init(
        initialText: String,
        isSent: Bool = false,
        isLinked: Bool = true,
        recipientUserId: UUID?,
        onAction: @escaping (Action) -> Void
    ) {
        self.initialText = initialText
        self.isSent = isSent
        self.isLinked = isLinked
        self.recipientUserId = recipientUserId
        self._text = State(initialValue: initialText)
        self.onAction = onAction
    }

    var body: some View {
        VStack(alignment: .leading) {
            HStack {
                Text("Message")
                    .font(.footnote)
                    .foregroundColor(Color.secondary)
                    .offset(y: -4)

                Spacer()

                MessageActionsView(text: text)
                    .offset(y: -4)
            }

            Divider()
                .padding(.horizontal, -12)
                .offset(y: -4)

            // NOTE:
            // We used to measure the content height via GeometryReader + PreferenceKey and then
            // drive a TextEditor frame from that measurement. That pattern can create SwiftUI
            // AttributeGraph cycles (layout -> preference -> state write -> layout ...).
            //
            // This is a read-only block, so a plain Text view (with selection enabled) is simpler,
            // faster, and avoids any layout feedback loops.
            Text(text.isEmpty ? " " : text)
                .font(.callout)
                .foregroundColor(.primary)
                .multilineTextAlignment(.leading)
                .frame(maxWidth: .infinity, alignment: .leading)
                .textSelection(.enabled)
                .padding(.vertical, 8)
                .padding(.horizontal, 4)

            HStack {
                if isConfirmingNormalSend {
                    Button(action: {
                        Haptics.impact(.light)
                        withAnimation(.spring(response: 0.22, dampingFraction: 0.9)) {
                            isConfirmingNormalSend = false
                        }
                    }) {
                        Text("Cancel")
                            .font(.subheadline)
                            .foregroundColor(Color.secondary)
                    }
                    .buttonStyle(.plain)
                    .transition(.scale.combined(with: .opacity))
                }

                Spacer()

                HStack(spacing: 8) {
                    Button(action: {
                        guard !(isLinked && (isSent || showSentLocally)) else { return }
                        let trimmed = text.trimmingCharacters(in: .whitespacesAndNewlines)
                        guard !trimmed.isEmpty else { return }

                        Haptics.impact(.light)

                        if isConfirmingNormalSend {
                            if isLinked {
                                withAnimation(.spring(response: 0.22, dampingFraction: 0.9)) {
                                    showSentLocally = true
                                    isConfirmingNormalSend = false
                                }
                                onAction(.send(trimmed))
                            } else {
                                withAnimation(.spring(response: 0.22, dampingFraction: 0.9)) {
                                    isConfirmingNormalSend = false
                                }
                                onAction(.send(trimmed))
                            }
                        } else {
                            withAnimation(.spring(response: 0.22, dampingFraction: 0.9)) {
                                isConfirmingNormalSend = true
                            }
                        }
                    }) {
                        ZStack {
                            if isLinked && (isSent || showSentLocally) {
                                HStack(spacing: 6) {
                                    let resolvedFriend: FriendSummary? = {
                                        guard let recipientUserId else { return nil }
                                        return friendsViewModel.friends.first(where: { $0.id == recipientUserId })
                                    }()
                                    let fullName = (resolvedFriend?.fullName ?? "Friend").trimmingCharacters(in: .whitespacesAndNewlines)
                                    let firstName = fullName.split(separator: " ").first.map(String.init) ?? "Friend"
                                    Text("Sent to \(firstName)")
                                        .font(.subheadline)
                                        .foregroundColor(Color.secondary)
                                        .lineLimit(1)
                                        .truncationMode(.tail)
                                    Image(systemName: "checkmark.circle.fill")
                                        .foregroundColor(Color.green)
                                }
                                .transition(.scale.combined(with: .opacity))
                            } else if isConfirmingNormalSend {
                                Text("Confirm sending")
                                    .font(.subheadline)
                                    .foregroundColor(Color.accentColor)
                                    .transition(.scale.combined(with: .opacity))
                            } else {
                                HStack(spacing: 6) {
                                    Text("Send now")
                                        .font(.subheadline)
                                        .foregroundColor(Color.accentColor)
                                    Image(systemName: "arrow.turn.up.right")
                                        .foregroundColor(Color.accentColor)
                                }
                                .transition(.scale.combined(with: .opacity))
                            }
                        }
                    }
                    .disabled(isLinked && (isSent || showSentLocally))
                }
            }
        }
        .padding(12)
        .background(
            RoundedRectangle(cornerRadius: 16)
                .strokeBorder(Color(.separator), lineWidth: 1)
                .background(
                    RoundedRectangle(cornerRadius: 16).fill(Color(.systemBackground))
                )
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
}

#Preview {
    PartnerDraftBlockView(
        initialText: "Hey love — I've been feeling a bit overwhelmed lately and could use a little extra help this week.",
        isSent: false,
        recipientUserId: nil
    ) { _ in }
        .padding()
}

