import SwiftUI

struct PartnerDraftBlockView: View {

    enum Action { case send(String) }

    @State private var text: String
    @State private var measuredTextHeight: CGFloat = 0
    @State private var isSending: Bool = false
    @State private var isConfirmingNormalSend: Bool = false
    @State private var showSentLocally: Bool = false

    let initialText: String
    let isSent: Bool
    let isLinked: Bool
    let onAction: (Action) -> Void

    init(initialText: String, isSent: Bool = false, isLinked: Bool = true, onAction: @escaping (Action) -> Void) {
        self.initialText = initialText
        self.isSent = isSent
        self.isLinked = isLinked
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

            ZStack(alignment: .topLeading) {
                TextEditor(text: $text)
                    .background(Color.clear)
                    .scrollContentBackground(.hidden)
                    .font(.callout)
                    .foregroundColor(.primary)
                    .disabled(true)
                    .frame(height: max(40, measuredTextHeight))

                Text(text.isEmpty ? " " : text)
                    .font(.callout)
                    .foregroundColor(.clear)
                    .padding(.vertical, 8)
                    .padding(.horizontal, 4)
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .background(HeightReader(height: $measuredTextHeight))
                    .allowsHitTesting(false)
            }

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
                        guard !isSent && !isSending else { return }
                        let trimmed = text.trimmingCharacters(in: .whitespacesAndNewlines)
                        guard !trimmed.isEmpty else { return }

                        Haptics.impact(.light)

                        if isConfirmingNormalSend {
                            if isLinked {
                                isSending = true
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
                                    Text("Sent")
                                        .font(.subheadline)
                                        .foregroundColor(Color.secondary)
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
                    .disabled((isLinked && (isSent || isSending)))
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
            if self.text.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                self.text = self.initialText
            }
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

private struct ViewHeightKey: PreferenceKey {
    static var defaultValue: CGFloat = 0
    static func reduce(value: inout CGFloat, nextValue: () -> CGFloat) {
        value = max(value, nextValue())
    }
}

private struct HeightReader: View {
    @Binding var height: CGFloat

    var body: some View {
        GeometryReader { proxy in
            Color.clear
                .preference(key: ViewHeightKey.self, value: proxy.size.height)
        }
        .onPreferenceChange(ViewHeightKey.self) { newValue in
            if abs(newValue - height) > 0.5 {
                height = newValue
            }
        }
    }
}

#Preview {
    PartnerDraftBlockView(initialText: "Hey love — I've been feeling a bit overwhelmed lately and could use a little extra help this week.", isSent: false) { _ in }
        .padding()
}


