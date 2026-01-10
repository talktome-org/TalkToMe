import SwiftUI

struct MessagesListView: View {

    @ObservedObject var chatViewModel: ChatViewModel

    @State private var isNearBottom: Bool = false

    let messages: [ChatMessage]
    let isInputFocused: Bool
    let isAssistantTyping: Bool
    let initialJumpToken: Int

    // Keep this small: only auto-scroll on focus when the user is basically at the bottom.
    private let nearBottomThreshold: CGFloat = 20

    var body: some View {
        ScrollViewReader { proxy in
            ScrollView {
                VStack(spacing: 18) {
                    ForEach(Array(messages.enumerated()), id: \.element.id) { index, message in
                        MessageBubbleView(chatViewModel: chatViewModel, message: message, onSendToPartner: { text in
                            NotificationCenter.default.post(name: .sendPartnerMessageFromBubble, object: nil, userInfo: ["content": text])
                        })
                            .id(message.id)
                            .padding(.top, index > 0 && (messages[index - 1].isFromUser != message.isFromUser) ? 4 : 0)
                    }
                    if isAssistantTyping {
                        HStack(alignment: .top, spacing: 0) {
                            TypingIndicatorView(showAfter: 0)
                                .padding(.top, -10)
                            Spacer(minLength: 0)
                        }
                        .id("typing-indicator")
                    }
                }
                .padding(.top, 24)
                .padding(.horizontal)
                // Observe the underlying UIScrollView so "near bottom" updates reliably while the user scrolls.
                .background(
                    ScrollViewBottomProximityObserver { scrollView in
                        let visibleBottomY = scrollView.contentOffset.y
                            + scrollView.bounds.height
                            - scrollView.adjustedContentInset.bottom
                        let rawDistance = scrollView.contentSize.height - visibleBottomY
                        let distanceFromBottom = max(0, rawDistance)
                        isNearBottom = distanceFromBottom <= nearBottomThreshold
                    }
                )
            }
            .scrollBounceBehavior(.always)
            .scrollIndicators(.visible)
            .onChange(of: chatViewModel.streamingScrollToken, initial: false) { _, _ in
                // Keep view pinned to the assistant's streaming message as tokens arrive
                let targetId = chatViewModel.assistantScrollTargetId ?? messages.last?.id
                if let targetId = targetId {
                    withAnimation(nil) { proxy.scrollTo(targetId, anchor: .bottom) }
                }
            }
            .onChange(of: chatViewModel.assistantScrollTargetId, initial: false) { _, newId in
                // When a new assistant placeholder appears, jump to it immediately
                if let id = newId {
                    withAnimation(nil) { proxy.scrollTo(id, anchor: .bottom) }
                }
            }
            .onChange(of: initialJumpToken, initial: false) { _, token in
                guard token > 0 else { return }
                guard let lastId = messages.last?.id else { return }
                withAnimation(nil) { proxy.scrollTo(lastId, anchor: .bottom) }
            }
            .onChange(of: isAssistantTyping, initial: false) { _, typing in
                // If typing indicator appears before any tokens, make sure it's visible
                if typing {
                    withAnimation(nil) { proxy.scrollTo("typing-indicator", anchor: .bottom) }
                }
            }
            .onChange(of: isInputFocused, initial: false) { _, focused in
                // Keyboard focus should NOT yank you to the bottom unless you're already near the end.
                guard focused, isNearBottom else { return }

                let targetId: AnyHashable? = {
                    if isAssistantTyping { return "typing-indicator" }
                    return messages.last?.id
                }()

                guard let targetId else { return }

                DispatchQueue.main.asyncAfter(deadline: .now() + 0.08) {
                    withAnimation(.spring(response: 0.4, dampingFraction: 0.94)) {
                        proxy.scrollTo(targetId, anchor: .bottom)
                    }
                }
            }
        }
    }
}

private struct ScrollViewBottomProximityObserver: UIViewRepresentable {
    let onChange: (UIScrollView) -> Void

    func makeUIView(context: Context) -> UIView {
        let view = UIView(frame: .zero)
        view.isUserInteractionEnabled = false
        view.backgroundColor = .clear
        return view
    }

    func updateUIView(_ uiView: UIView, context: Context) {
        // This representable is embedded in the ScrollView *content*, so its superview chain will include
        // the underlying UIScrollView.
        DispatchQueue.main.async {
            guard let scrollView = findScrollView(from: uiView) else { return }
            context.coordinator.attach(to: scrollView, onChange: onChange)
        }
    }

    func makeCoordinator() -> Coordinator {
        Coordinator()
    }

    final class Coordinator {
        private weak var scrollView: UIScrollView?
        private var contentOffsetObs: NSKeyValueObservation?
        private var contentSizeObs: NSKeyValueObservation?
        private var insetObs: NSKeyValueObservation?
        private var boundsObs: NSKeyValueObservation?

        func attach(to scrollView: UIScrollView, onChange: @escaping (UIScrollView) -> Void) {
            guard self.scrollView !== scrollView else { return }
            self.scrollView = scrollView

            contentOffsetObs?.invalidate()
            contentSizeObs?.invalidate()
            insetObs?.invalidate()
            boundsObs?.invalidate()

            contentOffsetObs = scrollView.observe(\.contentOffset, options: [.initial, .new]) { sv, _ in
                onChange(sv)
            }
            contentSizeObs = scrollView.observe(\.contentSize, options: [.initial, .new]) { sv, _ in
                onChange(sv)
            }
            insetObs = scrollView.observe(\.adjustedContentInset, options: [.initial, .new]) { sv, _ in
                onChange(sv)
            }
            boundsObs = scrollView.observe(\.bounds, options: [.initial, .new]) { sv, _ in
                onChange(sv)
            }
        }

        deinit {
            contentOffsetObs?.invalidate()
            contentSizeObs?.invalidate()
            insetObs?.invalidate()
            boundsObs?.invalidate()
        }
    }

    private func findScrollView(from view: UIView) -> UIScrollView? {
        var current: UIView? = view
        while let v = current {
            if let sv = v as? UIScrollView { return sv }
            current = v.superview
        }
        return nil
    }
}