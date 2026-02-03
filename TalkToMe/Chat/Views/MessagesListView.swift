import SwiftUI
import UIKit

struct SendAnimationNamespaceKey: EnvironmentKey {
    static let defaultValue: Namespace.ID? = nil
}

struct OutgoingAnimatingMessageIdKey: EnvironmentKey {
    static let defaultValue: UUID? = nil
}

extension EnvironmentValues {
    var sendAnimationNamespace: Namespace.ID? {
        get { self[SendAnimationNamespaceKey.self] }
        set { self[SendAnimationNamespaceKey.self] = newValue }
    }

    var outgoingAnimatingMessageId: UUID? {
        get { self[OutgoingAnimatingMessageIdKey.self] }
        set { self[OutgoingAnimatingMessageIdKey.self] = newValue }
    }
}

struct MessagesListView: View {

    @ObservedObject var chatViewModel: ChatViewModel

    @Binding var isNearBottom: Bool
    @Binding var followBottom: Bool
    @State private var underlyingScrollView: UIScrollView? = nil
    @State private var scrollAnimator: UIViewPropertyAnimator? = nil

    let sendAnimationNamespace: Namespace.ID?
    let outgoingAnimatingMessageId: UUID?
    let outgoingSourceMessageId: UUID?
    let bottomReservedSpace: CGFloat

    let messages: [ChatMessage]
    let isInputFocused: Bool
    let isAssistantTyping: Bool
    let initialJumpToken: Int
    let scrollToBottomToken: Int

    init(
        chatViewModel: ChatViewModel,
        isNearBottom: Binding<Bool>,
        followBottom: Binding<Bool>,
        messages: [ChatMessage],
        isInputFocused: Bool,
        isAssistantTyping: Bool,
        initialJumpToken: Int,
        scrollToBottomToken: Int,
        sendAnimationNamespace: Namespace.ID? = nil,
        outgoingAnimatingMessageId: UUID? = nil,
        outgoingSourceMessageId: UUID? = nil,
        bottomReservedSpace: CGFloat = 0
    ) {
        self._chatViewModel = ObservedObject(wrappedValue: chatViewModel)
        self._isNearBottom = isNearBottom
        self._followBottom = followBottom
        self.messages = messages
        self.isInputFocused = isInputFocused
        self.isAssistantTyping = isAssistantTyping
        self.initialJumpToken = initialJumpToken
        self.scrollToBottomToken = scrollToBottomToken

        self.sendAnimationNamespace = sendAnimationNamespace
        self.outgoingAnimatingMessageId = outgoingAnimatingMessageId
        self.outgoingSourceMessageId = outgoingSourceMessageId
        self.bottomReservedSpace = bottomReservedSpace
    }

    // Keep this small: only auto-scroll on focus when the user is basically at the bottom.
    private let nearBottomThreshold: CGFloat = 20

    private func scrollToBottomUIKit(_ scrollView: UIScrollView, animated: Bool) {
        let minOffsetY = -scrollView.adjustedContentInset.top
        let maxOffsetY = max(minOffsetY, scrollView.contentSize.height - scrollView.bounds.height + scrollView.adjustedContentInset.bottom)
        let target = CGPoint(x: 0, y: maxOffsetY)

        // Default UIScrollView animation can feel "snappy" / uneven when content size is changing.
        // Use an explicit animator for a smoother ease-in-out and to cancel in-flight animations.
        if animated {
            scrollAnimator?.stopAnimation(true)
            let animator = UIViewPropertyAnimator(duration: 0.38, curve: .easeInOut) {
                scrollView.setContentOffset(target, animated: false)
            }
            animator.startAnimation()
            scrollAnimator = animator
        } else {
            scrollAnimator?.stopAnimation(true)
            scrollAnimator = nil
            scrollView.setContentOffset(target, animated: false)
        }
    }

    var body: some View {
        ScrollView {
            VStack(spacing: 18) {
                ForEach(Array(messages.enumerated()), id: \.element.id) { index, message in
                    MessageBubbleView(
                        chatViewModel: chatViewModel,
                        message: message,
                        onSendToPartner: { text in
                            NotificationCenter.default.post(name: .sendPartnerMessageFromBubble, object: nil, userInfo: ["content": text])
                        }
                    )
                    .opacity(message.id == outgoingSourceMessageId ? 0 : 1)
                    .animation(nil, value: outgoingSourceMessageId)
                    .padding(.top, index > 0 && (messages[index - 1].isFromUser != message.isFromUser) ? 4 : 0)
                }
                if isAssistantTyping {
                    HStack(alignment: .top, spacing: 0) {
                        TypingIndicatorView(showAfter: 0)
                            .padding(.top, -10)
                        Spacer(minLength: 0)
                    }
                }
            }
            .padding(.top, 24)
            .padding(.horizontal)
            .padding(.bottom, bottomReservedSpace)
            // Observe the underlying UIScrollView so "near bottom" updates reliably while the user scrolls.
            .background(
                ScrollViewBottomProximityObserver(bottomScrollIndicatorInset: bottomReservedSpace, onChange: { scrollView in
                    let visibleBottomY = scrollView.contentOffset.y
                        + scrollView.bounds.height
                        - scrollView.adjustedContentInset.bottom
                    let rawDistance = scrollView.contentSize.height - visibleBottomY
                    let distanceFromBottom = max(0, rawDistance)
                    isNearBottom = distanceFromBottom <= nearBottomThreshold

                    let isUserDragging = scrollView.isDragging || scrollView.isTracking
                    if isUserDragging && distanceFromBottom > nearBottomThreshold {
                        // User scrolled away; stop following.
                        if followBottom { followBottom = false }
                    }

                    // If user tapped the button to follow, keep pinned while assistant streams.
                    if followBottom && !isUserDragging && distanceFromBottom > 1 {
                        scrollToBottomUIKit(scrollView, animated: false)
                    }
                }, onAttach: { scrollView in
                    underlyingScrollView = scrollView
                })
            )
        }
        .scrollBounceBehavior(.always)
        .scrollIndicators(.visible)
        .environment(\.sendAnimationNamespace, sendAnimationNamespace)
        .environment(\.outgoingAnimatingMessageId, outgoingAnimatingMessageId)
        .onChange(of: scrollToBottomToken, initial: false) { _, _ in
            guard let sv = underlyingScrollView else { return }
            sv.layoutIfNeeded()
            scrollToBottomUIKit(sv, animated: true)
        }
        .onChange(of: initialJumpToken, initial: false) { _, token in
            guard token > 0 else { return }
            guard let sv = underlyingScrollView else { return }
            sv.layoutIfNeeded()
            scrollToBottomUIKit(sv, animated: false)
        }
        .onChange(of: isInputFocused, initial: false) { _, focused in
            // Keyboard focus should NOT yank you to the bottom unless you're already near the end.
            guard focused, isNearBottom else { return }
            guard let sv = underlyingScrollView else { return }
            DispatchQueue.main.asyncAfter(deadline: .now() + 0.08) {
                sv.layoutIfNeeded()
                scrollToBottomUIKit(sv, animated: true)
            }
        }
    }
}

private struct ScrollViewBottomProximityObserver: UIViewRepresentable {
    let bottomScrollIndicatorInset: CGFloat
    let onChange: (UIScrollView) -> Void
    let onAttach: (UIScrollView) -> Void

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
            context.coordinator.attach(
                to: scrollView,
                bottomScrollIndicatorInset: bottomScrollIndicatorInset,
                onChange: onChange,
                onAttach: onAttach
            )
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

        func attach(
            to scrollView: UIScrollView,
            bottomScrollIndicatorInset: CGFloat,
            onChange: @escaping (UIScrollView) -> Void,
            onAttach: @escaping (UIScrollView) -> Void
        ) {
            // NOTE: We no longer override scroll-indicator insets here.
            // The chat now uses a bottom `safeAreaInset` for the composer, and letting UIKit manage
            // `verticalScrollIndicatorInsets` ensures the scroll indicator stays above the input bar.
            if self.scrollView === scrollView { return }

            self.scrollView = scrollView
            onAttach(scrollView)

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