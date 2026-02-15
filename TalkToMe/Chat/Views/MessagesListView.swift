import SwiftUI
import UIKit

private struct MessageMinYKey: PreferenceKey {
    static var defaultValue: [UUID: CGFloat] = [:]
    static func reduce(value: inout [UUID: CGFloat], nextValue: () -> [UUID: CGFloat]) {
        value.merge(nextValue(), uniquingKeysWith: { $1 })
    }
}

struct MessagesListView: View {
    @ObservedObject var chatViewModel: ChatViewModel

    let isInputFocused: Bool
    let inputAreaHeight: CGFloat
    let onBackgroundTap: () -> Void

    var sendAnimationNamespace: Namespace.ID? = nil
    var outgoingAnimatingMessageId: UUID? = nil
    var outgoingSourceMessageId: UUID? = nil

    @State private var isNearBottom: Bool = true
    @State private var followBottom: Bool = false
    @State private var scrollToBottomToken: Int = 0
    @State private var underlyingScrollView: UIScrollView? = nil
    @State private var scrollAnimator: UIViewPropertyAnimator? = nil
    @State private var toastMessage: String? = nil
    @State private var toastWorkItem: DispatchWorkItem? = nil
    @State private var scrollToTopPadding: CGFloat = 0
    @State private var messagePositions: [UUID: CGFloat] = [:]
    @State private var pendingScrollTargetId: UUID? = nil
    @State private var enforcedOffsetY: CGFloat? = nil
    @State private var scrollToTopTargetId: UUID? = nil
    @State private var isAnimatingToTop: Bool = false

    private var messages: [ChatMessage] { chatViewModel.messages }
    private var initialJumpToken: Int { chatViewModel.initialJumpToken }

    private let nearBottomThreshold: CGFloat = 20
    private let scrollButtonSize: CGFloat = 36
    private let scrollButtonIconSize: CGFloat = 14

    // Scrolls to the "effective bottom" (ignoring extra scroll-to-top padding)
    private func scrollToBottomUIKit(_ scrollView: UIScrollView, animated: Bool) {
        let minOffsetY = -scrollView.adjustedContentInset.top
        let effectiveContentHeight = scrollView.contentSize.height - scrollToTopPadding
        let maxOffsetY = max(minOffsetY, effectiveContentHeight - scrollView.bounds.height + scrollView.adjustedContentInset.bottom)
        let target = CGPoint(x: 0, y: maxOffsetY)

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

    // Animates scroll so the message at `messageY` lands at the top, then locks enforcement
    private func scrollToTopUIKit(_ scrollView: UIScrollView, messageY: CGFloat) {
        let targetOffset = messageY - scrollView.adjustedContentInset.top
        isAnimatingToTop = true

        scrollAnimator?.stopAnimation(true)
        let animator = UIViewPropertyAnimator(duration: 0.38, curve: .easeInOut) {
            scrollView.setContentOffset(CGPoint(x: 0, y: targetOffset), animated: false)
        }
        animator.addCompletion { _ in
            self.isAnimatingToTop = false

            let stableVisibleHeight = UIScreen.main.bounds.height - scrollView.adjustedContentInset.top

            let recalcPadding = { (mY: CGFloat) in
                let currentTargetOffset = mY - scrollView.adjustedContentInset.top
                let naturalContentHeight = scrollView.contentSize.height - self.scrollToTopPadding
                let contentBelowMessage = naturalContentHeight - mY
                let visibleHeight = scrollView.bounds.height - scrollView.adjustedContentInset.top - scrollView.adjustedContentInset.bottom

                if contentBelowMessage >= visibleHeight {
                    self.scrollToTopPadding = 0
                    self.enforcedOffsetY = nil
                    self.scrollToTopTargetId = nil
                    self.followBottom = true
                } else {
                    self.enforcedOffsetY = currentTargetOffset
                    let neededPadding = max(0, stableVisibleHeight - contentBelowMessage)
                    if self.scrollToTopPadding - neededPadding > 2 {
                        self.scrollToTopPadding = neededPadding
                    }
                }
            }

            recalcPadding(messageY)

            // Recalculate after layout settles, using current position from preferences
            for delay in [0.15, 0.5] {
                DispatchQueue.main.asyncAfter(deadline: .now() + delay) {
                    guard let targetId = self.scrollToTopTargetId else { return }
                    scrollView.layoutIfNeeded()
                    let currentMY = self.messagePositions[targetId] ?? messageY
                    recalcPadding(currentMY)
                }
            }
        }
        animator.startAnimation()
        scrollAnimator = animator
    }

    private var shouldShowScrollButton: Bool {
        !messages.isEmpty && !isNearBottom && scrollToTopTargetId == nil
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
                        },
                        onRegenerate: {
                            chatViewModel.streamingController.regenerateResponse(forAssistantMessageId: message.id)
                        },
                        onToast: { text in
                            showToast(text)
                        },
                        sendAnimationNamespace: sendAnimationNamespace,
                        outgoingAnimatingMessageId: outgoingAnimatingMessageId
                    )
                    .id(message.id)
                    .background(
                        GeometryReader { geo in
                            Color.clear.preference(
                                key: MessageMinYKey.self,
                                value: [message.id: geo.frame(in: .named("scrollContent")).minY]
                            )
                        }
                    )
                    .opacity(message.id == outgoingSourceMessageId ? 0 : 1)
                    .animation(nil, value: outgoingSourceMessageId)
                    .padding(.top, index > 0 && (messages[index - 1].isFromUser != message.isFromUser) ? 4 : 0)
                }
            }
            .padding(.top, 24)
            .padding(.horizontal)
            .padding(.bottom, 2 + scrollToTopPadding)
            .coordinateSpace(name: "scrollContent")
            .background(
                ScrollViewBottomProximityObserver(onChange: { scrollView in
                    // Don't interfere while the scroll-to-top animation is playing
                    guard !isAnimatingToTop else { return }

                    // Release enforcement when user drags (must be checked BEFORE enforcement block)
                    let isUserDragging = scrollView.isDragging || scrollView.isTracking
                    if isUserDragging && enforcedOffsetY != nil {
                        enforcedOffsetY = nil
                        scrollToTopTargetId = nil
                        scrollToTopPadding = 0
                    }

                    // Always update isNearBottom (must run before enforcement early-return)
                    let visibleBottomY = scrollView.contentOffset.y
                        + scrollView.bounds.height
                        - scrollView.adjustedContentInset.bottom
                    let rawDistance = scrollView.contentSize.height - visibleBottomY
                    let distanceFromBottom = max(0, rawDistance - scrollToTopPadding)
                    isNearBottom = distanceFromBottom <= nearBottomThreshold

                    // Shrink padding as content grows; release to follow-bottom when screen is filled
                    if scrollToTopTargetId != nil, let targetY = enforcedOffsetY {
                        let messageY = targetY + scrollView.adjustedContentInset.top
                        let naturalContentHeight = scrollView.contentSize.height - scrollToTopPadding
                        let contentBelowMessage = naturalContentHeight - messageY

                        // Use real visibleHeight for release check (keyboard-sensitive — releases earlier with keyboard open)
                        let visibleHeight = scrollView.bounds.height - scrollView.adjustedContentInset.top - scrollView.adjustedContentInset.bottom
                        if contentBelowMessage >= visibleHeight {
                            scrollToTopPadding = 0
                            enforcedOffsetY = nil
                            scrollToTopTargetId = nil
                            followBottom = true
                            return
                        }

                        // Use keyboard-independent height for padding (ignores bottom inset so padding stays sufficient after keyboard dismissal)
                        let stableVisibleHeight = UIScreen.main.bounds.height - scrollView.adjustedContentInset.top
                        let neededPadding = max(0, stableVisibleHeight - contentBelowMessage)
                        if scrollToTopPadding - neededPadding > 2 {
                            scrollToTopPadding = neededPadding
                        }
                    }

                    // If we're enforcing a scroll position, keep it locked
                    // Recalculate from stored position so inset changes (toolbar) don't shift the message
                    if enforcedOffsetY != nil {
                        var targetY = enforcedOffsetY!
                        if let targetId = scrollToTopTargetId, let messageY = messagePositions[targetId] {
                            targetY = messageY - scrollView.adjustedContentInset.top
                            enforcedOffsetY = targetY
                        }
                        let current = scrollView.contentOffset.y
                        if abs(current - targetY) > 1 {
                            scrollView.setContentOffset(CGPoint(x: 0, y: targetY), animated: false)
                        }
                        return
                    }

                    if isUserDragging && distanceFromBottom > nearBottomThreshold {
                        if followBottom { followBottom = false }
                    }

                    if followBottom && !isUserDragging && distanceFromBottom > 1 {
                        scrollToBottomUIKit(scrollView, animated: false)
                    }
                }, onAttach: { scrollView in
                    underlyingScrollView = scrollView
                    scrollView.clipsToBounds = true
                })
            )
        }
        .onPreferenceChange(MessageMinYKey.self) { positions in
            messagePositions = positions
            guard !isAnimatingToTop else { return }

            // If we have a pending scroll target and its position just arrived, animate now
            if let targetId = pendingScrollTargetId, let messageY = positions[targetId] {
                pendingScrollTargetId = nil
                scrollToTopTargetId = targetId
                guard let sv = underlyingScrollView else { return }
                // Delay to let scroll view content size settle after padding change
                DispatchQueue.main.asyncAfter(deadline: .now() + 0.05) {
                    guard let sv = underlyingScrollView else { return }
                    sv.layoutIfNeeded()
                    scrollToTopUIKit(sv, messageY: messageY)
                }
            }

            // Continuously re-enforce position for scroll-to-top target.
            // Only UPDATE an already-set offset — don't initiate enforcement before
            // the scroll-to-top animation has had a chance to run and set it.
            if enforcedOffsetY != nil, let targetId = scrollToTopTargetId, let messageY = positions[targetId] {
                guard let sv = underlyingScrollView else { return }
                let targetOffset = messageY - sv.adjustedContentInset.top
                enforcedOffsetY = targetOffset
            }
        }
        .contentShape(Rectangle())
        .onTapGesture {
            onBackgroundTap()
        }
        .scrollBounceBehavior(.always)
        .scrollIndicators(.visible)
        .overlay(alignment: .bottomTrailing) {
            scrollToBottomButton
        }
        .overlay(alignment: .top) {
            if let toastMessage {
                ToastOverlayView(message: toastMessage, onDismiss: {
                    dismissToast()
                })
                .padding(.top, 8)
            }
        }
        .animation(.spring(response: 0.3, dampingFraction: 0.8), value: toastMessage)
        .onChange(of: scrollToBottomToken, initial: false) { _, _ in
            guard let sv = underlyingScrollView else { return }
            sv.layoutIfNeeded()
            scrollToBottomUIKit(sv, animated: true)
        }
        .onChange(of: initialJumpToken, initial: false) { _, token in
            guard token > 0 else { return }
            isAnimatingToTop = false
            enforcedOffsetY = nil
            scrollToTopTargetId = nil
            scrollToTopPadding = 0
            guard let sv = underlyingScrollView else { return }
            DispatchQueue.main.asyncAfter(deadline: .now() + 0.05) {
                sv.layoutIfNeeded()
                scrollToBottomUIKit(sv, animated: false)
            }
        }
        .onChange(of: isInputFocused, initial: false) { _, focused in
            guard focused, isNearBottom, enforcedOffsetY == nil else { return }
            guard let sv = underlyingScrollView else { return }
            DispatchQueue.main.asyncAfter(deadline: .now() + 0.08) {
                sv.layoutIfNeeded()
                scrollToBottomUIKit(sv, animated: true)
            }
        }
        .onChange(of: chatViewModel.pendingOutgoingUserMessageId, initial: false) { _, newId in
            guard let id = newId else { return }
            chatViewModel.pendingOutgoingUserMessageId = nil

            followBottom = false
            scrollToTopPadding = UIScreen.main.bounds.height
            pendingScrollTargetId = id
            scrollToTopTargetId = id

            // If position is already known, animate scroll after padding takes effect
            if let messageY = messagePositions[id] {
                pendingScrollTargetId = nil
                guard let sv = underlyingScrollView else { return }
                DispatchQueue.main.asyncAfter(deadline: .now() + 0.05) {
                    guard let sv = underlyingScrollView else { return }
                    sv.layoutIfNeeded()
                    scrollToTopUIKit(sv, messageY: messageY)
                }
            }
        }
    }

    @ViewBuilder
    private var scrollToBottomButton: some View {
        let bottomPadding: CGFloat = -10

        ZStack(alignment: .bottomTrailing) {
            if shouldShowScrollButton {
                Group {
                    if #available(iOS 26.0, *) {
                        Button(action: scrollToBottom) {
                            Image(systemName: "chevron.down")
                                .font(.system(size: scrollButtonIconSize, weight: .semibold))
                                .foregroundColor(.primary)
                                .frame(width: scrollButtonSize, height: scrollButtonSize)
                        }
                        .buttonStyle(ScrollButtonStyle())
                        .glassEffect(.regular.interactive(), in: Circle())
                        .contentShape(Rectangle())
                    } else {
                        Button(action: scrollToBottom) {
                            Image(systemName: "chevron.down")
                                .font(.system(size: scrollButtonIconSize, weight: .semibold))
                                .foregroundColor(.primary)
                                .frame(width: scrollButtonSize, height: scrollButtonSize)
                        }
                        .buttonStyle(ScrollButtonStyle())
                        .background(.thinMaterial, in: Circle())
                        .overlay(Circle().strokeBorder(Color.primary.opacity(0.10), lineWidth: 1))
                        .contentShape(Rectangle())
                    }
                }
                .transition(
                    .scale(scale: 0.86, anchor: .bottom)
                    .combined(with: .opacity)
                )
            }
        }
        .frame(width: scrollButtonSize, height: scrollButtonSize)
        .padding(.bottom, bottomPadding)
        .padding(.trailing, 23)
        .allowsHitTesting(shouldShowScrollButton)
        .animation(.spring(response: 0.30, dampingFraction: 0.86), value: shouldShowScrollButton)
    }

    private func scrollToBottom() {
        isAnimatingToTop = false
        enforcedOffsetY = nil
        scrollToTopTargetId = nil
        scrollToTopPadding = 0
        pendingScrollTargetId = nil
        followBottom = true
        DispatchQueue.main.asyncAfter(deadline: .now() + 0.05) {
            scrollToBottomToken &+= 1
        }
    }

    private func showToast(_ message: String) {
        toastWorkItem?.cancel()
        withAnimation {
            toastMessage = message
        }
        let work = DispatchWorkItem {
            withAnimation {
                toastMessage = nil
            }
        }
        toastWorkItem = work
        DispatchQueue.main.asyncAfter(deadline: .now() + 4, execute: work)
    }

    private func dismissToast() {
        toastWorkItem?.cancel()
        withAnimation {
            toastMessage = nil
        }
    }
}


private struct ScrollButtonStyle: ButtonStyle {
    func makeBody(configuration: Configuration) -> some View {
        configuration.label
            .scaleEffect(configuration.isPressed ? 0.92 : 1.0)
            .opacity(configuration.isPressed ? 0.92 : 1.0)
            .animation(.spring(response: 0.22, dampingFraction: 0.88, blendDuration: 0.02), value: configuration.isPressed)
    }
}

private struct ScrollViewBottomProximityObserver: UIViewRepresentable {
    let onChange: (UIScrollView) -> Void
    let onAttach: (UIScrollView) -> Void

    func makeUIView(context: Context) -> UIView {
        let view = UIView(frame: .zero)
        view.isUserInteractionEnabled = false
        view.backgroundColor = .clear
        return view
    }

    func updateUIView(_ uiView: UIView, context: Context) {
        DispatchQueue.main.async {
            guard let scrollView = findScrollView(from: uiView) else { return }
            context.coordinator.attach(
                to: scrollView,
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
            onChange: @escaping (UIScrollView) -> Void,
            onAttach: @escaping (UIScrollView) -> Void
        ) {
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
