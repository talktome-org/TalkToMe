import SwiftUI
import UIKit

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

    private var messages: [ChatMessage] { chatViewModel.messages }
    private var isAssistantTyping: Bool { chatViewModel.isAssistantTyping }
    private var initialJumpToken: Int { chatViewModel.initialJumpToken }

    private let nearBottomThreshold: CGFloat = 20
    private let scrollButtonSize: CGFloat = 36
    private let scrollButtonIconSize: CGFloat = 14

    private func scrollToBottomUIKit(_ scrollView: UIScrollView, animated: Bool) {
        let minOffsetY = -scrollView.adjustedContentInset.top
        let maxOffsetY = max(minOffsetY, scrollView.contentSize.height - scrollView.bounds.height + scrollView.adjustedContentInset.bottom)
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

    private var shouldShowScrollButton: Bool {
        !messages.isEmpty && !isNearBottom
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
                        onRegenerate: { messageId in
                            chatViewModel.regenerateResponse(for: messageId)
                        },
                        sendAnimationNamespace: sendAnimationNamespace,
                        outgoingAnimatingMessageId: outgoingAnimatingMessageId
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
            .padding(.bottom, 14) // breathing room above input
            .background(
                ScrollViewBottomProximityObserver(onChange: { scrollView in
                    let visibleBottomY = scrollView.contentOffset.y
                        + scrollView.bounds.height
                        - scrollView.adjustedContentInset.bottom
                    let rawDistance = scrollView.contentSize.height - visibleBottomY
                    let distanceFromBottom = max(0, rawDistance)
                    isNearBottom = distanceFromBottom <= nearBottomThreshold

                    let isUserDragging = scrollView.isDragging || scrollView.isTracking
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
        .contentShape(Rectangle())
        .onTapGesture {
            onBackgroundTap()
        }
        .scrollBounceBehavior(.always)
        .scrollIndicators(.visible)
        .overlay(alignment: .bottomTrailing) {
            scrollToBottomButton
        }
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
            guard focused, isNearBottom else { return }
            guard let sv = underlyingScrollView else { return }
            DispatchQueue.main.asyncAfter(deadline: .now() + 0.08) {
                sv.layoutIfNeeded()
                scrollToBottomUIKit(sv, animated: true)
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
        .padding(.trailing, 20)
        .allowsHitTesting(shouldShowScrollButton)
        .animation(.spring(response: 0.30, dampingFraction: 0.86), value: shouldShowScrollButton)
    }

    private func scrollToBottom() {
        followBottom = true
        scrollToBottomToken &+= 1
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