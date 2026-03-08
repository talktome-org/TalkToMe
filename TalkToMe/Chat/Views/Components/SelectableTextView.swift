import SwiftUI
import UIKit

/// A UIViewRepresentable that wraps a read-only UITextView with a custom "Reply"
/// item in the native text selection menu. When the user selects text and taps Reply,
/// the selected text is passed to the `onReply` callback.
struct SelectableTextView: UIViewRepresentable {
    let attributedText: NSAttributedString
    var replyToName: String = "Reply"
    let onReply: (String) -> Void

    func makeCoordinator() -> Coordinator {
        Coordinator(onReply: onReply)
    }

    func makeUIView(context: Context) -> ReplyTextView {
        let textView = ReplyTextView()
        textView.replyMenuTitle = "Reply to \(replyToName)"
        textView.isEditable = false
        textView.isScrollEnabled = false
        textView.textContainerInset = .zero
        textView.textContainer.lineFragmentPadding = 0
        textView.backgroundColor = .clear
        textView.attributedText = attributedText
        textView.dataDetectorTypes = .link
        textView.linkTextAttributes = [
            .foregroundColor: UIColor.tintColor,
            .underlineStyle: NSUnderlineStyle.single.rawValue,
        ]
        textView.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
        textView.setContentHuggingPriority(.defaultLow, for: .horizontal)
        textView.setContentCompressionResistancePriority(.required, for: .vertical)
        textView.setContentHuggingPriority(.required, for: .vertical)
        textView.replyHandler = context.coordinator
        return textView
    }

    func updateUIView(_ textView: ReplyTextView, context: Context) {
        context.coordinator.onReply = onReply
        textView.replyHandler = context.coordinator
        textView.replyMenuTitle = "Reply to \(replyToName)"
        if textView.attributedText != attributedText {
            textView.attributedText = attributedText
        }
        textView.invalidateIntrinsicContentSize()
    }

    class Coordinator {
        var onReply: (String) -> Void

        init(onReply: @escaping (String) -> Void) {
            self.onReply = onReply
        }
    }
}

/// Custom UITextView subclass that adds "Reply" to the text selection menu.
final class ReplyTextView: UITextView {
    weak var replyHandler: SelectableTextView.Coordinator?
    var replyMenuTitle: String = "Reply"

    override var canBecomeFirstResponder: Bool { true }

    override var intrinsicContentSize: CGSize {
        guard bounds.width > 0 else { return super.intrinsicContentSize }
        let fixedWidth = bounds.width
        let size = sizeThatFits(CGSize(width: fixedWidth, height: .greatestFiniteMagnitude))
        return CGSize(width: UIView.noIntrinsicMetric, height: size.height)
    }

    override func layoutSubviews() {
        super.layoutSubviews()
        invalidateIntrinsicContentSize()
    }

    override func canPerformAction(_ action: Selector, withSender sender: Any?) -> Bool {
        if action == #selector(replyAction(_:)) {
            return selectedRange.length > 0
        }
        // Allow standard actions (copy, select all, etc.)
        return super.canPerformAction(action, withSender: sender)
    }

    override func buildMenu(with builder: any UIMenuBuilder) {
        super.buildMenu(with: builder)

        let replyAction = UIAction(title: replyMenuTitle, image: UIImage(systemName: "arrowshape.turn.up.left")) { [weak self] _ in
            self?.replyAction(nil)
        }

        let replyMenu = UIMenu(title: "", options: .displayInline, children: [replyAction])

        // Insert at the beginning so it appears prominently
        if builder.menu(for: .standardEdit) != nil {
            builder.insertSibling(replyMenu, beforeMenu: .standardEdit)
        } else {
            builder.insertChild(replyMenu, atStartOfMenu: .root)
        }
    }

    @objc private func replyAction(_ sender: Any?) {
        guard selectedRange.length > 0,
              let text = text else { return }
        let selectedText = (text as NSString).substring(with: selectedRange)
        let trimmed = selectedText.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return }

        // Dismiss selection
        selectedTextRange = nil
        resignFirstResponder()

        replyHandler?.onReply(trimmed)
    }
}
