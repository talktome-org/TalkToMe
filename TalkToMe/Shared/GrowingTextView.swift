import SwiftUI
import UIKit

/// A Messenger-style multiline composer:
/// - Grows with content up to `maxHeight`
/// - After hitting `maxHeight`, the text view scrolls internally
/// - Supports external focus binding
struct GrowingTextView: UIViewRepresentable {
    @Binding var text: String
    @Binding var isFocused: Bool

    let minHeight: CGFloat
    let maxHeight: CGFloat

    var font: UIFont = .systemFont(ofSize: 17)
    var textColor: UIColor = .label
    var isEditable: Bool = true
    var isSelectable: Bool = true
    var keyboardType: UIKeyboardType = .default
    var autocorrectionType: UITextAutocorrectionType = .default
    var autocapitalizationType: UITextAutocapitalizationType = .sentences

    func makeUIView(context: Context) -> UITextView {
        let tv = UITextView()
        tv.backgroundColor = .clear
        tv.font = font
        tv.textColor = textColor
        tv.textContainerInset = .zero
        tv.textContainer.lineFragmentPadding = 0
        // We keep scrolling enabled and clamp height via `sizeThatFits`.
        // This avoids feedback loops from toggling `isScrollEnabled` while SwiftUI is measuring.
        tv.isScrollEnabled = true
        tv.showsVerticalScrollIndicator = false
        tv.alwaysBounceVertical = false
        tv.delegate = context.coordinator
        tv.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
        tv.setContentHuggingPriority(.defaultLow, for: .horizontal)
        tv.setContentHuggingPriority(.defaultHigh, for: .vertical)
        tv.keyboardType = keyboardType
        tv.autocorrectionType = autocorrectionType
        tv.autocapitalizationType = autocapitalizationType
        return tv
    }

    func updateUIView(_ uiView: UITextView, context: Context) {
        if uiView.text != text {
            uiView.text = text
        }

        uiView.font = font
        uiView.textColor = textColor
        uiView.isEditable = isEditable
        uiView.isSelectable = isSelectable

        // Focus bridging.
        if isFocused, !uiView.isFirstResponder {
            uiView.becomeFirstResponder()
        } else if !isFocused, uiView.isFirstResponder {
            uiView.resignFirstResponder()
        }
    }

    func sizeThatFits(_ proposal: ProposedViewSize, uiView: UITextView, context: Context) -> CGSize? {
        let width = max(proposal.width ?? uiView.bounds.width, 1)
        let size = uiView.sizeThatFits(CGSize(width: width, height: .greatestFiniteMagnitude))
        let height = min(max(size.height, minHeight), maxHeight)
        return CGSize(width: width, height: height)
    }

    func makeCoordinator() -> Coordinator {
        Coordinator(parent: self)
    }

    final class Coordinator: NSObject, UITextViewDelegate {
        var parent: GrowingTextView

        init(parent: GrowingTextView) {
            self.parent = parent
        }

        func textViewDidBeginEditing(_ textView: UITextView) {
            if !parent.isFocused {
                parent.isFocused = true
            }
        }

        func textViewDidEndEditing(_ textView: UITextView) {
            if parent.isFocused {
                parent.isFocused = false
            }
        }

        func textViewDidChange(_ textView: UITextView) {
            let newText = textView.text ?? ""
            if parent.text != newText {
                parent.text = newText
            }
        }
    }
}

