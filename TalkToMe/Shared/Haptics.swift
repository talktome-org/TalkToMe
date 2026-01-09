import Foundation
import UIKit

enum Haptics {
    private static var isEnabled: Bool {
        if UserDefaults.standard.object(forKey: PreferenceKeys.hapticsEnabled) == nil {
            return true
        }
        return UserDefaults.standard.bool(forKey: PreferenceKeys.hapticsEnabled)
    }

    static func impact(_ style: UIImpactFeedbackGenerator.FeedbackStyle) {
        guard isEnabled else { return }
        UIImpactFeedbackGenerator(style: style).impactOccurred()
    }

    static func selection() {
        guard isEnabled else { return }
        UISelectionFeedbackGenerator().selectionChanged()
    }

    static func notification(_ type: UINotificationFeedbackGenerator.FeedbackType) {
        guard isEnabled else { return }
        UINotificationFeedbackGenerator().notificationOccurred(type)
    }
}


