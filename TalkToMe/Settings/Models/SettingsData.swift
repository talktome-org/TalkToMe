import Foundation
import SwiftUI

struct SettingsData: Codable {
    var emailNotifications: Bool = true
    var pushNotifications: Bool = true
    var dailyCheckinsEnabled: Bool = false
    var darkModeEnabled: Bool = false
    var hapticFeedbackEnabled: Bool = true
    var saveChatsEnabled: Bool = true
    var crashReportingEnabled: Bool = true
    var ttsVoiceIdentifier: String? = nil

    init() {
    }
}

struct SettingsSection {
    let title: String
    let icon: String
    let gradient: [Color]
    let settings: [SettingItem]
}

struct SettingItem {
    let title: String
    let subtitle: String?
    let type: SettingType
    let icon: String
}

enum SettingType {
    case toggle(Bool)
    case navigation
    case action
    case picker([String])
    case linkPartner
}

enum SettingsDestination: Hashable, Identifiable {
    case contactSupport
    case privacyPolicy

    var id: String {
        switch self {
        case .contactSupport:
            return "contactSupport"
        case .privacyPolicy:
            return "privacyPolicy"
        }
    }
}
