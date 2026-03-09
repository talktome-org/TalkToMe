import Foundation
import SwiftUI

struct SettingsData: Codable {
    var emailNotifications: Bool = true
    var pushNotifications: Bool = true
    var darkModeEnabled: Bool = false
    var hapticFeedbackEnabled: Bool = true
    var dictationEnabled: Bool = true
    var saveChatsEnabled: Bool = true
    var crashReportingEnabled: Bool = true
    var ttsVoiceIdentifier: String? = nil
    var elevenLabsVoiceId: String? = nil
    var elevenLabsVoiceName: String? = nil

    init() {
    }
}

struct SettingsSection: Identifiable {
    let id: String
    let title: String
    let icon: String
    let gradient: [Color]
    var settings: [SettingItem]

    init(id: String? = nil, title: String, icon: String, gradient: [Color], settings: [SettingItem]) {
        self.id = id ?? title
        self.title = title
        self.icon = icon
        self.gradient = gradient
        self.settings = settings
    }
}

struct SettingItem: Identifiable {
    let id: String
    let title: String
    let subtitle: String?
    var type: SettingType
    let icon: String

    init(title: String, subtitle: String?, type: SettingType, icon: String) {
        self.id = title
        self.title = title
        self.subtitle = subtitle
        self.type = type
        self.icon = icon
    }
}

enum SettingType {
    case toggle(Bool)
    case navigation
    case action
    case picker([String])
    case friendsCode
    case info
}
