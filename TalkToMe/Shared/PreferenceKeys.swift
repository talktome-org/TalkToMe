import Foundation

enum PreferenceKeys {
    static let appearancePreference = "appearance_preference"
    static let hapticsEnabled = "haptics_enabled"
    static let dailyCheckinsEnabled = "daily_checkins_enabled"
    static let dailyCheckinsInitialized = "daily_checkins_initialized"
    static let myAvatarURL = "my_avatar_url"
    static let partnerConnected = "partner_connected"
    static let partnerName = "partner_name"
    static let partnerAvatarURL = "partner_avatar_url"
    static let partnerDisplayName = "partner_display_name"
    static let ttsVoiceIdentifier = "tts_voice_identifier"

    /// Returns the partner display name to use in UI.
    /// Prioritizes user-entered partner_display_name from onboarding over the actual partner's name.
    /// Falls back to "Partner" if neither is available.
    static func getPartnerDisplayName() -> String {
        // First, check if user entered a partner_display_name during onboarding
        if let displayName = UserDefaults.standard.string(forKey: PreferenceKeys.partnerDisplayName),
           !displayName.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            return displayName
        }
        // Fall back to the actual partner's name
        if let partnerName = UserDefaults.standard.string(forKey: PreferenceKeys.partnerName),
           !partnerName.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            return partnerName
        }
        // Default fallback
        return "Partner"
    }
}