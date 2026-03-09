import Foundation

enum PreferenceKeys {
    static let appearancePreference = "appearance_preference"
    static let fontSizePreference = "font_size_preference"
    static let chatWallpaperType = "chat_wallpaper_type"
    static let chatWallpaperValue = "chat_wallpaper_value"
    static let hapticsEnabled = "haptics_enabled"
    static let dictationEnabled = "dictation_enabled"
    static let elevenLabsVoiceId = "eleven_labs_voice_id"
    static let elevenLabsVoiceName = "eleven_labs_voice_name"
    static let myAvatarURL = "my_avatar_url"
    static let partnerUserId = "partner_user_id"
    static let partnerName = "partner_name"
    static let partnerAvatarURL = "partner_avatar_url"
    static let partnerDisplayName = "partner_display_name"
    static let partnerVoiceName = "partner_voice_name"
    static let ttsVoiceIdentifier = "tts_voice_identifier"
    static let currentUserId = "current_user_id"
    static let didExplicitSignOut = "did_explicit_sign_out"
    static let buddyExplicitlyChosen = "buddy_explicitly_chosen"

    static func buddyCustomPromptKey(_ buddyKey: String) -> String {
        "buddy_custom_prompt_\(buddyKey)"
    }

    static func getPartnerDisplayName() -> String {
        if let displayName = UserDefaults.standard.string(forKey: PreferenceKeys.partnerDisplayName),
           !displayName.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            return displayName
        }
        if let partnerName = UserDefaults.standard.string(forKey: PreferenceKeys.partnerName),
           !partnerName.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            return partnerName
        }
        return "Partner"
    }
}