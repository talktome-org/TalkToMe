import Foundation
import SwiftUI
import Combine


@MainActor
class SettingsViewModel: ObservableObject {

    @Published var settingsData = SettingsData()
    @Published var settingsSections: [SettingsSection] = []
    @Published var isUploadingAvatar: Bool = false
    @Published var avatarURL: String? = nil
    @Published var showPersonalizationEdit: Bool = false
    @Published var shouldHighlightCustomization: Bool = false
    @Published var shouldNavigateToBuddyChooser: Bool = false
    @Published var showDeleteAccountConfirmation: Bool = false
    @Published var isDeletingAccount: Bool = false

    private let avatarCacheManager = AvatarCacheManager.shared
    private var pushEnabledCancellable: AnyCancellable?

    @Published var fullName: String = ""
    @Published var bio: String = ""
    @Published var isProfileLoaded: Bool = false

    init() {
        APNSService.shared.loadPushEnabledFromDefaults()
        loadSettings()
        // Warm name from cache immediately to avoid flicker
        if let cached = UserDefaults.standard.string(forKey: "talktome_profile_full_name"),
           !cached.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            self.fullName = cached
            self.isProfileLoaded = true
        }
        setupSettingsSections()

        // Keep notifications toggle in sync when changed from outside (e.g. NotificationsView)
        pushEnabledCancellable = APNSService.shared.$isPushEnabled
            .receive(on: RunLoop.main)
            .sink { [weak self] enabled in
                self?.syncNotificationsToggle(enabled)
            }
    }

    private func syncNotificationsToggle(_ enabled: Bool) {
        guard let sectionIdx = settingsSections.firstIndex(where: { $0.id == "toggles" }),
              let settingIdx = settingsSections[sectionIdx].settings.firstIndex(where: { $0.title == "Notifications" })
        else { return }
        settingsSections[sectionIdx].settings[settingIdx].type = .toggle(enabled)
    }

    private func loadSettings() {
        if UserDefaults.standard.object(forKey: PreferenceKeys.hapticsEnabled) != nil {
            settingsData.hapticFeedbackEnabled = UserDefaults.standard.bool(forKey: PreferenceKeys.hapticsEnabled)
        } else {
            settingsData.hapticFeedbackEnabled = true
            UserDefaults.standard.set(true, forKey: PreferenceKeys.hapticsEnabled)
        }

        if UserDefaults.standard.object(forKey: PreferenceKeys.dictationEnabled) != nil {
            settingsData.dictationEnabled = UserDefaults.standard.bool(forKey: PreferenceKeys.dictationEnabled)
        } else {
            settingsData.dictationEnabled = true
            UserDefaults.standard.set(true, forKey: PreferenceKeys.dictationEnabled)
        }

        if let vid = UserDefaults.standard.string(forKey: PreferenceKeys.elevenLabsVoiceId),
           !vid.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            settingsData.elevenLabsVoiceId = vid
        }
        if let vname = UserDefaults.standard.string(forKey: PreferenceKeys.elevenLabsVoiceName),
           !vname.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            settingsData.elevenLabsVoiceName = vname
        }

        if let storedVoice = UserDefaults.standard.string(forKey: PreferenceKeys.ttsVoiceIdentifier) {
            settingsData.ttsVoiceIdentifier = storedVoice
        }

    }

    private func setupSettingsSections() {
        settingsSections = [
            SettingsSection(
                id: "contacts",
                title: "",
                icon: "",
                gradient: [],
                settings: [
                    SettingItem(title: "Contacts", subtitle: nil, type: .navigation, icon: "person.2")
                ]
            ),
            SettingsSection(
                id: "customization",
                title: "",
                icon: "",
                gradient: [],
                settings: [
                    SettingItem(title: "Appearance", subtitle: nil, type: .navigation, icon: "circle.lefthalf.filled"),
                    SettingItem(title: "Wallpapers", subtitle: nil, type: .navigation, icon: "photo"),
                    SettingItem(title: "Customize Buddies", subtitle: nil, type: .navigation, icon: "wand.and.stars")
                ]
            ),
            SettingsSection(
                id: "toggles",
                title: "",
                icon: "",
                gradient: [],
                settings: [
                    SettingItem(title: "Notifications", subtitle: nil, type: .toggle(APNSService.shared.isPushEnabled), icon: "bell"),
                    SettingItem(title: "Haptics", subtitle: nil, type: .toggle(settingsData.hapticFeedbackEnabled), icon: "iphone.radiowaves.left.and.right"),
                    SettingItem(title: "Dictation", subtitle: nil, type: .toggle(settingsData.dictationEnabled), icon: "waveform")
                ]
            ),
            SettingsSection(
                id: "support",
                title: "",
                icon: "",
                gradient: [],
                settings: [
                    SettingItem(title: "Contact Support", subtitle: nil, type: .navigation, icon: "envelope"),
                    SettingItem(title: "Privacy Policy", subtitle: nil, type: .navigation, icon: "lock")
                ]
            ),
            SettingsSection(
                id: "account",
                title: "",
                icon: "",
                gradient: [],
                settings: [
                    SettingItem(title: "Delete Account", subtitle: nil, type: .action, icon: "trash"),
                    SettingItem(title: "Sign Out", subtitle: nil, type: .action, icon: "rectangle.portrait.and.arrow.right")
                ]
            )
        ]
    }

    func toggleSetting(for sectionIndex: Int, settingIndex: Int) {
        let setting = settingsSections[sectionIndex].settings[settingIndex]
        var newValue = false

        switch setting.title {
        case "Haptics":
            settingsData.hapticFeedbackEnabled.toggle()
            newValue = settingsData.hapticFeedbackEnabled
            UserDefaults.standard.set(newValue, forKey: PreferenceKeys.hapticsEnabled)
            if newValue { Haptics.selection() }
        case "Dictation":
            settingsData.dictationEnabled.toggle()
            newValue = settingsData.dictationEnabled
            UserDefaults.standard.set(newValue, forKey: PreferenceKeys.dictationEnabled)
        case "Notifications":
            newValue = !APNSService.shared.isPushEnabled
            APNSService.shared.setPushEnabled(newValue)
        default:
            return
        }

        // Update in-place so SwiftUI sees a minimal diff instead of a full array rebuild
        settingsSections[sectionIndex].settings[settingIndex].type = .toggle(newValue)
    }

    func handleSettingAction(for sectionIndex: Int, settingIndex: Int) {
        let section = settingsSections[sectionIndex]
        let setting = section.settings[settingIndex]

        switch setting.title {
        case "Delete Account":
            showDeleteAccountConfirmation = true
        case "Sign Out":
            Task {
                await AuthService.shared.signOut()
            }
        default:
            break
        }
    }

    func deleteAccount() {
        isDeletingAccount = true
        Task {
            // 1. Grab the token BEFORE signing out so it's still valid
            let token = await AuthService.shared.getAccessToken()

            // 2. Purge local data and sign out
            await MainActor.run { Self.purgeAllLocalData() }
            await AuthService.shared.signOut()

            // 3. Delete on the backend with the captured token
            if let token {
                do {
                    try await BackendService.shared.deleteAccount(accessToken: token)
                } catch {
                    print("Failed to delete account on server: \(error)")
                }
            }

            await MainActor.run { isDeletingAccount = false }
        }
    }

    /// Wipes every piece of locally-persisted user data: GRDB database, avatar cache, UserDefaults.
    static func purgeAllLocalData() {
        // 1. Destroy the local GRDB chat database (must happen while currentUserId is still set)
        LocalDatabase.shared.destroyCurrentUserDatabase()

        // 2. Clear avatar disk + memory cache
        AvatarCacheManager.shared.clearCache()

        // 3. Remove all UserDefaults keys set by the app
        let keysToRemove = [
            PreferenceKeys.appearancePreference,
            PreferenceKeys.fontSizePreference,
            PreferenceKeys.chatWallpaperType,
            PreferenceKeys.chatWallpaperValue,
            PreferenceKeys.hapticsEnabled,
            PreferenceKeys.dictationEnabled,
            PreferenceKeys.elevenLabsVoiceId,
            PreferenceKeys.elevenLabsVoiceName,
            PreferenceKeys.myAvatarURL,
            PreferenceKeys.partnerUserId,
            PreferenceKeys.partnerName,
            PreferenceKeys.partnerAvatarURL,
            PreferenceKeys.partnerDisplayName,
            PreferenceKeys.partnerVoiceName,
            PreferenceKeys.ttsVoiceIdentifier,
            "talktome_profile_full_name",
            "cached_notifications",
            "get_started_dismissed",
            "get_started_say_hi",
            "get_started_connect_friend",
            "get_started_write_diary",
            "get_started_customize_buddy",
            PreferenceKeys.buddyExplicitlyChosen,
        ]
        for key in keysToRemove {
            UserDefaults.standard.removeObject(forKey: key)
        }

        // Onboarding cache is keyed per-user
        if let userId = AuthService.shared.currentUserId {
            UserDefaults.standard.removeObject(forKey: "onboarding_completed_\(userId)")
        }
    }

    func preloadAvatar() {
        Task { @MainActor in
            // Prefer persisted URL so Settings avatar is ready immediately on open.
            let url = UserDefaults.standard.string(forKey: PreferenceKeys.myAvatarURL)
                ?? avatarURL
            if let url, !url.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                let _ = await avatarCacheManager.getCachedImage(urlString: url)
            }
        }
    }

    func loadProfileInfo() {
        Task { @MainActor in
            do {
                guard let token = await AuthService.shared.getAccessToken() else {
                    self.isProfileLoaded = false
                    return
                }
                let profileInfo = try await BackendService.shared.fetchProfileInfo(accessToken: token)
                self.fullName = profileInfo.full_name
                self.bio = profileInfo.bio
                self.isProfileLoaded = true
                UserDefaults.standard.set(self.fullName, forKey: "talktome_profile_full_name")
            } catch {
                print("Failed to load profile info: \(error)")
                self.isProfileLoaded = false
            }
        }
    }

    func saveProfileInfo(fullName: String, bio: String) async -> Bool {
        do {
            guard let token = await AuthService.shared.getAccessToken() else {
                return false
            }
            let response = try await BackendService.shared.updateProfile(
                accessToken: token,
                fullName: fullName,
                bio: bio.isEmpty ? nil : bio
            )
            if response.success {
                await MainActor.run {
                    self.fullName = fullName
                    self.bio = bio
                    self.isProfileLoaded = true
                    if self.fullName.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                        UserDefaults.standard.removeObject(forKey: "talktome_profile_full_name")
                    } else {
                        UserDefaults.standard.set(self.fullName, forKey: "talktome_profile_full_name")
                    }
                    NotificationCenter.default.post(name: .profileChanged, object: nil)
                }
            }
            return response.success
        } catch {
            print("Failed to save profile info: \(error)")
            return false
        }
    }

}

extension SettingsViewModel {
    func uploadAvatar(data: Data) async {
        Thread.callStackSymbols.forEach { print("  \($0)") }
        guard !data.isEmpty else {
            return
        }
        isUploadingAvatar = true
        defer { isUploadingAvatar = false }
        guard let token = await AuthService.shared.getAccessToken() else {
            return
        }
        let result = try? await BackendService.shared.uploadAvatar(imageData: data, contentType: "image/jpeg", accessToken: token)
        await MainActor.run {
            self.avatarURL = result?.url
        }
    }
}

