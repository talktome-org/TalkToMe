import Foundation
import SwiftUI


@MainActor
class SettingsViewModel: ObservableObject {

    @Published var settingsData = SettingsData()
    @Published var settingsSections: [SettingsSection] = []
    @Published var isUploadingAvatar: Bool = false
    @Published var avatarURL: String? = nil
    @Published var showPersonalizationEdit: Bool = false
    @Published var shouldNavigateToContacts: Bool = false

    private let avatarCacheManager = AvatarCacheManager.shared

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
        case "Sign Out":
            Task {
                await AuthService.shared.signOut()
            }
        default:
            break
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

