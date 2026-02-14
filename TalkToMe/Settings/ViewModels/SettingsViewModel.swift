import Foundation
import SwiftUI


@MainActor
class SettingsViewModel: ObservableObject {

    @Published var settingsData = SettingsData()
    @Published var settingsSections: [SettingsSection] = []
    @Published var isUploadingAvatar: Bool = false
    @Published var avatarURL: String? = nil
    @Published var showPersonalizationEdit: Bool = false

    private let avatarCacheManager = AvatarCacheManager.shared

    @Published var fullName: String = ""
    @Published var bio: String = ""
    @Published var isProfileLoaded: Bool = false

    init() {
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

        if UserDefaults.standard.object(forKey: PreferenceKeys.voiceModeEnabled) != nil {
            settingsData.voiceModeEnabled = UserDefaults.standard.bool(forKey: PreferenceKeys.voiceModeEnabled)
        } else {
            settingsData.voiceModeEnabled = false
            UserDefaults.standard.set(false, forKey: PreferenceKeys.voiceModeEnabled)
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
                title: "",
                icon: "",
                gradient: [],
                settings: [
                    SettingItem(title: "Appearance", subtitle: nil, type: .picker(["Light", "Dark", "System"]), icon: "circle.lefthalf.filled"),
                    SettingItem(title: "Wallpapers", subtitle: nil, type: .navigation, icon: "photo")
                ]
            ),
            SettingsSection(
                title: "",
                icon: "",
                gradient: [],
                settings: [
                    SettingItem(title: "Notifications", subtitle: nil, type: .toggle(APNSService.shared.isPushEnabled), icon: "bell"),
                    SettingItem(title: "Haptics", subtitle: nil, type: .toggle(settingsData.hapticFeedbackEnabled), icon: "iphone.radiowaves.left.and.right"),
                    SettingItem(title: "Voice Mode", subtitle: nil, type: .toggle(settingsData.voiceModeEnabled), icon: "waveform")
                ]
            ),
            SettingsSection(
                title: "",
                icon: "",
                gradient: [],
                settings: [
                    SettingItem(title: "Contact Support", subtitle: nil, type: .navigation, icon: "envelope"),
                    SettingItem(title: "Privacy Policy", subtitle: nil, type: .navigation, icon: "hand.raised")
                ]
            ),
            SettingsSection(
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
        let section = settingsSections[sectionIndex]
        let setting = section.settings[settingIndex]

        switch (section.title, setting.title) {
        case ("App Settings", "Voice Mode"):
            settingsData.voiceModeEnabled.toggle()
            UserDefaults.standard.set(settingsData.voiceModeEnabled, forKey: PreferenceKeys.voiceModeEnabled)
        case ("App Settings", "Haptic Feedback"), ("App Settings", "Haptics"):
            settingsData.hapticFeedbackEnabled.toggle()
            UserDefaults.standard.set(settingsData.hapticFeedbackEnabled, forKey: PreferenceKeys.hapticsEnabled)
            if settingsData.hapticFeedbackEnabled {
                Haptics.selection()
            }
        case ("App Settings", "Push Notifications"), ("App Settings", "Notifications"):
            let current = UserDefaults.standard.object(forKey: "talktome_push_enabled") != nil ? UserDefaults.standard.bool(forKey: "talktome_push_enabled") : true
            let newValue = !current
            APNSService.shared.setPushEnabled(newValue)
            DispatchQueue.main.async { self.setupSettingsSections() }
        case ("Chat Settings", "Auto Scroll"):
            break
        default:
            break
        }

        setupSettingsSections()
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

