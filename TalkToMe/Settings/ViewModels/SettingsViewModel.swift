import Foundation
import SwiftUI


@MainActor
class SettingsViewModel: ObservableObject {

    @Published var settingsData = SettingsData()
    @Published var settingsSections: [SettingsSection] = []
    @Published var destination: SettingsDestination? = nil
    @Published var isUploadingAvatar: Bool = false
    @Published var avatarURL: String? = nil
    @Published var isConnectedToPartner: Bool = false
    @Published var partnerName: String? = nil
    @Published var partnerAvatarURL: String? = nil
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
        loadCachedPartnerConnection()
        loadDailyCheckinsFromBackend()
    }

    private func loadSettings() {
        if UserDefaults.standard.object(forKey: PreferenceKeys.hapticsEnabled) != nil {
            settingsData.hapticFeedbackEnabled = UserDefaults.standard.bool(forKey: PreferenceKeys.hapticsEnabled)
        } else {
            settingsData.hapticFeedbackEnabled = true
            UserDefaults.standard.set(true, forKey: PreferenceKeys.hapticsEnabled)
        }

        if let storedVoice = UserDefaults.standard.string(forKey: PreferenceKeys.ttsVoiceIdentifier) {
            settingsData.ttsVoiceIdentifier = storedVoice
        }

        if UserDefaults.standard.object(forKey: PreferenceKeys.dailyCheckinsEnabled) != nil {
            settingsData.dailyCheckinsEnabled = UserDefaults.standard.bool(forKey: PreferenceKeys.dailyCheckinsEnabled)
        } else {
            settingsData.dailyCheckinsEnabled = false
            UserDefaults.standard.set(false, forKey: PreferenceKeys.dailyCheckinsEnabled)
        }
    }

    private func setupSettingsSections() {
        settingsSections = [
            SettingsSection(
                title: "App Settings",
                icon: "gear",
                gradient: [Color.blue, Color.purple],
                settings: [
                    SettingItem(title: "Appearance", subtitle: nil, type: .picker(["Light", "Dark", "System"]), icon: "circle.lefthalf.filled"),
                    SettingItem(title: "Notifications", subtitle: nil, type: .toggle(PushNotificationManager.shared.isPushEnabled), icon: "bell"),
                    SettingItem(title: "Check-In Notifications", subtitle: nil, type: .toggle(settingsData.dailyCheckinsEnabled), icon: "sun.max"),
                    SettingItem(title: "Haptics", subtitle: nil, type: .toggle(settingsData.hapticFeedbackEnabled), icon: "iphone.radiowaves.left.and.right")
                ]
            ),
            SettingsSection(
                title: "Link Your Partner",
                icon: "link",
                gradient: [Color.pink, Color.purple],
                settings: [
                    SettingItem(title: "Link Your Partner", subtitle: "Invite or manage link", type: .linkPartner, icon: "link")
                ]
            ),
            SettingsSection(
                title: "Help & Support",
                icon: "questionmark.circle",
                gradient: [Color.green, Color.blue],
                settings: [
                    SettingItem(title: "Contact Support", subtitle: nil, type: .navigation, icon: "envelope"),
                    SettingItem(title: "Privacy Policy", subtitle: nil, type: .navigation, icon: "hand.raised")
                ]
            ),
            SettingsSection(
                title: "Account",
                icon: "person.circle",
                gradient: [Color.red, Color.orange],
                settings: {
                    var items: [SettingItem] = []
                    if isConnectedToPartner {
                        items.append(SettingItem(title: "Unlink Partner", subtitle: nil, type: .action, icon: "xmark.circle"))
                    }
                    items.append(SettingItem(title: "Sign Out", subtitle: nil, type: .action, icon: "rectangle.portrait.and.arrow.right"))
                    return items
                }()
            )
        ]
    }

    func toggleSetting(for sectionIndex: Int, settingIndex: Int) {
        let section = settingsSections[sectionIndex]
        let setting = section.settings[settingIndex]

        switch (section.title, setting.title) {
        case ("App Settings", "Haptic Feedback"), ("App Settings", "Haptics"):
            settingsData.hapticFeedbackEnabled.toggle()
            UserDefaults.standard.set(settingsData.hapticFeedbackEnabled, forKey: PreferenceKeys.hapticsEnabled)
            if settingsData.hapticFeedbackEnabled {
                Haptics.selection()
            }
        case ("App Settings", "Push Notifications"), ("App Settings", "Notifications"):
            let current = UserDefaults.standard.object(forKey: "talktome_push_enabled") != nil ? UserDefaults.standard.bool(forKey: "talktome_push_enabled") : true
            let newValue = !current
            PushNotificationManager.shared.setPushEnabled(newValue)
            DispatchQueue.main.async { self.setupSettingsSections() }
        case ("App Settings", "Check-In Notifications"):
            let newValue = !settingsData.dailyCheckinsEnabled
            settingsData.dailyCheckinsEnabled = newValue
            UserDefaults.standard.set(newValue, forKey: PreferenceKeys.dailyCheckinsEnabled)
            Task {
                if let token = await AuthService.shared.getAccessToken() {
                    let tz = TimeZone.current.identifier
                    try? await BackendService.shared.setDailyCheckins(
                        enabled: newValue,
                        hour: 10,
                        minute: 00,
                        timezone: tz,
                        accessToken: token
                    )
                }
            }
        case ("Chat Settings", "Auto Scroll"):
            break
        default:
            break
        }

        setupSettingsSections()
    }

    private func loadDailyCheckinsFromBackend() {
        Task { @MainActor in
            guard let token = await AuthService.shared.getAccessToken() else { return }
            do {
                let status = try await BackendService.shared.getDailyCheckins(accessToken: token)
                self.settingsData.dailyCheckinsEnabled = status.enabled
                UserDefaults.standard.set(status.enabled, forKey: PreferenceKeys.dailyCheckinsEnabled)
                self.setupSettingsSections()
            } catch {
                // Silent failure; keep local default
            }
        }
    }

    func handleSettingAction(for sectionIndex: Int, settingIndex: Int) {
        let section = settingsSections[sectionIndex]
        let setting = section.settings[settingIndex]

        switch setting.title {
        case "Link Your Partner":
            break
        case "Notifications":
            break
        case "Contact Support":
            destination = .contactSupport
        case "Privacy Policy":
            destination = .privacyPolicy
        case "Sign Out":
            Task {
                await AuthService.shared.signOut()
                await MainActor.run {
                    self.isConnectedToPartner = false
                    self.partnerName = nil
                    self.partnerAvatarURL = nil
                    self.clearPartnerConnectionCache()
                }
            }
        default:
            break
        }
    }

    func loadPartnerConnectionStatus() {
        Task { @MainActor in
            do {
                guard let token = await AuthService.shared.getAccessToken() else {
                    return
                }
                let partnerInfo = try await BackendService.shared.fetchPartnerInfo(accessToken: token)
                self.isConnectedToPartner = partnerInfo.linked

                if partnerInfo.linked, let partner = partnerInfo.partner {
                    self.partnerName = partner.name
                    self.partnerAvatarURL = partner.avatar_url
                    self.savePartnerConnectionCache()

                    if let url = self.partnerAvatarURL, !url.isEmpty {
                        Task { [weak self] in
                            guard let self = self else { return }
                            _ = await self.avatarCacheManager.getCachedImage(urlString: url)
                        }
                    }
                } else {
                    self.isConnectedToPartner = false
                    self.partnerName = nil
                    self.partnerAvatarURL = nil
                    self.clearPartnerConnectionCache()
                }
                self.setupSettingsSections()
            } catch {
                print("Failed to load partner connection status: \(error)")
            }
        }
    }

    func preloadAvatar() {
        Task { @MainActor in
            if let avatarURL = avatarURL, !avatarURL.isEmpty {
                let _ = await avatarCacheManager.getCachedImage(urlString: avatarURL)
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

    func applyPartnerInfo(_ info: BackendService.PartnerInfo?) {
        if let info = info, info.linked, let partner = info.partner {
            self.isConnectedToPartner = true
            self.partnerName = partner.name
            self.partnerAvatarURL = partner.avatar_url
            self.savePartnerConnectionCache()
            if let url = self.partnerAvatarURL, !url.isEmpty {
                Task { [weak self] in
                    guard let self = self else { return }
                    _ = await self.avatarCacheManager.getCachedImage(urlString: url)
                }
            }
        } else {
            self.isConnectedToPartner = false
            self.partnerName = nil
            self.partnerAvatarURL = nil
            self.clearPartnerConnectionCache()
        }
        self.setupSettingsSections()
    }

    private func loadCachedPartnerConnection() {
        if UserDefaults.standard.object(forKey: PreferenceKeys.partnerConnected) != nil {
            let connected = UserDefaults.standard.bool(forKey: PreferenceKeys.partnerConnected)
            self.isConnectedToPartner = connected
            if connected {
                self.partnerName = UserDefaults.standard.string(forKey: PreferenceKeys.partnerName)
                self.partnerAvatarURL = UserDefaults.standard.string(forKey: PreferenceKeys.partnerAvatarURL)
                if let url = self.partnerAvatarURL, !url.isEmpty {
                    Task { [weak self] in
                        guard let self = self else { return }
                        _ = await self.avatarCacheManager.getCachedImage(urlString: url)
                    }
                }
            } else {
                self.partnerName = nil
                self.partnerAvatarURL = nil
            }
        }
    }

    private func savePartnerConnectionCache() {
        UserDefaults.standard.set(self.isConnectedToPartner, forKey: PreferenceKeys.partnerConnected)
        if self.isConnectedToPartner {
            if let name = self.partnerName {
                UserDefaults.standard.set(name, forKey: PreferenceKeys.partnerName)
            }
            if let avatar = self.partnerAvatarURL {
                UserDefaults.standard.set(avatar, forKey: PreferenceKeys.partnerAvatarURL)
            }
        }
    }

    private func clearPartnerConnectionCache() {
        UserDefaults.standard.set(false, forKey: PreferenceKeys.partnerConnected)
        UserDefaults.standard.removeObject(forKey: PreferenceKeys.partnerName)
        UserDefaults.standard.removeObject(forKey: PreferenceKeys.partnerAvatarURL)
    }

    func preloadPartnerAvatarIfAvailable() {
        Task { @MainActor in
            if let url = self.partnerAvatarURL, !url.isEmpty {
                _ = await self.avatarCacheManager.getCachedImage(urlString: url)
            }
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

