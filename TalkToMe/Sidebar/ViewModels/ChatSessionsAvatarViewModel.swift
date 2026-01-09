import Foundation
import UIKit

extension ChatSessionsViewModel {

    func loadPairedAvatars() async {
        do {
            let session = try await AuthService.shared.client.auth.session
            let accessToken = session.accessToken
            let res = try await BackendService.shared.fetchPairedAvatars(accessToken: accessToken)
            await MainActor.run {
                self.myAvatarURL = res.me.url
                self.partnerAvatarURL = res.partner.url
                if let myURL = res.me.url, !myURL.isEmpty {
                    UserDefaults.standard.set(myURL, forKey: PreferenceKeys.myAvatarURL)
                } else {
                    UserDefaults.standard.removeObject(forKey: PreferenceKeys.myAvatarURL)
                }
            }
        } catch {
            print("Failed to load avatars: \(error)")
        }
    }

    func preloadAvatars() async {
        var avatarURLs: [String] = []
        if let myAvatar = myAvatarURL, !myAvatar.isEmpty {
            avatarURLs.append(myAvatar)
        }
        if let partnerAvatar = partnerAvatarURL, !partnerAvatar.isEmpty {
            avatarURLs.append(partnerAvatar)
        }
        if !avatarURLs.isEmpty {
            await avatarCacheManager.preloadAvatars(urls: avatarURLs)
        }
    }

    func ensureProfilePictureCached() async {
        if let myAvatar = myAvatarURL, !myAvatar.isEmpty {
            _ = await avatarCacheManager.getCachedImage(urlString: myAvatar)
        }
    }

    func getCachedAvatar(urlString: String?) async -> UIImage? {
        guard let urlString = urlString, !urlString.isEmpty else { return nil }
        return await avatarCacheManager.getCachedImage(urlString: urlString)
    }
}


