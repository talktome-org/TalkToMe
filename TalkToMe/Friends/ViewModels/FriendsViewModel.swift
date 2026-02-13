import Foundation

@MainActor
final class FriendsViewModel: ObservableObject {

    @Published private(set) var myCode: String? = nil
    @Published private(set) var myCodeExpiresAt: Date? = nil
    @Published private(set) var friends: [FriendSummary] = []
    @Published private(set) var lastActionMessage: String? = nil
    @Published private(set) var isRefreshingMyCode: Bool = false
    @Published private(set) var isAddingFriend: Bool = false
    @Published private(set) var isLoadingFriends: Bool = false
    @Published private(set) var friendsLoadErrorMessage: String? = nil

    private var accessTokenProvider: () async throws -> String
    private var userIdProvider: () -> String?
    private var refreshMyCodeTask: Task<Void, Never>? = nil
    private var addFriendTask: Task<Void, Never>? = nil
    private var loadFriendsTask: Task<Void, Never>? = nil

    private enum Cache {
        static let myCodeKey = "FriendsViewModel.myCode"
        static let myCodeExpiresAtKey = "FriendsViewModel.myCodeExpiresAt"
        static let friendsKey = "FriendsViewModel.friends"
    }

    init(
        accessTokenProvider: @escaping () async throws -> String,
        userIdProvider: @escaping () -> String? = { nil }
    ) {
        self.accessTokenProvider = accessTokenProvider
        self.userIdProvider = userIdProvider
        restoreMyCodeFromCache()
        restoreFriendsFromCache()
    }

    func refreshMyCode(force: Bool = false) async {
        if let existingTask = refreshMyCodeTask {
            await existingTask.value
            return
        }

        if !force, let expires = myCodeExpiresAt, expires > Date(), let code = myCode, !code.isEmpty {
            return
        }

        let task = Task { @MainActor [weak self] in
            guard let self else { return }
            defer { self.refreshMyCodeTask = nil }
            self.isRefreshingMyCode = true
            defer { self.isRefreshingMyCode = false }

            do {
                // Important: Supabase session/token retrieval can touch Keychain/disk.
                // Do the whole token+network flow off the main actor to avoid UI hangs.
                let accessTokenProvider = self.accessTokenProvider
                let dto = try await Task.detached(priority: .userInitiated) {
                    let token = try await accessTokenProvider()
                    return try await BackendService.shared.fetchMyFriendCode(accessToken: token)
                }.value

                self.myCode = dto.code
                self.myCodeExpiresAt = FriendsViewModel.parseISO8601(dto.expires_at)
                self.lastActionMessage = nil
                self.persistMyCodeToCache()
            } catch {
                // Don't wipe an existing code (especially in "offline but authenticated" mode).
                // Just surface a message if we have nothing to show.
                if (self.myCode ?? "").isEmpty {
                    self.lastActionMessage = error.localizedDescription
                }
            }
        }
        refreshMyCodeTask = task
        await task.value
    }

    func addFriendByCode(_ code: String) async {
        if let existingTask = addFriendTask {
            await existingTask.value
            return
        }

        let cleaned = code.trimmingCharacters(in: .whitespacesAndNewlines)
        guard cleaned.count == 4 else {
            lastActionMessage = "Enter a 4-digit code."
            return
        }

        let task = Task { @MainActor [weak self] in
            guard let self else { return }
            defer { self.addFriendTask = nil }
            self.isAddingFriend = true
            defer { self.isAddingFriend = false }
            self.lastActionMessage = nil

            do {
                let accessTokenProvider = self.accessTokenProvider
                let (friendId, updatedFriends) = try await Task.detached(priority: .userInitiated) {
                    let token = try await accessTokenProvider()
                    let friendId = try await BackendService.shared.addFriendByCode(code: cleaned, accessToken: token)
                    let friends = try await BackendService.shared.fetchFriends(accessToken: token)
                    return (friendId, friends)
                }.value

                self.friends = updatedFriends
                self.friendsLoadErrorMessage = nil

                // Warm avatar images so the Friends list sheet can render immediately.
                await warmFriendAvatars(updatedFriends)

                // Persist "partner" defaults so partner message UI can render name/avatar instantly.
                self.updatePartnerDefaults(from: updatedFriends, preferredPartnerUserId: friendId)

                self.persistFriendsToCache(updatedFriends)

                if let added = updatedFriends.first(where: { $0.id == friendId }) {
                    let name = added.fullName.trimmingCharacters(in: .whitespacesAndNewlines)
                    self.lastActionMessage = name.isEmpty ? "Added friend" : "Added \(name)"
                } else {
                    self.lastActionMessage = "Added friend"
                }
            } catch {
                self.lastActionMessage = error.localizedDescription
            }
        }

        addFriendTask = task
        await task.value
    }

    func loadFriends() async throws {
        if let existingTask = loadFriendsTask {
            await existingTask.value
            return
        }

        let task = Task { @MainActor [weak self] in
            guard let self else { return }
            defer { self.loadFriendsTask = nil }
            self.isLoadingFriends = true
            defer { self.isLoadingFriends = false }
            self.friendsLoadErrorMessage = nil

            do {
                let accessTokenProvider = self.accessTokenProvider
                let updatedFriends = try await Task.detached(priority: .userInitiated) {
                    let token = try await accessTokenProvider()
                    return try await BackendService.shared.fetchFriends(accessToken: token)
                }.value
                self.friends = updatedFriends
                await warmFriendAvatars(updatedFriends)
                self.updatePartnerDefaults(from: updatedFriends, preferredPartnerUserId: nil)
                self.persistFriendsToCache(updatedFriends)
            } catch {
                self.lastActionMessage = error.localizedDescription
                self.friendsLoadErrorMessage = error.localizedDescription
            }
        }

        loadFriendsTask = task
        await task.value
    }

    private func warmFriendAvatars(_ friends: [FriendSummary]) async {
        let urls = friends
            .compactMap { $0.avatarURL?.trimmingCharacters(in: .whitespacesAndNewlines) }
            .filter { !$0.isEmpty }
        guard !urls.isEmpty else { return }
        await AvatarCacheManager.shared.preloadAvatars(urls: urls)
    }

    private func updatePartnerDefaults(from friends: [FriendSummary], preferredPartnerUserId: UUID?) {
        // Determine which friend should be treated as the "partner" for UI purposes.
        // Priority:
        // - explicitly preferred (e.g. just added / explicitly picked)
        // - previously cached partner_user_id
        // - if exactly one friend exists, use it

        let preferredId = preferredPartnerUserId

        let cachedId: UUID? = {
            guard let raw = UserDefaults.standard.string(forKey: PreferenceKeys.partnerUserId),
                  let id = UUID(uuidString: raw) else { return nil }
            return id
        }()

        let resolvedId: UUID? = {
            if let preferredId { return preferredId }
            if let cachedId, friends.contains(where: { $0.id == cachedId }) { return cachedId }
            if friends.count == 1 { return friends.first?.id }
            return nil
        }()

        guard let partnerId = resolvedId,
              let partner = friends.first(where: { $0.id == partnerId }) else {
            return
        }

        let name = partner.fullName.trimmingCharacters(in: .whitespacesAndNewlines)
        if !name.isEmpty {
            UserDefaults.standard.set(name, forKey: PreferenceKeys.partnerName)
        }
        if let url = partner.avatarURL?.trimmingCharacters(in: .whitespacesAndNewlines), !url.isEmpty {
            UserDefaults.standard.set(url, forKey: PreferenceKeys.partnerAvatarURL)
        }
        if let voice = partner.voiceName?.trimmingCharacters(in: .whitespacesAndNewlines), !voice.isEmpty {
            UserDefaults.standard.set(voice, forKey: PreferenceKeys.partnerVoiceName)
        }
        UserDefaults.standard.set(partnerId.uuidString, forKey: PreferenceKeys.partnerUserId)
    }

    private static func parseISO8601(_ iso: String) -> Date? {
        let f1 = ISO8601DateFormatter()
        f1.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        if let d = f1.date(from: iso) { return d }
        let f2 = ISO8601DateFormatter()
        f2.formatOptions = [.withInternetDateTime]
        return f2.date(from: iso)
    }

    private func cacheKey(_ base: String) -> String {
        let raw = userIdProvider()?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        if raw.isEmpty {
            return base
        }
        return "\(base).\(raw)"
    }

    private func restoreMyCodeFromCache() {
        let codeKey = cacheKey(Cache.myCodeKey)
        let expiresKey = cacheKey(Cache.myCodeExpiresAtKey)

        let cachedCode = UserDefaults.standard.string(forKey: codeKey)
        let cachedExpires = UserDefaults.standard.object(forKey: expiresKey) as? Double

        if let cachedCode, !cachedCode.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            myCode = cachedCode
        }
        if let cachedExpires {
            myCodeExpiresAt = Date(timeIntervalSince1970: cachedExpires)
        }
    }

    private func persistMyCodeToCache() {
        let code = (myCode ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
        guard !code.isEmpty else { return }

        let codeKey = cacheKey(Cache.myCodeKey)
        let expiresKey = cacheKey(Cache.myCodeExpiresAtKey)

        UserDefaults.standard.set(code, forKey: codeKey)
        if let expiresAt = myCodeExpiresAt {
            UserDefaults.standard.set(expiresAt.timeIntervalSince1970, forKey: expiresKey)
        }
    }

    private func restoreFriendsFromCache() {
        let key = cacheKey(Cache.friendsKey)
        guard let raw = UserDefaults.standard.data(forKey: key) else { return }
        let decoded = (try? JSONDecoder().decode([FriendSummary].self, from: raw)) ?? []
        if !decoded.isEmpty {
            friends = decoded
            // Best-effort warm so the sidebar domino avatars render immediately.
            Task { await warmFriendAvatars(decoded) }
        }
    }

    private func persistFriendsToCache(_ friends: [FriendSummary]) {
        let key = cacheKey(Cache.friendsKey)
        guard let data = try? JSONEncoder().encode(friends) else { return }
        UserDefaults.standard.set(data, forKey: key)
    }
}

