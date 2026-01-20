import Foundation

@MainActor
final class FriendsViewModel: ObservableObject {

    @Published private(set) var myCode: String? = nil
    @Published private(set) var myCodeExpiresAt: Date? = nil
    @Published private(set) var friends: [UUID] = []
    @Published private(set) var lastActionMessage: String? = nil
    @Published private(set) var isRefreshingMyCode: Bool = false
    @Published private(set) var isAddingFriend: Bool = false
    @Published private(set) var isLoadingFriends: Bool = false

    private var accessTokenProvider: () async throws -> String
    private var userIdProvider: () -> String?
    private var refreshMyCodeTask: Task<Void, Never>? = nil
    private var addFriendTask: Task<Void, Never>? = nil
    private var loadFriendsTask: Task<Void, Never>? = nil

    private enum Cache {
        static let myCodeKey = "FriendsViewModel.myCode"
        static let myCodeExpiresAtKey = "FriendsViewModel.myCodeExpiresAt"
    }

    init(
        accessTokenProvider: @escaping () async throws -> String,
        userIdProvider: @escaping () -> String? = { nil }
    ) {
        self.accessTokenProvider = accessTokenProvider
        self.userIdProvider = userIdProvider
        restoreMyCodeFromCache()
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
                self.lastActionMessage = "Added friend: \(friendId.uuidString)"
                UserDefaults.standard.set(true, forKey: PreferenceKeys.partnerConnected)
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

            do {
                let accessTokenProvider = self.accessTokenProvider
                let updatedFriends = try await Task.detached(priority: .userInitiated) {
                    let token = try await accessTokenProvider()
                    return try await BackendService.shared.fetchFriends(accessToken: token)
                }.value
                self.friends = updatedFriends
            } catch {
                self.lastActionMessage = error.localizedDescription
            }
        }

        loadFriendsTask = task
        await task.value
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
}

