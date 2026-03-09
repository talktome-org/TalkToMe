import Foundation
import UserNotifications
import UIKit

final class APNSService: NSObject, ObservableObject {
    static let shared = APNSService()

    @Published private(set) var authorizationGranted: Bool = false
    @Published var isPushEnabled: Bool = false

    private(set) var currentDeviceToken: String?
    private var processingRequestIds: Set<UUID> = []
    var pendingRequestId: UUID?

    private let pushEnabledDefaultsKey = "bobo_push_enabled"

    private override init() {
        super.init()
    }

    func requestAuthorizationAndRegister() {
        if Thread.isMainThread {
            let center = UNUserNotificationCenter.current()
            center.requestAuthorization(options: [.alert, .badge, .sound]) { [weak self] granted, _ in
                DispatchQueue.main.async {
                    guard let self else { return }
                    self.authorizationGranted = granted
                    if granted {
                        // Auto-enable only if the user has never explicitly set the toggle
                        // (i.e. first launch). Respect their choice if they turned it off.
                        let neverExplicitlySet = UserDefaults.standard.object(forKey: self.pushEnabledDefaultsKey) == nil
                        if neverExplicitlySet && !self.isPushEnabled {
                            self.isPushEnabled = true
                            UserDefaults.standard.set(true, forKey: self.pushEnabledDefaultsKey)
                        }
                        if self.isPushEnabled {
                            UIApplication.shared.registerForRemoteNotifications()
                        }
                    }
                }
            }
        } else {
            DispatchQueue.main.async { [weak self] in self?.requestAuthorizationAndRegister() }
        }
    }

    func didReceiveDeviceToken(_ token: String) {
        currentDeviceToken = token
        Task { await registerTokenWithBackendIfPossible(token) }
    }

    func tryUploadIfAuthenticated() {
        guard let token = currentDeviceToken else { return }
        Task { await registerTokenWithBackendIfPossible(token) }
    }

    private func registerTokenWithBackendIfPossible(_ token: String) async {
        let bundleId = Bundle.main.bundleIdentifier ?? ""
        do {
            let session = try await AuthService.shared.client.auth.session
            try await BackendService.shared.registerPushToken(
                token: token,
                platform: "ios",
                bundleId: bundleId,
                accessToken: session.accessToken
            )
        } catch {
            // Retry once after a short delay
            try? await Task.sleep(nanoseconds: 2_000_000_000)
            do {
                let session = try await AuthService.shared.client.auth.session
                try await BackendService.shared.registerPushToken(
                    token: token,
                    platform: "ios",
                    bundleId: bundleId,
                    accessToken: session.accessToken
                )
            } catch {}
        }
    }

    func setPushEnabled(_ enabled: Bool) {
        DispatchQueue.main.async {
            self.isPushEnabled = enabled
            UserDefaults.standard.set(enabled, forKey: self.pushEnabledDefaultsKey)
        }
        if enabled {
            if authorizationGranted {
                UIApplication.shared.registerForRemoteNotifications()
                tryUploadIfAuthenticated()
            } else {
                requestAuthorizationAndRegister()
            }
        } else {
            if let token = currentDeviceToken {
                Task {
                    do {
                        let session = try await AuthService.shared.client.auth.session
                        let accessToken = session.accessToken
                        try await BackendService.shared.unregisterPushToken(token: token, accessToken: accessToken)
                    } catch {}
                }
            }
            DispatchQueue.main.async { UIApplication.shared.unregisterForRemoteNotifications() }
        }
    }

    func loadPushEnabledFromDefaults() {
        if UserDefaults.standard.object(forKey: pushEnabledDefaultsKey) != nil {
            isPushEnabled = UserDefaults.standard.bool(forKey: pushEnabledDefaultsKey)
        } else {
            // Don't write to UserDefaults yet — leave the key absent so
            // requestAuthorizationAndRegister() can auto-enable on first OS grant.
            isPushEnabled = false
        }
    }
}

extension APNSService: UNUserNotificationCenterDelegate {
    @MainActor
    func userNotificationCenter(_ center: UNUserNotificationCenter, willPresent notification: UNNotification) async -> UNNotificationPresentationOptions {
        let userInfo = notification.request.content.userInfo

        // Silent unsend push — retract notification and refresh, no banner
        if let type = userInfo["type"] as? String, type == "partner_unsend" {
            if let sessionIdString = userInfo["session_id"] as? String,
               let sessionId = UUID(uuidString: sessionIdString) {
                handleUnsendPush(sessionId: sessionId)
            }
            return []
        }

        if let sessionIdString = userInfo["session_id"] as? String,
           let sessionId = UUID(uuidString: sessionIdString) {
            if AuthService.shared.isAuthenticated {
                NotificationCenter.default.post(name: .partnerMessageReceived, object: nil, userInfo: ["sessionId": sessionId])
            }
        }
        if let type = userInfo["type"] as? String, type == "friend_added" {
            if AuthService.shared.isAuthenticated {
                NotificationCenter.default.post(name: .friendAdded, object: nil)
            }
        }
        return [.banner, .list, .sound, .badge]
    }

    @MainActor
    func userNotificationCenter(_ center: UNUserNotificationCenter, didReceive response: UNNotificationResponse) async {
        let userInfo = response.notification.request.content.userInfo
        if let requestIdString = userInfo["request_id"] as? String,
           let requestId = UUID(uuidString: requestIdString) {
            if AuthService.shared.isAuthenticated {
                if pendingRequestId == requestId {
                    pendingRequestId = nil
                }
                guard processingRequestIds.insert(requestId).inserted else { return }
                NotificationCenter.default.post(name: .partnerRequestOpen, object: nil, userInfo: ["requestId": requestId])
                DispatchQueue.main.asyncAfter(deadline: .now() + 10) { [weak self] in self?.processingRequestIds.remove(requestId) }
            } else {
                pendingRequestId = requestId
            }
            return
        }

        if let sessionIdString = userInfo["session_id"] as? String,
           let sessionId = UUID(uuidString: sessionIdString) {
            if AuthService.shared.isAuthenticated {
                NotificationCenter.default.post(name: .partnerMessageOpen, object: nil, userInfo: ["sessionId": sessionId])
            } else {
                DispatchQueue.main.asyncAfter(deadline: .now() + 0.5) {
                    if AuthService.shared.isAuthenticated {
                        NotificationCenter.default.post(name: .partnerMessageOpen, object: nil, userInfo: ["sessionId": sessionId])
                    }
                }
            }
            return
        }

        if let type = userInfo["type"] as? String, type == "friend_added" {
            if AuthService.shared.isAuthenticated {
                NotificationCenter.default.post(name: .friendAdded, object: nil)
            }
        }
    }

    /// Called from AppDelegate for background silent pushes.
    @MainActor
    func handleBackgroundPush(userInfo: [AnyHashable: Any]) {
        if let type = userInfo["type"] as? String, type == "partner_unsend",
           let sessionIdString = userInfo["session_id"] as? String,
           let sessionId = UUID(uuidString: sessionIdString) {
            handleUnsendPush(sessionId: sessionId)
        }
    }

    private func handleUnsendPush(sessionId: UUID) {
        // Remove any delivered notifications for this session
        removeDeliveredNotifications(forSessionId: sessionId)
        // Notify the app to refresh
        if AuthService.shared.isAuthenticated {
            NotificationCenter.default.post(name: .partnerMessageUnsent, object: nil, userInfo: ["sessionId": sessionId])
        }
    }

    private func removeDeliveredNotifications(forSessionId sessionId: UUID) {
        let center = UNUserNotificationCenter.current()
        center.getDeliveredNotifications { notifications in
            let idsToRemove = notifications
                .filter { notification in
                    guard let nSessionId = notification.request.content.userInfo["session_id"] as? String else { return false }
                    return nSessionId == sessionId.uuidString
                }
                .map { $0.request.identifier }
            if !idsToRemove.isEmpty {
                center.removeDeliveredNotifications(withIdentifiers: idsToRemove)
            }
        }
    }

    @MainActor
    func consumePendingIfReady() {
        guard let req = pendingRequestId, AuthService.shared.isAuthenticated else { return }
        guard processingRequestIds.insert(req).inserted else {
            pendingRequestId = nil
            return
        }
        pendingRequestId = nil
        NotificationCenter.default.post(name: .partnerRequestOpen, object: nil, userInfo: ["requestId": req])
        DispatchQueue.main.asyncAfter(deadline: .now() + 10) { [weak self] in self?.processingRequestIds.remove(req) }
    }
}


