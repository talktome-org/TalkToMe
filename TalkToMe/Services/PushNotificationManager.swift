import Foundation
import UserNotifications
import UIKit

final class PushNotificationManager: NSObject, ObservableObject {
    static let shared = PushNotificationManager()

    @Published private(set) var authorizationGranted: Bool = false
    @Published var isPushEnabled: Bool = true

    private(set) var currentDeviceToken: String?
    private var processingRequestIds: Set<UUID> = []

    var pendingRequestId: UUID?

    private override init() {
        super.init()
    }

    func requestAuthorizationAndRegister() {
        if Thread.isMainThread {
            let center = UNUserNotificationCenter.current()
            center.requestAuthorization(options: [.alert, .badge, .sound]) { [weak self] granted, _ in
                DispatchQueue.main.async {
                    self?.authorizationGranted = granted
                    if granted, self?.isPushEnabled ?? true {
                        UIApplication.shared.registerForRemoteNotifications()
                    }
                }
            }
        } else {
            DispatchQueue.main.async { [weak self] in self?.requestAuthorizationAndRegister() }
        }
    }

    func didReceiveDeviceToken(_ token: String) {
        currentDeviceToken = token
        print("[Push] Device token (hex)=\(token)")
        Task { await registerTokenWithBackendIfPossible(token) }
    }

    func tryUploadIfAuthenticated() {
        guard let token = currentDeviceToken else { return }
        Task { await registerTokenWithBackendIfPossible(token) }
    }

    private func registerTokenWithBackendIfPossible(_ token: String) async {
        do {
            let session = try await AuthService.shared.client.auth.session
            let accessToken = session.accessToken
            guard isPushEnabled else { return }
            try await BackendService.shared.registerPushToken(
                token: token,
                platform: "ios",
                bundleId: Bundle.main.bundleIdentifier ?? "",
                accessToken: accessToken
            )
        } catch {
        }
    }

    func setPushEnabled(_ enabled: Bool) {
        DispatchQueue.main.async {
            self.isPushEnabled = enabled
            UserDefaults.standard.set(enabled, forKey: "talktome_push_enabled")
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
                    } catch { }
                }
            }
            DispatchQueue.main.async { UIApplication.shared.unregisterForRemoteNotifications() }
        }
    }

    func loadPushEnabledFromDefaults() {
        if UserDefaults.standard.object(forKey: "talktome_push_enabled") != nil {
            isPushEnabled = UserDefaults.standard.bool(forKey: "talktome_push_enabled")
        } else {
            isPushEnabled = true
            UserDefaults.standard.set(true, forKey: "talktome_push_enabled")
        }
    }
}

extension PushNotificationManager: UNUserNotificationCenterDelegate {
    @MainActor
    func userNotificationCenter(_ center: UNUserNotificationCenter, willPresent notification: UNNotification) async -> UNNotificationPresentationOptions {
        let userInfo = notification.request.content.userInfo
        print("[Push] willPresent notification with userInfo: \(userInfo)")
        if let sessionIdString = userInfo["session_id"] as? String,
           let sessionId = UUID(uuidString: sessionIdString) {
            print("[Push] Partner message for session \(sessionId), authenticated=\(AuthService.shared.isAuthenticated)")
            if AuthService.shared.isAuthenticated {
                print("[Push] Posting partnerMessageReceived notification for session \(sessionId)")
                NotificationCenter.default.post(name: .partnerMessageReceived, object: nil, userInfo: ["sessionId": sessionId])
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


