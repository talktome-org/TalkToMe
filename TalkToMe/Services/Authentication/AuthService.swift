import Foundation
import Supabase
import AuthenticationServices


class AuthService: ObservableObject {

    static let shared = AuthService()

    let client: SupabaseClient
    private let osVersion = ProcessInfo.processInfo.operatingSystemVersionString

    @Published var isAuthenticated = false
    @Published var currentUser: User?
    @Published var isLoadingInitialData = false

    private let redirectURL: URL

    private let appleService = AppleSignIn()
    private let googleService = GoogleSignIn()

    private init() {
        guard let supabaseURL = AuthService.getInfoPlistValue(for: "SUPABASE_URL") as? String,
              let supabaseKey = AuthService.getInfoPlistValue(for: "SUPABASE_PUBLISHABLE_KEY") as? String else {
            fatalError("Missing Supabase configuration in Secrets.plist")
        }

        let projectRef = URL(string: supabaseURL)?.host?.components(separatedBy: ".").first ?? ""
        let scheme = "supabase-\(projectRef)"
        guard let redirectURL = URL(string: "\(scheme)://auth/callback") else {
            fatalError("Failed to construct redirect URL for Supabase OAuth")
        }
        self.redirectURL = redirectURL
        print("[Auth] Init - iOS: \(osVersion)")
        print("[Auth] Init - Supabase URL: \(supabaseURL)")
        print("[Auth] Init - Redirect URL: \(redirectURL.absoluteString)")

        client = SupabaseClient(
            supabaseURL: URL(string: supabaseURL)!,
            supabaseKey: supabaseKey,
            options: SupabaseClientOptions(
                auth: .init(
                    storage: KeychainLocalStorage(),
                    redirectToURL: redirectURL
                )
            )
        )

        checkAuthStatus()  // Initialises auth state on app launch to check for an existing Supabase session
    }

    static func getInfoPlistValue(for key: String) -> Any? {
        if let path = Bundle.main.path(forResource: "Secrets", ofType: "plist"),
           let plist = NSDictionary(contentsOfFile: path),
           let value = plist[key] {
            return value
        }

        return nil
    }

    private func checkAuthStatus() {
        Task {
            do {
                let session = try await client.auth.session
                await MainActor.run {
                    self.isAuthenticated = true
                    self.currentUser = session.user
                    // Don't set loading state for existing sessions
                    self.isLoadingInitialData = false
                }
                await enableDailyCheckinsByDefaultIfFirstTime()
            } catch {
                await MainActor.run {
                    self.isAuthenticated = false
                    self.currentUser = nil
                    self.isLoadingInitialData = false
                }
            }
        }
    }

    func signInWithGoogle() async {
        print("[Auth] Google sign-in start - iOS: \(osVersion), redirect: \(redirectURL.absoluteString)")
        do {
            let session = try await googleService.signIn(redirectURL: redirectURL, client: client)
            print("[Auth] Google sign-in success - user id: \(session.user.id)")
            await MainActor.run {
                self.isLoadingInitialData = true
                self.isAuthenticated = true
                self.currentUser = session.user
            }
            await enableDailyCheckinsByDefaultIfFirstTime()
        } catch {
            let nsErr = error as NSError
            print("[Auth][Google] error domain: \(nsErr.domain), code: \(nsErr.code)")
            print("[Auth][Google] description: \(nsErr.localizedDescription)")
            if !nsErr.userInfo.isEmpty {
                print("[Auth][Google] userInfo: \(nsErr.userInfo)")
            }
            if let underlying = nsErr.userInfo[NSUnderlyingErrorKey] as? NSError {
                print("[Auth][Google] underlying: domain=\(underlying.domain) code=\(underlying.code) desc=\(underlying.localizedDescription)")
            }
        }
    }

    func signInWithApple(presentationAnchor anchor: ASPresentationAnchor) async {
        do {
            let session = try await appleService.signIn(presentationAnchor: anchor, client: client)
            await MainActor.run {
                self.isLoadingInitialData = true
                self.isAuthenticated = true
                self.currentUser = session.user
            }
            await enableDailyCheckinsByDefaultIfFirstTime()
        } catch {
            let nsErr = error as NSError
            print("[Auth][Apple] error domain: \(nsErr.domain), code: \(nsErr.code)")
            print("[Auth][Apple] description: \(nsErr.localizedDescription)")
            if !nsErr.userInfo.isEmpty {
                print("[Auth][Apple] userInfo: \(nsErr.userInfo)")
            }
            if let underlying = nsErr.userInfo[NSUnderlyingErrorKey] as? NSError {
                print("[Auth][Apple] underlying: domain=\(underlying.domain) code=\(underlying.code) desc=\(underlying.localizedDescription)")
            }
        }
    }

    func signOut() async {
        // Immediately update UI state
        await MainActor.run {
            self.isAuthenticated = false
            self.currentUser = nil
        }

        // Best-effort: unregister current push token server-side to prevent further pushes after sign-out
        do {
            if let token = PushNotificationManager.shared.currentDeviceToken,
               let access = try? await client.auth.session.accessToken {
                try await BackendService.shared.unregisterPushToken(token: token, accessToken: access)
            }
        } catch {
            // ignore
        }

        // Then perform the actual sign out
        do {
            try await client.auth.signOut()
        } catch {
            print("Sign out error: \(error)")
            // If sign out fails, restore the auth state
            checkAuthStatus()
        }
    }

    func getAccessToken() async -> String? {
        do {
            let session = try await client.auth.session
            let token = session.accessToken
            return token
        } catch {
            return nil
        }
    }

    func setInitialDataLoaded() {
        Task { @MainActor in
            self.isLoadingInitialData = false
        }
    }

    // MARK: - Defaults
    private func enableDailyCheckinsByDefaultIfFirstTime() async {
        // Ensure timezone is set for daily check-ins (runs on every sign-in, but only updates if needed)
        guard let token = await self.getAccessToken() else { return }
        do {
            // Check if timezone is already set in backend
            let status = try await BackendService.shared.getDailyCheckins(accessToken: token)
            if status.timezone == nil || status.timezone?.isEmpty == true {
                // Timezone not set - save preferences with user's actual timezone
                let tz = TimeZone.current.identifier
                try await BackendService.shared.setDailyCheckins(
                    enabled: true,
                    hour: 17,
                    minute: 00,
                    timezone: tz,
                    accessToken: token
                )
                UserDefaults.standard.set(true, forKey: PreferenceKeys.dailyCheckinsEnabled)
                print("[Auth] Daily check-ins initialized: enabled=true, hour=17, tz=\(tz)")
            } else {
                // Already configured, just sync local state
                UserDefaults.standard.set(status.enabled, forKey: PreferenceKeys.dailyCheckinsEnabled)
                print("[Auth] Daily check-ins already configured: enabled=\(status.enabled), tz=\(status.timezone ?? "nil")")
            }
        } catch {
            print("[Auth] Failed to initialize daily check-ins: \(error)")
        }
    }
}
