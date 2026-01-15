import Foundation
import Supabase
import AuthenticationServices


class AuthService: ObservableObject {
    static let shared = AuthService()
    private let providerSignIn = ProviderSignIn()

    let client: SupabaseClient
    private let redirectURL: URL

    @Published var isAuthenticated = false
    @Published var currentUser: User?
    @Published var isCheckingAuth = true
    @Published var lastAuthError: String?

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

        checkAuthStatus()
    }

    func signIn(_ provider: ProviderSignIn.Provider) async {
        do {
            let session = try await providerSignIn.signIn(provider: provider, redirectURL: redirectURL, client: client)
            await MainActor.run {
                self.lastAuthError = nil
                self.applyAuthenticatedSession(session)
            }
        } catch {
            await MainActor.run {
                self.lastAuthError = error.localizedDescription
            }
        }
    }

    func signOut() async {
        await MainActor.run {
            self.clearAuthenticatedSession()
            self.lastAuthError = nil
        }

        await unregisterPushTokenAfterSignOut()

        do {
            try await client.auth.signOut()
        } catch {
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

    private func unregisterPushTokenAfterSignOut() async {
        do {
            if let token = APNSService.shared.currentDeviceToken,
               let access = try? await client.auth.session.accessToken {
                try await BackendService.shared.unregisterPushToken(token: token, accessToken: access)
            }
        } catch {}
    }

    private func checkAuthStatus() {
        Task { @MainActor in self.isCheckingAuth = true }
        Task {
            do {
                let session = try await client.auth.session
                await MainActor.run {
                    self.applyAuthenticatedSession(session)
                }
            } catch {
                await MainActor.run {
                    self.clearAuthenticatedSession()
                }
            }
        }
    }

    @MainActor
    private func applyAuthenticatedSession(_ session: Session) {
        self.isAuthenticated = true
        self.currentUser = session.user
        self.isCheckingAuth = false
        UserDefaults.standard.set(session.user.id.uuidString, forKey: PreferenceKeys.currentUserId)
    }

    @MainActor
    private func clearAuthenticatedSession() {
        self.isAuthenticated = false
        self.currentUser = nil
        self.isCheckingAuth = false
        UserDefaults.standard.removeObject(forKey: PreferenceKeys.currentUserId)
    }

    static func getInfoPlistValue(for key: String) -> Any? {
        if let path = Bundle.main.path(forResource: "Secrets", ofType: "plist"),
           let plist = NSDictionary(contentsOfFile: path),
           let value = plist[key] {
            return value
        }
        return nil
    }
}
