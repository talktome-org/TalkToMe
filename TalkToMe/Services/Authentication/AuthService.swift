import Foundation
import Supabase
import AuthenticationServices


class AuthService: ObservableObject {
    static let shared = AuthService()
    private let providerSignIn = ProviderSignIn()

    let client: SupabaseClient
    private let redirectURL: URL

    enum EmailSignUpOutcome {
        /// User is fully signed in (Supabase returned a session).
        case signedIn
        /// Account was created but Supabase did not return a session (typically because "Confirm email" is enabled).
        case needsEmailConfirmation
    }

    @Published var isAuthenticated = false
    @Published var currentUser: User?
    @Published var isCheckingAuth = true
    @Published var lastAuthError: String?

    /// A stable user id string for UI + cache scoping.
    /// - When online and authenticated, this is `currentUser.id`.
    /// - When offline but previously authenticated, we fall back to the cached id in `UserDefaults`.
    var currentUserId: String? {
        if let id = currentUser?.id.uuidString {
            return id
        }
        let cached = UserDefaults.standard.string(forKey: PreferenceKeys.currentUserId)
        let trimmed = cached?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        return trimmed.isEmpty ? nil : trimmed
    }

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
                UserDefaults.standard.set(false, forKey: PreferenceKeys.didExplicitSignOut)
                self.lastAuthError = nil
                self.applyAuthenticatedSession(session)
            }
        } catch {
            await MainActor.run {
                self.lastAuthError = error.localizedDescription
            }
        }
    }

    func signIn(email: String, password: String) async {
        do {
            let session = try await client.auth.signIn(email: email, password: password)
            await MainActor.run {
                UserDefaults.standard.set(false, forKey: PreferenceKeys.didExplicitSignOut)
                self.lastAuthError = nil
                self.applyAuthenticatedSession(session)
            }
        } catch {
            await MainActor.run {
                self.lastAuthError = error.localizedDescription
            }
        }
    }

    /// Creates a new Supabase auth user using email+password.
    /// - Note: If Supabase "Confirm email" is enabled, Supabase returns a user but **no session**.
    ///         In that case we return `.needsEmailConfirmation` and do not set `isAuthenticated=true`.
    func signUp(fullName: String, email: String, password: String) async -> EmailSignUpOutcome {
        do {
            print("[AuthService] signUp starting for email: \(email)")
            let response = try await client.auth.signUp(email: email, password: password)
            print("[AuthService] signUp succeeded – user id: \(response.user.id)")

            // If confirm-email is enabled, signUp succeeds but there is no session yet.
            let session: Session
            do {
                session = try await client.auth.session
                print("[AuthService] session obtained – accessToken present: \(!session.accessToken.isEmpty)")
            } catch {
                print("[AuthService] no session after signUp (confirm-email likely enabled): \(error)")
                await MainActor.run { self.lastAuthError = nil }
                return .needsEmailConfirmation
            }

            // Best-effort: persist the entered name into `public.profiles` immediately
            // so onboarding can start pre-filled.
            do {
                let trimmed = fullName.trimmingCharacters(in: .whitespacesAndNewlines)
                if !trimmed.isEmpty {
                    print("[AuthService] updating onboarding with name: \(trimmed)")
                    _ = try await BackendService.shared.updateOnboarding(
                        accessToken: session.accessToken,
                        update: .init(
                            onboarding_step: nil,
                            full_name: trimmed,
                            gender: nil,
                            date_of_birth: nil,
                            relationship_topics: nil
                        )
                    )
                    print("[AuthService] onboarding update succeeded")
                }
            } catch {
                print("[AuthService] onboarding update failed (non-blocking): \(error)")
            }

            await MainActor.run {
                UserDefaults.standard.set(false, forKey: PreferenceKeys.didExplicitSignOut)
                self.lastAuthError = nil
                self.applyAuthenticatedSession(session)
            }
            return .signedIn
        } catch {
            print("[AuthService] signUp FAILED: \(error)")
            print("[AuthService] signUp error localizedDescription: \(error.localizedDescription)")
            print("[AuthService] signUp error type: \(type(of: error))")
            if let urlError = error as? URLError {
                print("[AuthService] URLError code: \(urlError.code.rawValue)")
            }
            await MainActor.run {
                self.lastAuthError = error.localizedDescription
            }
            return .needsEmailConfirmation
        }
    }

    func signOut() async {
        await MainActor.run {
            UserDefaults.standard.set(true, forKey: PreferenceKeys.didExplicitSignOut)
            self.clearAuthenticatedSession()
            self.lastAuthError = nil
        }

        await unregisterPushTokenAfterSignOut()

        do {
            try await client.auth.signOut()
        } catch {
            // If we're offline (or sign-out fails), keep the app logged out. Don't re-check and resurrect the session.
            await MainActor.run {
                self.lastAuthError = error.localizedDescription
            }
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
                    UserDefaults.standard.set(false, forKey: PreferenceKeys.didExplicitSignOut)
                    self.applyAuthenticatedSession(session)
                }
            } catch {
                await MainActor.run {
                    // Important: "offline" must not be treated as "logged out".
                    // If we have a cached user id and the user didn't explicitly sign out, keep showing the app.
                    let didSignOut = UserDefaults.standard.bool(forKey: PreferenceKeys.didExplicitSignOut)
                    let cachedUserId = UserDefaults.standard.string(forKey: PreferenceKeys.currentUserId)
                    if didSignOut == false,
                       NetworkMonitor.shared.isOnline == false,
                       cachedUserId?.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty == false {
                        self.isAuthenticated = true
                        self.currentUser = nil
                        self.isCheckingAuth = false
                        return
                    }
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
