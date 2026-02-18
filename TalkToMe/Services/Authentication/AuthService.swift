import Foundation
import Supabase
import AuthenticationServices


class AuthService: ObservableObject {
    static let shared = AuthService()
    private let providerSignIn = ProviderSignIn()
    private var authStateChangesTask: Task<Void, Never>?

    let client: SupabaseClient
    private let redirectURL: URL

    enum EmailSignUpOutcome {
        /// User is fully signed in (Supabase returned a session).
        case signedIn
        /// Account was created but Supabase did not return a session (typically because "Confirm email" is enabled).
        case needsEmailConfirmation
        /// An account with this email is already registered.
        case emailAlreadyExists
        /// Sign-up failed for another reason.
        case failed
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

        observeAuthStateChanges()
        checkAuthStatus()
    }

    deinit {
        authStateChangesTask?.cancel()
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
                self.lastAuthError = self.userFacingAuthMessage(for: error, flow: .signIn)
            }
        }
    }

    func signIn(email: String, password: String) async {
        let normalizedEmail = normalizeEmail(email)
        await MainActor.run { self.lastAuthError = nil }
        do {
            let session = try await client.auth.signIn(email: normalizedEmail, password: password)
            await MainActor.run {
                UserDefaults.standard.set(false, forKey: PreferenceKeys.didExplicitSignOut)
                self.lastAuthError = nil
                self.applyAuthenticatedSession(session)
            }
        } catch {
            await MainActor.run {
                self.lastAuthError = self.userFacingAuthMessage(for: error, flow: .signIn)
            }
        }
    }

    func checkEmailExists(email: String) async throws -> Bool {
        let normalizedEmail = normalizeEmail(email)
        return try await BackendService.shared.checkEmailExists(email: normalizedEmail)
    }

    /// Creates a new Supabase auth user using email+password.
    /// - Note: If Supabase "Confirm email" is enabled, Supabase returns a user but **no session**.
    ///         In that case we return `.needsEmailConfirmation` and do not set `isAuthenticated=true`.
    func signUp(fullName: String? = nil, email: String, password: String) async -> EmailSignUpOutcome {
        let normalizedEmail = normalizeEmail(email)
        await MainActor.run { self.lastAuthError = nil }

        do {
            print("[AuthService] signUp starting for email: \(normalizedEmail)")
            let response = try await client.auth.signUp(email: normalizedEmail, password: password)
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
                let trimmed = fullName?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
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
            print("[AuthService] signUp failed: \(error)")
            let outcome = signUpOutcome(for: error)
            await MainActor.run {
                self.lastAuthError = self.userFacingAuthMessage(for: error, flow: .signUp)
            }
            return outcome
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

    /// Handles auth callback URLs from deep links.
    /// Supports:
    /// - OAuth/PKCE callback links (`code`, `access_token`, etc.)
    /// - Email confirmation links with `token_hash` + `type` (e.g. `signup`)
    func handleAuthCallbackURL(_ url: URL) async {
        let params = Self.urlParams(from: url)

        // Email confirmation / magic link callback path.
        if let tokenHash = params["token_hash"],
           let rawType = params["type"],
           let otpType = EmailOTPType(rawValue: rawType.lowercased()) {
            do {
                _ = try await client.auth.verifyOTP(tokenHash: tokenHash, type: otpType)
            } catch {
                await MainActor.run { self.lastAuthError = error.localizedDescription }
            }
            return
        }

        // OAuth / PKCE callback path.
        if params["code"] != nil || params["access_token"] != nil {
            do {
                _ = try await client.auth.session(from: url)
            } catch {
                await MainActor.run { self.lastAuthError = error.localizedDescription }
            }
            return
        }

        // Fallback to SDK's generic handler for any future callback variants.
        client.auth.handle(url)
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

    private static func urlParams(from url: URL) -> [String: String] {
        var result: [String: String] = [:]

        if let comps = URLComponents(url: url, resolvingAgainstBaseURL: false),
           let queryItems = comps.queryItems {
            for item in queryItems {
                if let value = item.value, !value.isEmpty {
                    result[item.name] = value
                }
            }
        }

        if let fragment = URLComponents(url: url, resolvingAgainstBaseURL: false)?.fragment,
           !fragment.isEmpty {
            for pair in fragment.split(separator: "&") {
                let parts = pair.split(separator: "=", maxSplits: 1, omittingEmptySubsequences: false)
                guard parts.count == 2 else { continue }
                let key = String(parts[0]).removingPercentEncoding ?? String(parts[0])
                let value = String(parts[1]).removingPercentEncoding ?? String(parts[1])
                if !key.isEmpty, !value.isEmpty {
                    result[key] = value
                }
            }
        }

        return result
    }

    private func observeAuthStateChanges() {
        authStateChangesTask?.cancel()
        authStateChangesTask = Task { [weak self] in
            guard let self else { return }
            for await (event, session) in self.client.auth.authStateChanges {
                if Task.isCancelled { return }
                await self.handleAuthStateChange(event: event, session: session)
            }
        }
    }

    private func handleAuthStateChange(event: AuthChangeEvent, session: Session?) async {
        await MainActor.run {
            switch event {
            case .initialSession:
                if let session {
                    UserDefaults.standard.set(false, forKey: PreferenceKeys.didExplicitSignOut)
                    self.lastAuthError = nil
                    self.applyAuthenticatedSession(session)
                }
            case .signedIn, .tokenRefreshed, .userUpdated, .passwordRecovery, .mfaChallengeVerified:
                guard let session else { return }
                UserDefaults.standard.set(false, forKey: PreferenceKeys.didExplicitSignOut)
                self.lastAuthError = nil
                self.applyAuthenticatedSession(session)
            case .signedOut:
                // Mirror startup behavior: if the user did not explicitly sign out and we're offline,
                // keep the app in cached-auth mode.
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
            case .userDeleted:
                self.clearAuthenticatedSession()
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

    private enum AuthFlow {
        case signIn
        case signUp
    }

    private static let duplicateEmailMessage = "An account with this email already exists. Try a different one or log in."

    private func normalizeEmail(_ value: String) -> String {
        value.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    }

    private func signUpOutcome(for error: Error) -> EmailSignUpOutcome {
        isDuplicateEmailError(error) ? .emailAlreadyExists : .failed
    }

    private func isDuplicateEmailError(_ error: Error) -> Bool {
        if let authError = error as? AuthError {
            if case let .api(_, errorCode, _, _) = authError {
                switch errorCode {
                case .emailExists, .userAlreadyExists, .identityAlreadyExists, .emailConflictIdentityNotDeletable, .conflict:
                    return true
                default:
                    break
                }
            }
        }

        let text = error.localizedDescription.lowercased()
        return text.contains("already registered")
            || text.contains("already exists")
            || (text.contains("email") && text.contains("exists"))
    }

    private func userFacingAuthMessage(for error: Error, flow: AuthFlow) -> String {
        if let urlError = error as? URLError {
            switch urlError.code {
            case .notConnectedToInternet, .networkConnectionLost, .timedOut, .cannotFindHost, .cannotConnectToHost:
                return "Network error. Check your internet connection and try again."
            default:
                break
            }
        }

        if let authError = error as? AuthError {
            switch authError {
            case let .weakPassword(message, reasons):
                if !reasons.isEmpty { return reasons.joined(separator: " ") }
                return message.isEmpty ? "Password is too weak. Try a stronger password." : message
            case let .api(message, errorCode, _, _):
                switch errorCode {
                case .emailExists, .userAlreadyExists, .identityAlreadyExists, .emailConflictIdentityNotDeletable, .conflict:
                    return Self.duplicateEmailMessage
                case .invalidCredentials, .userNotFound:
                    return "Invalid email or password."
                case .emailNotConfirmed:
                    return "Please verify your email first, then log in."
                case .overRequestRateLimit, .overEmailSendRateLimit:
                    return "Too many attempts. Please wait a moment and try again."
                case .signupDisabled:
                    return "Sign-up is currently unavailable. Please try again later."
                case .emailAddressNotAuthorized:
                    return "This email address is not authorized for sign-up."
                case .requestTimeout:
                    return "The request timed out. Please try again."
                default:
                    if !message.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                        return message
                    }
                }
            default:
                break
            }
        }

        let fallback = error.localizedDescription.trimmingCharacters(in: .whitespacesAndNewlines)
        if !fallback.isEmpty {
            return fallback
        }
        return flow == .signUp ? "Could not create account. Please try again." : "Could not log in. Please try again."
    }
}
