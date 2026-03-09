import Foundation
import AuthenticationServices
import CryptoKit
import Supabase


final class ProviderSignIn {
    enum Provider {
        case apple(presentationAnchor: ASPresentationAnchor)
        case google
    }

    private let apple = AppleSignIn()
    private let google = GoogleSignIn()

    func signIn(provider: Provider, redirectURL: URL, client: SupabaseClient) async throws -> Session {
        switch provider {
        case .google:
            return try await google.signIn(redirectURL: redirectURL, client: client)
        case let .apple(presentationAnchor: anchor):
            return try await apple.signIn(presentationAnchor: anchor, client: client)
        }
    }
}


final class GoogleSignIn {
    func signIn(redirectURL: URL, client: SupabaseClient) async throws -> Session {
        return try await client.auth.signInWithOAuth(
            provider: .google,
            redirectTo: redirectURL,
            queryParams: [(name: "prompt", value: "select_account")]
        )
    }
}

final class AppleSignIn: NSObject {
    private var appleAuthDelegate: AppleAuthDelegate?

    func signIn(presentationAnchor anchor: ASPresentationAnchor, client: SupabaseClient) async throws -> Session {
        let nonce = randomNonceString()
        defer { self.appleAuthDelegate = nil }

        let request = ASAuthorizationAppleIDProvider().createRequest()
        request.requestedScopes = [.fullName, .email]
        request.nonce = sha256(nonce)

        let credential = try await fetchCredential(presentationAnchor: anchor, request: request)
        guard let tokenData = credential.identityToken,
              let idToken = String(data: tokenData, encoding: .utf8) else {
            let userInfo: [String: Any] = [
                NSLocalizedDescriptionKey: "Missing identity token",
                "hint": "Ensure the device/simulator is signed into an Apple ID and that 'Sign in with Apple' is enabled for this app/bundle. On Simulator, sign into Settings > Apple ID.",
            ]
            throw NSError(domain: "Auth", code: -2, userInfo: userInfo)
        }

        return try await client.auth.signInWithIdToken(
            credentials: .init(provider: .apple, idToken: idToken, nonce: nonce)
        )
    }

    private func fetchCredential(presentationAnchor anchor: ASPresentationAnchor, request: ASAuthorizationAppleIDRequest) async throws -> ASAuthorizationAppleIDCredential {
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<ASAuthorizationAppleIDCredential, Error>) in
            Task { @MainActor in
                let controller = ASAuthorizationController(authorizationRequests: [request])
                let delegate = AppleAuthDelegate(continuation, anchor: anchor)
                self.appleAuthDelegate = delegate
                controller.delegate = delegate
                controller.presentationContextProvider = delegate
                controller.performRequests()
            }
        }
    }

    private func randomNonceString(length: Int = 32) -> String {
        let charset: Array<Character> = Array("0123456789ABCDEFGHIJKLMNOPQRSTUVXYZabcdefghijklmnopqrstuvwxyz-._")
        var result = ""
        result.reserveCapacity(length)
        while result.count < length {
            var random: UInt8 = 0
            let status = SecRandomCopyBytes(kSecRandomDefault, 1, &random)
            if status == errSecSuccess {
                if random < charset.count {
                    result.append(charset[Int(random)])
                }
            }
        }
        return result
    }

    private func sha256(_ input: String) -> String {
        let data = Data(input.utf8)
        let hashed = SHA256.hash(data: data)
        return hashed.compactMap { String(format: "%02x", $0) }.joined()
    }
}

private final class AppleAuthDelegate: NSObject, ASAuthorizationControllerDelegate, ASAuthorizationControllerPresentationContextProviding {
    let continuation: CheckedContinuation<ASAuthorizationAppleIDCredential, Error>
    let anchor: ASPresentationAnchor

    init(_ continuation: CheckedContinuation<ASAuthorizationAppleIDCredential, Error>, anchor: ASPresentationAnchor) {
        self.continuation = continuation
        self.anchor = anchor
    }

    func authorizationController(controller: ASAuthorizationController, didCompleteWithAuthorization authorization: ASAuthorization) {
        guard let credential = authorization.credential as? ASAuthorizationAppleIDCredential else {
            continuation.resume(throwing: NSError(domain: "Auth", code: -1, userInfo: [NSLocalizedDescriptionKey: "Invalid Apple credential"]))
            return
        }
        continuation.resume(returning: credential)
    }

    func authorizationController(controller: ASAuthorizationController, didCompleteWithError error: Error) {
        continuation.resume(throwing: error)
    }

    func presentationAnchor(for controller: ASAuthorizationController) -> ASPresentationAnchor {
        return anchor
    }
}