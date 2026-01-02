import Foundation
import AuthenticationServices
import CryptoKit
import Supabase

final class AppleSignIn: NSObject {

    private var appleAuthDelegate: AppleAuthDelegate?

    func signIn(presentationAnchor anchor: ASPresentationAnchor, client: SupabaseClient) async throws -> Session {
        let nonce = randomNonceString()
        let request = ASAuthorizationAppleIDProvider().createRequest()
        request.requestedScopes = [.fullName, .email]
        request.nonce = sha256(nonce)

        let credential: ASAuthorizationAppleIDCredential = try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<ASAuthorizationAppleIDCredential, Error>) in
            Task { @MainActor in
                let controller = ASAuthorizationController(authorizationRequests: [request])
                let delegate = AppleAuthDelegate(continuation, anchor: anchor)
                self.appleAuthDelegate = delegate
                controller.delegate = delegate
                controller.presentationContextProvider = delegate
                controller.performRequests()
            }
        }

        guard let tokenData = credential.identityToken,
              let idToken = String(data: tokenData, encoding: .utf8) else {
            // Provide a clearer error to help diagnose common simulator/device issues
            let userInfo: [String: Any] = [
                NSLocalizedDescriptionKey: "Missing identity token",
                "hint": "Ensure the device/simulator is signed into an Apple ID and that 'Sign in with Apple' is enabled for this app/bundle. On Simulator, sign into Settings > Apple ID.",
            ]
            throw NSError(domain: "Auth", code: -2, userInfo: userInfo)
        }

        let session = try await client.auth.signInWithIdToken(
            credentials: .init(provider: .apple, idToken: idToken, nonce: nonce)
        )

        self.appleAuthDelegate = nil
        return session
    }

    private func randomNonceString(length: Int = 32) -> String {
        let charset: Array<Character> = Array("0123456789ABCDEFGHIJKLMNOPQRSTUVXYZabcdefghijklmnopqrstuvwxyz-._")
        var result = ""
        var remaining = length
        while remaining > 0 {
            var random: UInt8 = 0
            let status = SecRandomCopyBytes(kSecRandomDefault, 1, &random)
            if status == errSecSuccess {
                if random < charset.count {
                    result.append(charset[Int(random)])
                    remaining -= 1
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


