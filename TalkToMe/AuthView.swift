import SwiftUI
import AuthenticationServices
import UIKit

struct AuthView: View {
    private let authService = AuthService.shared

    var body: some View {
        ZStack {
            Color(.systemBackground)
                .ignoresSafeArea()

            // App icon - centered and moved up
            Image("AppIconDisplay")
                .resizable()
                .renderingMode(.original)
                .aspectRatio(contentMode: .fit)
                .frame(width: 100, height: 100)
                .cornerRadius(20)
                .offset(y: -60)

            // Sign in content
            VStack {
                Spacer()

                // Sign in buttons
                VStack(spacing: 12) {
                Button(action: {
                    Haptics.selection()
                    Task { await authService.signIn(.google) }
                }) {
                    HStack(spacing: 10) {
                        Image("icons8-google-48 copy")
                            .renderingMode(.original)
                            .resizable()
                            .aspectRatio(contentMode: .fit)
                            .frame(width: 20, height: 20)
                        Text("Continue with Google")
                            .font(.system(size: 16, weight: .medium))
                    }
                    .frame(maxWidth: .infinity)
                    .padding(.vertical, 14)
                    .background(Color(.systemBackground))
                    .foregroundColor(.primary)
                    .overlay(
                        RoundedRectangle(cornerRadius: 12)
                            .stroke(Color.gray.opacity(0.3), lineWidth: 1)
                    )
                    .cornerRadius(12)
                }

                Button(action: {
                    if let anchor = getPresentationAnchor() {
                        Haptics.selection()
                        Task { await authService.signIn(.apple(presentationAnchor: anchor)) }
                    }
                }) {
                    HStack(spacing: 10) {
                        Image(systemName: "applelogo")
                            .font(.system(size: 18, weight: .medium))
                        Text("Continue with Apple")
                            .font(.system(size: 16, weight: .medium))
                    }
                    .frame(maxWidth: .infinity)
                    .padding(.vertical, 14)
                    .background(Color.black)
                    .foregroundColor(.white)
                    .cornerRadius(12)
                }
                }
                .padding(.horizontal, 40)

                // Privacy policy and terms
                Text("By continuing, you agree to our Privacy Policy and Terms of Service")
                    .font(.footnote)
                    .foregroundColor(.secondary)
                    .multilineTextAlignment(.center)
                    .padding(.horizontal, 40)
                    .padding(.top, 20)
                    .padding(.bottom, 30)
            }
        }
    }

    private func getPresentationAnchor() -> ASPresentationAnchor? {
        return UIApplication.shared.connectedScenes
            .compactMap { $0 as? UIWindowScene }
            .flatMap { $0.windows }
            .first { $0.isKeyWindow }
    }
}

#Preview {
    AuthView()
}
