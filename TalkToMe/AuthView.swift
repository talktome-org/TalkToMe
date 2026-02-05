import SwiftUI
import AuthenticationServices
import UIKit

struct AuthView: View {
    @ObservedObject private var network = NetworkMonitor.shared

    private let authService = AuthService.shared

    @State private var showOfflineAlert: Bool = false
    @State private var emailAuthMode: EmailAuthMode? = nil

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
                        guard network.isOnline else {
                            showOfflineAlert = true
                            return
                        }
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
                            guard network.isOnline else {
                                showOfflineAlert = true
                                return
                            }
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

                    Divider()
                        .padding(.vertical, 6)

                    Button(action: {
                        Haptics.selection()
                        emailAuthMode = .login
                    }) {
                        Text("Log in with email")
                            .font(.system(size: 16, weight: .semibold))
                            .frame(maxWidth: .infinity)
                            .padding(.vertical, 14)
                            .background(Color(.secondarySystemBackground))
                            .foregroundColor(.primary)
                            .overlay(
                                RoundedRectangle(cornerRadius: 12)
                                    .stroke(Color.primary.opacity(0.08), lineWidth: 1)
                            )
                            .cornerRadius(12)
                    }

                    Button(action: {
                        Haptics.selection()
                        emailAuthMode = .signUp
                    }) {
                        Text("Create account")
                            .font(.system(size: 16, weight: .semibold))
                            .frame(maxWidth: .infinity)
                            .padding(.vertical, 14)
                            .background(Color.purple)
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
        .alert("You’re offline", isPresented: $showOfflineAlert) {
            Button("OK", role: .cancel) {}
        } message: {
            Text("Connect to the internet to sign in or create an account.")
        }
        .sheet(item: $emailAuthMode) { mode in
            NavigationStack {
                EmailAuthSheetView(initialMode: mode)
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

private enum EmailAuthMode: Identifiable {
    case login
    case signUp

    var id: Int {
        switch self {
        case .login: return 1
        case .signUp: return 2
        }
    }
}

private struct EmailAuthSheetView: View {
    let initialMode: EmailAuthMode
    @State private var showingLogin: Bool

    init(initialMode: EmailAuthMode) {
        self.initialMode = initialMode
        _showingLogin = State(initialValue: initialMode == .login)
    }

    var body: some View {
        Group {
            if showingLogin {
                EmailLoginView(onSwitchToSignUp: { showingLogin = false })
            } else {
                EmailSignUpView(onSwitchToLogIn: { showingLogin = true })
            }
        }
    }
}

private struct AuthTextFieldStyle: ViewModifier {
    func body(content: Content) -> some View {
        content
            .padding(.horizontal, 16)
            .padding(.vertical, 16)
            .background(
                RoundedRectangle(cornerRadius: 14, style: .continuous)
                    .fill(Color(UIColor.secondarySystemBackground))
            )
    }
}

private struct EmailLoginView: View {
    var onSwitchToSignUp: (() -> Void)? = nil
    @ObservedObject private var network = NetworkMonitor.shared
    @ObservedObject private var authService = AuthService.shared
    @Environment(\.dismiss) private var dismiss

    @State private var email: String = ""
    @State private var password: String = ""
    @State private var isSubmitting: Bool = false
    @State private var showOfflineAlert: Bool = false

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 28) {
                VStack(alignment: .leading, spacing: 8) {
                    Text("Welcome back")
                        .font(.system(size: 28, weight: .bold))
                    Text("Sign in with your email and password.")
                        .font(.subheadline)
                        .foregroundStyle(.secondary)
                }

                VStack(spacing: 14) {
                    TextField("Email", text: $email)
                        .keyboardType(.emailAddress)
                        .textInputAutocapitalization(.never)
                        .autocorrectionDisabled(true)
                        .textContentType(.username)
                        .modifier(AuthTextFieldStyle())

                    SecureField("Password", text: $password)
                        .textContentType(.password)
                        .modifier(AuthTextFieldStyle())
                }

                if let err = authService.lastAuthError, !err.isEmpty {
                    Text(err)
                        .font(.footnote)
                        .foregroundStyle(.red)
                }

                Button(action: submit) {
                    HStack(spacing: 10) {
                        if isSubmitting {
                            ProgressView().tint(.white)
                        }
                        Text(isSubmitting ? "Signing in…" : "Sign in")
                            .font(.system(size: 17, weight: .semibold))
                    }
                    .frame(maxWidth: .infinity)
                    .padding(.vertical, 16)
                    .background(Color.purple)
                    .foregroundColor(.white)
                    .clipShape(RoundedRectangle(cornerRadius: 14, style: .continuous))
                }
                .disabled(isSubmitting)
                .padding(.top, 4)

                if let switchUp = onSwitchToSignUp {
                    HStack(spacing: 4) {
                        Text("Don't have an account?")
                            .foregroundStyle(.secondary)
                        Button("Create account", action: {
                            Haptics.selection()
                            switchUp()
                        })
                        .fontWeight(.semibold)
                        .foregroundStyle(.purple)
                    }
                    .font(.subheadline)
                    .frame(maxWidth: .infinity)
                    .padding(.top, 20)
                }
            }
            .padding(.horizontal, 24)
            .padding(.top, 20)
            .padding(.bottom, 32)
        }
        .scrollDismissesKeyboard(.interactively)
        .navigationBarTitleDisplayMode(.inline)
        .toolbar {
            ToolbarItem(placement: .topBarLeading) {
                Button("Close") { dismiss() }
            }
        }
        .alert("You’re offline", isPresented: $showOfflineAlert) {
            Button("OK", role: .cancel) {}
        } message: {
            Text("Connect to the internet to log in.")
        }
    }

    private func submit() {
        Haptics.selection()
        guard network.isOnline else {
            showOfflineAlert = true
            return
        }

        let e = email.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !e.isEmpty, !password.isEmpty else { return }

        isSubmitting = true
        Task {
            await authService.signIn(email: e, password: password)
            await MainActor.run {
                isSubmitting = false
                if authService.isAuthenticated {
                    dismiss()
                }
            }
        }
    }
}

private struct EmailSignUpView: View {
    var onSwitchToLogIn: (() -> Void)? = nil
    @ObservedObject private var network = NetworkMonitor.shared
    @ObservedObject private var authService = AuthService.shared
    @Environment(\.dismiss) private var dismiss

    @State private var fullName: String = ""
    @State private var email: String = ""
    @State private var password: String = ""
    @State private var confirmPassword: String = ""

    @State private var isSubmitting: Bool = false
    @State private var showOfflineAlert: Bool = false
    @State private var infoMessage: String? = nil

    private var trimmedName: String { fullName.trimmingCharacters(in: .whitespacesAndNewlines) }
    private var trimmedEmail: String { email.trimmingCharacters(in: .whitespacesAndNewlines) }

    private var hasValidDetails: Bool {
        !trimmedName.isEmpty && isPlausibleEmail(trimmedEmail)
    }

    private var hasValidPassword: Bool { password.count >= 6 }
    private var passwordsMatch: Bool { !password.isEmpty && password == confirmPassword }
    private var canSubmit: Bool { hasValidDetails && hasValidPassword && passwordsMatch && !isSubmitting }

    // 0% at start → 50% when details are filled → 100% when ready to submit.
    private var progress: CGFloat {
        if !hasValidDetails { return 0.0 }
        if !(hasValidPassword && passwordsMatch) { return 0.5 }
        return 1.0
    }

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 0) {
                GeometryReader { geo in
                    ZStack(alignment: .leading) {
                        Capsule().fill(Color.primary.opacity(0.08)).frame(height: 4)
                        Capsule()
                            .fill(Color.purple.opacity(0.7))
                            .frame(width: geo.size.width * progress, height: 4)
                            .animation(.easeInOut(duration: 0.2), value: progress)
                    }
                }
                .frame(height: 4)
                .padding(.horizontal, 24)
                .padding(.bottom, 28)

                VStack(alignment: .leading, spacing: 18) {
                VStack(alignment: .leading, spacing: 6) {
                    Text("Create account")
                        .font(.system(size: 28, weight: .bold))
                }
                .frame(maxWidth: .infinity, alignment: .leading)

                VStack(spacing: 14) {
                    TextField("Full name", text: $fullName)
                        .textInputAutocapitalization(.words)
                        .autocorrectionDisabled(true)
                        .textContentType(.name)
                        .modifier(AuthTextFieldStyle())

                    TextField("Email", text: $email)
                        .keyboardType(.emailAddress)
                        .textInputAutocapitalization(.never)
                        .autocorrectionDisabled(true)
                        .textContentType(.username)
                        .modifier(AuthTextFieldStyle())

                    SecureField("Password", text: $password)
                        .textContentType(.newPassword)
                        .modifier(AuthTextFieldStyle())

                    SecureField("Confirm password", text: $confirmPassword)
                        .textContentType(.newPassword)
                        .modifier(AuthTextFieldStyle())
                }

                Text("Password: at least 6 characters.")
                    .font(.footnote)
                    .foregroundStyle(.secondary)

                if let msg = infoMessage, !msg.isEmpty {
                    Text(msg)
                        .font(.footnote)
                        .foregroundStyle(.secondary)
                }

                if let err = authService.lastAuthError, !err.isEmpty {
                    Text(err)
                        .font(.footnote)
                        .foregroundStyle(.red)
                }

                Button(action: submit) {
                    HStack(spacing: 10) {
                        if isSubmitting {
                            ProgressView().tint(.white)
                        }
                        Text(isSubmitting ? "Creating account…" : "Create account")
                            .font(.system(size: 17, weight: .semibold))
                    }
                    .frame(maxWidth: .infinity)
                    .padding(.vertical, 16)
                    .background(Color.purple)
                    .foregroundColor(.white)
                    .clipShape(RoundedRectangle(cornerRadius: 14, style: .continuous))
                }
                .disabled(!canSubmit)
                .padding(.top, 4)

                if let switchIn = onSwitchToLogIn {
                    HStack(spacing: 4) {
                        Text("Already have an account?")
                            .foregroundStyle(.secondary)
                        Button("Log in", action: { Haptics.selection(); switchIn() })
                            .fontWeight(.semibold)
                            .foregroundStyle(.purple)
                    }
                    .font(.subheadline)
                    .frame(maxWidth: .infinity)
                    .padding(.top, 18)
                }

                Spacer(minLength: 10)
            }
            .padding(.horizontal, 24)
            .padding(.top, 20)
            .padding(.bottom, 32)
            }
        }
        .scrollDismissesKeyboard(.interactively)
        .navigationBarTitleDisplayMode(.inline)
        .toolbar {
            ToolbarItem(placement: .topBarLeading) {
                Button("Close") { dismiss() }
            }
        }
        .alert("You're offline", isPresented: $showOfflineAlert) {
            Button("OK", role: .cancel) {}
        } message: {
            Text("Connect to the internet to create an account.")
        }
    }

    private func submit() {
        Haptics.selection()
        infoMessage = nil

        guard network.isOnline else {
            showOfflineAlert = true
            return
        }

        let n = fullName.trimmingCharacters(in: .whitespacesAndNewlines)
        let e = email.trimmingCharacters(in: .whitespacesAndNewlines)

        guard !n.isEmpty, !e.isEmpty else { return }
        guard isPlausibleEmail(e) else {
            infoMessage = "Please enter a valid email."
            return
        }
        guard password.count >= 6 else {
            infoMessage = "Password must be at least 6 characters."
            return
        }
        guard password == confirmPassword else {
            infoMessage = "Passwords don't match."
            return
        }

        isSubmitting = true
        Task {
            let outcome = await authService.signUp(fullName: n, email: e, password: password)
            await MainActor.run {
                isSubmitting = false
                if authService.isAuthenticated {
                    dismiss()
                    return
                }
                if case .needsEmailConfirmation = outcome, authService.lastAuthError == nil {
                    infoMessage = "Check your email to confirm your account, then come back and log in."
                }
            }
        }
    }

    private func isPlausibleEmail(_ value: String) -> Bool {
        // Lightweight check: good UX without over-validating.
        let v = value.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !v.isEmpty else { return false }
        guard v.contains("@") else { return false }
        let parts = v.split(separator: "@", maxSplits: 1, omittingEmptySubsequences: false)
        guard parts.count == 2, !parts[0].isEmpty, !parts[1].isEmpty else { return false }
        return parts[1].contains(".")
    }
}

#Preview {
    AuthView()
}
