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
            AppTheme.background
                .ignoresSafeArea()

            authBackdrop

            ScrollView(showsIndicators: false) {
                VStack(spacing: 24) {
                    heroSection
                    actionCard
                    termsText
                }
                .frame(maxWidth: 560)
                .frame(maxWidth: .infinity)
                .padding(.horizontal, 20)
                .padding(.top, 24)
                .padding(.bottom, 30)
            }
        }
        .alert("You're offline", isPresented: $showOfflineAlert) {
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

    private var authBackdrop: some View {
        ZStack {
            Circle()
                .fill(AppTheme.accent.opacity(0.14))
                .frame(width: 260, height: 260)
                .blur(radius: 30)
                .offset(x: -140, y: -320)

            Circle()
                .fill(AppTheme.brand.opacity(0.2))
                .frame(width: 300, height: 300)
                .blur(radius: 44)
                .offset(x: 170, y: -230)

            RoundedRectangle(cornerRadius: 36, style: .continuous)
                .fill(AppTheme.surface.opacity(0.2))
                .frame(width: 320, height: 180)
                .rotationEffect(.degrees(-18))
                .blur(radius: 26)
                .offset(x: 70, y: -50)
        }
        .allowsHitTesting(false)
    }

    private var heroSection: some View {
        VStack(spacing: 12) {
            Image("AppIconDisplay")
                .resizable()
                .renderingMode(.original)
                .aspectRatio(contentMode: .fit)
                .frame(width: 116, height: 116)
                .clipShape(RoundedRectangle(cornerRadius: 28, style: .continuous))
                .shadow(color: AppTheme.accent.opacity(0.26), radius: 14, x: 0, y: 8)

            Text("TalkToMe")
                .font(.system(size: 31, weight: .bold, design: .rounded))
                .foregroundStyle(AppTheme.textPrimary)

            Text("Chat, reflect, and stay close to the people that matter.")
                .font(.system(size: 15, weight: .medium))
                .foregroundStyle(AppTheme.textSecondary)
                .multilineTextAlignment(.center)
                .padding(.horizontal, 12)
        }
        .padding(.top, 2)
    }

    private var actionCard: some View {
        VStack(spacing: 14) {
            Text("Get started")
                .font(.system(size: 14, weight: .semibold, design: .rounded))
                .foregroundStyle(AppTheme.textSecondary)
                .frame(maxWidth: .infinity, alignment: .leading)

            Button(action: signInWithGoogle) {
                HStack(spacing: 10) {
                    Image("icons8-google-48 copy")
                        .renderingMode(.original)
                        .resizable()
                        .aspectRatio(contentMode: .fit)
                        .frame(width: 20, height: 20)
                    Text("Continue with Google")
                        .font(.system(size: 16, weight: .semibold))
                }
                .frame(maxWidth: .infinity)
                .padding(.vertical, 15)
                .contentShape(RoundedRectangle(cornerRadius: 14, style: .continuous))
            }
            .buttonStyle(.plain)
            .modifier(AuthSecondaryButtonStyle())

            Button(action: signInWithApple) {
                HStack(spacing: 10) {
                    Image(systemName: "applelogo")
                        .font(.system(size: 18, weight: .semibold))
                    Text("Continue with Apple")
                        .font(.system(size: 16, weight: .semibold))
                }
                .frame(maxWidth: .infinity)
                .padding(.vertical, 15)
                .contentShape(RoundedRectangle(cornerRadius: 14, style: .continuous))
            }
            .buttonStyle(.plain)
            .modifier(AuthAppleButtonStyle())

            HStack(spacing: 12) {
                Rectangle()
                    .fill(Color.primary.opacity(0.12))
                    .frame(height: 1)
                Text("or")
                    .font(.system(size: 13, weight: .medium))
                    .foregroundStyle(.secondary)
                Rectangle()
                    .fill(Color.primary.opacity(0.12))
                    .frame(height: 1)
            }
            .padding(.vertical, 4)

            Button(action: {
                Haptics.selection()
                emailAuthMode = .login
            }) {
                Text("Log in with email")
                    .font(.system(size: 16, weight: .semibold))
                    .frame(maxWidth: .infinity)
                    .padding(.vertical, 15)
                    .contentShape(RoundedRectangle(cornerRadius: 14, style: .continuous))
            }
            .buttonStyle(.plain)
            .modifier(AuthSecondaryButtonStyle())

            Button(action: {
                Haptics.selection()
                emailAuthMode = .signUp
            }) {
                Text("Create account")
                    .font(.system(size: 16, weight: .semibold))
                    .frame(maxWidth: .infinity)
                    .padding(.vertical, 15)
                    .contentShape(RoundedRectangle(cornerRadius: 14, style: .continuous))
            }
            .buttonStyle(.plain)
            .modifier(AuthPrimaryButtonStyle())
        }
        .padding(18)
        .modifier(AuthPanelCardStyle())
    }

    private var termsText: some View {
        Text("By continuing, you agree to our Privacy Policy and Terms of Service")
            .font(.footnote)
            .foregroundColor(.secondary)
            .multilineTextAlignment(.center)
            .padding(.horizontal, 8)
    }

    private func signInWithGoogle() {
        Haptics.selection()
        guard network.isOnline else {
            showOfflineAlert = true
            return
        }
        Task { await authService.signIn(.google) }
    }

    private func signInWithApple() {
        guard let anchor = getPresentationAnchor() else { return }
        Haptics.selection()
        guard network.isOnline else {
            showOfflineAlert = true
            return
        }
        Task { await authService.signIn(.apple(presentationAnchor: anchor)) }
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

private struct AuthSheetHeader: View {
    let title: String
    let subtitle: String

    var body: some View {
        VStack(alignment: .leading, spacing: 6) {
            Text(title)
                .font(.system(size: 30, weight: .bold))
            Text(subtitle)
                .font(.subheadline)
                .foregroundStyle(.secondary)
                .fixedSize(horizontal: false, vertical: true)
        }
    }
}

private struct AuthPanelCardStyle: ViewModifier {
    func body(content: Content) -> some View {
        content
            .background(
                RoundedRectangle(cornerRadius: 22, style: .continuous)
                    .fill(AppTheme.surface)
                    .overlay(
                        RoundedRectangle(cornerRadius: 22, style: .continuous)
                            .stroke(Color.primary.opacity(0.09), lineWidth: 1)
                    )
                    .shadow(color: .black.opacity(0.08), radius: 14, x: 0, y: 8)
            )
    }
}

private struct AuthTextFieldStyle: ViewModifier {
    func body(content: Content) -> some View {
        content
            .padding(.horizontal, 16)
            .padding(.vertical, 15)
            .background(
                RoundedRectangle(cornerRadius: 14, style: .continuous)
                    .fill(Color(.secondarySystemBackground))
                    .overlay(
                        RoundedRectangle(cornerRadius: 14, style: .continuous)
                            .stroke(Color.primary.opacity(0.08), lineWidth: 1)
                    )
            )
    }
}

private struct AuthPrimaryButtonStyle: ViewModifier {
    func body(content: Content) -> some View {
        content
            .foregroundStyle(.white)
            .background(
                LinearGradient(
                    colors: [AppTheme.accent, AppTheme.brand],
                    startPoint: .topLeading,
                    endPoint: .bottomTrailing
                )
            )
            .clipShape(RoundedRectangle(cornerRadius: 14, style: .continuous))
            .shadow(color: AppTheme.accent.opacity(0.34), radius: 10, x: 0, y: 6)
    }
}

private struct AuthSecondaryButtonStyle: ViewModifier {
    func body(content: Content) -> some View {
        content
            .foregroundStyle(AppTheme.textPrimary)
            .background(AppTheme.surface)
            .clipShape(RoundedRectangle(cornerRadius: 14, style: .continuous))
            .overlay(
                RoundedRectangle(cornerRadius: 14, style: .continuous)
                    .stroke(Color.primary.opacity(0.10), lineWidth: 1)
            )
    }
}

private struct AuthAppleButtonStyle: ViewModifier {
    func body(content: Content) -> some View {
        content
            .foregroundStyle(.white)
            .background(Color.black)
            .clipShape(RoundedRectangle(cornerRadius: 14, style: .continuous))
            .shadow(color: .black.opacity(0.16), radius: 8, x: 0, y: 5)
    }
}

private struct AuthSheetBackdrop: View {
    var body: some View {
        ZStack {
            AppTheme.background.ignoresSafeArea()

            Circle()
                .fill(AppTheme.accent.opacity(0.14))
                .frame(width: 240, height: 240)
                .blur(radius: 26)
                .offset(x: -110, y: -290)

            Circle()
                .fill(AppTheme.brand.opacity(0.2))
                .frame(width: 260, height: 260)
                .blur(radius: 36)
                .offset(x: 160, y: -210)
        }
        .allowsHitTesting(false)
    }
}

private struct EmailLoginView: View {
    var onSwitchToSignUp: (() -> Void)? = nil
    @ObservedObject private var network = NetworkMonitor.shared
    @ObservedObject private var authService = AuthService.shared
    @Environment(\.dismiss) private var dismiss

    @State private var email: String = ""
    @State private var password: String = ""
    @State private var showPassword: Bool = false
    @State private var isSubmitting: Bool = false
    @State private var showOfflineAlert: Bool = false
    @State private var toastMessage: String? = nil
    @State private var toastWorkItem: DispatchWorkItem? = nil

    var body: some View {
        ZStack {
            AuthSheetBackdrop()

            ScrollView {
                VStack(alignment: .leading, spacing: 22) {
                    AuthSheetHeader(
                        title: "Welcome back",
                        subtitle: "Log in with your email and password."
                    )

                    VStack(alignment: .leading, spacing: 14) {
                        Text("Your account")
                            .font(.system(size: 13, weight: .semibold, design: .rounded))
                            .foregroundStyle(AppTheme.textSecondary)

                        TextField("Email", text: $email)
                            .keyboardType(.emailAddress)
                            .textInputAutocapitalization(.never)
                            .autocorrectionDisabled(true)
                            .textContentType(.username)
                            .modifier(AuthTextFieldStyle())

                        ZStack {
                            if showPassword {
                                TextField("Password", text: $password)
                                    .textContentType(.password)
                                    .textInputAutocapitalization(.never)
                                    .autocorrectionDisabled(true)
                            } else {
                                SecureField("Password", text: $password)
                                    .textContentType(.password)
                            }
                        }
                        .modifier(AuthTextFieldStyle())
                        .overlay(alignment: .trailing) {
                            Button {
                                showPassword.toggle()
                            } label: {
                                Image(systemName: showPassword ? "eye.slash" : "eye")
                                    .font(.system(size: 15))
                                    .foregroundStyle(AppTheme.textSecondary)
                            }
                            .padding(.trailing, 14)
                        }
                    }
                    .padding(16)
                    .modifier(AuthPanelCardStyle())

                    Button(action: submit) {
                        HStack(spacing: 10) {
                            if isSubmitting {
                                ProgressView().tint(.white)
                            }
                            Text(isSubmitting ? "Signing in..." : "Sign in")
                                .font(.system(size: 17, weight: .semibold))
                        }
                        .frame(maxWidth: .infinity)
                        .padding(.vertical, 16)
                        .contentShape(RoundedRectangle(cornerRadius: 14, style: .continuous))
                    }
                    .buttonStyle(.plain)
                    .modifier(AuthPrimaryButtonStyle())
                    .disabled(isSubmitting)

                    if let switchUp = onSwitchToSignUp {
                        HStack(spacing: 4) {
                            Text("Don't have an account?")
                                .foregroundStyle(.secondary)
                            Button("Create account", action: {
                                Haptics.selection()
                                switchUp()
                            })
                            .fontWeight(.semibold)
                            .foregroundStyle(AppTheme.accent)
                        }
                        .font(.subheadline)
                        .frame(maxWidth: .infinity)
                        .padding(.top, 2)
                    }
                }
                .padding(.horizontal, 24)
                .padding(.top, 20)
                .padding(.bottom, 32)
            }
            .scrollDismissesKeyboard(.interactively)
        }
        .navigationBarTitleDisplayMode(.inline)
        .toolbar {
            ToolbarItem(placement: .topBarLeading) {
                Button("Close") { dismiss() }
            }
        }
        .alert("You're offline", isPresented: $showOfflineAlert) {
            Button("OK", role: .cancel) {}
        } message: {
            Text("Connect to the internet to log in.")
        }
        .overlay(alignment: .top) {
            if let toastMessage {
                ToastOverlayView(message: toastMessage, onDismiss: dismissToast)
                    .padding(.top, 8)
            }
        }
        .animation(.spring(response: 0.3, dampingFraction: 0.8), value: toastMessage)
    }

    private func showToast(_ message: String) {
        toastWorkItem?.cancel()
        Haptics.notification(.warning)
        withAnimation { toastMessage = message }
        let work = DispatchWorkItem { withAnimation { toastMessage = nil } }
        toastWorkItem = work
        DispatchQueue.main.asyncAfter(deadline: .now() + 3, execute: work)
    }

    private func dismissToast() {
        toastWorkItem?.cancel()
        withAnimation { toastMessage = nil }
    }

    private func submit() {
        Haptics.selection()

        guard network.isOnline else {
            showOfflineAlert = true
            return
        }

        let e = email.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !e.isEmpty else {
            showToast("Enter your email address.")
            return
        }
        guard isPlausibleEmail(e) else {
            showToast("Enter a valid email address.")
            return
        }
        guard !password.isEmpty else {
            showToast("Enter your password.")
            return
        }

        isSubmitting = true
        Task {
            await authService.signIn(email: e, password: password)
            await MainActor.run {
                isSubmitting = false
                if authService.isAuthenticated {
                    dismiss()
                } else if let err = authService.lastAuthError, !err.isEmpty {
                    showToast(err)
                }
            }
        }
    }

    private func isPlausibleEmail(_ value: String) -> Bool {
        let v = value.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !v.isEmpty else { return false }
        guard v.contains("@") else { return false }
        let parts = v.split(separator: "@", maxSplits: 1, omittingEmptySubsequences: false)
        guard parts.count == 2, !parts[0].isEmpty, !parts[1].isEmpty else { return false }
        return parts[1].contains(".")
    }
}

private struct EmailSignUpView: View {
    var onSwitchToLogIn: (() -> Void)? = nil
    @ObservedObject private var network = NetworkMonitor.shared
    @ObservedObject private var authService = AuthService.shared
    @Environment(\.dismiss) private var dismiss

    @State private var firstName: String = ""
    @State private var email: String = ""
    @State private var password: String = ""
    @State private var confirmPassword: String = ""
    @State private var showPassword: Bool = false
    @State private var showConfirmPassword: Bool = false

    @FocusState private var focusedField: Field?
    @State private var isSubmitting: Bool = false
    @State private var showOfflineAlert: Bool = false
    @State private var toastMessage: String? = nil
    @State private var toastWorkItem: DispatchWorkItem? = nil
    @State private var emailCheckTask: Task<Void, Never>? = nil
    @State private var emailCheckState: EmailCheckState = .idle

    private enum Field { case firstName, email, password, confirm }
    private enum EmailCheckState: Equatable {
        case idle
        case checking
        case available
        case taken
        case failed
    }

    private var trimmedEmail: String { email.trimmingCharacters(in: .whitespacesAndNewlines) }
    private var normalizedEmail: String { normalizeEmail(email) }

    private var trimmedFirstName: String { firstName.trimmingCharacters(in: .whitespacesAndNewlines) }
    private var hasValidDetails: Bool {
        !trimmedFirstName.isEmpty && isPlausibleEmail(trimmedEmail)
    }

    private var hasValidPassword: Bool { password.count >= 6 }
    private var passwordsMatch: Bool { !password.isEmpty && password == confirmPassword }
    private var canSubmit: Bool {
        hasValidDetails
            && hasValidPassword
            && passwordsMatch
            && emailCheckState == .available
            && !isSubmitting
    }

    private var validationHint: String {
        if trimmedFirstName.isEmpty { return "Enter your first name." }
        if !isPlausibleEmail(trimmedEmail) { return "Enter a valid email address." }
        switch emailCheckState {
        case .idle, .checking:
            return "Checking email..."
        case .taken:
            return "This email is taken."
        case .failed:
            return "Could not verify this email right now. Please try again."
        case .available:
            break
        }
        if !hasValidPassword { return "Password: at least 6 characters." }
        if !passwordsMatch { return "Passwords don't match." }
        return ""
    }

    var body: some View {
        ZStack {
            AuthSheetBackdrop()

            ScrollView {
                VStack(alignment: .leading, spacing: 22) {
                    AuthSheetHeader(
                        title: "Create account",
                        subtitle: "Let's get you set up."
                    )

                    VStack(alignment: .leading, spacing: 14) {
                        Text("Your details")
                            .font(.system(size: 13, weight: .semibold, design: .rounded))
                            .foregroundStyle(AppTheme.textSecondary)

                        TextField("First name", text: $firstName)
                            .focused($focusedField, equals: .firstName)
                            .textContentType(.givenName)
                            .autocorrectionDisabled(true)
                            .submitLabel(.next)
                            .onSubmit { focusedField = .email }
                            .modifier(AuthTextFieldStyle())

                        TextField("Email", text: $email)
                            .focused($focusedField, equals: .email)
                            .keyboardType(.emailAddress)
                            .textInputAutocapitalization(.never)
                            .autocorrectionDisabled(true)
                            .textContentType(.username)
                            .submitLabel(.next)
                            .onSubmit { focusedField = .password }
                            .modifier(AuthTextFieldStyle())
                            .overlay(alignment: .trailing) {
                                if emailCheckState == .checking {
                                    ProgressView()
                                        .scaleEffect(0.8)
                                        .padding(.trailing, 14)
                                }
                            }

                        ZStack {
                            if showPassword {
                                TextField("Password", text: $password)
                                    .focused($focusedField, equals: .password)
                                    .textContentType(.newPassword)
                                    .textInputAutocapitalization(.never)
                                    .autocorrectionDisabled(true)
                                    .submitLabel(.next)
                                    .onSubmit { focusedField = .confirm }
                            } else {
                                SecureField("Password", text: $password)
                                    .focused($focusedField, equals: .password)
                                    .textContentType(.newPassword)
                                    .submitLabel(.next)
                                    .onSubmit { focusedField = .confirm }
                            }
                        }
                        .modifier(AuthTextFieldStyle())
                        .overlay(alignment: .trailing) {
                            Button {
                                showPassword.toggle()
                            } label: {
                                Image(systemName: showPassword ? "eye.slash" : "eye")
                                    .font(.system(size: 15))
                                    .foregroundStyle(AppTheme.textSecondary)
                            }
                            .padding(.trailing, 14)
                        }

                        ZStack {
                            if showConfirmPassword {
                                TextField("Confirm password", text: $confirmPassword)
                                    .focused($focusedField, equals: .confirm)
                                    .textContentType(.newPassword)
                                    .textInputAutocapitalization(.never)
                                    .autocorrectionDisabled(true)
                                    .submitLabel(.go)
                                    .onSubmit { if canSubmit { submit() } }
                            } else {
                                SecureField("Confirm password", text: $confirmPassword)
                                    .focused($focusedField, equals: .confirm)
                                    .textContentType(.newPassword)
                                    .submitLabel(.go)
                                    .onSubmit { if canSubmit { submit() } }
                            }
                        }
                        .modifier(AuthTextFieldStyle())
                        .overlay(alignment: .trailing) {
                            Button {
                                showConfirmPassword.toggle()
                            } label: {
                                Image(systemName: showConfirmPassword ? "eye.slash" : "eye")
                                    .font(.system(size: 15))
                                    .foregroundStyle(AppTheme.textSecondary)
                            }
                            .padding(.trailing, 14)
                        }
                    }
                    .padding(16)
                    .modifier(AuthPanelCardStyle())

                    Button(action: submit) {
                        HStack(spacing: 10) {
                            if isSubmitting {
                                ProgressView().tint(.white)
                            }
                            Text(isSubmitting ? "Creating account..." : "Create account")
                                .font(.system(size: 17, weight: .semibold))
                        }
                        .frame(maxWidth: .infinity)
                        .padding(.vertical, 16)
                        .contentShape(RoundedRectangle(cornerRadius: 14, style: .continuous))
                    }
                    .buttonStyle(.plain)
                    .modifier(AuthPrimaryButtonStyle())
                    .opacity(canSubmit ? 1.0 : 0.5)

                    if let switchIn = onSwitchToLogIn {
                        HStack(spacing: 4) {
                            Text("Already have an account?")
                                .foregroundStyle(.secondary)
                            Button("Log in", action: { Haptics.selection(); switchIn() })
                                .fontWeight(.semibold)
                                .foregroundStyle(AppTheme.accent)
                        }
                        .font(.subheadline)
                        .frame(maxWidth: .infinity)
                        .padding(.top, 2)
                    }

                    Spacer(minLength: 10)
                }
                .padding(.horizontal, 24)
                .padding(.top, 20)
                .padding(.bottom, 32)
            }
        }
        .scrollDismissesKeyboard(.immediately)
        .overlay(alignment: .top) {
            if let toastMessage {
                ToastOverlayView(message: toastMessage, onDismiss: dismissToast)
                    .padding(.top, 8)
            }
        }
        .animation(.spring(response: 0.3, dampingFraction: 0.8), value: toastMessage)
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
        .onChange(of: email) {
            scheduleEmailCheck()
        }
        .onDisappear {
            emailCheckTask?.cancel()
            emailCheckTask = nil
        }
    }

    private func showToast(_ message: String) {
        toastWorkItem?.cancel()
        Haptics.notification(.warning)
        withAnimation { toastMessage = message }
        let work = DispatchWorkItem { withAnimation { toastMessage = nil } }
        toastWorkItem = work
        DispatchQueue.main.asyncAfter(deadline: .now() + 3, execute: work)
    }

    private func dismissToast() {
        toastWorkItem?.cancel()
        withAnimation { toastMessage = nil }
    }

    private func submit() {
        Haptics.selection()

        guard network.isOnline else {
            showOfflineAlert = true
            return
        }

        guard canSubmit else {
            showToast(validationHint)
            return
        }

        let e = trimmedEmail

        isSubmitting = true
        Task {
            let outcome = await authService.signUp(fullName: trimmedFirstName, email: e, password: password)
            await MainActor.run {
                isSubmitting = false
                if authService.isAuthenticated {
                    dismiss()
                    return
                }
                switch outcome {
                case .signedIn:
                    break
                case .needsEmailConfirmation:
                    showToast("We sent a confirmation link to your email. Tap it to get started!")
                case .emailAlreadyExists:
                    emailCheckState = .taken
                    showToast(authService.lastAuthError ?? "An account with this email already exists. Try a different one or log in.")
                case .failed:
                    showToast(authService.lastAuthError ?? "Could not create account. Please try again.")
                }
            }
        }
    }

    private func scheduleEmailCheck() {
        emailCheckTask?.cancel()

        guard isPlausibleEmail(trimmedEmail) else {
            emailCheckState = .idle
            return
        }

        let emailToCheck = normalizedEmail
        emailCheckState = .checking

        emailCheckTask = Task {
            try? await Task.sleep(nanoseconds: 350_000_000)
            guard !Task.isCancelled else { return }

            do {
                let exists = try await authService.checkEmailExists(email: emailToCheck)
                guard !Task.isCancelled else { return }
                await MainActor.run {
                    guard normalizeEmail(email) == emailToCheck else { return }
                    emailCheckState = exists ? .taken : .available
                    if exists { showToast("This email is taken.") }
                }
            } catch {
                guard !Task.isCancelled else { return }
                await MainActor.run {
                    guard normalizeEmail(email) == emailToCheck else { return }
                    emailCheckState = .failed
                }
            }
        }
    }

    private func normalizeEmail(_ value: String) -> String {
        value.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    }

    private func isPlausibleEmail(_ value: String) -> Bool {
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
