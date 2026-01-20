import Foundation
import SwiftUI
import Supabase

final class OnboardingViewModel: ObservableObject {
    enum Step: String { case none, asked_name, completed }

    @Published var isLoading: Bool = false
    @Published var fullName: String = ""
    @Published var step: Step = .none
    @Published var errorMessage: String? = nil

    init() {}

    func load() async {
        guard let token = try? await AuthService.shared.client.auth.session.accessToken else { return }
        await MainActor.run { self.isLoading = true }
        do {
            let info = try await BackendService.shared.fetchOnboarding(accessToken: token)
            await MainActor.run {
                self.fullName = info.full_name
                let fetchedStep = Step(rawValue: info.onboarding_step) ?? .none
                self.step = fetchedStep
                self.isLoading = false
            }
        } catch {
            await MainActor.run { self.isLoading = false }
        }
    }

    func setFullName(_ name: String?) async {
        guard let token = try? await AuthService.shared.client.auth.session.accessToken else { return }
        let trimmed = (name ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
        if trimmed.isEmpty {
            await MainActor.run { self.errorMessage = "Please enter your name or tap Skip." }
            return
        }
        // Optimistically update UI and advance immediately
        await MainActor.run {
            self.fullName = trimmed
            self.errorMessage = nil
            self.step = .completed
        }
        // Persist in background; do not block UI flow
        Task { _ = try? await BackendService.shared.updateProfile(accessToken: token, fullName: trimmed, bio: nil) }
        Task { try? await self.advance(to: .completed) }
    }

    func skipCurrent() async {
        guard (try? await AuthService.shared.client.auth.session.accessToken) != nil else { return }
        switch step {
        case .none, .asked_name:
            try? await complete()

        case .completed:
            break
        }
    }

    func complete() async throws {
        guard let token = try? await AuthService.shared.client.auth.session.accessToken else { return }
        do {
            _ = try await BackendService.shared.updateOnboarding(accessToken: token, update: .init(onboarding_step: Step.completed.rawValue))
            await MainActor.run { self.step = .completed }
        } catch {
            // Try alternate base path/methods via a profile update (no-op, just to test connectivity)
            _ = try? await BackendService.shared.updateProfile(accessToken: token, fullName: nil, bio: nil)
            await MainActor.run { self.step = .completed }
        }
    }

    private func advance(to newStep: Step) async throws {
        guard let token = try? await AuthService.shared.client.auth.session.accessToken else { return }
        _ = try await BackendService.shared.updateOnboarding(accessToken: token, update: .init(onboarding_step: newStep.rawValue))
        await MainActor.run { self.step = newStep }
    }
}


