import Foundation
import SwiftUI
import Supabase

final class OnboardingViewModel: ObservableObject {
    enum Step: String { case none, asked_name, completed }

    @Published var isLoading: Bool = false
    @Published var fullName: String = ""
    @Published var gender: String = ""
    /// Calendar date (no time component)
    @Published var dateOfBirth: Date? = nil
    @Published var relationshipTopics: Set<String> = []
    @Published var step: Step = .none
    @Published var errorMessage: String? = nil

    init() {}

    func reset() {
        isLoading = false
        fullName = ""
        gender = ""
        dateOfBirth = nil
        relationshipTopics = []
        step = .none
        errorMessage = nil
    }

    func load() async {
        guard let token = try? await AuthService.shared.client.auth.session.accessToken else { return }
        await MainActor.run { self.isLoading = true }
        do {
            let info = try await BackendService.shared.fetchOnboarding(accessToken: token)
            await MainActor.run {
                self.fullName = info.full_name
                let fetchedStep = Step(rawValue: info.onboarding_step) ?? .none
                self.step = fetchedStep
                self.gender = info.gender ?? ""
                self.relationshipTopics = Set(info.relationship_topics ?? [])
                if let dob = info.date_of_birth, let parsed = Self.parseISODateOnly(dob) {
                    self.dateOfBirth = parsed
                }
                self.isLoading = false
            }
        } catch {
            await MainActor.run { self.isLoading = false }
        }
    }

    func markStartedIfNeeded() async {
        guard step == .none else { return }
        try? await advance(to: .asked_name)
    }

    func persistPartial() async throws {
        guard let token = try? await AuthService.shared.client.auth.session.accessToken else { return }
        _ = try await BackendService.shared.updateOnboarding(
            accessToken: token,
            update: .init(
                onboarding_step: nil,
                full_name: fullName.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty ? nil : fullName,
                gender: gender.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty ? nil : gender,
                date_of_birth: dateOfBirth.map { Self.formatISODateOnly($0) },
                relationship_topics: Array(relationshipTopics).sorted()
            )
        )
    }

    func complete() async throws {
        guard let token = try? await AuthService.shared.client.auth.session.accessToken else { return }
        // Strict: only mark completed locally if the server write succeeded.
        _ = try await BackendService.shared.updateOnboarding(
            accessToken: token,
            update: .init(
                onboarding_step: Step.completed.rawValue,
                full_name: fullName.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty ? nil : fullName,
                gender: gender.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty ? nil : gender,
                date_of_birth: dateOfBirth.map { Self.formatISODateOnly($0) },
                relationship_topics: Array(relationshipTopics).sorted()
            )
        )

        // Verify via GET so we don't dismiss unless Supabase really has it.
        let refreshed = try await BackendService.shared.fetchOnboarding(accessToken: token)
        if Step(rawValue: refreshed.onboarding_step) == .completed {
            await MainActor.run { self.step = .completed }
        } else {
            throw NSError(domain: "Onboarding", code: -1, userInfo: [NSLocalizedDescriptionKey: "Onboarding completion was not saved."])
        }
    }

    private func advance(to newStep: Step) async throws {
        guard let token = try? await AuthService.shared.client.auth.session.accessToken else { return }
        _ = try await BackendService.shared.updateOnboarding(
            accessToken: token,
            update: .init(
                onboarding_step: newStep.rawValue,
                full_name: nil,
                gender: nil,
                date_of_birth: nil,
                relationship_topics: nil
            )
        )
        await MainActor.run { self.step = newStep }
    }

    private static func parseISODateOnly(_ value: String) -> Date? {
        let f = DateFormatter()
        f.calendar = Calendar(identifier: .iso8601)
        f.locale = Locale(identifier: "en_US_POSIX")
        f.timeZone = TimeZone(secondsFromGMT: 0)
        f.dateFormat = "yyyy-MM-dd"
        return f.date(from: value)
    }

    private static func formatISODateOnly(_ date: Date) -> String {
        let f = DateFormatter()
        f.calendar = Calendar(identifier: .iso8601)
        f.locale = Locale(identifier: "en_US_POSIX")
        f.timeZone = TimeZone(secondsFromGMT: 0)
        f.dateFormat = "yyyy-MM-dd"
        return f.string(from: date)
    }
}


