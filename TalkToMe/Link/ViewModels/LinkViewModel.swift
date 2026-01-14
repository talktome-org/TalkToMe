import Foundation
import SwiftUI

final class LinkViewModel: ObservableObject {

    enum LinkingState: Equatable {
        case idle
        case creating
        case shareReady(url: URL)
        case accepting
        case linked
        case unlinking
        case unlinked
        case error(message: String)
    }

    @Published private(set) var state: LinkingState = .idle
    @Published var pendingInviteToken: String? = nil
    @Published private(set) var linkedAt: Date? = nil

    private var accessTokenProvider: () async throws -> String

    init(accessTokenProvider: @escaping () async throws -> String) {
        self.accessTokenProvider = accessTokenProvider
    }

    func createInviteLink() async {
        await MainActor.run { self.state = .creating }
        do {
            let token = try await accessTokenProvider()
            let url = try await BackendService.shared.createLinkInvite(accessToken: token)
            await MainActor.run { self.state = .shareReady(url: url) }
        } catch {
            await MainActor.run { self.state = .error(message: error.localizedDescription) }
        }
    }

    func acceptInvite(using inviteToken: String) async {
        await MainActor.run { self.state = .accepting }
        do {
            let token = try await accessTokenProvider()
            try await BackendService.shared.acceptLinkInvite(inviteToken: inviteToken, accessToken: token)
            try await refreshStatus()
            // Eagerly fetch partner info and cache to drive immediate UI updates
            do {
                let info = try await BackendService.shared.fetchPartnerInfo(accessToken: token)
                await MainActor.run {
                    UserDefaults.standard.set(info.linked, forKey: PreferenceKeys.partnerConnected)
                    if info.linked, let partner = info.partner {
                        UserDefaults.standard.set(partner.name, forKey: PreferenceKeys.partnerName)
                        if let avatar = partner.avatar_url {
                            UserDefaults.standard.set(avatar, forKey: PreferenceKeys.partnerAvatarURL)
                        }
                    }
                    self.objectWillChange.send()
                }
            } catch {
                // Ignore; backend refresh will still update shortly
            }
        } catch {
            await MainActor.run { self.state = .error(message: error.localizedDescription) }
        }
    }

    func unlink() async {
        await MainActor.run { self.state = .unlinking }
        do {
            let token = try await accessTokenProvider()
            _ = try await BackendService.shared.unlink(accessToken: token)
            await createInviteLink()
        } catch {
            await MainActor.run { self.state = .error(message: error.localizedDescription) }
        }
    }

    func refreshStatus() async throws {
        let token = try await accessTokenProvider()
        let status = try await BackendService.shared.fetchLinkStatus(accessToken: token)
        await MainActor.run {
            self.linkedAt = status.linkedAt
            self.state = status.linked ? .linked : .idle
        }
    }

    func ensureInviteReady() async {
        do { try await refreshStatus() } catch {}
        await MainActor.run {
            switch self.state {
            case .linked, .shareReady:
                return
            case .creating, .accepting, .unlinking:
                return
            case .idle, .unlinked, .error:
                Task { await self.createInviteLink() }
            }
        }
    }

    func captureIncomingInviteToken(_ token: String) {
        pendingInviteToken = token
    }
}

#if DEBUG
extension LinkViewModel {
    static func preview(state: LinkingState) -> LinkViewModel {
        let viewModel = LinkViewModel(accessTokenProvider: { "" })
        viewModel.state = state
        return viewModel
    }
}
#endif
