import Foundation
import UIKit

@MainActor
final class ChatSessionsEventRouter {
    private var tokens: [NSObjectProtocol] = []

    func start(_ viewModel: ChatSessionsViewModel) {
        guard tokens.isEmpty else { return }

        let nc = NotificationCenter.default

        tokens.append(
            nc.addObserver(forName: .chatSessionCreated, object: nil, queue: .main) { [weak viewModel] note in
                Task { @MainActor in
                    await viewModel?.handleChatSessionCreated(note)
                }
            }
        )

        tokens.append(
            nc.addObserver(forName: .chatSessionRekeyed, object: nil, queue: .main) { [weak viewModel] note in
                Task { @MainActor in
                    await viewModel?.handleChatSessionRekeyed(note)
                }
            }
        )

        tokens.append(
            nc.addObserver(forName: .chatMessageSent, object: nil, queue: .main) { [weak viewModel] note in
                Task { @MainActor in
                    await viewModel?.handleChatMessageSent(note)
                }
            }
        )

        tokens.append(
            nc.addObserver(forName: .chatSessionsNeedRefresh, object: nil, queue: .main) { [weak viewModel] _ in
                Task { @MainActor in
                    await viewModel?.handleChatSessionsNeedRefresh()
                }
            }
        )

        tokens.append(
            nc.addObserver(forName: UIApplication.willEnterForegroundNotification, object: nil, queue: .main) { [weak viewModel] _ in
                Task { @MainActor in
                    await viewModel?.handleWillEnterForeground()
                }
            }
        )

        tokens.append(
            nc.addObserver(forName: .avatarChanged, object: nil, queue: .main) { [weak viewModel] _ in
                Task { @MainActor in
                    await viewModel?.handleAvatarChanged()
                }
            }
        )

        tokens.append(
            nc.addObserver(forName: .partnerMessageReceived, object: nil, queue: .main) { [weak viewModel] note in
                Task { @MainActor in
                    await viewModel?.handlePartnerMessageReceived(note)
                }
            }
        )

        tokens.append(
            nc.addObserver(forName: .partnerRequestOpen, object: nil, queue: .main) { [weak viewModel] note in
                Task { @MainActor in
                    await viewModel?.handlePartnerRequestOpen(note)
                }
            }
        )

        tokens.append(
            nc.addObserver(forName: .partnerMessageOpen, object: nil, queue: .main) { [weak viewModel] note in
                Task { @MainActor in
                    await viewModel?.handlePartnerMessageOpen(note)
                }
            }
        )
    }

    func stop() {
        for t in tokens {
            NotificationCenter.default.removeObserver(t)
        }
        tokens.removeAll()
    }
}


