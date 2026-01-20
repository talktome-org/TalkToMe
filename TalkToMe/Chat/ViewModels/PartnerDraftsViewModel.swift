import Foundation

@MainActor
final class PartnerDraftsViewModel: ObservableObject {

    @Published private(set) var sentPartnerDrafts: Set<String> = []

    private static let globalSentDraftsKey = "globalSentPartnerDrafts"

    init() {
        loadSentDrafts()
    }

    func markPartnerDraftAsSent(sessionId: UUID?, messageContent: String) {
        guard let sessionId = sessionId else { return }
        sentPartnerDrafts.insert(makeKey(sessionId: sessionId, messageContent: messageContent))
        saveSentDrafts()
    }

    func isPartnerDraftSent(sessionId: UUID?, messageContent: String) -> Bool {
        guard let sessionId = sessionId else { return false }
        return sentPartnerDrafts.contains(makeKey(sessionId: sessionId, messageContent: messageContent))
    }

    private func loadSentDrafts() {
        sentPartnerDrafts = Set(UserDefaults.standard.stringArray(forKey: Self.globalSentDraftsKey) ?? [])
    }

    private func saveSentDrafts() {
        UserDefaults.standard.set(Array(sentPartnerDrafts), forKey: Self.globalSentDraftsKey)
    }

    private func makeKey(sessionId: UUID, messageContent: String) -> String {
        let contentKey = String(messageContent.prefix(100))
        return "\(sessionId.uuidString)_\(contentKey)"
    }
}

