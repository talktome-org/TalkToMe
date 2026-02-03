import Foundation

@MainActor
final class PartnerDraftsViewModel {

    private var sentPartnerDrafts: Set<String> = []

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

    func rekeySentDrafts(oldSessionId: UUID, newSessionId: UUID) {
        guard oldSessionId != newSessionId else { return }
        let oldPrefix = oldSessionId.uuidString + "_"
        let newPrefix = newSessionId.uuidString + "_"

        var changed = false
        let updated: Set<String> = Set(sentPartnerDrafts.map { key in
            if key.hasPrefix(oldPrefix) {
                changed = true
                return newPrefix + key.dropFirst(oldPrefix.count)
            }
            return key
        })

        if changed {
            sentPartnerDrafts = updated
            saveSentDrafts()
        }
    }

    private func loadSentDrafts() {
        sentPartnerDrafts = Set(UserDefaults.standard.stringArray(forKey: Self.globalSentDraftsKey) ?? [])
    }

    private func saveSentDrafts() {
        UserDefaults.standard.set(Array(sentPartnerDrafts), forKey: Self.globalSentDraftsKey)
    }

    private func makeKey(sessionId: UUID, messageContent: String) -> String {
        let normalized = messageContent.trimmingCharacters(in: .whitespacesAndNewlines)
        let contentKey = String(normalized.prefix(100))
        return "\(sessionId.uuidString)_\(contentKey)"
    }
}

