import Foundation

actor AppSyncGate {
    static let shared = AppSyncGate()

    private var syncing: Bool = false

    func setSyncing(_ value: Bool) {
        syncing = value
    }

    func isSyncing() -> Bool {
        syncing
    }
}

