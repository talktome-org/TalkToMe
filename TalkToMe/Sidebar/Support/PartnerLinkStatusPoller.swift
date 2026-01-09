import Foundation

@MainActor
final class PartnerLinkStatusPoller {
    private var linkTask: Task<Void, Never>?
    private var unlinkTask: Task<Void, Never>?

    func stop() {
        linkTask?.cancel()
        unlinkTask?.cancel()
        linkTask = nil
        unlinkTask = nil
    }

    func startLinkPolling(isLinked: @escaping () -> Bool, refresh: @escaping () async -> Void) {
        if linkTask != nil { return }
        unlinkTask?.cancel()
        unlinkTask = nil

        linkTask = Task {
            var attempts = 0
            while !Task.isCancelled {
                if isLinked() { break }
                attempts += 1
                await refresh()
                if attempts >= 24 { break } // ~2 minutes at 5s interval
                try? await Task.sleep(nanoseconds: 5_000_000_000)
            }
        }
    }

    func startUnlinkPolling(isLinked: @escaping () -> Bool, refresh: @escaping () async -> Void) {
        if unlinkTask != nil { return }
        linkTask?.cancel()
        linkTask = nil

        unlinkTask = Task {
            var attempts = 0
            while !Task.isCancelled {
                if !isLinked() { break }
                attempts += 1
                await refresh()
                if attempts >= 24 { break } // ~2 minutes at 5s interval
                try? await Task.sleep(nanoseconds: 5_000_000_000)
            }
        }
    }
}


