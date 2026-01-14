import Foundation

@MainActor
final class BackendConnectionMonitor: ObservableObject {

    static let shared = BackendConnectionMonitor()

    enum State: Equatable {
        case idle
        case connecting
        case connected
    }

    @Published private(set) var state: State = .idle

    private var task: Task<Void, Never>?

    private init() {}

    func start() {
        if task != nil { return }

        task = Task { [weak self] in
            guard let self else { return }

            var backoffSeconds: Double = 1.0

            while !Task.isCancelled {
                // Gate on device connectivity + auth.
                if NetworkMonitor.shared.isOnline == false || AuthService.shared.isAuthenticated == false {
                    self.state = .idle
                    backoffSeconds = 1.0
                    try? await Task.sleep(nanoseconds: 800_000_000)
                    continue
                }

                self.state = .connecting
                let ok = await pingBackend(timeoutSeconds: 2.5)

                if ok {
                    self.state = .connected
                    backoffSeconds = 1.0
                    // Poll occasionally to detect weak-signal/server flaps.
                    try? await Task.sleep(nanoseconds: 12_000_000_000)
                } else {
                    self.state = .connecting
                    let delay = min(backoffSeconds, 12.0)
                    backoffSeconds = min(backoffSeconds * 1.8, 12.0)
                    try? await Task.sleep(nanoseconds: UInt64(delay * 1_000_000_000))
                }
            }
        }
    }

    private func pingBackend(timeoutSeconds: Double) async -> Bool {
        let base = BackendService.shared.baseURL
        let url = base

        var req = URLRequest(url: url)
        req.httpMethod = "HEAD"
        req.timeoutInterval = timeoutSeconds

        // A reachable backend is any HTTP response (even 404/405).
        do {
            let (_, response) = try await URLSession.shared.data(for: req)
            return (response as? HTTPURLResponse) != nil
        } catch {
            return false
        }
    }
}

