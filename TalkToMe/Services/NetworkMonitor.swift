import Foundation
import Combine
import Network

@MainActor
final class NetworkMonitor: ObservableObject {
    static let shared = NetworkMonitor()

    @Published private(set) var isOnline: Bool

    private let monitor = NWPathMonitor()
    private let queue = DispatchQueue(label: "TalkToMe.NetworkMonitor")

    private init() {
        // Set an accurate initial value immediately (so UI can show Offline on cold start).
        self.isOnline = (monitor.currentPath.status == .satisfied)
        monitor.pathUpdateHandler = { [weak self] path in
            let online = (path.status == .satisfied)
            Task { @MainActor in
                self?.isOnline = online
            }
        }
        monitor.start(queue: queue)
    }
}

