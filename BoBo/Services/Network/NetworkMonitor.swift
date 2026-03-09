import Foundation
import Combine
import Network

@MainActor
final class NetworkMonitor: ObservableObject {
    static let shared = NetworkMonitor()

    @Published private(set) var isOnline: Bool
    @Published private(set) var pathStatus: NWPath.Status

    private let monitor = NWPathMonitor()
    private let queue = DispatchQueue(label: "BoBo.NetworkMonitor")

    private init() {
        self.pathStatus = monitor.currentPath.status
        self.isOnline = (monitor.currentPath.status == .satisfied)
        monitor.pathUpdateHandler = { [weak self] path in
            let online = (path.status == .satisfied)
            Task { @MainActor in
                self?.pathStatus = path.status
                self?.isOnline = online
            }
        }
        monitor.start(queue: queue)
    }
}

