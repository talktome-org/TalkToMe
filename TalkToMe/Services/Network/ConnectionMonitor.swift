import Foundation
import Combine
import UIKit

@MainActor
final class ConnectionMonitor: ObservableObject {
    static let shared = ConnectionMonitor()

    typealias State = ConnectionMonitorState

    @Published private(set) var state: State = .idle

    private let core: ConnectionMonitorCore
    private let timings: ConnectionMonitorTimings
    private var stateConsumerTask: Task<Void, Never>?
    private var gateCancellable: AnyCancellable?
    private var appActiveCancellable: AnyCancellable?

    private let stateStream: AsyncStream<State>
    private let stateContinuation: AsyncStream<State>.Continuation

    private init(
        timings: ConnectionMonitorTimings = .default
    ) {
        self.timings = timings

        var continuation: AsyncStream<State>.Continuation!
        self.stateStream = AsyncStream<State> { c in
            continuation = c
        }
        self.stateContinuation = continuation

        let base = BackendService.shared.baseURL
        self.core = ConnectionMonitorCore(
            baseURL: base,
            timings: timings,
            emit: { [stateContinuation] newState in
                stateContinuation.yield(newState)
            }
        )
    }

    func start() {
        if stateConsumerTask != nil { return }

        stateConsumerTask = Task { [weak self] in
            guard let self else { return }
            for await newState in stateStream {
                self.state = newState
            }
        }

        gateCancellable = Publishers.CombineLatest(
            NetworkMonitor.shared.$isOnline.removeDuplicates(),
            AuthService.shared.$isAuthenticated.removeDuplicates()
        )
        .map { $0 && $1 }
        .removeDuplicates()
        .sink { [weak self] gateOpen in
            guard let self else { return }
            Task { await self.core.setGateOpen(gateOpen) }
        }

        appActiveCancellable = Publishers.Merge(
            NotificationCenter.default.publisher(for: UIApplication.didEnterBackgroundNotification).map { _ in false },
            NotificationCenter.default.publisher(for: UIApplication.willEnterForegroundNotification).map { _ in true }
        )
        .removeDuplicates()
        .sink { [weak self] active in
            guard let self else { return }
            Task { await self.core.setAppActive(active) }
        }

        Task { [core] in
            let gateOpen = NetworkMonitor.shared.isOnline && AuthService.shared.isAuthenticated
            await core.setGateOpen(gateOpen)
        }

        Task { [core] in
            let active = (UIApplication.shared.applicationState == .active)
            await core.setAppActive(active)
        }

        Task { [core] in
            await core.start()
        }
    }

    func stop() {
        stateConsumerTask?.cancel()
        stateConsumerTask = nil
        gateCancellable?.cancel()
        gateCancellable = nil
        appActiveCancellable?.cancel()
        appActiveCancellable = nil
        Task { await core.stop() }
        state = .idle
    }
}