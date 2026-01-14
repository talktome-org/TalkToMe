import Foundation

struct ConnectionMonitorTimings: Sendable, Equatable {
    var pingTimeoutSeconds: Double = 2.5
    var healthStreamPathComponents: [String] = ["health", "stream"]
    var backoffInitialSeconds: Double = 1.0
    var backoffMultiplier: Double = 1.8
    var backoffMaxSeconds: Double = 12.0
    static let `default` = ConnectionMonitorTimings()
}

enum ConnectionMonitorState: Equatable, Sendable {
    case idle
    case connecting
    case connected
}

actor ConnectionMonitorCore {
    private var task: Task<Void, Never>?
    private var gateOpen: Bool = false
    private var appActive: Bool = true
    private var gateChangeId: UInt64 = 0
    private var gateChangeWaiters: [UUID: CheckedContinuation<UInt64, Never>] = [:]
    private var currentStreamHasReceivedBytes: Bool = false

    private let baseURL: URL
    private let emit: @Sendable (ConnectionMonitorState) -> Void
    private let timings: ConnectionMonitorTimings

    init(
        baseURL: URL,
        timings: ConnectionMonitorTimings,
        emit: @escaping @Sendable (ConnectionMonitorState) -> Void
    ) {
        self.baseURL = baseURL
        self.timings = timings
        self.emit = emit
    }

    func start() {
        guard task == nil else { return }
        task = Task { [weak self] in
            await self?.runLoop()
        }
    }

    func stop() {
        task?.cancel()
        task = nil
        emit(.idle)
        signalGateChange()
    }

    func setGateOpen(_ open: Bool) {
        gateOpen = open
        if open == false { emit(.idle) }
        signalGateChange()
    }

    func setAppActive(_ active: Bool) {
        appActive = active
        if active == false { emit(.idle) }
        signalGateChange()
    }

    private func runLoop() async {
        var backoffSeconds: Double = timings.backoffInitialSeconds

        while !Task.isCancelled {
            if preconditionsMet == false {
                emit(.idle)
                backoffSeconds = timings.backoffInitialSeconds
                await waitForPreconditions()
                continue
            }

            emit(.connecting)
            do {
                let bytes = try await openHealthStream()
                if preconditionsMet == false { continue }
                currentStreamHasReceivedBytes = false
                backoffSeconds = timings.backoffInitialSeconds
                try await holdHealthStream(bytes)
            } catch {
                emit(.connecting)
                let delay = min(backoffSeconds, timings.backoffMaxSeconds)
                backoffSeconds = min(backoffSeconds * timings.backoffMultiplier, timings.backoffMaxSeconds)
                await sleepOrGateChange(seconds: delay)
            }
        }
    }

    private enum HealthStreamError: Error {
        case invalidResponse
        case httpStatus(Int)
        case ended
    }

    private func openHealthStream() async throws -> URLSession.AsyncBytes {
        var url = baseURL
        for comp in timings.healthStreamPathComponents {
            url = url.appendingPathComponent(comp)
        }
        var request = URLRequest(url: url)
        request.httpMethod = "GET"
        request.timeoutInterval = timings.pingTimeoutSeconds
        request.setValue("text/event-stream", forHTTPHeaderField: "Accept")
        request.setValue("identity", forHTTPHeaderField: "Accept-Encoding")
        request.setValue("no-cache", forHTTPHeaderField: "Cache-Control")
        request.setValue("keep-alive", forHTTPHeaderField: "Connection")

        let (bytes, response) = try await URLSession.shared.bytes(for: request)
        guard let http = response as? HTTPURLResponse else { throw HealthStreamError.invalidResponse }
        guard (200..<300).contains(http.statusCode) else { throw HealthStreamError.httpStatus(http.statusCode) }
        return bytes
    }

    private func holdHealthStream(_ bytes: URLSession.AsyncBytes) async throws {
        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask { [weak self] in
                guard let self else { return }
                try await self.consumeHealthStream(bytes)
            }
            group.addTask { [weak self] in
                guard let self else { return }
                await self.waitForPreconditionsToFail()
            }
            _ = try await group.next()
            group.cancelAll()
        }
    }

    private var preconditionsMet: Bool { gateOpen && appActive }

    private func waitForPreconditionsToFail() async {
        var seen = gateChangeId
        while !Task.isCancelled, preconditionsMet == true {
            seen = await waitForGateChange(after: seen)
        }
    }

    private func consumeHealthStream(_ bytes: URLSession.AsyncBytes) async throws {
        do {
            for try await line in bytes.lines {
                if Task.isCancelled { return }
                let trimmed = String(line).trimmingCharacters(in: .whitespacesAndNewlines)
                if !trimmed.isEmpty {
                    markStreamReadyIfNeeded()
                }
            }
            throw HealthStreamError.ended
        } catch {
            if Task.isCancelled { return }
            throw error
        }
    }

    private func markStreamReadyIfNeeded() {
        if currentStreamHasReceivedBytes { return }
        currentStreamHasReceivedBytes = true
        if preconditionsMet { emit(.connected) }
    }

    private func waitForPreconditions() async {
        var seen = gateChangeId
        while !Task.isCancelled, preconditionsMet == false {
            seen = await waitForGateChange(after: seen)
        }
    }

    private func waitForGateChange(after lastSeen: UInt64) async -> UInt64 {
        if gateChangeId != lastSeen { return gateChangeId }
        let token = UUID()
        return await withTaskCancellationHandler(
            operation: {
                await withCheckedContinuation { (cont: CheckedContinuation<UInt64, Never>) in
                    if gateChangeId != lastSeen {
                        cont.resume(returning: gateChangeId)
                        return
                    }
                    gateChangeWaiters[token] = cont
                }
            },
            onCancel: {
                Task { [token] in
                    await self.removeGateWaiter(token)
                }
            }
        )
    }

    private func removeGateWaiter(_ token: UUID) {
        gateChangeWaiters[token] = nil
    }

    private func signalGateChange() {
        gateChangeId &+= 1
        let newId = gateChangeId
        let waiters = gateChangeWaiters
        gateChangeWaiters.removeAll(keepingCapacity: true)
        for (_, cont) in waiters {
            cont.resume(returning: newId)
        }
    }

    private func sleepOrGateChange(seconds: Double) async {
        let ns = Self.ns(fromSeconds: seconds)
        let seen = gateChangeId
        await withTaskGroup(of: Void.self) { group in
            group.addTask { try? await Task.sleep(nanoseconds: ns) }
            group.addTask { [weak self] in
                guard let self else { return }
                _ = await self.waitForGateChange(after: seen)
            }
            _ = await group.next()
            group.cancelAll()
        }
    }

    private static func ns(fromSeconds seconds: Double) -> UInt64 {
        UInt64(max(0.0, seconds) * 1_000_000_000)
    }
}