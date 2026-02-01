import Foundation

enum WebSocketHandshakeError: LocalizedError {
    case timeout(TimeInterval)

    var errorDescription: String? {
        switch self {
        case .timeout(let t):
            return "WebSocket handshake timed out after \(String(format: "%.1f", t))s"
        }
    }
}

extension URLSessionWebSocketTask {
    /// `URLSessionWebSocketTask.sendPing` is callback-based and (in some edge cases) its completion
    /// may not fire. This wrapper adds a timeout and guarantees the continuation is resumed once.
    func awaitPing(timeout: TimeInterval = 5.0) async throws {
        try await withCheckedThrowingContinuation { (cont: CheckedContinuation<Void, Error>) in
            let lock = NSLock()
            var didResume = false

            func resumeOnce(_ result: Result<Void, Error>) {
                lock.lock()
                defer { lock.unlock() }
                guard didResume == false else { return }
                didResume = true
                switch result {
                case .success:
                    cont.resume(returning: ())
                case .failure(let err):
                    cont.resume(throwing: err)
                }
            }

            self.sendPing { err in
                if let err { resumeOnce(.failure(err)) }
                else { resumeOnce(.success(())) }
            }

            if timeout > 0 {
                DispatchQueue.global(qos: .utility).asyncAfter(deadline: .now() + timeout) {
                    resumeOnce(.failure(WebSocketHandshakeError.timeout(timeout)))
                }
            }
        }
    }

    /// Await a single received WebSocket message with a timeout.
    /// Useful for application-level handshakes (e.g. waiting for an initial "session" event).
    func awaitReceive(timeout: TimeInterval = 5.0) async throws -> URLSessionWebSocketTask.Message {
        try await withCheckedThrowingContinuation { (cont: CheckedContinuation<URLSessionWebSocketTask.Message, Error>) in
            let lock = NSLock()
            var didResume = false

            func resumeOnce(_ result: Result<URLSessionWebSocketTask.Message, Error>) {
                lock.lock()
                defer { lock.unlock() }
                guard didResume == false else { return }
                didResume = true
                switch result {
                case .success(let msg):
                    cont.resume(returning: msg)
                case .failure(let err):
                    cont.resume(throwing: err)
                }
            }

            self.receive { result in
                resumeOnce(result)
            }

            if timeout > 0 {
                DispatchQueue.global(qos: .utility).asyncAfter(deadline: .now() + timeout) {
                    resumeOnce(.failure(WebSocketHandshakeError.timeout(timeout)))
                }
            }
        }
    }
}

