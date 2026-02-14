import Foundation

final class SSEService {
    static let shared = SSEService()

    private init() {}

    func stream(request: URLRequest) -> AsyncStream<BackendService.StreamEvent> {
        return AsyncStream { continuation in
            let task = Task {
                do {
                    let config = URLSessionConfiguration.default
                    config.httpAdditionalHeaders = [
                        "Accept": "text/event-stream",
                        "Accept-Encoding": "identity",
                        "Cache-Control": "no-cache",
                        "Connection": "keep-alive"
                    ]
                    let session = URLSession(configuration: config)
                    let (bytes, response) = try await session.bytes(for: request)
                    if let http = response as? HTTPURLResponse, !(200..<300).contains(http.statusCode) {
                        continuation.yield(.error("HTTP \(http.statusCode)"))
                        continuation.finish()
                        return
                    }

                    var currentEvent: String? = nil
                    var dataLines: [String] = []
                    var sawToolStart = false

                    func flush() {
                        let dataString = dataLines.joined(separator: "\n")
                        let event = currentEvent ?? ""
                        if event == "response_id" {
                            if let json = dataString.data(using: .utf8),
                               let obj = try? JSONSerialization.jsonObject(with: json) as? [String: Any],
                               let rid = obj["response_id"] as? String {
                                continuation.yield(.responseId(rid))
                            }
                        } else if event == "session" {
                            if let json = dataString.data(using: .utf8),
                               let obj = try? JSONSerialization.jsonObject(with: json) as? [String: Any],
                               let sidStr = obj["session_id"] as? String,
                               let sid = UUID(uuidString: sidStr) {
                                continuation.yield(.session(sid))
                            }
                        } else if event == "token" {
                            if dataString.isEmpty {
                                currentEvent = nil
                                dataLines.removeAll(keepingCapacity: false)
                                return
                            }
                            let token = decodeTokenPayload(dataString)
                            continuation.yield(.token(token))
                        } else if event == "partner_message" {
                            let text = decodePossiblyQuotedJSONString(dataString) ?? dataString
                            continuation.yield(.partnerMessage(text))
                        } else if event == "tool_start" {
                            if let data = dataString.data(using: .utf8),
                               let obj = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
                               let name = obj["name"] as? String {
                                continuation.yield(.toolStart(name))
                            } else {
                                continuation.yield(.toolStart(""))
                            }
                            sawToolStart = true
                        } else if event == "tool_args" {
                            if !sawToolStart {
                                continuation.yield(.toolStart(""))
                                sawToolStart = true
                            }
                            continuation.yield(.toolArgs(dataString))
                        } else if event == "tool_done" {
                            continuation.yield(.toolDone)
                        } else if event == "thinking" {
                            let text = decodeTokenPayload(dataString)
                            if !text.isEmpty {
                                continuation.yield(.thinking(text))
                            }
                        } else if event == "thinking_done" {
                            continuation.yield(.thinkingDone)
                        } else if event == "done" {
                            continuation.yield(.done)
                            continuation.finish()
                        } else if event == "error" {
                            continuation.yield(.error(decodeErrorPayload(dataString)))
                            continuation.finish()
                        } else {}
                        currentEvent = nil
                        dataLines.removeAll(keepingCapacity: false)
                    }

                    for try await rawLine in bytes.lines {
                        var line = String(rawLine)
                        if line.hasSuffix("\r") { line.removeLast() }

                        if line.trimmingCharacters(in: .whitespaces).isEmpty {
                            if currentEvent != nil || !dataLines.isEmpty { flush() }
                            continue
                        }

                        if line.hasPrefix(":") {
                            continue
                        }

                        if line.hasPrefix("event:") {
                            if currentEvent != nil || !dataLines.isEmpty { flush() }
                            var value = String(line.dropFirst("event:".count))
                            if value.first == " " { value.removeFirst() }
                            currentEvent = value.trimmingCharacters(in: .whitespaces)
                        } else if line.hasPrefix("data:") {
                            var value = String(line.dropFirst("data:".count))
                            if value.first == " " { value.removeFirst() }
                            if currentEvent == "token" {
                                let token = decodeTokenPayload(value)
                                continuation.yield(.token(token))
                                currentEvent = nil
                                dataLines.removeAll(keepingCapacity: false)
                            } else if currentEvent == "thinking" {
                                let text = decodeTokenPayload(value)
                                if !text.isEmpty {
                                    continuation.yield(.thinking(text))
                                }
                                currentEvent = nil
                                dataLines.removeAll(keepingCapacity: false)
                            } else {
                                dataLines.append(value)
                            }
                        } else {
                            continue
                        }
                    }

                    if currentEvent != nil || !dataLines.isEmpty { flush() }
                    continuation.finish()
                } catch {
                    continuation.yield(.error(error.localizedDescription))
                    continuation.finish()
                }
            }

            continuation.onTermination = { _ in
                task.cancel()
            }
        }
    }

    private func decodePossiblyQuotedJSONString(_ value: String) -> String? {
        guard let data = value.data(using: .utf8) else { return nil }
        return try? JSONDecoder().decode(String.self, from: data)
    }

    private func decodeTokenPayload(_ value: String) -> String {
        if let decoded = decodePossiblyQuotedJSONString(value) {
            return decoded
        }
        return value
            .replacingOccurrences(of: "\\n", with: "\n")
            .replacingOccurrences(of: "\\t", with: "\t")
            .replacingOccurrences(of: "\\\"", with: "\"")
            .replacingOccurrences(of: "\\\\", with: "\\")
    }

    private func decodeErrorPayload(_ value: String) -> String {
        if let decoded = decodePossiblyQuotedJSONString(value) {
            return decoded
        }
        return value.trimmingCharacters(in: CharacterSet(charactersIn: "\""))
    }
}