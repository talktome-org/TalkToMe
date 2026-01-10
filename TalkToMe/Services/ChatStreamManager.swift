import Foundation

final class ChatStreamManager {

    static let shared = ChatStreamManager()

    private init() {}

    private var tasks: [UUID: Task<Void, Never>] = [:]

    struct StartParams {
        let message: String
        let sessionId: UUID?
        let chatHistory: [ChatHistoryMessage]?
        let attachments: [BackendService.ChatAttachment]?
        let accessToken: String
        let focusSnippet: String?
        let previousResponseId: String?
    }

    @discardableResult
    func startStream(
        params: StartParams,
        onEvent: @escaping (BackendService.StreamEvent) -> Void,
        onFinish: (() -> Void)? = nil
    ) -> UUID {
        let handleId = UUID()

        let task = Task.detached { [handleId] in
            let stream = BackendService.shared.streamChatMessage(
                params.message,
                sessionId: params.sessionId,
                chatHistory: params.chatHistory,
                attachments: params.attachments,
                accessToken: params.accessToken,
                focusSnippet: params.focusSnippet,
                previousResponseId: params.previousResponseId
            )
            for await event in stream {
                onEvent(event)
                if case .done = event { break }
                if case .error(_) = event { break }
            }
            onFinish?()
            await ChatStreamManager.shared.removeTask(handleId)
        }

        tasks[handleId] = task
        return handleId
    }

    func cancel(handleId: UUID?) {
        guard let id = handleId, let t = tasks[id] else { return }
        t.cancel()
        tasks[id] = nil
    }

    @MainActor
    private func removeTask(_ id: UUID) {
        tasks[id] = nil
    }
}


