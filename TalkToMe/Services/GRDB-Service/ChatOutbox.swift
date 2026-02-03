import Foundation
import GRDB
import Combine

struct OutboxItem: Codable, FetchableRecord, PersistableRecord {
    static let databaseTableName = "outbox"
    var id: String
    var kind: String
    var session_id: String
    var server_session_id: String?
    var friend_user_id: String?
    var message_id: String?
    var message: String
    var attachments_json: String?
    var status: String
    var created_at: String
    var last_error: String?
}

struct OutboxAttachment: Codable {
    var type: String
    var filename: String
    var contentType: String
    var localPath: String
}


@MainActor
final class ChatOutboxProcessor {
    static let shared = ChatOutboxProcessor()

    private static let isoFormatter = ISO8601DateFormatter()

    private var task: Task<Void, Never>?

    private init() {}


    func start() {
        if task != nil { return }
        task = Task { [weak self] in
            guard let self else { return }
            for await online in NetworkMonitor.shared.$isOnline.values {
                if online {
                    await self.flush()
                }
            }
        }
        Task { [weak self] in
            await self?.flush()
        }
    }


    func enqueueChatMessage(
        sessionId: UUID,
        serverSessionId: UUID?,
        friendUserId: UUID?,
        messageId: UUID,
        message: String,
        attachments: [OutboxAttachment]
    ) async {
        let id = UUID().uuidString
        let createdAt = isoNow()
        let attachmentsJSON = encodeAttachments(attachments)

        let item = OutboxItem(
            id: id,
            kind: "chat_message",
            session_id: sessionId.uuidString,
            server_session_id: serverSessionId?.uuidString,
            friend_user_id: friendUserId?.uuidString,
            message_id: messageId.uuidString,
            message: message,
            attachments_json: attachmentsJSON,
            status: "pending",
            created_at: createdAt,
            last_error: nil
        )

        _ = try? await LocalDatabase.shared.dbQueue.write { db in
            try item.insert(db)
        }
    }


    func flush() async {
        guard NetworkMonitor.shared.isOnline else { return }
        guard AuthService.shared.isAuthenticated else { return }

        let pending: [OutboxItem]
        do {
            pending = try await fetchPending()
        } catch {
            return
        }
        if pending.isEmpty { return }

        for item in pending {
            do {
                switch item.kind {
                case "chat_message":
                    try await flushChatMessage(item)
                default:
                    try await updateItem(item.id, status: "failed", lastError: "Unknown outbox kind")
                }
            } catch {
                let msg = (error as NSError).localizedDescription
                try? await updateItem(item.id, status: "failed", lastError: msg)
            }
        }
    }

    private func flushChatMessage(_ item: OutboxItem) async throws {
        // The outbox table can be mutated mid-flush (e.g. when a previous item triggers a local→server rekey).
        // Always reload the latest row by id so we don't act on stale session/server ids.
        let current: OutboxItem
        do {
            guard let fresh = try await fetchItem(id: item.id) else { return }
            current = fresh
        } catch {
            current = item
        }

        guard let localSid = UUID(uuidString: current.session_id) else { throw NSError() }
        let existingServer = current.server_session_id.flatMap { UUID(uuidString: $0) }
        let serverSid = try await ensureServerSessionId(localSessionId: localSid, existing: existingServer)

        try await updateItem(current.id, status: "sending", serverSessionId: serverSid.uuidString, lastError: nil)

        let accessToken = try await requireAccessToken()

        let attachments = decodeAttachments(current.attachments_json)
        var uploaded: [BackendService.ChatAttachment] = []
        if !attachments.isEmpty {
            for att in attachments {
                let fileURL = URL(fileURLWithPath: att.localPath)
                let data = try Data(contentsOf: fileURL)
                let res = try await BackendService.shared.uploadChatAttachment(
                    fileData: data,
                    filename: att.filename,
                    contentType: att.contentType,
                    accessToken: accessToken
                )
                uploaded.append(
                    BackendService.ChatAttachment(
                        type: att.type,
                        path: res.path,
                        filename: att.filename,
                        contentType: att.contentType
                    )
                )
            }
        }

        let friendUserId = current.friend_user_id.flatMap { UUID(uuidString: $0) }
        let messageId = current.message_id.flatMap { UUID(uuidString: $0) }
        let stream = BackendService.shared.streamChatMessage(
            current.message,
            sessionId: serverSid,
            chatHistory: nil,
            attachments: uploaded.isEmpty ? nil : uploaded,
            accessToken: accessToken,
            previousResponseId: nil,
            friendUserId: friendUserId,
            messageId: messageId
        )

        for await event in stream {
            switch event {
            case .done:
                try await updateItem(current.id, status: "sent")
                let previewToSend: String = {
                    let trimmed = current.message.trimmingCharacters(in: .whitespacesAndNewlines)
                    if !trimmed.isEmpty { return trimmed }
                    let atts = decodeAttachments(current.attachments_json)
                    if atts.isEmpty { return "" }
                    let hasImage = atts.contains(where: { $0.type == "image" })
                    return hasImage ? "Sent a photo." : "Sent an attachment."
                }()
                NotificationCenter.default.post(name: .chatMessageSent, object: nil, userInfo: [
                    "sessionId": serverSid,
                    "messageContent": previewToSend
                ])
                NotificationCenter.default.post(name: .chatSessionsNeedRefresh, object: nil)
                return
            case .error(let msg):
                throw NSError(domain: "Outbox", code: -3, userInfo: [NSLocalizedDescriptionKey: msg])
            default:
                continue
            }
        }
    }

    private func isoNow() -> String {
        Self.isoFormatter.string(from: Date())
    }

    private func requireAccessToken() async throws -> String {
        if let t = await AuthService.shared.getAccessToken() { return t }
        throw NSError(domain: "Outbox", code: -1, userInfo: [NSLocalizedDescriptionKey: "Missing access token"])
    }

    private func encodeAttachments(_ attachments: [OutboxAttachment]) -> String? {
        guard !attachments.isEmpty else { return nil }
        let data = try? JSONEncoder().encode(attachments)
        return data.flatMap { String(data: $0, encoding: .utf8) }
    }

    private func decodeAttachments(_ json: String?) -> [OutboxAttachment] {
        guard let json, let data = json.data(using: .utf8) else { return [] }
        return (try? JSONDecoder().decode([OutboxAttachment].self, from: data)) ?? []
    }

    private func fetchPending() async throws -> [OutboxItem] {
        try await LocalDatabase.shared.dbQueue.read { db in
            try OutboxItem
                .filter(sql: "status IN (?, ?)", arguments: ["pending", "failed"])
                .order(Column("created_at").asc)
                .fetchAll(db)
        }
    }

    private func fetchItem(id: String) async throws -> OutboxItem? {
        try await LocalDatabase.shared.dbQueue.read { db in
            try OutboxItem
                .filter(Column("id") == id)
                .fetchOne(db)
        }
    }

    private func updateItem(_ id: String, status: String, serverSessionId: String? = nil, lastError: String? = nil) async throws {
        try await LocalDatabase.shared.dbQueue.write { db in
            try db.execute(
                sql: """
                UPDATE outbox
                SET status = ?, server_session_id = COALESCE(?, server_session_id), last_error = ?
                WHERE id = ?
                """,
                arguments: [status, serverSessionId, lastError, id]
            )
        }
    }

    private func ensureServerSessionId(localSessionId: UUID, existing: UUID?) async throws -> UUID {
        if let existing { return existing }
        let accessToken = try await requireAccessToken()
        let dto = try await BackendService.shared.createEmptySession(accessToken: accessToken)
        let serverId = dto.id

        await ChatStore.shared.rekeySession(oldId: localSessionId, newId: serverId)
        NotificationCenter.default.post(name: .chatSessionRekeyed, object: nil, userInfo: [
            "oldSessionId": localSessionId,
            "newSessionId": serverId
        ])
        NotificationCenter.default.post(name: .chatSessionCreated, object: nil, userInfo: [
            "sessionId": serverId,
            "title": ChatSession.defaultTitle,
            "lastUsedISO8601": isoNow(),
            "lastMessageContent": ""
        ])

        return serverId
    }
}