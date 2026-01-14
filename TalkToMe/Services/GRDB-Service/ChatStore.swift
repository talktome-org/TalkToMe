import Foundation
import GRDB

struct ChatSessionRecord: Codable, FetchableRecord, PersistableRecord {
    static let databaseTableName = "sessions"
    var id: String
    var title: String?
    var last_message_at: String?
    var last_message_content: String?
}

struct ChatMessageRecord: Codable, FetchableRecord, PersistableRecord {
    static let databaseTableName = "messages"
    var id: String
    var session_id: String
    var user_id: String
    var role: String
    var content: String
    var created_at: String
}

struct ChatAttachmentRecord: Codable, FetchableRecord, PersistableRecord {
    static let databaseTableName = "attachments"
    var pk: Int64?
    var message_id: String
    var kind: String
    var remote_url: String
    var local_relpath: String?
    var content_type: String?
    var filename: String?
    var created_at: String
}


final class ChatStore {
    static let shared = ChatStore()

    private init() {}

    var dbQueue: DatabaseQueue { LocalDatabase.shared.dbQueue }
    let isoFormatter = ISO8601DateFormatter()

    struct RemoteAttachment {
        let messageId: String
        let kind: String
        let remoteURL: String
        let downloadURL: String
        let contentType: String?
        let filename: String?
    }


    func loadSessions() async -> [ChatSession] {
        do {
            let rows = try await dbQueue.read { db in
                try ChatSessionRecord.fetchAll(
                    db,
                    sql: """
                    SELECT *
                    FROM sessions
                    ORDER BY
                      CASE WHEN last_message_at IS NULL OR last_message_at = '' THEN 1 ELSE 0 END,
                      last_message_at DESC,
                      id DESC
                    """
                )
            }
            return rows.compactMap { r -> ChatSession? in
                guard let uuid = UUID(uuidString: r.id) else { return nil }
                return ChatSession(id: uuid, title: r.title, lastUsedISO8601: r.last_message_at, lastMessageContent: r.last_message_content)
            }
        } catch { return [] }
    }


    func upsertSessions(_ sessions: [ChatSession]) async {
        if sessions.isEmpty { return }
        do {
            try await dbQueue.write { db in
                for s in sessions {
                    let rec = ChatSessionRecord(
                        id: s.id.uuidString,
                        title: s.title,
                        last_message_at: s.lastUsedISO8601,
                        last_message_content: s.lastMessageContent
                    )
                    try rec.save(db)
                }
            }
        } catch {}
    }


    func loadMessages(sessionId: UUID, currentUserId: UUID) async -> [ChatMessage] {
        let sid = sessionId.uuidString
        do {
            let (messages, attachments) = try await dbQueue.read {
                db -> ([ChatMessageRecord], [ChatAttachmentRecord]) in
                let msgs = try ChatMessageRecord
                    .filter(Column("session_id") == sid)
                    .order(Column("created_at").asc)
                    .fetchAll(db)
                let atts = try ChatAttachmentRecord.fetchAll(
                    db,
                    sql: """
                    SELECT a.*
                    FROM attachments a
                    JOIN messages m ON m.id = a.message_id
                    WHERE m.session_id = ?
                    """,
                    arguments: [sid]
                )
                return (msgs, atts)
            }

            let base: URL = {
                let fm = FileManager.default
                let appSupport = (try? fm.url(for: .applicationSupportDirectory, in: .userDomainMask, appropriateFor: nil, create: true)) ?? fm.temporaryDirectory
                let dir = appSupport.appendingPathComponent("TalkToMe/ChatAttachments", isDirectory: true)
                try? fm.createDirectory(at: dir, withIntermediateDirectories: true)
                return dir
            }()
            let replacements: [String: String] = attachments.reduce(into: [:]) { dict, a in
                guard let rel = a.local_relpath else { return }
                let key = canonicalRemoteURL(a.remote_url)
                guard dict[key] == nil else { return }
                dict[key] = base.appendingPathComponent(rel).absoluteString
            }

            return messages.compactMap { r -> ChatMessage? in
                guard
                    let mid = UUID(uuidString: r.id),
                    let uid = UUID(uuidString: r.user_id)
                else { return nil }

                let content = replacements.isEmpty ? r.content : rewriteSegmentURLs(content: r.content, replacements: replacements)
                let dto = BackendService.ChatMessageDTO(
                    id: mid,
                    user_id: uid,
                    session_id: sessionId,
                    role: r.role,
                    content: content,
                    created_at: r.created_at
                )
                return ChatMessage(dto: dto, currentUserId: currentUserId)
            }
        } catch {
            return []
        }
    }


    func upsertMessages(_ dtos: [BackendService.ChatMessageDTO]) async {
        if dtos.isEmpty { return }
        let wrote: Bool
        do {
            try await dbQueue.write { db in
                let nowISO = isoFormatter.string(from: Date())
                for dto in dtos {
                    let created = dto.created_at ?? nowISO
                    let rec = ChatMessageRecord(
                        id: dto.id.uuidString,
                        session_id: dto.session_id.uuidString,
                        user_id: dto.user_id.uuidString,
                        role: dto.role,
                        content: dto.content,
                        created_at: created
                    )
                    try rec.save(db)
                }
            }
            wrote = true
        } catch {
            wrote = false
        }
        guard wrote else { return }  // Cache ONLY if messages were successfully written
        await cacheAttachmentsIfNeeded(from: dtos)
    }


    func rekeySession(oldId: UUID, newId: UUID) async {
        guard oldId != newId else { return }
        let old = oldId.uuidString
        let new = newId.uuidString

        do {
            try await dbQueue.write { db in
                try db.execute(sql: "UPDATE sessions SET id = ? WHERE id = ?", arguments: [new, old])
                try db.execute(sql: "UPDATE messages SET session_id = ? WHERE session_id = ?", arguments: [new, old])
                try db.execute(
                    sql: "UPDATE outbox SET session_id = ?, server_session_id = COALESCE(server_session_id, ?) WHERE session_id = ?",
                    arguments: [new, new, old]
                )
            }
        } catch {}
    }
}