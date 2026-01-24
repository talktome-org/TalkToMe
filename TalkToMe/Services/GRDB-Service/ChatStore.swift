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

    private func currentUserKey() -> String {
        let raw = UserDefaults.standard.string(forKey: PreferenceKeys.currentUserId)?
            .trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        if raw.isEmpty { return "unauthenticated" }
        let allowed = CharacterSet(charactersIn: "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-")
        let cleaned = raw.unicodeScalars.map { allowed.contains($0) ? Character($0) : "-" }
        return String(cleaned)
    }

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

    // MARK: - Server reconciliation / pruning

    private func chatAttachmentsBaseDir() -> URL {
        let fm = FileManager.default
        let appSupport = (try? fm.url(
            for: .applicationSupportDirectory,
            in: .userDomainMask,
            appropriateFor: nil,
            create: true
        )) ?? fm.temporaryDirectory
        let dir = appSupport.appendingPathComponent("TalkToMe/ChatAttachments/\(currentUserKey())", isDirectory: true)
        try? fm.createDirectory(at: dir, withIntermediateDirectories: true)
        return dir
    }

    private func removeCachedAttachmentFiles(relativePaths: [String]) {
        guard !relativePaths.isEmpty else { return }
        let base = chatAttachmentsBaseDir()
        let fm = FileManager.default
        for rel in relativePaths {
            let trimmed = rel.trimmingCharacters(in: .whitespacesAndNewlines)
            guard !trimmed.isEmpty else { continue }
            let url = base.appendingPathComponent(trimmed)
            try? fm.removeItem(at: url)
        }
    }

    // NOTE: We do NOT "wipe on account switch" in prod.
    // Local chat isolation is handled by per-user database files and per-user attachment directories.

    /// Reconciles the local cache with the server's session list.
    /// - Upserts all server sessions locally
    /// - Deletes any local session that is missing from the server *unless* it has pending outbox items
    func reconcileSessionsWithServer(_ serverSessions: [ChatSession]) async {
        let serverIds = Set(serverSessions.map { $0.id.uuidString })
        let attachmentRelpathsToDelete: [String]

        do {
            attachmentRelpathsToDelete = try await dbQueue.write { db -> [String] in
                var collectedRelpaths: [String] = []
                // 1) Upsert server sessions
                for s in serverSessions {
                    let rec = ChatSessionRecord(
                        id: s.id.uuidString,
                        title: s.title,
                        last_message_at: s.lastUsedISO8601,
                        last_message_content: s.lastMessageContent
                    )
                    try rec.save(db)
                }

                // 2) Find local-only sessions (not on server)
                let localIds = try String.fetchAll(db, sql: "SELECT id FROM sessions")
                let candidates = localIds.filter { !serverIds.contains($0) }
                if candidates.isEmpty { return [] }

                for sid in candidates {
                    // Preserve sessions that still have unsent/retriable outbox items.
                    let pendingCount = try Int.fetchOne(
                        db,
                        sql: """
                        SELECT COUNT(1)
                        FROM outbox
                        WHERE (session_id = ? OR server_session_id = ?)
                          AND status IN ('pending', 'failed', 'sending')
                        """,
                        arguments: [sid, sid]
                    ) ?? 0
                    if pendingCount > 0 { continue }

                    // Collect cached attachment files for cleanup.
                    let rels = try String.fetchAll(
                        db,
                        sql: """
                        SELECT a.local_relpath
                        FROM attachments a
                        JOIN messages m ON m.id = a.message_id
                        WHERE m.session_id = ?
                          AND a.local_relpath IS NOT NULL
                        """,
                        arguments: [sid]
                    )
                    collectedRelpaths.append(contentsOf: rels)

                    // Remove outbox entries for this session (even if already 'sent', it's now orphaned).
                    try db.execute(
                        sql: "DELETE FROM outbox WHERE session_id = ? OR server_session_id = ?",
                        arguments: [sid, sid]
                    )

                    // Delete session (cascades messages + attachments rows).
                    try db.execute(sql: "DELETE FROM sessions WHERE id = ?", arguments: [sid])
                }

                return collectedRelpaths
            }
        } catch {
            return
        }

        // Best-effort file cleanup after DB commit.
        removeCachedAttachmentFiles(relativePaths: attachmentRelpathsToDelete)
    }

    /// Deletes a session from the local cache (sessions/messages/attachments) and cleans up cached attachment files.
    func deleteSessionLocal(sessionId: UUID) async {
        let sid = sessionId.uuidString
        let attachmentRelpathsToDelete: [String]

        do {
            attachmentRelpathsToDelete = try await dbQueue.write { db -> [String] in
                let rels = try String.fetchAll(
                    db,
                    sql: """
                    SELECT a.local_relpath
                    FROM attachments a
                    JOIN messages m ON m.id = a.message_id
                    WHERE m.session_id = ?
                      AND a.local_relpath IS NOT NULL
                    """,
                    arguments: [sid]
                )

                try db.execute(
                    sql: "DELETE FROM outbox WHERE session_id = ? OR server_session_id = ?",
                    arguments: [sid, sid]
                )
                try db.execute(sql: "DELETE FROM sessions WHERE id = ?", arguments: [sid])

                return rels
            }
        } catch {
            return
        }

        removeCachedAttachmentFiles(relativePaths: attachmentRelpathsToDelete)
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
                let dir = appSupport.appendingPathComponent("TalkToMe/ChatAttachments/\(currentUserKey())", isDirectory: true)
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
                // Messages have a FK to `sessions(id)`. Ensure the parent session rows exist so inserts never fail.
                // This matters on cold start (messages may load before sessions) and during local→server rekey races.
                let sessionIds = Set(dtos.map { $0.session_id.uuidString })
                for sid in sessionIds {
                    try db.execute(
                        sql: """
                        INSERT OR IGNORE INTO sessions (id, title, last_message_at, last_message_content)
                        VALUES (?, NULL, NULL, NULL)
                        """,
                        arguments: [sid]
                    )
                }

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
        // Attachment caching can involve network I/O; don't block message persistence on it.
        // Messages are already committed at this point, so chat navigation stays instant even if attachment caching is deferred.
        Task.detached { [dtos] in
            await ChatStore.shared.cacheAttachmentsIfNeeded(from: dtos)
        }
    }


    func rekeySession(oldId: UUID, newId: UUID) async {
        guard oldId != newId else { return }
        let old = oldId.uuidString
        let new = newId.uuidString

        do {
            try await dbQueue.write { db in
                let newExists = (try Int.fetchOne(
                    db,
                    sql: "SELECT COUNT(1) FROM sessions WHERE id = ?",
                    arguments: [new]
                ) ?? 0) > 0

                // If the server session already exists locally (e.g. a background refresh inserted it),
                // we can't UPDATE the PK. Instead, merge the old session's children into the new session and delete the old row.
                if newExists {
                    try db.execute(sql: "UPDATE messages SET session_id = ? WHERE session_id = ?", arguments: [new, old])
                    try db.execute(
                        sql: """
                        UPDATE outbox
                        SET session_id = ?, server_session_id = COALESCE(server_session_id, ?)
                        WHERE session_id = ?
                        """,
                        arguments: [new, new, old]
                    )
                    try db.execute(sql: "DELETE FROM sessions WHERE id = ?", arguments: [old])
                } else {
                    try db.execute(sql: "UPDATE sessions SET id = ? WHERE id = ?", arguments: [new, old])
                    try db.execute(sql: "UPDATE messages SET session_id = ? WHERE session_id = ?", arguments: [new, old])
                    try db.execute(
                        sql: """
                        UPDATE outbox
                        SET session_id = ?, server_session_id = COALESCE(server_session_id, ?)
                        WHERE session_id = ?
                        """,
                        arguments: [new, new, old]
                    )
                }
            }
        } catch {}
    }
}