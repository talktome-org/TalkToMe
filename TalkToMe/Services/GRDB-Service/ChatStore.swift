import Foundation
import GRDB

struct ChatSessionRecord: Codable, FetchableRecord, PersistableRecord {
    static let databaseTableName = "sessions"
    var id: String
    var title: String?
    var last_message_at: String?
    var last_message_content: String?
    var unread_count: Int = 0
}

struct ChatMessageRecord: Codable, FetchableRecord, PersistableRecord {
    static let databaseTableName = "messages"
    var id: String
    var session_id: String
    var user_id: String
    var role: String
    var content: String
    var created_at: String
    var regeneration_count: Int
    var ghost_name: String?
    var thinking_summary: String?
    var is_voice_mode: Bool
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
                return ChatSession(id: uuid, title: r.title, lastUsedISO8601: r.last_message_at, lastMessageContent: r.last_message_content, unreadCount: r.unread_count)
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
                        last_message_content: s.lastMessageContent,
                        unread_count: s.unreadCount
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
                        last_message_content: s.lastMessageContent,
                        unread_count: s.unreadCount
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

    /// Deletes all messages in a session that come after the given message (by created_at).
    /// When `includeAnchor` is true the anchor message itself is also deleted.
    func deleteMessagesAfter(messageId: UUID, sessionId: UUID, includeAnchor: Bool = false) async {
        let mid = messageId.uuidString
        let sid = sessionId.uuidString

        let attachmentRelpathsToDelete: [String]
        do {
            attachmentRelpathsToDelete = try await dbQueue.write { db -> [String] in
                // Find the anchor message's timestamp
                guard let anchorCreatedAt = try String.fetchOne(
                    db,
                    sql: "SELECT created_at FROM messages WHERE id = ?",
                    arguments: [mid]
                ) else { return [] }

                let rels: [String]
                if includeAnchor {
                    rels = try String.fetchAll(
                        db,
                        sql: """
                        SELECT a.local_relpath
                        FROM attachments a
                        JOIN messages m ON m.id = a.message_id
                        WHERE m.session_id = ? AND m.created_at >= ?
                          AND a.local_relpath IS NOT NULL
                        """,
                        arguments: [sid, anchorCreatedAt]
                    )
                    try db.execute(
                        sql: "DELETE FROM messages WHERE session_id = ? AND created_at >= ?",
                        arguments: [sid, anchorCreatedAt]
                    )
                } else {
                    rels = try String.fetchAll(
                        db,
                        sql: """
                        SELECT a.local_relpath
                        FROM attachments a
                        JOIN messages m ON m.id = a.message_id
                        WHERE m.session_id = ?
                          AND (m.created_at > ? OR (m.created_at = ? AND m.id != ?))
                          AND a.local_relpath IS NOT NULL
                        """,
                        arguments: [sid, anchorCreatedAt, anchorCreatedAt, mid]
                    )
                    try db.execute(
                        sql: """
                        DELETE FROM messages
                        WHERE session_id = ?
                          AND (created_at > ? OR (created_at = ? AND id != ?))
                        """,
                        arguments: [sid, anchorCreatedAt, anchorCreatedAt, mid]
                    )
                }

                return rels
            }
        } catch {
            return
        }

        removeCachedAttachmentFiles(relativePaths: attachmentRelpathsToDelete)
    }

    struct MessageLocalMetadata {
        let thinkingSummary: String?
        let regenerationCount: Int
        let isVoiceMode: Bool
        let ghostName: String?
    }

    /// Returns local-only metadata (thinking_summary, regeneration_count, is_voice_mode, ghost_name) for all messages in a session, keyed by message ID.
    func loadLocalMetadata(sessionId: UUID) async -> [String: MessageLocalMetadata] {
        let sid = sessionId.uuidString
        do {
            return try await dbQueue.read { db in
                let rows = try Row.fetchAll(
                    db,
                    sql: """
                    SELECT id, thinking_summary, regeneration_count, is_voice_mode, ghost_name FROM messages
                    WHERE session_id = ?
                      AND (
                        (thinking_summary IS NOT NULL AND thinking_summary != '')
                        OR regeneration_count > 0
                        OR is_voice_mode = 1
                        OR (ghost_name IS NOT NULL AND ghost_name != '')
                      )
                    """,
                    arguments: [sid]
                )
                var result: [String: MessageLocalMetadata] = [:]
                for row in rows {
                    let id: String = row["id"]
                    let ts: String? = row["thinking_summary"]
                    let rc: Int = row["regeneration_count"] ?? 0
                    let vm: Bool = row["is_voice_mode"] ?? false
                    let gn: String? = row["ghost_name"]
                    result[id] = MessageLocalMetadata(thinkingSummary: ts, regenerationCount: rc, isVoiceMode: vm, ghostName: gn)
                }
                return result
            }
        } catch {
            return [:]
        }
    }

    /// Sets thinking_summary on the last assistant message in a session.
    func setThinkingSummaryForLastAssistant(sessionId: UUID, summary: String) async {
        let sid = sessionId.uuidString
        do {
            try await dbQueue.write { db in
                try db.execute(
                    sql: """
                    UPDATE messages SET thinking_summary = ?
                    WHERE id = (
                        SELECT id FROM messages
                        WHERE session_id = ? AND role = 'assistant'
                        ORDER BY created_at DESC
                        LIMIT 1
                    )
                    """,
                    arguments: [summary, sid]
                )
            }
        } catch {}
    }

    /// Sets ghost_name on the last assistant message in a session.
    func setGhostNameForLastAssistant(sessionId: UUID, ghostName: String) async {
        let sid = sessionId.uuidString
        do {
            try await dbQueue.write { db in
                try db.execute(
                    sql: """
                    UPDATE messages SET ghost_name = ?
                    WHERE id = (
                        SELECT id FROM messages
                        WHERE session_id = ? AND role = 'assistant'
                        ORDER BY created_at DESC
                        LIMIT 1
                    )
                    """,
                    arguments: [ghostName, sid]
                )
            }
        } catch {}
    }

    /// Sets ghost_name and is_voice_mode on the last user message in a session.
    func setVoiceMetadataForLastUserMessage(sessionId: UUID, ghostName: String) async {
        let sid = sessionId.uuidString
        do {
            try await dbQueue.write { db in
                try db.execute(
                    sql: """
                    UPDATE messages SET ghost_name = ?, is_voice_mode = 1
                    WHERE id = (
                        SELECT id FROM messages
                        WHERE session_id = ? AND role = 'user'
                        ORDER BY created_at DESC
                        LIMIT 1
                    )
                    """,
                    arguments: [ghostName, sid]
                )
            }
        } catch {}
    }

    /// Sets is_voice_mode on the last assistant message in a session.
    func setVoiceModeForLastAssistant(sessionId: UUID) async {
        let sid = sessionId.uuidString
        do {
            try await dbQueue.write { db in
                try db.execute(
                    sql: """
                    UPDATE messages SET is_voice_mode = 1
                    WHERE id = (
                        SELECT id FROM messages
                        WHERE session_id = ? AND role = 'assistant'
                        ORDER BY created_at DESC
                        LIMIT 1
                    )
                    """,
                    arguments: [sid]
                )
            }
        } catch {}
    }

    /// Sets regeneration_count on the last assistant message in a session.
    func setRegenerationCountForLastAssistant(sessionId: UUID, count: Int) async {
        let sid = sessionId.uuidString
        do {
            try await dbQueue.write { db in
                try db.execute(
                    sql: """
                    UPDATE messages SET regeneration_count = ?
                    WHERE id = (
                        SELECT id FROM messages
                        WHERE session_id = ? AND role = 'assistant'
                        ORDER BY created_at DESC
                        LIMIT 1
                    )
                    """,
                    arguments: [count, sid]
                )
            }
        } catch {}
    }

    /// Updates the regeneration_count for a single message in the local DB.
    func updateRegenerationCount(messageId: UUID, count: Int) async {
        do {
            try await dbQueue.write { db in
                try db.execute(
                    sql: "UPDATE messages SET regeneration_count = ? WHERE id = ?",
                    arguments: [count, messageId.uuidString]
                )
            }
        } catch {}
    }

    /// Sets ghost_name and is_voice_mode on a specific user message (used for speak-mode user messages).
    func setUserMessageVoiceMetadata(messageId: UUID, ghostName: String) async {
        let mid = messageId.uuidString
        do {
            try await dbQueue.write { db in
                try db.execute(
                    sql: "UPDATE messages SET ghost_name = ?, is_voice_mode = 1 WHERE id = ?",
                    arguments: [ghostName, mid]
                )
            }
        } catch {}
    }

    /// Sets ghost_name on all messages in a session that contain partner_draft content and don't yet have a ghost_name.
    func setGhostNameForPartnerDrafts(sessionId: UUID, ghostName: String) async {
        let sid = sessionId.uuidString
        do {
            try await dbQueue.write { db in
                try db.execute(
                    sql: "UPDATE messages SET ghost_name = ? WHERE session_id = ? AND ghost_name IS NULL AND content LIKE '%partner_draft%'",
                    arguments: [ghostName, sid]
                )
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
                var msg = ChatMessage(dto: dto, currentUserId: currentUserId)
                msg.regenerationCount = r.regeneration_count
                if let gn = r.ghost_name { msg.ghostName = gn }
                if let ts = r.thinking_summary, !ts.isEmpty { msg.thinkingSummary = ts }
                if r.is_voice_mode { msg.isFromVoiceMode = true }
                return msg
            }
        } catch {
            return []
        }
    }


    /// Reconciles local messages for a session with the server's message list.
    /// Upserts all server messages, then deletes local messages that are
    /// missing from the server response (unless tied to pending outbox items).
    func reconcileMessagesWithServer(_ dtos: [BackendService.ChatMessageDTO], sessionId: UUID) async {
        let sid = sessionId.uuidString
        let serverMessageIds = Set(dtos.map { $0.id.uuidString })

        if !dtos.isEmpty {
            await upsertMessages(dtos)
        }

        do {
            let attachmentRelpathsToDelete = try await dbQueue.write { db -> [String] in
                let localIds = try String.fetchAll(
                    db,
                    sql: "SELECT id FROM messages WHERE session_id = ?",
                    arguments: [sid]
                )
                let orphanIds = localIds.filter { !serverMessageIds.contains($0) }
                if orphanIds.isEmpty { return [] }

                var collectedRelpaths: [String] = []
                for mid in orphanIds {
                    // Preserve messages with pending outbox items
                    let pendingCount = try Int.fetchOne(
                        db,
                        sql: """
                        SELECT COUNT(1) FROM outbox
                        WHERE message_id = ?
                          AND status IN ('pending', 'failed', 'sending')
                        """,
                        arguments: [mid]
                    ) ?? 0
                    if pendingCount > 0 { continue }

                    let rels = try String.fetchAll(
                        db,
                        sql: """
                        SELECT local_relpath FROM attachments
                        WHERE message_id = ? AND local_relpath IS NOT NULL
                        """,
                        arguments: [mid]
                    )
                    collectedRelpaths.append(contentsOf: rels)

                    try db.execute(sql: "DELETE FROM messages WHERE id = ?", arguments: [mid])
                }

                return collectedRelpaths
            }

            removeCachedAttachmentFiles(relativePaths: attachmentRelpathsToDelete)
        } catch {}
    }

    func upsertMessages(_ dtos: [BackendService.ChatMessageDTO], voiceMetadata: (ghostName: String, messageId: UUID)? = nil) async {
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
                    // Preserve local-only metadata when upserting server messages.
                    // First try matching by ID; if not found, fall back to matching by
                    // session+role+content to handle server-assigned IDs that differ from
                    // client-generated ones (e.g. after reconciliation).
                    let existing = try ChatMessageRecord.fetchOne(db, key: dto.id.uuidString)
                        ?? ChatMessageRecord.fetchOne(db, sql: """
                            SELECT * FROM messages
                            WHERE session_id = ? AND role = ? AND content = ? AND id != ?
                            ORDER BY created_at DESC LIMIT 1
                            """, arguments: [dto.session_id.uuidString, dto.role, dto.content, dto.id.uuidString])
                    // If voice metadata targets this message, apply it in the same transaction
                    let isVoiceTarget = voiceMetadata.map { $0.messageId == dto.id } ?? false
                    let rec = ChatMessageRecord(
                        id: dto.id.uuidString,
                        session_id: dto.session_id.uuidString,
                        user_id: dto.user_id.uuidString,
                        role: dto.role,
                        content: dto.content,
                        created_at: created,
                        regeneration_count: existing?.regeneration_count ?? 0,
                        ghost_name: isVoiceTarget ? voiceMetadata?.ghostName : existing?.ghost_name,
                        thinking_summary: existing?.thinking_summary,
                        is_voice_mode: isVoiceTarget ? true : (existing?.is_voice_mode ?? false)
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