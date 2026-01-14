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

    private var dbQueue: DatabaseQueue { ChatDatabase.shared.dbQueue }

    private func canonicalRemoteURL(_ urlString: String) -> String {
        guard let url = URL(string: urlString) else { return urlString }
        guard let scheme = url.scheme, (scheme == "http" || scheme == "https") else { return urlString }
        guard let host = url.host else { return urlString }
        var out = "\(scheme)://\(host)"
        if let port = url.port { out += ":\(port)" }
        out += url.path
        return out
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
        } catch {
            return []
        }
    }

    func upsertSessions(_ sessions: [ChatSession]) async {
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
        } catch {
            // ignore
        }
    }

    func loadMessages(sessionId: UUID, currentUserId: UUID) async -> [ChatMessage] {
        let sid = sessionId.uuidString
        do {
            let (messages, attachments) = try await dbQueue.read { db -> ([ChatMessageRecord], [ChatAttachmentRecord]) in
                let msgs = try ChatMessageRecord
                    .filter(Column("session_id") == sid)
                    .order(Column("created_at").asc)
                    .fetchAll(db)

                let msgIds = msgs.map { $0.id }
                let atts: [ChatAttachmentRecord]
                if msgIds.isEmpty {
                    atts = []
                } else {
                    // Use SQL for IN() to avoid API differences across GRDB versions.
                    let placeholders = msgIds.map { _ in "?" }.joined(separator: ",")
                    let sql = "SELECT * FROM attachments WHERE message_id IN (\(placeholders))"
                    atts = try ChatAttachmentRecord.fetchAll(db, sql: sql, arguments: StatementArguments(msgIds))
                }
                return (msgs, atts)
            }

            let base = attachmentsBaseDir()
            // Use canonical URL keys (no query) to avoid signed URL churn breaking replacement.
            var replacements: [String: String] = [:]
            for a in attachments {
                guard let rel = a.local_relpath else { continue }
                let fileURL = base.appendingPathComponent(rel)
                let key = canonicalRemoteURL(a.remote_url)
                // Keep the first cached file we see for a canonical URL.
                if replacements[key] == nil {
                    replacements[key] = fileURL.absoluteString
                }
            }

            return messages.compactMap { r in
                guard
                    let mid = UUID(uuidString: r.id),
                    let uid = UUID(uuidString: r.user_id),
                    let sidUUID = UUID(uuidString: r.session_id)
                else { return nil }

                let content = replacements.isEmpty ? r.content : rewriteSegmentURLs(content: r.content, replacements: replacements)
                let dto = ChatMessageDTO(
                    id: mid,
                    user_id: uid,
                    session_id: sidUUID,
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

    func upsertMessages(_ dtos: [ChatMessageDTO]) async {
        do {
            try await dbQueue.write { db in
                for dto in dtos {
                    let created = (dto.created_at ?? ISO8601DateFormatter().string(from: Date()))
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
        } catch {
            // ignore
        }

        // Best-effort: schedule attachment caching.
        await cacheAttachmentsIfNeeded(from: dtos)
    }

    func rekeySession(oldId: UUID, newId: UUID) async {
        guard oldId != newId else { return }
        let old = oldId.uuidString
        let new = newId.uuidString

        do {
            try await dbQueue.write { db in
                // Update sessions primary key and cascade all references manually.
                try db.execute(sql: "UPDATE sessions SET id = ? WHERE id = ?", arguments: [new, old])
                try db.execute(sql: "UPDATE messages SET session_id = ? WHERE session_id = ?", arguments: [new, old])
                try db.execute(
                    sql: "UPDATE outbox SET session_id = ?, server_session_id = COALESCE(server_session_id, ?) WHERE session_id = ?",
                    arguments: [new, new, old]
                )
            }
        } catch {
            // ignore
        }
    }

    // MARK: - Attachments

    private func attachmentsBaseDir() -> URL {
        let fm = FileManager.default
        let appSupport = (try? fm.url(for: .applicationSupportDirectory, in: .userDomainMask, appropriateFor: nil, create: true)) ?? fm.temporaryDirectory
        let dir = appSupport.appendingPathComponent("TalkToMe/ChatAttachments", isDirectory: true)
        try? fm.createDirectory(at: dir, withIntermediateDirectories: true)
        return dir
    }

    private func cacheAttachmentsIfNeeded(from dtos: [ChatMessageDTO]) async {
        let extracted = dtos.flatMap { dto in
            extractRemoteAttachments(messageId: dto.id.uuidString, content: dto.content)
        }
        guard !extracted.isEmpty else { return }
        // Deduplicate within this batch (important when URLs are signed or repeated).
        var unique: [RemoteAttachment] = []
        var seen: Set<String> = []
        for item in extracted {
            let key = item.messageId + "|" + item.remoteURL
            if seen.insert(key).inserted {
                unique.append(item)
            }
        }

        // Prefetch in parallel (bounded) so images become available quickly without spiking network.
        let batchSize = 6
        var idx = 0
        while idx < unique.count {
            let end = min(unique.count, idx + batchSize)
            let slice = unique[idx..<end]
            await withTaskGroup(of: Void.self) { group in
                for item in slice {
                    group.addTask { await self.cacheOneAttachment(item) }
                }
            }
            idx = end
        }
    }

    private struct RemoteAttachment {
        let messageId: String
        let kind: String
        let remoteURL: String // canonical (no query)
        let downloadURL: String // original (may be signed)
        let contentType: String?
        let filename: String?
    }

    private func cacheOneAttachment(_ att: RemoteAttachment) async {
        guard let url = URL(string: att.downloadURL) else { return }
        let canonical = att.remoteURL
        let likeSigned = canonical + "?%"

        // If already cached on disk, skip (even if the DB row uses an older signed URL).
        let existingRows: [ChatAttachmentRecord]
        do {
            existingRows = try await dbQueue.read { db in
                try ChatAttachmentRecord.fetchAll(
                    db,
                    sql: """
                    SELECT *
                    FROM attachments
                    WHERE message_id = ?
                      AND (remote_url = ? OR remote_url LIKE ?)
                    """,
                    arguments: [att.messageId, canonical, likeSigned]
                )
            }
        } catch {
            return
        }

        let base = attachmentsBaseDir()
        let existingWithFile = existingRows.first { row in
            guard let rel = row.local_relpath else { return false }
            let fileURL = base.appendingPathComponent(rel)
            return FileManager.default.fileExists(atPath: fileURL.path)
        }

        if let existingWithFile {
            // Best-effort cleanup: consolidate duplicates to the canonical URL.
            do {
                try await dbQueue.write { db in
                    let rows = try ChatAttachmentRecord.fetchAll(
                        db,
                        sql: """
                        SELECT *
                        FROM attachments
                        WHERE message_id = ?
                          AND (remote_url = ? OR remote_url LIKE ?)
                        """,
                        arguments: [att.messageId, canonical, likeSigned]
                    )

                    let canonicalRow = rows.first(where: { $0.remote_url == canonical })
                    if canonicalRow != nil {
                        // Ensure canonical row carries local_relpath.
                        if canonicalRow?.local_relpath == nil, let rel = existingWithFile.local_relpath {
                            try db.execute(
                                sql: """
                                UPDATE attachments
                                SET local_relpath = ?, content_type = COALESCE(?, content_type), filename = COALESCE(?, filename)
                                WHERE message_id = ? AND remote_url = ?
                                """,
                                arguments: [rel, existingWithFile.content_type, existingWithFile.filename, att.messageId, canonical]
                            )
                        }
                        // Delete signed-url duplicates.
                        for r in rows where r.remote_url != canonical {
                            if let pk = r.pk {
                                try db.execute(sql: "DELETE FROM attachments WHERE pk = ?", arguments: [pk])
                            }
                        }
                    } else {
                        // No canonical row yet: rewrite one existing row to canonical and delete the rest.
                        if let pk = existingWithFile.pk {
                            try db.execute(
                                sql: "UPDATE attachments SET remote_url = ? WHERE pk = ?",
                                arguments: [canonical, pk]
                            )
                        }
                        for r in rows where r.pk != existingWithFile.pk {
                            if let pk = r.pk {
                                try db.execute(sql: "DELETE FROM attachments WHERE pk = ?", arguments: [pk])
                            }
                        }
                    }
                }
            } catch {
                // ignore cleanup errors
            }
            return
        }

        do {
            let (data, _) = try await URLSession.shared.data(from: url)
            guard !data.isEmpty else { return }

            let rel = "m/\(att.messageId)/\(UUID().uuidString)"
            let fileURL = base.appendingPathComponent(rel)
            try FileManager.default.createDirectory(at: fileURL.deletingLastPathComponent(), withIntermediateDirectories: true)
            try data.write(to: fileURL, options: [.atomic])

            let createdAt = ISO8601DateFormatter().string(from: Date())
            do {
                try await dbQueue.write { db in
                    // Re-check for duplicates and consolidate to canonical row.
                    let rows = try ChatAttachmentRecord.fetchAll(
                        db,
                        sql: """
                        SELECT *
                        FROM attachments
                        WHERE message_id = ?
                          AND (remote_url = ? OR remote_url LIKE ?)
                        """,
                        arguments: [att.messageId, canonical, likeSigned]
                    )
                    let canonicalRow = rows.first(where: { $0.remote_url == canonical })

                    if canonicalRow != nil {
                        try db.execute(
                            sql: """
                            UPDATE attachments
                            SET local_relpath = ?, content_type = COALESCE(?, content_type), filename = COALESCE(?, filename)
                            WHERE message_id = ? AND remote_url = ?
                            """,
                            arguments: [rel, att.contentType, att.filename, att.messageId, canonical]
                        )
                        for r in rows where r.remote_url != canonical {
                            if let pk = r.pk {
                                try db.execute(sql: "DELETE FROM attachments WHERE pk = ?", arguments: [pk])
                            }
                        }
                    } else if let keep = rows.first {
                        // Convert one existing row to canonical and update it.
                        if let pk = keep.pk {
                            try db.execute(
                                sql: """
                                UPDATE attachments
                                SET remote_url = ?, local_relpath = ?, content_type = COALESCE(?, content_type), filename = COALESCE(?, filename)
                                WHERE pk = ?
                                """,
                                arguments: [canonical, rel, att.contentType, att.filename, pk]
                            )
                        }
                        for r in rows where r.pk != keep.pk {
                            if let pk = r.pk {
                                try db.execute(sql: "DELETE FROM attachments WHERE pk = ?", arguments: [pk])
                            }
                        }
                    } else {
                        // No row exists yet: insert canonical record.
                        let rec = ChatAttachmentRecord(
                            pk: nil,
                            message_id: att.messageId,
                            kind: att.kind,
                            remote_url: canonical,
                            local_relpath: rel,
                            content_type: att.contentType,
                            filename: att.filename,
                            created_at: createdAt
                        )
                        try rec.save(db)
                    }
                }
            } catch {
                // ignore
            }
        } catch {
            return
        }
    }

    private func extractRemoteAttachments(messageId: String, content: String) -> [RemoteAttachment] {
        guard let data = content.data(using: .utf8) else { return [] }
        guard let objAny = try? JSONSerialization.jsonObject(with: data) else { return [] }
        guard let obj = objAny as? [String: Any] else { return [] }

        let talktomeAny = obj["_talktome"]
        let talktome: [String: Any]? = {
            if let d = talktomeAny as? [String: Any] { return d }
            if let s = talktomeAny as? String,
               let sd = s.data(using: .utf8),
               let dd = try? JSONSerialization.jsonObject(with: sd) as? [String: Any] {
                return dd
            }
            return nil
        }()
        guard let meta = talktome, (meta["type"] as? String) == "segments" else { return [] }
        guard let segs = meta["segments"] as? [Any] else { return [] }

        var out: [RemoteAttachment] = []
        for segAny in segs {
            guard let seg = segAny as? [String: Any] else { continue }
            guard let type = seg["type"] as? String else { continue }
            guard type == "image" || type == "file" else { continue }
            guard let url = seg["url"] as? String, !url.isEmpty else { continue }
            let canonical = canonicalRemoteURL(url)
            let filename = seg["filename"] as? String
            let contentType = seg["content_type"] as? String
            out.append(
                RemoteAttachment(
                    messageId: messageId,
                    kind: type,
                    remoteURL: canonical,
                    downloadURL: url,
                    contentType: contentType,
                    filename: filename
                )
            )
        }
        return out
    }

    private func rewriteSegmentURLs(content: String, replacements: [String: String]) -> String {
        guard !replacements.isEmpty else { return content }
        guard let data = content.data(using: .utf8) else { return content }
        guard var obj = (try? JSONSerialization.jsonObject(with: data)) as? [String: Any] else { return content }

        func decodeMeta(_ any: Any?) -> [String: Any]? {
            if let d = any as? [String: Any] { return d }
            if let s = any as? String, let sd = s.data(using: .utf8) {
                return (try? JSONSerialization.jsonObject(with: sd)) as? [String: Any]
            }
            return nil
        }

        guard var meta = decodeMeta(obj["_talktome"]) else { return content }
        guard (meta["type"] as? String) == "segments" else { return content }
        guard var segs = meta["segments"] as? [Any] else { return content }

        var changed = false
        for i in 0..<segs.count {
            guard var seg = segs[i] as? [String: Any] else { continue }
            guard let url = seg["url"] as? String else { continue }
            let key = canonicalRemoteURL(url)
            if let repl = replacements[key] {
                seg["url"] = repl
                segs[i] = seg
                changed = true
            }
        }
        if !changed { return content }

        meta["segments"] = segs
        obj["_talktome"] = meta
        guard let out = try? JSONSerialization.data(withJSONObject: obj) else { return content }
        return String(data: out, encoding: .utf8) ?? content
    }
}

