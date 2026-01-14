import Foundation
import GRDB

extension ChatStore {

    func cacheAttachmentsIfNeeded(from dtos: [BackendService.ChatMessageDTO], isRetry: Bool = false) async {
        if dtos.isEmpty { return }
        if await AppSyncGate.shared.isSyncing() {
            if !isRetry {
                Task.detached { [dtos] in
                    await ChatStore.waitForSyncToFinish(timeoutSeconds: 8)
                    await ChatStore.shared.cacheAttachmentsIfNeeded(from: dtos, isRetry: true)
                }
            }
            return
        }

        let extracted = dtos.flatMap { dto in
            extractRemoteAttachments(messageId: dto.id.uuidString, content: dto.content)
        }
        guard !extracted.isEmpty else { return }

        var seen: Set<String> = []
        let unique = extracted.filter { seen.insert($0.messageId + "|" + $0.remoteURL).inserted }
        let batchSize = 6
        for start in stride(from: 0, to: unique.count, by: batchSize) {
            let end = min(unique.count, start + batchSize)
            let slice = unique[start..<end]
            await withTaskGroup(of: Void.self) { group in
                for item in slice {
                    group.addTask { await self.cacheOneAttachment(item) }
                }
            }
        }
    }

    static func waitForSyncToFinish(timeoutSeconds: Double) async {
        let pollNanoseconds: UInt64 = 200_000_000
        let deadline = Date().addingTimeInterval(timeoutSeconds)
        while Date() < deadline {
            if await AppSyncGate.shared.isSyncing() == false { return }
            try? await Task.sleep(nanoseconds: pollNanoseconds)
        }
    }


    func cacheOneAttachment(_ att: RemoteAttachment) async {
        guard let url = URL(string: att.downloadURL) else { return }

        let canonical = att.remoteURL
        let likeSigned = canonical + "?%"

        let existingRows: [ChatAttachmentRecord]
        do {
            existingRows = try await dbQueue.read { db in
                try fetchAttachmentRows(db, messageId: att.messageId, canonical: canonical, likeSigned: likeSigned)
            }
        } catch { return }

        let base: URL = {
            let fm = FileManager.default
            let appSupport = (try? fm.url(for: .applicationSupportDirectory, in: .userDomainMask, appropriateFor: nil, create: true)) ?? fm.temporaryDirectory
            let dir = appSupport.appendingPathComponent("TalkToMe/ChatAttachments", isDirectory: true)
            try? fm.createDirectory(at: dir, withIntermediateDirectories: true)
            return dir
        }()
        let existingWithFile = existingRows.first { row in
            guard let rel = row.local_relpath else { return false }
            let fileURL = base.appendingPathComponent(rel)
            return FileManager.default.fileExists(atPath: fileURL.path)
        }

        // If the attachment exists locally, consolidate duplicates to the canonical URL
        if let existingWithFile {
            do {
                try await dbQueue.write { db in
                    let rows = try fetchAttachmentRows(db, messageId: att.messageId, canonical: canonical, likeSigned: likeSigned)
                    if let canonicalRow = rows.first(where: { $0.remote_url == canonical }) {
                        if canonicalRow.local_relpath == nil, let rel = existingWithFile.local_relpath {
                            try db.execute(
                                sql: """
                                UPDATE attachments
                                SET local_relpath = ?, content_type = COALESCE(?, content_type), filename = COALESCE(?, filename)
                                WHERE message_id = ? AND remote_url = ?
                                """,
                                arguments: [rel, existingWithFile.content_type, existingWithFile.filename, att.messageId, canonical]
                            )
                        }
                        try deleteAttachmentRows(db, rows: rows.filter { $0.remote_url != canonical }, keepingPK: nil)
                    } else {
                        if let pk = existingWithFile.pk {  // No canonical row yet: rewrite one existing row to canonical and delete the rest
                            try db.execute(
                                sql: "UPDATE attachments SET remote_url = ? WHERE pk = ?",
                                arguments: [canonical, pk]
                            )
                        }
                        try deleteAttachmentRows(db, rows: rows, keepingPK: existingWithFile.pk)
                    }
                }
            } catch {}
            return
        }

        // Download the attachment and cache it locally
        do {
            let (data, _) = try await URLSession.shared.data(from: url)
            guard !data.isEmpty else { return }

            let rel = "m/\(att.messageId)/\(UUID().uuidString)"
            let fileURL = base.appendingPathComponent(rel)
            try FileManager.default.createDirectory(at: fileURL.deletingLastPathComponent(), withIntermediateDirectories: true)
            try data.write(to: fileURL, options: [.atomic])

            let createdAt = isoFormatter.string(from: Date())

            try? await dbQueue.write { db in  // Re-check for duplicates and consolidate to canonical row
                let rows = try fetchAttachmentRows(db, messageId: att.messageId, canonical: canonical, likeSigned: likeSigned)
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
                    try deleteAttachmentRows(db, rows: rows.filter { $0.remote_url != canonical }, keepingPK: nil)
                } else if let keep = rows.first {  // Convert one existing row to canonical and update it
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
                    try deleteAttachmentRows(db, rows: rows, keepingPK: keep.pk)
                } else {  // No row exists yet: insert canonical record
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
            return
        }
    }

    func fetchAttachmentRows(_ db: Database, messageId: String, canonical: String, likeSigned: String) throws -> [ChatAttachmentRecord] {
        try ChatAttachmentRecord.fetchAll(
            db,
            sql: """
            SELECT *
            FROM attachments
            WHERE message_id = ?
              AND (remote_url = ? OR remote_url LIKE ?)
            """,
            arguments: [messageId, canonical, likeSigned]
        )
    }

    func deleteAttachmentRows(_ db: Database, rows: [ChatAttachmentRecord], keepingPK: Int64?) throws {
        for r in rows {
            guard let pk = r.pk else { continue }
            if let keepingPK, pk == keepingPK { continue }
            try db.execute(sql: "DELETE FROM attachments WHERE pk = ?", arguments: [pk])
        }
    }
}