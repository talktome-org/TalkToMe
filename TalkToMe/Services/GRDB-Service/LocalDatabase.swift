import Foundation
import GRDB


final class LocalDatabase {
    static let shared = LocalDatabase()

    private let fm = FileManager.default
    private let lock = NSLock()
    private var queuesByKey: [String: DatabaseQueue] = [:]

    private init() {
        // No eager DB creation. We open a per-user database lazily based on PreferenceKeys.currentUserId.
    }

    var dbQueue: DatabaseQueue {
        let key = currentUserKey()
        lock.lock()
        defer { lock.unlock() }
        if let q = queuesByKey[key] { return q }

        let url = databaseURL(for: key)
        let config = makeConfig(labelSuffix: key)
        let q = try! DatabaseQueue(path: url.path, configuration: config)
        try! migrator.migrate(q)
        queuesByKey[key] = q
        return q
    }

    private func makeConfig(labelSuffix: String) -> Configuration {
        var config = Configuration()
        config.label = "TalkToMe.LocalDatabase.\(labelSuffix)"
        config.prepareDatabase { db in
            try db.execute(sql: "PRAGMA journal_mode = WAL;")
            try db.execute(sql: "PRAGMA foreign_keys = ON;")
            try db.execute(sql: "PRAGMA busy_timeout = 5000;")
        }
        return config
    }

    private func currentUserKey() -> String {
        let raw = UserDefaults.standard.string(forKey: PreferenceKeys.currentUserId)?
            .trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        if raw.isEmpty { return "unauthenticated" }
        // File-safe key (UUIDs are already safe, but keep it defensive)
        let allowed = CharacterSet(charactersIn: "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-")
        let cleaned = raw.unicodeScalars.map { allowed.contains($0) ? Character($0) : "-" }
        return String(cleaned)
    }

    /// Closes and deletes the SQLite database for the current user.
    func destroyCurrentUserDatabase() {
        let key = currentUserKey()
        lock.lock()
        queuesByKey.removeValue(forKey: key)
        lock.unlock()

        let path = databaseURL(for: key).path
        for suffix in ["", "-wal", "-shm"] {
            try? fm.removeItem(atPath: path + suffix)
        }
    }

    private func baseDir() -> URL {
        let appSupport = try! fm.url(
            for: .applicationSupportDirectory,
            in: .userDomainMask,
            appropriateFor: nil,
            create: true
        )
        let dir = appSupport.appendingPathComponent("TalkToMe", isDirectory: true)
        try? fm.createDirectory(at: dir, withIntermediateDirectories: true)
        return dir
    }

    private func databaseURL(for key: String) -> URL {
        let dir = baseDir()
        return dir.appendingPathComponent("chat-\(key).sqlite3")
    }

    private var migrator: DatabaseMigrator {
        var migrator = DatabaseMigrator()

        migrator.registerMigration("create_chat_tables") { db in
            try db.create(table: "sessions", ifNotExists: true) { t in
                t.column("id", .text).primaryKey()
                t.column("title", .text)
                t.column("last_message_at", .text)
                t.column("last_message_content", .text)
            }
            try db.create(table: "messages", ifNotExists: true) { t in
                t.column("id", .text).primaryKey()
                t.column("session_id", .text).notNull().indexed().references("sessions", onDelete: .cascade)
                t.column("user_id", .text).notNull()
                t.column("role", .text).notNull()
                t.column("content", .text).notNull()
                t.column("created_at", .text).notNull().indexed()
            }
            try db.create(table: "attachments", ifNotExists: true) { t in
                t.autoIncrementedPrimaryKey("pk")
                t.column("message_id", .text).notNull().indexed().references("messages", onDelete: .cascade)
                t.column("kind", .text).notNull()
                t.column("remote_url", .text).notNull().indexed()
                t.column("local_relpath", .text)
                t.column("content_type", .text)
                t.column("filename", .text)
                t.column("created_at", .text).notNull()
                t.uniqueKey(["message_id", "remote_url"])
            }
        }

        migrator.registerMigration("create_outbox") { db in
            try db.create(table: "outbox", ifNotExists: true) { t in
                t.column("id", .text).primaryKey()
                t.column("kind", .text).notNull()
                t.column("session_id", .text).notNull()
                t.column("server_session_id", .text)
                t.column("message", .text).notNull()
                t.column("attachments_json", .text)
                t.column("status", .text).notNull()
                t.column("created_at", .text).notNull().indexed()
                t.column("last_error", .text)
            }
        }

        migrator.registerMigration("outbox_add_friend_user_id") { db in
            try db.alter(table: "outbox") { t in
                t.add(column: "friend_user_id", .text)
            }
        }

        migrator.registerMigration("outbox_add_message_id") { db in
            try db.alter(table: "outbox") { t in
                t.add(column: "message_id", .text)
            }
        }

        // One-time cleanup for duplicates created by older clients:
        // - local optimistic user messages used client ISO timestamps ending in 'Z'
        // - server messages are stored with a different id and a non-'Z' timestamp
        //
        // After we switched to idempotent client message ids, new duplicates should stop.
        // This migration removes existing duplicates so we can delete runtime de-dupe logic.
        migrator.registerMigration("cleanup_optimistic_user_message_duplicates_v1") { db in
            try db.execute(
                sql: """
                DELETE FROM messages
                WHERE role = 'user'
                  AND (created_at LIKE '%Z' OR created_at LIKE '%z')
                  AND EXISTS (
                    SELECT 1
                    FROM messages m2
                    WHERE m2.session_id = messages.session_id
                      AND m2.user_id = messages.user_id
                      AND m2.role = messages.role
                      AND m2.content = messages.content
                      AND m2.id <> messages.id
                      AND NOT (m2.created_at LIKE '%Z' OR m2.created_at LIKE '%z')
                      AND abs(strftime('%s', m2.created_at) - strftime('%s', messages.created_at)) <= 120
                  )
                """
            )
        }

        migrator.registerMigration("messages_add_regeneration_count") { db in
            try db.alter(table: "messages") { t in
                t.add(column: "regeneration_count", .integer).defaults(to: 0)
            }
        }

        migrator.registerMigration("messages_add_ghost_name") { db in
            try db.alter(table: "messages") { t in
                t.add(column: "ghost_name", .text)
            }
        }

        migrator.registerMigration("messages_add_thinking_summary") { db in
            try db.alter(table: "messages") { t in
                t.add(column: "thinking_summary", .text)
            }
        }

        migrator.registerMigration("messages_add_is_voice_mode") { db in
            try db.alter(table: "messages") { t in
                t.add(column: "is_voice_mode", .boolean).defaults(to: false)
            }
        }

        migrator.registerMigration("sessions_add_unread_count") { db in
            try db.alter(table: "sessions") { t in
                t.add(column: "unread_count", .integer).defaults(to: 0)
            }
        }

        return migrator
    }
}

