import Foundation
import GRDB


final class LocalDatabase {
    static let shared = LocalDatabase()

    let dbQueue: DatabaseQueue

    private init() {
        let fm = FileManager.default
        let appSupport = try! fm.url(
            for: .applicationSupportDirectory,
            in: .userDomainMask,
            appropriateFor: nil,
            create: true
        )

        let dir = appSupport.appendingPathComponent("TalkToMe", isDirectory: true)
        try? fm.createDirectory(at: dir, withIntermediateDirectories: true)

        let dbURL = dir.appendingPathComponent("chat.sqlite3")

        var config = Configuration()
        config.label = "TalkToMe.LocalDatabase"

        config.prepareDatabase { db in
            try db.execute(sql: "PRAGMA journal_mode = WAL;")
            try db.execute(sql: "PRAGMA foreign_keys = ON;")
            try db.execute(sql: "PRAGMA busy_timeout = 5000;")
        }

        self.dbQueue = try! DatabaseQueue(path: dbURL.path, configuration: config)
        try! migrator.migrate(dbQueue)
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

        return migrator
    }
}

