import SwiftUI
import GRDB

#if DEBUG
struct LocalDatabaseDebugView: View {
    @Environment(\.dismiss) private var dismiss

    @State private var isLoading: Bool = false
    @State private var loadError: String? = nil
    @State private var counts: [String: Int] = [:]
    @State private var exportURLs: [URL] = []
    @State private var showShareSheet: Bool = false

    var body: some View {
        NavigationStack {
            List {
                Section("Database") {
                    LabeledContent("File", value: ChatDatabase.shared.databaseURL.lastPathComponent)
                    LabeledContent("Path", value: ChatDatabase.shared.databaseURL.path)
                    LabeledContent("Size", value: formattedSize(for: ChatDatabase.shared.databaseURL))
                }

                Section("Tables") {
                    LabeledContent("sessions", value: "\(counts["sessions"] ?? 0)")
                    LabeledContent("messages", value: "\(counts["messages"] ?? 0)")
                    LabeledContent("attachments", value: "\(counts["attachments"] ?? 0)")
                    LabeledContent("outbox", value: "\(counts["outbox"] ?? 0)")
                }

                if let err = loadError, !err.isEmpty {
                    Section("Error") {
                        Text(err)
                            .foregroundStyle(.secondary)
                    }
                }

                Section {
                    Button(action: {
                        Haptics.impact(.light)
                        Task { await refresh() }
                    }) {
                        HStack {
                            Text("Refresh")
                            Spacer()
                            if isLoading { ProgressView().controlSize(.small) }
                        }
                    }

                    Button(action: {
                        Haptics.impact(.light)
                        Task { await prepareExportAndShare() }
                    }) {
                        Text("Share database files (sqlite + wal + shm)")
                    }
                    .disabled(isLoading)
                }

                Section {
                    Text("Tip: share these files to your Mac and open them with a SQLite viewer (e.g., DB Browser for SQLite).")
                        .font(.footnote)
                        .foregroundStyle(.secondary)
                }
            }
            .navigationTitle("Local Database")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .topBarTrailing) {
                    Button("Done") { dismiss() }
                        .font(.system(size: 16, weight: .semibold))
                }
            }
            .task { await refresh() }
            .sheet(isPresented: $showShareSheet) {
                ActivityView(activityItems: exportURLs)
            }
        }
    }

    private func refresh() async {
        await MainActor.run {
            isLoading = true
            loadError = nil
        }
        do {
            let result = try await ChatDatabase.shared.dbQueue.read { db -> [String: Int] in
                func count(_ table: String) throws -> Int {
                    try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM \(table)") ?? 0
                }
                return [
                    "sessions": try count("sessions"),
                    "messages": try count("messages"),
                    "attachments": try count("attachments"),
                    "outbox": try count("outbox")
                ]
            }
            await MainActor.run {
                counts = result
                isLoading = false
            }
        } catch {
            await MainActor.run {
                isLoading = false
                loadError = error.localizedDescription
            }
        }
    }

    private func prepareExportAndShare() async {
        await MainActor.run {
            isLoading = true
            loadError = nil
        }
        do {
            // Best-effort: ask SQLite to checkpoint WAL without requiring an exclusive lock.
            // `TRUNCATE` can fail with SQLITE_BUSY/SQLITE_LOCKED when other work is ongoing.
            do {
                try await ChatDatabase.shared.dbQueue.write { db in
                    try db.execute(sql: "PRAGMA wal_checkpoint(PASSIVE);")
                }
            } catch {
                // Ignore — we will still export sqlite + wal + shm.
            }

            let fm = FileManager.default
            let tempDir = fm.temporaryDirectory
                .appendingPathComponent("TalkToMe-DB-Export", isDirectory: true)
                .appendingPathComponent(UUID().uuidString, isDirectory: true)
            try fm.createDirectory(at: tempDir, withIntermediateDirectories: true)

            let db = ChatDatabase.shared.databaseURL
            let wal = URL(fileURLWithPath: db.path + "-wal")
            let shm = URL(fileURLWithPath: db.path + "-shm")

            var urls: [URL] = []
            urls.append(try copyIfExists(from: db, toDir: tempDir))
            if fm.fileExists(atPath: wal.path) { urls.append(try copyIfExists(from: wal, toDir: tempDir)) }
            if fm.fileExists(atPath: shm.path) { urls.append(try copyIfExists(from: shm, toDir: tempDir)) }

            await MainActor.run {
                exportURLs = urls
                isLoading = false
                showShareSheet = true
            }
        } catch {
            await MainActor.run {
                isLoading = false
                loadError = error.localizedDescription
            }
        }
    }

    private func copyIfExists(from src: URL, toDir dir: URL) throws -> URL {
        let fm = FileManager.default
        let dst = dir.appendingPathComponent(src.lastPathComponent)
        if fm.fileExists(atPath: dst.path) {
            try fm.removeItem(at: dst)
        }
        try fm.copyItem(at: src, to: dst)
        return dst
    }

    private func formattedSize(for url: URL) -> String {
        let fm = FileManager.default
        guard
            let attrs = try? fm.attributesOfItem(atPath: url.path),
            let bytes = attrs[.size] as? NSNumber
        else { return "—" }
        return ByteCountFormatter.string(fromByteCount: bytes.int64Value, countStyle: .file)
    }
}

private struct ActivityView: UIViewControllerRepresentable {
    let activityItems: [Any]

    func makeUIViewController(context: Context) -> UIActivityViewController {
        UIActivityViewController(activityItems: activityItems, applicationActivities: nil)
    }

    func updateUIViewController(_ uiViewController: UIActivityViewController, context: Context) {}
}
#endif

