//
//  DiaryService.swift
//  TalkToMe
//
//  Persists diary settings and entries (including body blocks and photos) via Supabase.
//

import Foundation
import Supabase
import UIKit

// MARK: - API DTOs

struct DiarySettingsRow: Codable {
    let user_id: UUID
    var name: String
    var description: String
    var header_color_hex: String
    var created_at: String?
    var updated_at: String?
}

struct DiaryEntryRow: Codable {
    let id: UUID
    let user_id: UUID
    var date: String  // "yyyy-MM-dd"
    var title: String
    var body_blocks: [[String: String]]  // [["id": uuid, "type": "text", "content": "..."], ["id": uuid, "type": "image", "storage_path": "user/entry/block.jpg"]]
    var created_at: String?
    var timezone_abbreviation: String
}

// MARK: - Service

final class DiaryService {
    static let shared = DiaryService()
    private let bucketName = "diary-photos"

    private init() {}

    private var client: SupabaseClient { AuthService.shared.client }

    private func requireUserId() throws -> UUID {
        guard let id = AuthService.shared.currentUserId, let uuid = UUID(uuidString: id) else {
            throw NSError(domain: "DiaryService", code: 401, userInfo: [NSLocalizedDescriptionKey: "Not authenticated"])
        }
        return uuid
    }

    // MARK: - Settings

    func fetchSettings(userId: UUID) async throws -> (name: String, description: String, headerColorHex: String) {
        let rows: [DiarySettingsRow] = try await client
            .from("diary_settings")
            .select()
            .eq("user_id", value: userId.uuidString)
            .limit(1)
            .execute()
            .value

        if let row = rows.first {
            return (row.name, row.description, row.header_color_hex)
        }
        return ("My Diary", "", "#B8DEFF")
    }

    func upsertSettings(userId: UUID, name: String, description: String, headerColorHex: String) async throws {
        let row = DiarySettingsRow(
            user_id: userId,
            name: name,
            description: description,
            header_color_hex: headerColorHex,
            created_at: nil,
            updated_at: nil
        )
        try await client
            .from("diary_settings")
            .upsert(row, onConflict: "user_id")
            .execute()
    }

    // MARK: - Entries (list)

    func fetchEntries(userId: UUID) async throws -> [DiaryEntryRow] {
        let rows: [DiaryEntryRow] = try await client
            .from("diary_entries")
            .select()
            .eq("user_id", value: userId.uuidString)
            .order("date", ascending: false)
            .order("created_at", ascending: false)
            .execute()
            .value
        return rows
    }

    func fetchEntry(userId: UUID, entryId: UUID) async throws -> DiaryEntryRow? {
        let rows: [DiaryEntryRow] = try await client
            .from("diary_entries")
            .select()
            .eq("user_id", value: userId.uuidString)
            .eq("id", value: entryId.uuidString)
            .limit(1)
            .execute()
            .value
        return rows.first
    }

    /// Download image from storage public URL; returns nil on failure.
    func loadImage(storagePath: String) async -> UIImage? {
        guard let url = publicURL(for: storagePath) else { return nil }
        do {
            let (data, _) = try await URLSession.shared.data(from: url)
            return UIImage(data: data)
        } catch {
            return nil
        }
    }

    // MARK: - Entry (single) + body_blocks with images

    func saveEntry(
        userId: UUID,
        entryId: UUID?,
        date: Date,
        title: String,
        bodyBlocks: [DiaryBlockPayload],
        timezoneAbbreviation: String
    ) async throws -> UUID {
        let entryId = entryId ?? UUID()
        let dateStr = DiaryService.isoDate(date)
        var bodyBlocksJson: [[String: String]] = []

        for block in bodyBlocks {
            switch block {
            case .text(let id, let content):
                bodyBlocksJson.append(["id": id.uuidString, "type": "text", "content": content])
            case .imageLocal(let id, let image):
                let path = "\(userId.uuidString)/\(entryId.uuidString)/\(id.uuidString).jpg"
                guard let data = image.jpegData(compressionQuality: 0.85) else { continue }
                try await uploadPhoto(path: path, data: data)
                bodyBlocksJson.append(["id": id.uuidString, "type": "image", "storage_path": path])
            case .imageRemote(let id, let storagePath):
                bodyBlocksJson.append(["id": id.uuidString, "type": "image", "storage_path": storagePath])
            }
        }

        let row = DiaryEntryRow(
            id: entryId,
            user_id: userId,
            date: dateStr,
            title: title,
            body_blocks: bodyBlocksJson,
            created_at: nil,
            timezone_abbreviation: timezoneAbbreviation
        )

        try await client
            .from("diary_entries")
            .upsert(row, onConflict: "id")
            .execute()

        return entryId
    }

    func deleteEntry(userId: UUID, entryId: UUID) async throws {
        try await client
            .from("diary_entries")
            .delete()
            .eq("id", value: entryId.uuidString)
            .eq("user_id", value: userId.uuidString)
            .execute()
    }

    func deletePhoto(storagePath: String) async throws {
        try await client.storage
            .from(bucketName)
            .remove(paths: [storagePath])
    }

    private func uploadPhoto(path: String, data: Data) async throws {
        try await client.storage
            .from(bucketName)
            .upload(
                path: path,
                file: data,
                options: FileOptions(contentType: "image/jpeg")
            )
    }

    func publicURL(for storagePath: String) -> URL? {
        try? client.storage
            .from(bucketName)
            .getPublicURL(path: storagePath)
    }

    // MARK: - Helpers

    static func isoDate(_ date: Date) -> String {
        let cal = Calendar.current
        let y = cal.component(.year, from: date)
        let m = cal.component(.month, from: date)
        let d = cal.component(.day, from: date)
        return String(format: "%04d-%02d-%02d", y, m, d)
    }

    static func date(from isoDate: String) -> Date? {
        let formatter = DateFormatter()
        formatter.dateFormat = "yyyy-MM-dd"
        formatter.timeZone = TimeZone(identifier: "UTC")
        return formatter.date(from: isoDate)
    }
}

// MARK: - Body block payload (for encoding)

enum DiaryBlockPayload {
    case text(id: UUID, content: String)
    case imageLocal(id: UUID, image: UIImage)
    case imageRemote(id: UUID, storagePath: String)
}

// MARK: - Decode body_blocks to blocks (text + image URLs; caller loads images)

struct DiaryBlockDecoded {
    let id: UUID
    enum Content {
        case text(String)
        case imageStoragePath(String)
    }
    let content: Content
}

extension DiaryService {
    /// Plain text from body_blocks (for excerpt / list display).
    static func textContentFromBodyBlocks(_ bodyBlocks: [[String: String]]) -> String {
        bodyBlocks
            .compactMap { dict -> String? in
                guard dict["type"] == "text" else { return nil }
                return dict["content"]
            }
            .joined(separator: "\n\n")
    }

    /// Decode body_blocks from a row into blocks. Image blocks have storage paths; use `loadImage(storagePath:)` to get UIImage.
    static func decodeBodyBlocks(_ bodyBlocks: [[String: String]]) -> [DiaryBlockDecoded] {
        bodyBlocks.compactMap { dict -> DiaryBlockDecoded? in
            guard let idStr = dict["id"], let id = UUID(uuidString: idStr), let type = dict["type"] else { return nil }
            switch type {
            case "text":
                let content = dict["content"] ?? ""
                return DiaryBlockDecoded(id: id, content: .text(content))
            case "image":
                guard let path = dict["storage_path"] else { return nil }
                return DiaryBlockDecoded(id: id, content: .imageStoragePath(path))
            default:
                return nil
            }
        }
    }
}
