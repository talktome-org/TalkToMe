//
//  DiaryService.swift
//  TalkToMe
//
//  Persists diary settings and entries via Backend API. Photos are uploaded to the backend uploads table.
//

import Foundation
import Supabase
import UIKit

// MARK: - API DTOs

struct DiarySettingsRow: Codable, Sendable {
  let user_id: UUID
  var name: String
  var description: String
  var header_color_hex: String
  var created_at: String?
  var updated_at: String?
}

struct DiaryEntryRow: Codable, Sendable {
  let id: UUID
  let user_id: UUID
  var date: String  // "yyyy-MM-dd"
  var title: String
  var body_blocks: [[String: String]]  // [["id": uuid, "type": "text", "content": "..."], ["id": uuid, "type": "image", "path": "uploads/UUID", "url": "..."]]
  var created_at: String?
  var timezone_abbreviation: String
}

// MARK: - Service

final class DiaryService {
  static let shared = DiaryService()
  /// Keep below backend upload limits to avoid failures.
  private let maxPhotoUploadBytes = 19 * 1024 * 1024
  private let memoryCache = DiaryServiceMemoryCache()

  private init() {}

  private func accessToken() async throws -> String {
    let session = try await AuthService.shared.client.auth.session
    return session.accessToken
  }

  // MARK: - Settings (via Backend)

  func fetchSettings(userId: UUID) async throws -> (
    name: String, description: String, headerColorHex: String
  ) {
    let token = try await accessToken()
    let settings = try await BackendService.shared.fetchDiarySettings(accessToken: token)
    await memoryCache.storeSettings(
      for: userId,
      name: settings.name,
      description: settings.description,
      headerColorHex: settings.headerColorHex
    )
    return settings
  }

  func upsertSettings(userId: UUID, name: String, description: String, headerColorHex: String)
    async throws
  {
    let token = try await accessToken()
    try await BackendService.shared.upsertDiarySettings(
      name: name,
      description: description,
      headerColorHex: headerColorHex,
      accessToken: token
    )
  }

  // MARK: - Entries (via Backend)

  func fetchEntries(userId: UUID) async throws -> [DiaryEntryRow] {
    let token = try await accessToken()
    let dtos = try await BackendService.shared.fetchDiaryEntries(accessToken: token)
    let rows = dtos.map { dto in
      DiaryEntryRow(
        id: dto.id,
        user_id: dto.user_id,
        date: dto.date,
        title: dto.title,
        body_blocks: dto.body_blocks,
        created_at: dto.created_at,
        timezone_abbreviation: dto.timezone_abbreviation
      )
    }
    await memoryCache.storeEntries(for: userId, rows: rows)
    LocalDatabase.shared.replaceDiaryEntries(rows, userId: userId)
    return rows
  }

  func cachedSettings(userId: UUID) async -> (name: String, description: String, headerColorHex: String)? {
    await memoryCache.settings(for: userId)
  }

  func cachedEntries(userId: UUID) async -> [DiaryEntryRow]? {
    await memoryCache.entries(for: userId)
  }

  func localEntries(userId: UUID) -> [DiaryEntryRow] {
    LocalDatabase.shared.loadDiaryEntries(userId: userId)
  }

  func preloadDiaryData(userId: UUID) async {
    async let settingsTask = fetchSettings(userId: userId)
    async let entriesTask = fetchEntries(userId: userId)
    _ = try? await (settingsTask, entriesTask)
  }

  func fetchEntry(userId: UUID, entryId: UUID) async throws -> DiaryEntryRow? {
    let token = try await accessToken()
    guard let dto = try await BackendService.shared.fetchDiaryEntry(entryId: entryId, accessToken: token) else {
      return nil
    }
    return DiaryEntryRow(
      id: dto.id,
      user_id: dto.user_id,
      date: dto.date,
      title: dto.title,
      body_blocks: dto.body_blocks,
      created_at: dto.created_at,
      timezone_abbreviation: dto.timezone_abbreviation
    )
  }

  /// Load image from a signed URL (backend uploads).
  func loadImageFromURL(_ urlString: String) async -> UIImage? {
    guard let url = URL(string: urlString) else { return nil }
    guard let (data, _) = try? await URLSession.shared.data(from: url) else { return nil }
    return UIImage(data: data)
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
    let resolvedEntryId = entryId ?? UUID()
    let dateStr = DiaryService.isoDate(date)
    let token = try await accessToken()
    var bodyBlocksJson: [[String: String]] = []

    for block in bodyBlocks {
      switch block {
      case .text(let id, let content):
        bodyBlocksJson.append(["id": id.uuidString, "type": "text", "content": content])
      case .imageLocal(let id, let image):
        guard let data = preparedUploadJPEGData(from: image, maxBytes: maxPhotoUploadBytes) else {
          throw NSError(
            domain: "DiaryService",
            code: 413,
            userInfo: [
              NSLocalizedDescriptionKey: "One photo is too large to upload. Try a smaller image."
            ]
          )
        }
        let filename = "\(id.uuidString.lowercased()).jpg"
        let result = try await BackendService.shared.uploadDiaryPhoto(
          fileData: data, filename: filename, contentType: "image/jpeg", accessToken: token)
        bodyBlocksJson.append(["id": id.uuidString, "type": "image", "path": result.path])
      case .imageRemote(let id, let remotePath):
        bodyBlocksJson.append(["id": id.uuidString, "type": "image", "path": remotePath])
      }
    }

    _ = try await BackendService.shared.saveDiaryEntry(
      id: resolvedEntryId,
      date: dateStr,
      title: title,
      bodyBlocks: bodyBlocksJson,
      timezoneAbbreviation: timezoneAbbreviation,
      accessToken: token
    )

    return resolvedEntryId
  }

  func deleteEntry(userId: UUID, entryId: UUID) async throws {
    let token = try await accessToken()
    try await BackendService.shared.deleteDiaryEntry(entryId: entryId, accessToken: token)
  }

  private func preparedUploadJPEGData(from sourceImage: UIImage, maxBytes: Int) -> Data? {
    var image = sourceImage
    let qualities: [CGFloat] = [0.9, 0.82, 0.74, 0.66, 0.58, 0.5, 0.42, 0.34, 0.26]

    for _ in 0..<4 {
      for quality in qualities {
        if let data = image.jpegData(compressionQuality: quality), data.count <= maxBytes {
          return data
        }
      }

      let nextSize = CGSize(
        width: max(720, image.size.width * 0.8),
        height: max(720, image.size.height * 0.8)
      )
      guard let resized = resizedImage(image, toFit: nextSize) else { break }
      image = resized
    }

    guard let data = image.jpegData(compressionQuality: 0.22), data.count <= maxBytes else {
      return nil
    }
    return data
  }

  private func resizedImage(_ image: UIImage, toFit target: CGSize) -> UIImage? {
    let scale = min(target.width / image.size.width, target.height / image.size.height, 1.0)
    guard scale < 1.0 else { return image }
    let newSize = CGSize(
      width: floor(image.size.width * scale), height: floor(image.size.height * scale))
    let renderer = UIGraphicsImageRenderer(size: newSize)
    return renderer.image { _ in
      image.draw(in: CGRect(origin: .zero, size: newSize))
    }
  }

  // MARK: - Helpers

  /// Prefer backend/storage message so user sees the real reason (e.g. RLS, NOT NULL, bucket).
  static func userFacingMessage(from error: Error) -> String {
    let ns = error as NSError
    if let msg = ns.userInfo["message"] as? String, !msg.isEmpty { return msg }
    if let reason = ns.userInfo[NSLocalizedFailureReasonErrorKey] as? String, !reason.isEmpty {
      return reason
    }
    return error.localizedDescription
  }

  static func isoDate(_ date: Date) -> String {
    var cal = Calendar(identifier: .gregorian)
    cal.timeZone = .current
    let y = cal.component(.year, from: date)
    let m = cal.component(.month, from: date)
    let d = cal.component(.day, from: date)
    return String(format: "%04d-%02d-%02d", y, m, d)
  }

  static func date(from isoDate: String) -> Date? {
    let trimmed = isoDate.trimmingCharacters(in: .whitespacesAndNewlines)
    let parts = trimmed.split(separator: "-", omittingEmptySubsequences: false)
    guard parts.count == 3,
      let year = Int(parts[0]),
      let month = Int(parts[1]),
      let day = Int(parts[2])
    else { return nil }

    // Date-only values must be interpreted in local calendar space (not UTC)
    // to avoid previous-day shifts on devices west of UTC.
    var cal = Calendar(identifier: .gregorian)
    cal.timeZone = .current
    var comps = DateComponents()
    comps.year = year
    comps.month = month
    comps.day = day
    comps.hour = 12
    comps.minute = 0
    comps.second = 0
    return cal.date(from: comps)
  }

  static func parseISO8601(_ isoDateTime: String?) -> Date? {
    guard let raw = isoDateTime?.trimmingCharacters(in: .whitespacesAndNewlines), !raw.isEmpty else {
      return nil
    }

    let withFractional = ISO8601DateFormatter()
    withFractional.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
    if let parsed = withFractional.date(from: raw) { return parsed }

    let internet = ISO8601DateFormatter()
    internet.formatOptions = [.withInternetDateTime]
    if let parsed = internet.date(from: raw) { return parsed }

    // Fallback for backend timestamps missing timezone info.
    let fallback = DateFormatter()
    fallback.locale = Locale(identifier: "en_US_POSIX")
    fallback.timeZone = TimeZone(secondsFromGMT: 0)
    fallback.dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"
    if let parsed = fallback.date(from: raw) { return parsed }
    fallback.dateFormat = "yyyy-MM-dd'T'HH:mm:ss"
    return fallback.date(from: raw)
  }
}

// MARK: - Body block payload (for encoding)

enum DiaryBlockPayload {
  case text(id: UUID, content: String)
  case imageLocal(id: UUID, image: UIImage)
  /// Already-uploaded image with path like `"uploads/UUID"`.
  case imageRemote(id: UUID, remotePath: String)
}

private actor DiaryServiceMemoryCache {
  private var settingsByUserId: [UUID: (name: String, description: String, headerColorHex: String)] = [:]
  private var entriesByUserId: [UUID: [DiaryEntryRow]] = [:]

  func storeSettings(
    for userId: UUID,
    name: String,
    description: String,
    headerColorHex: String
  ) {
    settingsByUserId[userId] = (name, description, headerColorHex)
  }

  func settings(for userId: UUID) -> (name: String, description: String, headerColorHex: String)? {
    settingsByUserId[userId]
  }

  func storeEntries(for userId: UUID, rows: [DiaryEntryRow]) {
    entriesByUserId[userId] = rows
  }

  func entries(for userId: UUID) -> [DiaryEntryRow]? {
    entriesByUserId[userId]
  }
}

// MARK: - Decode body_blocks to blocks (text + image URLs; caller loads images)

struct DiaryBlockDecoded {
  let id: UUID
  enum Content {
    case text(String)
    /// Backend-hosted image with signed URL and path reference.
    case imageURL(url: String, path: String)
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

  static func decodeBodyBlocks(_ bodyBlocks: [[String: String]]) -> [DiaryBlockDecoded] {
    bodyBlocks.compactMap { dict -> DiaryBlockDecoded? in
      guard let idStr = dict["id"], let id = UUID(uuidString: idStr), let type = dict["type"] else {
        return nil
      }
      switch type {
      case "text":
        let content = dict["content"] ?? ""
        return DiaryBlockDecoded(id: id, content: .text(content))
      case "image":
        guard let url = dict["url"], !url.isEmpty else { return nil }
        let path = dict["path"] ?? ""
        return DiaryBlockDecoded(id: id, content: .imageURL(url: url, path: path))
      default:
        return nil
      }
    }
  }
}
