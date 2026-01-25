import Foundation

enum MessageSegment: Equatable {
    case text(String)
    case partnerMessage(String)
    case partnerReceived(String)
    case imageData(Data)
    case imageURL(String)
    case fileData(name: String, data: Data)
    case fileURL(name: String, url: String)
}

struct ChatMessage: Identifiable {
    let id: UUID
    /// Backend `user_id` for the author (when available).
    /// For locally-created/inferred messages this may be nil.
    let senderUserId: UUID?
    let segments: [MessageSegment]
    let isFromUser: Bool
    let isFromPartnerUser: Bool
    let timestamp: Date
    let isToolLoading: Bool

    var partnerDrafts: [String] {
        return segments.compactMap { segment in
            if case .partnerMessage(let text) = segment { return text }
            return nil
        }
    }

    var partnerMessageContent: String? {
        var firstDraft: String? = nil
        var firstReceived: String? = nil

        for segment in segments {
            switch segment {
            case .partnerMessage(let text):
                if firstDraft == nil, !text.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                    firstDraft = text
                }
            case .partnerReceived(let text):
                if firstReceived == nil, !text.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                    firstReceived = text
                }
            default:
                break
            }
        }

        if let draft = firstDraft { return draft }
        return firstReceived
    }

    var isPartnerMessage: Bool { partnerMessageContent != nil }


    static func text(_ text: String, isFromUser: Bool, timestamp: Date = Date()) -> ChatMessage {
        return ChatMessage(
            segments: text.isEmpty ? (isFromUser ? [] : [.text("")]) : [.text(text)],
            isFromUser: isFromUser,
            isFromPartnerUser: false,
            timestamp: timestamp,
            isToolLoading: false
        )
    }

    init(
        id: UUID = UUID(),
        senderUserId: UUID? = nil,
        segments: [MessageSegment],
        isFromUser: Bool,
        isFromPartnerUser: Bool = false,
        timestamp: Date = Date(),
        isToolLoading: Bool = false
    ) {
        self.id = id
        self.senderUserId = senderUserId
        self.segments = segments
        self.isFromUser = isFromUser
        self.isFromPartnerUser = isFromPartnerUser
        self.timestamp = timestamp
        self.isToolLoading = isToolLoading
    }

    static func partnerReceived(_ text: String) -> ChatMessage {
        return ChatMessage(
            segments: [.partnerReceived(text)],
            isFromUser: false,
            isFromPartnerUser: false,
            timestamp: Date(),
            isToolLoading: false
        )
    }

    init(dto: BackendService.ChatMessageDTO, currentUserId: UUID) {
        let id = dto.id
        let timestamp = ChatMessage.parseISO8601(dto.created_at) ?? Date()

        // NOTE: `dto.user_id` is the *row owner* (for RLS) and is not always the "sender"
        // (e.g. partner messages are stored as `assistant` messages owned by the recipient).
        var senderUserId: UUID? = (dto.role == "user") ? dto.user_id : nil

        let isOwnUserRole = (dto.user_id == currentUserId) && dto.role == "user"
        let isFromUser = isOwnUserRole
        let isFromPartnerUser = (dto.user_id != currentUserId) && dto.role == "user"

        var segments: [MessageSegment] = dto.content.isEmpty ? [] : [.text(dto.content)]

        if let obj = ChatMessage.tryDecodeJSONDictionary(from: dto.content) {
            let talktome = (obj["_talktome"] as? [String: Any]) ?? ChatMessage.tryDecodeJSONDictionary(from: obj["_talktome"]) ?? [:]
            let type = talktome["type"] as? String
            if type == "segments" {
                let segmentsArr = (talktome["segments"] as? [Any]) ?? (obj["segments"] as? [Any]) ?? []
                var segs: [MessageSegment] = []
                for item in segmentsArr {
                    if let dict = item as? [String: Any], let t = dict["type"] as? String {
                        if t == "text" {
                            let c = (dict["content"] as? String) ?? ""
                            if !c.isEmpty { segs.append(.text(c)) }
                        } else if t == "image" {
                            let url = (dict["url"] as? String) ?? ""
                            if !url.isEmpty {
                                segs.append(.imageURL(url))
                            }
                        } else if t == "file" {
                            let name = (dict["filename"] as? String) ?? "File"
                            let url = (dict["url"] as? String) ?? ""
                            if !url.isEmpty {
                                segs.append(.fileURL(name: name, url: url))
                            }
                        } else if t == "partner_draft" {
                            let txt = (dict["text"] as? String) ?? ""
                            if !txt.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                                segs.append(.partnerMessage(txt))
                            }
                        } else if t == "partner_received" {
                            let txt = (dict["text"] as? String) ?? ""
                            if !txt.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                                segs.append(.partnerReceived(txt))
                            }
                        }
                    }
                }
                segments = segs.isEmpty ? [.text("")] : segs
            } else if type == "partner_received" {
                // Backend includes the true sender id for partner messages.
                if senderUserId == nil {
                    let raw = (talktome["sender_user_id"] as? String)
                        ?? (talktome["senderUserId"] as? String)
                        ?? (talktome["from_user_id"] as? String)
                    if let raw, let uid = UUID(uuidString: raw) {
                        senderUserId = uid
                    }
                }
                if let text = talktome["text"] as? String {
                    let body = obj["body"] as? String ?? ""
                    var segs: [MessageSegment] = []
                    if !body.isEmpty {
                        segs.append(.text(body))
                    }
                    if !text.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                        segs.append(.partnerReceived(text))
                    }
                    segments = segs.isEmpty ? [.text("")] : segs
                }
            }
        }

        self.id = id
        self.senderUserId = senderUserId
        self.segments = segments
        self.isFromUser = isFromUser
        self.isFromPartnerUser = isFromPartnerUser
        self.timestamp = timestamp
        self.isToolLoading = false
    }

    private static func parseISO8601(_ iso: String?) -> Date? {
        guard let raw = iso?.trimmingCharacters(in: .whitespacesAndNewlines), !raw.isEmpty else { return nil }
        let f1 = ISO8601DateFormatter()
        f1.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        if let d = f1.date(from: raw) { return d }
        return ISO8601DateFormatter().date(from: raw)
    }

    private static func tryDecodeJSONDictionary(from value: Any?) -> [String: Any]? {
        if let dict = value as? [String: Any] { return dict }
        if let str = value as? String, let data = str.data(using: .utf8) {
            if let dict = try? JSONSerialization.jsonObject(with: data) as? [String: Any] {
                return dict
            }
        }
        return nil
    }
}
