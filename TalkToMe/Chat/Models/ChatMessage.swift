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

    init(id: UUID = UUID(), segments: [MessageSegment], isFromUser: Bool, isFromPartnerUser: Bool = false, timestamp: Date = Date(), isToolLoading: Bool = false) {
        self.id = id
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
        self.id = dto.id
        let isOwnUserRole = (dto.user_id == currentUserId) && dto.role == "user"
        self.isFromUser = isOwnUserRole
        self.isFromPartnerUser = (dto.user_id != currentUserId) && dto.role == "user"
        self.timestamp = ChatMessage.parseISO8601(dto.created_at) ?? Date()
        self.isToolLoading = false

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
                self.segments = segs.isEmpty ? [.text("")] : segs
                return
            } else if type == "partner_received" {
                if let text = talktome["text"] as? String {
                    let body = obj["body"] as? String ?? ""
                    var segs: [MessageSegment] = []
                    if !body.isEmpty {
                        segs.append(.text(body))
                    }
                    if !text.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                        segs.append(.partnerReceived(text))
                    }
                    self.segments = segs.isEmpty ? [.text("")] : segs
                    return
                }
            }
        }
        self.segments = dto.content.isEmpty ? [] : [.text(dto.content)]
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
