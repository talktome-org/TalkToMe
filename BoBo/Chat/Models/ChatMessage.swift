import Foundation

enum MessageSegment: Equatable {
    case text(String)
    case quotedReply(String)
    case partnerMessage(text: String, ghostName: String?)
    case partnerReceived(String)
    case imageData(Data)
    case imageURL(String)
    case fileData(name: String, data: Data)
    case fileURL(name: String, url: String)
}

struct ChatMessage: Identifiable {
    let id: UUID
    let senderUserId: UUID?
    let segments: [MessageSegment]
    let isFromUser: Bool
    let isFromPartnerUser: Bool
    let timestamp: Date
    let isToolLoading: Bool
    var isFromVoiceMode: Bool
    var regenerationCount: Int
    var ghostName: String?
    var thinkingSummary: String?
    var wasStopped: Bool = false

    static func text(_ text: String, isFromUser: Bool, timestamp: Date = Date(), isFromVoiceMode: Bool = false) -> ChatMessage {
        return ChatMessage(
            segments: text.isEmpty ? (isFromUser ? [] : [.text("")]) : [.text(text)],
            isFromUser: isFromUser,
            isFromPartnerUser: false,
            timestamp: timestamp,
            isToolLoading: false,
            isFromVoiceMode: isFromVoiceMode,
            regenerationCount: 0
        )
    }

    init(
        id: UUID = UUID(),
        senderUserId: UUID? = nil,
        segments: [MessageSegment],
        isFromUser: Bool,
        isFromPartnerUser: Bool = false,
        timestamp: Date = Date(),
        isToolLoading: Bool = false,
        isFromVoiceMode: Bool = false,
        regenerationCount: Int = 0,
        ghostName: String? = nil
    ) {
        self.id = id
        self.senderUserId = senderUserId
        self.segments = segments
        self.isFromUser = isFromUser
        self.isFromPartnerUser = isFromPartnerUser
        self.timestamp = timestamp
        self.isToolLoading = isToolLoading
        self.isFromVoiceMode = isFromVoiceMode
        self.regenerationCount = regenerationCount
        self.ghostName = ghostName
    }

    static func partnerReceived(_ text: String) -> ChatMessage {
        return ChatMessage(
            segments: [.partnerReceived(text)],
            isFromUser: false,
            isFromPartnerUser: false,
            timestamp: Date(),
            isToolLoading: false,
            isFromVoiceMode: false,
            regenerationCount: 0
        )
    }

    init(dto: BackendService.ChatMessageDTO, currentUserId: UUID) {
        let id = dto.id
        let timestamp = ChatMessage.parseISO8601(dto.created_at) ?? Date()
        var senderUserId: UUID? = (dto.role == "user") ? dto.user_id : nil
        let isFromUser = (dto.user_id == currentUserId) && dto.role == "user"
        let isFromPartnerUser = (dto.user_id != currentUserId) && dto.role == "user"

        var segments: [MessageSegment] = dto.content.isEmpty ? [] : [.text(dto.content)]
        var parsedGhostName: String? = nil

        if let obj = ChatMessage.tryDecodeJSONDictionary(from: dto.content) {
            let talktome = (obj["_talktome"] as? [String: Any]) ?? [:]
            let type = talktome["type"] as? String

            if type == "segments", let segmentsArr = talktome["segments"] as? [[String: Any]] {
                var segs: [MessageSegment] = []
                for dict in segmentsArr {
                    guard let t = dict["type"] as? String else { continue }
                    switch t {
                    case "text":
                        if let c = dict["content"] as? String, !c.isEmpty {
                            segs.append(.text(c))
                        }
                    case "quoted_reply":
                        if let txt = dict["text"] as? String, !txt.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                            segs.append(.quotedReply(txt))
                        }
                    case "image":
                        if let url = dict["url"] as? String, !url.isEmpty {
                            segs.append(.imageURL(url))
                        }
                    case "file":
                        let name = (dict["filename"] as? String) ?? "File"
                        if let url = dict["url"] as? String, !url.isEmpty {
                            segs.append(.fileURL(name: name, url: url))
                        }
                    case "partner_draft":
                        if let txt = dict["text"] as? String, !txt.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                            let gn = dict["ghost_name"] as? String
                            segs.append(.partnerMessage(text: txt, ghostName: gn))
                            if parsedGhostName == nil, let gn = dict["ghost_name"] as? String, !gn.isEmpty {
                                parsedGhostName = gn
                            }
                        }
                    case "partner_received":
                        if let txt = dict["text"] as? String, !txt.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                            segs.append(.partnerReceived(txt))
                        }
                    default:
                        break
                    }
                }
                segments = segs.isEmpty ? [.text("")] : segs

            } else if type == "partner_received" {
                if senderUserId == nil,
                   let raw = talktome["sender_user_id"] as? String,
                   let uid = UUID(uuidString: raw) {
                    senderUserId = uid
                }
                if let text = talktome["text"] as? String,
                   !text.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                    segments = [.partnerReceived(text)]
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
        self.isFromVoiceMode = (dto.source == "voice")
        self.regenerationCount = 0
        self.ghostName = parsedGhostName
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
