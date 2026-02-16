import Foundation

extension Notification.Name {
    static let chatSessionCreated = Notification.Name("chat.session.created")
    static let chatSessionRekeyed = Notification.Name("chat.session.rekeyed")
    static let chatMessageSent = Notification.Name("chat.message.sent")
    static let chatSessionsNeedRefresh = Notification.Name("chat.sessions.need.refresh")
    static let relationshipTotalsChanged = Notification.Name("relationship.totals.changed")
    static let avatarChanged = Notification.Name("avatar.changed")
    static let profileChanged = Notification.Name("profile.changed")

    // Partner messaging (kept; linking method changed from link → code)
    static let partnerRequestOpen = Notification.Name("partner.request.open")
    static let partnerMessageOpen = Notification.Name("partner.message.open")
    static let partnerMessageReceived = Notification.Name("partner.message.received")
    static let partnerRequestAccepted = Notification.Name("partner.request.accepted")
    static let sendPartnerMessageFromBubble = Notification.Name("chat.partner.send.from.bubble")
    static let unsendPartnerMessageFromBubble = Notification.Name("chat.partner.unsend.from.bubble")
    static let unsendPartnerMessageResult = Notification.Name("chat.partner.unsend.result")
    static let openAddFriendSheet = Notification.Name("chat.open.add.friend.sheet")
    static let openChatSession = Notification.Name("chat.open.session")
}
