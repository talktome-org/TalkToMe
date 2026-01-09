import Foundation

extension Notification.Name {
    static let chatSessionCreated = Notification.Name("chat.session.created")
    static let chatMessageSent = Notification.Name("chat.message.sent")
    static let chatSessionsNeedRefresh = Notification.Name("chat.sessions.need.refresh")
    static let relationshipTotalsChanged = Notification.Name("relationship.totals.changed")
    static let avatarChanged = Notification.Name("avatar.changed")
    static let profileChanged = Notification.Name("profile.changed")
    static let partnerRequestOpen = Notification.Name("partner.request.open")
    static let partnerMessageOpen = Notification.Name("partner.message.open")
    static let partnerMessageReceived = Notification.Name("partner.message.received")
    static let partnerRequestAccepted = Notification.Name("partner.request.accepted")
    static let partnerLinkOpened = Notification.Name("partner.link.opened")
    static let sendPartnerMessageFromBubble = Notification.Name("chat.partner.send.from.bubble")
}

private var _hasPendingAcceptanceSeed: Bool = false

extension NotificationCenter {
    static var hasPendingAcceptanceSeed: Bool { _hasPendingAcceptanceSeed }
    static func withPendingAcceptanceSeed<T>(_ body: () -> T) -> T {
        _hasPendingAcceptanceSeed = true
        let result = body()
        _hasPendingAcceptanceSeed = false
        return result
    }
}


