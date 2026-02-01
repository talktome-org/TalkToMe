import SwiftUI

private struct SendAnimationNamespaceKey: EnvironmentKey {
    static let defaultValue: Namespace.ID? = nil
}

private struct OutgoingAnimatingMessageIdKey: EnvironmentKey {
    static let defaultValue: UUID? = nil
}

private struct OutgoingSourceMessageIdKey: EnvironmentKey {
    static let defaultValue: UUID? = nil
}

extension EnvironmentValues {
    var sendAnimationNamespace: Namespace.ID? {
        get { self[SendAnimationNamespaceKey.self] }
        set { self[SendAnimationNamespaceKey.self] = newValue }
    }

    var outgoingAnimatingMessageId: UUID? {
        get { self[OutgoingAnimatingMessageIdKey.self] }
        set { self[OutgoingAnimatingMessageIdKey.self] = newValue }
    }

    var outgoingSourceMessageId: UUID? {
        get { self[OutgoingSourceMessageIdKey.self] }
        set { self[OutgoingSourceMessageIdKey.self] = newValue }
    }
}

