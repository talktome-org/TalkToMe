import Foundation

struct PendingAttachment: Identifiable, Equatable {
    enum Kind: Equatable {
        case image(data: Data, contentType: String)
        case file(data: Data, filename: String, contentType: String)
    }

    let id: UUID
    let kind: Kind

    init(id: UUID = UUID(), kind: Kind) {
        self.id = id
        self.kind = kind
    }

    var isImage: Bool {
        if case .image = kind { return true }
        return false
    }
}


