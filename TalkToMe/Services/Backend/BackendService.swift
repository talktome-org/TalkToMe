import Foundation

struct BackendService {

    static let shared = BackendService()

    static let coreRequestTimeoutSeconds: TimeInterval = 15

    internal let urlSession: URLSession = .shared
    internal let jsonEncoder = JSONEncoder()
    internal let jsonDecoder = JSONDecoder()

    let baseURL: URL

    private init() {
        guard let backendURLString = BackendService.getSecretsPlistValue(for: "BACKEND_BASE_URL") as? String,
              let url = URL(string: backendURLString) else {
            fatalError("Missing or invalid BACKEND_BASE_URL in Secrets.plist")
        }
        self.baseURL = url
        print("🌐 BackendService: Initialized with base URL: \(url)")
    }

    enum StreamEvent: Equatable {
        case session(UUID)
        case token(String)
        case partnerMessage(String)
        case toolStart(String)
        case toolArgs(String)
        case toolDone
        case responseId(String)
        case done
        case error(String)
    }

    struct ChatAttachment: Codable, Equatable {
        let type: String
        let path: String
        let filename: String?
        let contentType: String?

        enum CodingKeys: String, CodingKey {
            case type
            case path
            case filename
            case contentType = "content_type"
        }
    }

    static func getSecretsPlistValue(for key: String) -> Any? {
        if let path = Bundle.main.path(forResource: "Secrets", ofType: "plist"),
           let plist = NSDictionary(contentsOfFile: path),
           let value = plist[key] {
            return value
        }
        return nil
    }

    internal func decodeSimpleDetail(from data: Data) -> String? {
        struct SimpleDetail: Decodable { let detail: String? }
        return (try? jsonDecoder.decode(SimpleDetail.self, from: data))?.detail
    }
}