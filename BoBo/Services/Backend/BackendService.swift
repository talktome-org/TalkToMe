import Foundation

struct BackendService {

    static let shared = BackendService()

    static let coreRequestTimeoutSeconds: TimeInterval = 15

    internal let urlSession: URLSession = .shared
    internal let jsonEncoder = JSONEncoder()
    internal let jsonDecoder = JSONDecoder()

    let baseURL: URL

    private init() {
        let backendURLString = BackendService.getConfigValue(for: "BACKEND_BASE_URL")
        let resolvedURL = URL(string: backendURLString ?? "") ?? URL(string: "http://localhost:8000")!
        self.baseURL = resolvedURL
        if backendURLString?.isEmpty ?? true {
            print("🌐 BackendService: Missing BACKEND_BASE_URL. Using default http://localhost:8000")
        } else if URL(string: backendURLString ?? "") == nil {
            print("🌐 BackendService: Invalid BACKEND_BASE_URL. Using default http://localhost:8000")
        }
        print("🌐 BackendService: Initialized with base URL: \(resolvedURL)")
    }

    enum StreamEvent: Equatable {
        case session(UUID)
        case token(String)
        case partnerMessage(String)
        case toolStart(String)
        case toolArgs(String)
        case toolDone
        case responseId(String)
        case thinking(String)
        case thinkingDone
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

    static func getConfigValue(for key: String) -> String? {
        if let path = Bundle.main.path(forResource: "Secrets", ofType: "plist"),
           let plist = NSDictionary(contentsOfFile: path),
           let value = plist[key] as? String {
            return value.trimmingCharacters(in: .whitespacesAndNewlines)
        }
        if let value = Bundle.main.object(forInfoDictionaryKey: key) as? String {
            return value.trimmingCharacters(in: .whitespacesAndNewlines)
        }
        return nil
    }

    internal func decodeSimpleDetail(from data: Data) -> String? {
        struct SimpleDetail: Decodable { let detail: String? }
        return (try? jsonDecoder.decode(SimpleDetail.self, from: data))?.detail
    }
}