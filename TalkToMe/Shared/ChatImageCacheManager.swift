import Foundation
import UIKit

final class ChatImageCacheManager {
    static let shared = ChatImageCacheManager()

    private let cache = NSCache<NSString, UIImage>()

    private init() {
        cache.countLimit = 200
        cache.totalCostLimit = 120 * 1024 * 1024
    }

    func image(fileURL: URL) -> UIImage? {
        guard fileURL.isFileURL else { return nil }
        let key = fileURL.path as NSString
        if let img = cache.object(forKey: key) { return img }
        guard let img = UIImage(contentsOfFile: fileURL.path) else { return nil }
        cache.setObject(img, forKey: key, cost: Int(img.size.width * img.size.height))
        return img
    }
}

