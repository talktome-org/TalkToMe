import Foundation
import UIKit
import ImageIO

final class ChatImageCacheManager {
    static let shared = ChatImageCacheManager()

    private let cache = NSCache<NSString, UIImage>()

    /// Max pixel dimension for cached thumbnails. Full-res images are loaded
    /// on demand for full-screen display, not kept in memory.
    private let maxCacheDimension: CGFloat = 600

    private init() {
        cache.countLimit = 100
        cache.totalCostLimit = 120 * 1024 * 1024 // 120 MB
    }

    /// Returns a display-sized thumbnail from cache or disk.
    /// Uses ImageIO to downsample without decoding the full image into memory.
    func image(fileURL: URL) -> UIImage? {
        guard fileURL.isFileURL else { return nil }
        let key = fileURL.path as NSString
        if let img = cache.object(forKey: key) { return img }
        guard let img = downsampledImage(at: fileURL) else { return nil }
        let cost = Int(img.size.width * img.size.height * 4) // RGBA bytes
        cache.setObject(img, forKey: key, cost: cost)
        return img
    }

    /// Returns the full-resolution image (not cached in NSCache).
    func fullImage(fileURL: URL) -> UIImage? {
        guard fileURL.isFileURL else { return nil }
        return UIImage(contentsOfFile: fileURL.path)
    }

    private func downsampledImage(at url: URL) -> UIImage? {
        let options: [CFString: Any] = [kCGImageSourceShouldCache: false]
        guard let source = CGImageSourceCreateWithURL(url as CFURL, options as CFDictionary) else {
            return nil
        }

        let maxDimension = maxCacheDimension * UIScreen.main.scale
        let downsampleOptions: [CFString: Any] = [
            kCGImageSourceCreateThumbnailFromImageAlways: true,
            kCGImageSourceShouldCacheImmediately: true,
            kCGImageSourceCreateThumbnailWithTransform: true,
            kCGImageSourceThumbnailMaxPixelSize: maxDimension,
        ]
        guard let cgImage = CGImageSourceCreateThumbnailAtIndex(source, 0, downsampleOptions as CFDictionary) else {
            return nil
        }
        return UIImage(cgImage: cgImage, scale: UIScreen.main.scale, orientation: .up)
    }
}
