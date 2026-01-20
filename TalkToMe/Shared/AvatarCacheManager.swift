import Foundation
import SwiftUI
import UIKit

@MainActor
class AvatarCacheManager: ObservableObject {
    @Published var cachedAvatars: [String: UIImage] = [:]

    private let memoryCache = NSCache<NSString, UIImage>()

    nonisolated private let urlSession: URLSession
    nonisolated static let shared = AvatarCacheManager()

    private var avatarChangeObserver: NSObjectProtocol?

    nonisolated private init() {
        let config = URLSessionConfiguration.default
        config.urlCache = URLCache(
            memoryCapacity: 50 * 1024 * 1024,
            diskCapacity: 200 * 1024 * 1024,
            diskPath: "avatar_cache"
        )
        config.requestCachePolicy = .returnCacheDataElseLoad

        self.urlSession = URLSession(configuration: config)

        Task { @MainActor in
            memoryCache.countLimit = 100
            memoryCache.totalCostLimit = 50 * 1024 * 1024
        }

        Task { @MainActor in
            setupAvatarChangeObserver()
        }
    }

    deinit {
        if let observer = avatarChangeObserver {
            NotificationCenter.default.removeObserver(observer)
        }
    }

    private func setupAvatarChangeObserver() {
        avatarChangeObserver = NotificationCenter.default.addObserver(
            forName: .avatarChanged,
            object: nil,
            queue: .main
        ) { [weak self] _ in
            Task { @MainActor in
                await self?.handleAvatarChange()
            }
        }
    }

    private func handleAvatarChange() async {
        clearCache()
        objectWillChange.send()
    }

    func preloadAvatars(urls: [String]) async {
        await withTaskGroup(of: Void.self) { group in
            for urlString in urls {
                guard !urlString.isEmpty else { continue }
                group.addTask {
                    _ = await self.loadAndCacheImage(urlString: urlString)
                }
            }
        }
    }

    func getCachedImage(urlString: String) async -> UIImage? {
        if let cachedImage = memoryCache.object(forKey: urlString as NSString) {
            return cachedImage
        }

        if let cachedImage = cachedAvatars[urlString] {
            memoryCache.setObject(cachedImage, forKey: urlString as NSString)
            return cachedImage
        }

        return await loadAndCacheImage(urlString: urlString)
    }

    func getImageIfCached(urlString: String) -> UIImage? {
        if let image = memoryCache.object(forKey: urlString as NSString) {
            return image
        }
        if let image = cachedAvatars[urlString] {
            memoryCache.setObject(image, forKey: urlString as NSString)
            return image
        }
        if let url = URL(string: urlString), let urlCache = urlSession.configuration.urlCache {
            let request = URLRequest(url: url, cachePolicy: .returnCacheDataDontLoad, timeoutInterval: 0.1)
            if let cached = urlCache.cachedResponse(for: request) {
                if let image = UIImage(data: cached.data) {
                    memoryCache.setObject(image, forKey: urlString as NSString)
                    Task { @MainActor in
                        self.cachedAvatars[urlString] = image
                    }
                    return image
                }
            }
        }
        return nil
    }

    func clearCache() {
        Task { @MainActor in
            memoryCache.removeAllObjects()
            cachedAvatars.removeAll()
        }
        urlSession.configuration.urlCache?.removeAllCachedResponses()
    }

    func forceRefreshAllAvatars() async {
        clearCache()
        await MainActor.run {
            objectWillChange.send()
        }
    }

    func clearSpecificImage(urlString: String) async {
        await MainActor.run {
            memoryCache.removeObject(forKey: urlString as NSString)
            _ = cachedAvatars.removeValue(forKey: urlString)
        }
        if let url = URL(string: urlString) {
            urlSession.configuration.urlCache?.removeCachedResponse(for: URLRequest(url: url))
        }
    }

    func cacheImageImmediately(urlString: String, image: UIImage) async {
        await MainActor.run {
            memoryCache.setObject(image, forKey: urlString as NSString)
            cachedAvatars[urlString] = image
        }
    }

    private func loadAndCacheImage(urlString: String) async -> UIImage? {
        guard let url = URL(string: urlString) else { return nil }

        do {
            let (data, response) = try await urlSession.data(from: url)

            guard let httpResponse = response as? HTTPURLResponse,
                  httpResponse.statusCode == 200,
                  let image = UIImage(data: data) else {
                return nil
            }

            memoryCache.setObject(image, forKey: urlString as NSString)
            cachedAvatars[urlString] = image

            return image

        } catch {
            print("Failed to load avatar image from \(urlString): \(error)")
            return nil
        }
    }
}

extension AvatarCacheManager {
    func cachedAsyncImage(urlString: String?,
                         placeholder: @escaping () -> AnyView = { AnyView(ProgressView()) },
                         fallback: @escaping () -> AnyView = { AnyView(EmptyView()) }) -> some View {
        Group {
            if let urlString = urlString, !urlString.isEmpty {
                CachedAsyncImageView(
                    urlString: urlString,
                    cacheManager: self,
                    placeholder: placeholder,
                    fallback: fallback
                )
            } else {
                fallback()
            }
        }
    }
}

struct CachedAsyncImageView: View {

    let urlString: String
    let cacheManager: AvatarCacheManager
    let placeholder: () -> AnyView
    let fallback: () -> AnyView

    @State private var image: UIImage?
    @State private var isLoading = false

    init(
        urlString: String,
        cacheManager: AvatarCacheManager,
        placeholder: @escaping () -> AnyView,
        fallback: @escaping () -> AnyView
    ) {
        self.urlString = urlString
        self.cacheManager = cacheManager
        self.placeholder = placeholder
        self.fallback = fallback

        // Prime initial state from cache so we can render the avatar on the first frame.
        let cached = cacheManager.getImageIfCached(urlString: urlString)
        self._image = State(initialValue: cached)
        self._isLoading = State(initialValue: cached == nil)
    }

    var body: some View {
        Group {
            if let image = image {
                Image(uiImage: image)
                    .resizable()
                    .scaledToFill()
            } else if isLoading {
                placeholder()
            } else {
                fallback()
            }
        }
        .task {
            await loadImage()
        }
    }

    private func loadImage() async {
        isLoading = true
        defer { isLoading = false }

        image = await cacheManager.getCachedImage(urlString: urlString)
    }
}
