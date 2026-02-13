import CryptoKit
import Photos
import PhotosUI
import SwiftUI
import UIKit
import UniformTypeIdentifiers

// MARK: - Shared Thumbnail Cache

/// Singleton cache for photo library thumbnails that persists across view presentations and app launches.
final class PhotoThumbnailCache {
    static let shared = PhotoThumbnailCache()

    private let memoryCache = NSCache<NSString, UIImage>()
    private let lock = NSLock()
    private var inflightRequests: Set<String> = []
    private let diskQueue = DispatchQueue(label: "com.talktome.PhotoThumbnailCache.disk", qos: .utility)

    private init() {
        memoryCache.countLimit = 100
        memoryCache.totalCostLimit = 50 * 1024 * 1024 // 50MB memory
    }

    func thumbnail(for assetId: String) -> UIImage? {
        // Check memory cache first
        if let image = memoryCache.object(forKey: assetId as NSString) {
            return image
        }
        // Check disk cache
        if let diskImage = loadFromDisk(assetId: assetId) {
            // Promote to memory cache
            let cost = Int(diskImage.size.width * diskImage.size.height * 4)
            memoryCache.setObject(diskImage, forKey: assetId as NSString, cost: cost)
            return diskImage
        }
        return nil
    }

    func setThumbnail(_ image: UIImage, for assetId: String) {
        let cost = Int(image.size.width * image.size.height * 4)
        memoryCache.setObject(image, forKey: assetId as NSString, cost: cost)
        // Save to disk asynchronously
        saveToDisk(image: image, assetId: assetId)
    }

    func isInflight(_ assetId: String) -> Bool {
        lock.lock()
        defer { lock.unlock() }
        return inflightRequests.contains(assetId)
    }

    func markInflight(_ assetId: String) {
        lock.lock()
        defer { lock.unlock() }
        inflightRequests.insert(assetId)
    }

    func clearInflight(_ assetId: String) {
        lock.lock()
        defer { lock.unlock() }
        inflightRequests.remove(assetId)
    }

    // MARK: - Disk Cache

    private func diskCacheDirectory() -> URL {
        let fm = FileManager.default
        let base = (try? fm.url(for: .cachesDirectory, in: .userDomainMask, appropriateFor: nil, create: true))
            ?? fm.temporaryDirectory
        let dir = base.appendingPathComponent("TalkToMe/PhotoThumbnailCache", isDirectory: true)
        try? fm.createDirectory(at: dir, withIntermediateDirectories: true)
        return dir
    }

    private func diskURL(for assetId: String) -> URL {
        // Hash the asset ID to create a safe filename
        let digest = SHA256.hash(data: Data(assetId.utf8))
        let hex = digest.map { String(format: "%02x", $0) }.joined()
        return diskCacheDirectory().appendingPathComponent(hex).appendingPathExtension("jpg")
    }

    private func loadFromDisk(assetId: String) -> UIImage? {
        let fileURL = diskURL(for: assetId)
        guard let data = try? Data(contentsOf: fileURL) else { return nil }
        return UIImage(data: data)
    }

    private func saveToDisk(image: UIImage, assetId: String) {
        diskQueue.async { [weak self] in
            guard let self else { return }
            let fileURL = self.diskURL(for: assetId)
            // Use JPEG for smaller file size
            guard let data = image.jpegData(compressionQuality: 0.8) else { return }
            try? data.write(to: fileURL, options: [.atomic])
        }
    }

    func clearDiskCache() {
        diskQueue.async { [weak self] in
            guard let self else { return }
            let fm = FileManager.default
            let dir = self.diskCacheDirectory()
            try? fm.removeItem(at: dir)
        }
    }
}

// MARK: - MediaPickerPanelView

struct MediaPickerPanelView: View {
    @Binding var attachments: [PendingAttachment]
    @Binding var pendingPhotoSelections: [String: PendingAttachment]
    @Binding var pendingSelectionOrder: [String]
    @Binding var attachmentIdToAssetId: [UUID: String]
    @Environment(\.dismiss) private var dismiss
    @State private var photoPickerItems: [PhotosPickerItem] = []
    @State private var showCamera: Bool = false
    @State private var showCameraUnavailableAlert: Bool = false
    @ObservedObject private var recentPhotos = RecentPhotosViewModel.shared
    private let recentThumbSize: CGFloat = 100

    var body: some View {
        VStack(spacing: 0) {
            // Photos section
            VStack(alignment: .leading, spacing: 24) {
                // See all button
                HStack {
                    Spacer()

                    PhotosPicker(selection: $photoPickerItems, maxSelectionCount: 12, matching: .images) {
                        Text("See all")
                            .font(.system(size: 17, weight: .semibold))
                            .foregroundStyle(Color.accentColor)
                    }
                    .buttonStyle(.plain)
                }
                .padding(.horizontal, 36)

                ScrollView(.horizontal, showsIndicators: false) {
                    HStack(spacing: 10) {
                        // Camera button as first item
                        Button(action: {
                            if UIImagePickerController.isSourceTypeAvailable(.camera) {
                                showCamera = true
                            } else {
                                showCameraUnavailableAlert = true
                            }
                        }) {
                            RoundedRectangle(cornerRadius: 12, style: .continuous)
                                .fill(Color.secondary.opacity(0.15))
                                .frame(width: recentThumbSize, height: recentThumbSize)
                                .overlay {
                                    Image(systemName: "camera.fill")
                                        .font(.system(size: 28, weight: .medium))
                                        .foregroundStyle(.primary.opacity(0.6))
                                }
                        }
                        .buttonStyle(.plain)

                        if recentPhotos.canShowRecents, !recentPhotos.recentAssets.isEmpty {
                            ForEach(recentPhotos.recentAssets, id: \.localIdentifier) { asset in
                                recentThumb(asset)
                            }
                        } else {
                            ForEach(0..<5, id: \.self) { _ in
                                RoundedRectangle(cornerRadius: 12, style: .continuous)
                                    .fill(Color.secondary.opacity(0.12))
                                    .frame(width: recentThumbSize, height: recentThumbSize)
                            }
                        }
                    }
                    .padding(.horizontal, 16)
                }
            }
            .padding(.top, 28)
            .padding(.bottom, 20)

            Divider()
                .padding(.horizontal, 16)

            // Buddies (voice selection grid)
            ElevenLabsVoiceSuggestionsView()
                .padding(.horizontal, 16)
                .padding(.top, 20)
                .padding(.bottom, 16)

            Spacer(minLength: 0)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .top)
        .task {
            await recentPhotos.bootstrapIfNeeded(limit: 20)
        }
        .onChange(of: photoPickerItems, initial: false) { _, newItems in
            Task {
                await loadPhotos(items: newItems)
                dismiss()
            }
        }
        .fullScreenCover(isPresented: $showCamera) {
            CameraPickerView(
                onImage: { image in
                    addCameraImage(image)
                    showCamera = false
                    dismiss()
                },
                onCancel: {
                    showCamera = false
                }
            )
            .ignoresSafeArea()
        }
        .onDisappear {
            // Add pending selections to attachments in selection order
            for assetId in pendingSelectionOrder {
                if let attachment = pendingPhotoSelections[assetId], attachments.count < 12 {
                    attachments.append(attachment)
                    attachmentIdToAssetId[attachment.id] = assetId
                }
            }
            // Clear pending selections after adding
            pendingPhotoSelections.removeAll()
            pendingSelectionOrder.removeAll()
        }
        .alert("Camera not available", isPresented: $showCameraUnavailableAlert) {
            Button("OK", role: .cancel) {}
        } message: {
            Text("Camera is not available on this device.")
        }
    }

    private func loadPhotos(items: [PhotosPickerItem]) async {
        for item in items {
            do {
                if let rawData = try await item.loadTransferable(type: Data.self) {
                    let jpegData: Data? = {
                        guard let uiImage = UIImage(data: rawData) else { return nil }
                        return uiImage.jpegData(compressionQuality: 0.92)
                    }()
                    guard let data = jpegData, !data.isEmpty else { continue }
                    let att = PendingAttachment(kind: .image(data: data, contentType: "image/jpeg"))
                    await MainActor.run {
                        if attachments.count < 12 {
                            attachments.append(att)
                        }
                    }
                }
            } catch {
                continue
            }
        }
        await MainActor.run { photoPickerItems = [] }
    }

    private func toggleRecentAssetSelection(_ asset: PHAsset) async {
        let assetId = asset.localIdentifier

        // If in pending selections, deselect from pending
        if pendingPhotoSelections[assetId] != nil {
            await MainActor.run {
                Haptics.impact(.light)
                withAnimation(.spring(response: 0.3, dampingFraction: 0.8)) {
                    pendingPhotoSelections.removeValue(forKey: assetId)
                    pendingSelectionOrder.removeAll { $0 == assetId }
                }
            }
            return
        }

        // If already in attachments (was added previously), remove from attachments
        if let attachmentId = attachmentIdToAssetId.first(where: { $0.value == assetId })?.key {
            await MainActor.run {
                Haptics.impact(.light)
                withAnimation(.spring(response: 0.3, dampingFraction: 0.8)) {
                    attachments.removeAll { $0.id == attachmentId }
                    attachmentIdToAssetId.removeValue(forKey: attachmentId)
                }
            }
            return
        }

        // Check limit (existing attachments + pending selections)
        if attachments.count + pendingPhotoSelections.count >= 12 { return }
        if !recentPhotos.canShowRecents {
            let granted = await recentPhotos.requestAccessIfNeeded()
            guard granted else { return }
        }

        // Load the image data
        let previewData: Data?
        if let img = recentPhotos.thumbnail(for: asset),
           let data = img.jpegData(compressionQuality: 0.92) {
            previewData = data
        } else {
            previewData = await recentPhotos.loadPreviewJPEGData(
                asset: asset,
                targetSize: CGSize(width: 600, height: 600)
            )
        }
        guard let previewData, !previewData.isEmpty else { return }

        // Load full quality in background
        let fullData = await recentPhotos.loadFullJPEGData(asset: asset, maxPixelSize: 3200)
        let finalData = fullData ?? previewData

        let attachment = PendingAttachment(
            kind: .image(data: finalData, contentType: "image/jpeg")
        )

        await MainActor.run {
            if attachments.count + pendingPhotoSelections.count < 12 {
                Haptics.impact(.light)
                withAnimation(.spring(response: 0.3, dampingFraction: 0.8)) {
                    pendingPhotoSelections[assetId] = attachment
                    pendingSelectionOrder.append(assetId)
                }
            }
        }
    }

    private func selectionNumber(for assetId: String) -> Int? {
        if let index = pendingSelectionOrder.firstIndex(of: assetId) {
            return index + 1
        }
        return nil
    }

    @ViewBuilder
    private func recentThumb(_ asset: PHAsset) -> some View {
        let assetId = asset.localIdentifier
        // Check if selected in pending OR already added to attachments
        let isSelected = pendingPhotoSelections[assetId] != nil ||
                         attachmentIdToAssetId.values.contains(assetId)

        Button(action: {
            Task { await toggleRecentAssetSelection(asset) }
        }) {
            ZStack(alignment: .topTrailing) {
                if let uiImage = recentPhotos.thumbnail(for: asset) {
                    Image(uiImage: uiImage)
                        .resizable()
                        .scaledToFill()
                        .frame(width: recentThumbSize, height: recentThumbSize)
                        .clipShape(RoundedRectangle(cornerRadius: 12, style: .continuous))
                } else {
                    RoundedRectangle(cornerRadius: 12, style: .continuous)
                        .fill(Color.secondary.opacity(0.12))
                        .frame(width: recentThumbSize, height: recentThumbSize)
                }

                if isSelected {
                    RoundedRectangle(cornerRadius: 12, style: .continuous)
                        .fill(Color.black.opacity(0.25))
                        .frame(width: recentThumbSize, height: recentThumbSize)
                        .transition(.opacity)

                    if let number = selectionNumber(for: assetId) {
                        Text("\(number)")
                            .font(.system(size: 13, weight: .bold))
                            .foregroundStyle(.white)
                            .frame(width: 24, height: 24)
                            .background(Circle().fill(Color.blue))
                            .padding(6)
                            .transition(.scale.combined(with: .opacity))
                    }
                }
            }
            .animation(.spring(response: 0.3, dampingFraction: 0.8), value: isSelected)
        }
        .buttonStyle(.plain)
        .onAppear {
            recentPhotos.prefetchThumbnail(
                asset: asset,
                targetSize: CGSize(width: 600, height: 600)
            )
        }
    }

    private func addCameraImage(_ image: UIImage) {
        let data = image.jpegData(compressionQuality: 0.9) ?? Data()
        guard !data.isEmpty else { return }
        let att = PendingAttachment(kind: .image(data: data, contentType: "image/jpeg"))
        if attachments.count < 12 {
            attachments.append(att)
        }
    }
}

@MainActor
private final class RecentPhotosViewModel: ObservableObject {
    static let shared = RecentPhotosViewModel()

    @Published private(set) var authStatus: PHAuthorizationStatus = PHPhotoLibrary.authorizationStatus(for: .readWrite)
    @Published private(set) var recentAssets: [PHAsset] = []
    /// Local trigger to force view updates when thumbnails load from shared cache.
    @Published private var thumbnailUpdateTrigger: Int = 0

    private let imageManager = PHCachingImageManager()
    private let thumbnailCache = PhotoThumbnailCache.shared
    private var hasBootstrapped = false

    var canShowRecents: Bool {
        authStatus == .authorized || authStatus == .limited
    }

    func bootstrap(limit: Int) async {
        refreshAuthStatus()
        if canShowRecents {
            loadRecentAssets(limit: limit)
        }
    }

    func bootstrapIfNeeded(limit: Int) async {
        guard !hasBootstrapped else { return }
        hasBootstrapped = true
        let _ = await requestAccessIfNeeded()
        await bootstrap(limit: limit)
    }

    func refreshAuthStatus() {
        authStatus = PHPhotoLibrary.authorizationStatus(for: .readWrite)
    }

    func requestAccessIfNeeded() async -> Bool {
        refreshAuthStatus()
        guard authStatus == .notDetermined else {
            return canShowRecents
        }
        let newStatus = await PHPhotoLibrary.requestAuthorization(for: .readWrite)
        authStatus = newStatus
        if canShowRecents, recentAssets.isEmpty {
            loadRecentAssets(limit: 20)
        }
        return canShowRecents
    }

    func loadRecentAssets(limit: Int) {
        let options = PHFetchOptions()
        options.sortDescriptors = [NSSortDescriptor(key: "creationDate", ascending: false)]
        options.fetchLimit = limit

        let fetchResult = PHAsset.fetchAssets(with: .image, options: options)
        var out: [PHAsset] = []
        out.reserveCapacity(min(limit, fetchResult.count))
        fetchResult.enumerateObjects { asset, _, stop in
            out.append(asset)
            if out.count >= limit {
                stop.pointee = true
            }
        }
        recentAssets = out
    }

    func thumbnail(for asset: PHAsset) -> UIImage? {
        thumbnailCache.thumbnail(for: asset.localIdentifier)
    }

    func prefetchThumbnail(asset: PHAsset, targetSize: CGSize) {
        let id = asset.localIdentifier
        // Already cached - no work needed
        if thumbnailCache.thumbnail(for: id) != nil { return }
        // Already being fetched
        if thumbnailCache.isInflight(id) { return }
        thumbnailCache.markInflight(id)

        let options = PHImageRequestOptions()
        options.isNetworkAccessAllowed = true
        options.deliveryMode = .highQualityFormat
        options.resizeMode = .exact

        imageManager.requestImage(
            for: asset,
            targetSize: targetSize,
            contentMode: .aspectFill,
            options: options
        ) { [weak self] image, _ in
            guard let self else { return }
            Task { @MainActor in
                self.thumbnailCache.clearInflight(id)
                if let image {
                    self.thumbnailCache.setThumbnail(image, for: id)
                    // Trigger view update
                    self.thumbnailUpdateTrigger += 1
                }
            }
        }
    }

    func loadPreviewJPEGData(asset: PHAsset, targetSize: CGSize) async -> Data? {
        let options = PHImageRequestOptions()
        options.isNetworkAccessAllowed = true
        options.deliveryMode = .fastFormat
        options.resizeMode = .fast

        return await withCheckedContinuation { cont in
            var didResume = false
            imageManager.requestImage(
                for: asset,
                targetSize: targetSize,
                contentMode: .aspectFill,
                options: options
            ) { image, _ in
                guard !didResume else { return }
                didResume = true
                cont.resume(returning: image?.jpegData(compressionQuality: 0.9))
            }
        }
    }

    func loadFullJPEGData(asset: PHAsset, maxPixelSize: CGFloat) async -> Data? {
        let w = CGFloat(asset.pixelWidth)
        let h = CGFloat(asset.pixelHeight)
        let maxDim = max(w, h)
        let scale = maxDim > 0 ? min(1, maxPixelSize / maxDim) : 1
        let targetSize = CGSize(width: w * scale, height: h * scale)

        let options = PHImageRequestOptions()
        options.isNetworkAccessAllowed = true
        options.deliveryMode = .highQualityFormat
        options.resizeMode = .exact
        options.version = .current

        return await withCheckedContinuation { cont in
            var didResume = false
            imageManager.requestImage(
                for: asset,
                targetSize: targetSize,
                contentMode: .aspectFit,
                options: options
            ) { image, info in
                if didResume { return }
                if let isDegraded = info?[PHImageResultIsDegradedKey] as? Bool, isDegraded { return }
                didResume = true
                cont.resume(returning: image?.jpegData(compressionQuality: 0.92))
            }
        }
    }

    func loadImageData(asset: PHAsset) async -> (Data, String)? {
        let options = PHImageRequestOptions()
        options.isNetworkAccessAllowed = true
        options.deliveryMode = .highQualityFormat
        options.version = .current

        return await withCheckedContinuation { cont in
            imageManager.requestImageDataAndOrientation(for: asset, options: options) { data, uti, _, _ in
                guard let data else {
                    cont.resume(returning: nil)
                    return
                }
                let mime: String = {
                    guard let uti, let type = UTType(uti) else { return "image/jpeg" }
                    return type.preferredMIMEType ?? "image/jpeg"
                }()
                cont.resume(returning: (data, mime))
            }
        }
    }
}

#Preview {
    MediaPickerPanelView(
        attachments: .constant([]),
        pendingPhotoSelections: .constant([:]),
        pendingSelectionOrder: .constant([]),
        attachmentIdToAssetId: .constant([:])
    )
        .background(Color.black)
}
