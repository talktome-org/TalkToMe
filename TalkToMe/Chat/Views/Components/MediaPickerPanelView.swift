import Photos
import PhotosUI
import SwiftUI
import UniformTypeIdentifiers
import UIKit

struct MediaPickerPanelView: View {
    @Binding var attachments: [PendingAttachment]
    let height: CGFloat
    let horizontalPadding: CGFloat
    let cornerRadius: CGFloat
    @Environment(\.colorScheme) private var colorScheme
    @State private var photoPickerItems: [PhotosPickerItem] = []
    @State private var showCamera: Bool = false
    @State private var showFiles: Bool = false
    @State private var showCameraUnavailableAlert: Bool = false
    @StateObject private var recentPhotos = RecentPhotosViewModel()
    @State private var selectedRecentAttachmentIdByAssetId: [String: UUID] = [:]
    @State private var selectedRecentAssetIdsInOrder: [String] = []
    private let recentThumbSize: CGFloat = 156

    init(
        attachments: Binding<[PendingAttachment]>,
        height: CGFloat = 290,
        horizontalPadding: CGFloat = 16,
        cornerRadius: CGFloat = 26
    ) {
        self._attachments = attachments
        self.height = height
        self.horizontalPadding = horizontalPadding
        self.cornerRadius = cornerRadius
    }

    var body: some View {
        VStack(spacing: 10) {
            ScrollView(.horizontal, showsIndicators: false) {
                HStack(spacing: 12) {
                    if recentPhotos.canShowRecents, !recentPhotos.recentAssets.isEmpty {
                        ForEach(recentPhotos.recentAssets, id: \.localIdentifier) { asset in
                            recentThumb(asset)
                        }
                    } else {
                        // No "Add a photo" placeholder — show lightweight placeholders while we load/ask permission.
                        ForEach(0..<6, id: \.self) { _ in
                            RoundedRectangle(cornerRadius: 18, style: .continuous)
                                .fill(Color.secondary.opacity(0.12))
                                .frame(width: recentThumbSize, height: recentThumbSize)
                        }
                    }
                }
                .padding(.horizontal, 16)
                .padding(.top, 6)
            }

            HStack(spacing: 22) {
                PhotosPicker(selection: $photoPickerItems, maxSelectionCount: 12, matching: .images) {
                    actionButtonLabel(title: "Library", systemImage: "photo.on.rectangle")
                }
                .buttonStyle(.plain)

                Button(action: {
                    if UIImagePickerController.isSourceTypeAvailable(.camera) {
                        showCamera = true
                    } else {
                        showCameraUnavailableAlert = true
                    }
                }) {
                    actionButtonLabel(title: "Camera", systemImage: "camera")
                }
                .buttonStyle(.plain)

                Button(action: {
                    showFiles = true
                }) {
                    actionButtonLabel(title: "Files", systemImage: "folder")
                }
                .buttonStyle(.plain)
            }
            // When selecting photos, give the recents strip a bit more breathing room (like ChatGPT),
            // and animate the controls down/up as selection changes.
            .padding(.top, attachments.isEmpty ? 12 : 16)
            .padding(.bottom, 6)
            .animation(.spring(response: 0.34, dampingFraction: 0.82, blendDuration: 0), value: attachments.isEmpty)

            Divider()
                .padding(.horizontal, 16)
                .opacity(0.25)

            ElevenLabsVoiceSuggestionsView()
                .padding(.horizontal, 16)
                .padding(.bottom, 6)
        }
        .frame(maxWidth: .infinity, alignment: .top)
        .task {
            // Match ChatGPT-style behavior: show recents immediately by requesting access on open if needed.
            let _ = await recentPhotos.requestAccessIfNeeded()
            await recentPhotos.bootstrap(limit: 20)
        }
        .onChange(of: attachments) { _, newAttachments in
            // Keep selection overlays in sync even if attachments are removed elsewhere (e.g. from the input bar).
            let ids = Set(newAttachments.map { $0.id })
            var newMap: [String: UUID] = [:]
            var newOrder: [String] = []
            for assetId in selectedRecentAssetIdsInOrder {
                if let attId = selectedRecentAttachmentIdByAssetId[assetId], ids.contains(attId) {
                    newMap[assetId] = attId
                    newOrder.append(assetId)
                }
            }
            selectedRecentAttachmentIdByAssetId = newMap
            selectedRecentAssetIdsInOrder = newOrder
        }
        .onChange(of: photoPickerItems, initial: false) { _, newItems in
            Task { await loadPhotos(items: newItems) }
        }
        .fullScreenCover(isPresented: $showCamera) {
            CameraPickerView(
                onImage: { image in
                    addCameraImage(image)
                    showCamera = false
                },
                onCancel: {
                    showCamera = false
                }
            )
            .ignoresSafeArea()
        }
        .fullScreenCover(isPresented: $showFiles) {
            DocumentPickerView(
                allowedTypes: [UTType.data, UTType.image, UTType.pdf, UTType.plainText],
                allowsMultipleSelection: true,
                onPick: { urls in
                    addFiles(urls)
                    showFiles = false
                },
                onCancel: {
                    showFiles = false
                }
            )
        }
        .alert("Camera not available", isPresented: $showCameraUnavailableAlert) {
            Button("OK", role: .cancel) {}
        } message: {
            Text("Camera is not available on this device.")
        }
    }

    @ViewBuilder
    private var panelBackground: some View {
        if #available(iOS 26.0, *) {
            Color.clear
                .glassEffect(.regular, in: Rectangle())
        } else {
            Rectangle()
                .fill(.ultraThinMaterial)
        }
    }

    @ViewBuilder
    private var panelOverlay: some View {
        if #available(iOS 26.0, *) {
            EmptyView()
        } else {
            TopRoundedRectangle(radius: cornerRadius)
                .stroke(
                    colorScheme == .dark ? Color.white.opacity(0.14) : Color.black.opacity(0.08),
                    lineWidth: 1
                )
        }
    }

    @ViewBuilder
    private func actionButtonLabel(title: String, systemImage: String) -> some View {
        HStack(spacing: 8) {
            Image(systemName: systemImage)
                .font(.system(size: 14, weight: .semibold))
            Text(title)
                .font(.system(size: 15, weight: .semibold))
        }
        .foregroundColor(.primary)
        .padding(.horizontal, 12)
        .padding(.vertical, 10)
        .background(.thinMaterial, in: RoundedRectangle(cornerRadius: 14, style: .continuous))
    }

    @ViewBuilder
    private func previewCard(_ att: PendingAttachment) -> some View {
        ZStack(alignment: .topTrailing) {
            RoundedRectangle(cornerRadius: 18, style: .continuous)
                .fill(.thinMaterial)
                .frame(width: 220, height: 160)
                .overlay(
                    Group {
                        switch att.kind {
                        case .image(let data, _):
                            if let uiImage = UIImage(data: data) {
                                Image(uiImage: uiImage)
                                    .resizable()
                                    .scaledToFill()
                                    .frame(width: 220, height: 160)
                                    .clipShape(RoundedRectangle(cornerRadius: 18, style: .continuous))
                            } else {
                                placeholderCardLabel(systemImage: "photo", title: "Image")
                            }
                        case .file(_, let filename, _):
                            placeholderCardLabel(systemImage: "doc", title: filename)
                        }
                    }
                )

            Button(action: {
                Haptics.impact(.light)
                withAnimation(.spring(response: 0.35, dampingFraction: 0.9)) {
                    attachments.removeAll { $0.id == att.id }
                }
            }) {
                Image(systemName: "xmark.circle.fill")
                    .font(.system(size: 18, weight: .semibold))
                    .foregroundColor(.secondary)
                    .padding(10)
            }
            .buttonStyle(.plain)
        }
    }

    @ViewBuilder
    private func placeholderCardLabel(systemImage: String, title: String) -> some View {
        VStack(spacing: 6) {
            Image(systemName: systemImage)
                .font(.system(size: 22, weight: .semibold))
            Text(title)
                .font(.system(size: 13, weight: .semibold))
                .foregroundColor(.secondary)
                .lineLimit(2)
                .multilineTextAlignment(.center)
                .padding(.horizontal, 12)
        }
    }

    private func loadPhotos(items: [PhotosPickerItem]) async {
        for item in items {
            do {
                if let rawData = try await item.loadTransferable(type: Data.self) {
                    // IMPORTANT: PhotosPicker commonly returns HEIC/HEIF. We always normalize images to JPEG bytes,
                    // since the upstream vision models / URL consumers may not support HEIC reliably.
                    let jpegData: Data? = {
                        guard let uiImage = UIImage(data: rawData) else { return nil }
                        return uiImage.jpegData(compressionQuality: 0.92)
                    }()
                    guard let data = jpegData, !data.isEmpty else { continue }
                    let att = PendingAttachment(kind: .image(data: data, contentType: "image/jpeg"))
                    await MainActor.run {
                        if attachments.count < 12 {
                            withAnimation(.spring(response: 0.34, dampingFraction: 0.82, blendDuration: 0)) {
                                attachments.append(att)
                            }
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
        if let attId = selectedRecentAttachmentIdByAssetId[assetId] {
            await MainActor.run {
                Haptics.impact(.light)
                withAnimation(.spring(response: 0.34, dampingFraction: 0.82, blendDuration: 0)) {
                    attachments.removeAll { $0.id == attId }
                    selectedRecentAttachmentIdByAssetId.removeValue(forKey: assetId)
                    selectedRecentAssetIdsInOrder.removeAll { $0 == assetId }
                }
            }
            return
        }

        if attachments.count >= 12 { return }
        if !recentPhotos.canShowRecents {
            let granted = await recentPhotos.requestAccessIfNeeded()
            guard granted else { return }
        }

        // 1) Add an immediate preview attachment (so InputAreaView shows it instantly),
        // 2) then replace with full-quality data once loaded.
        let placeholderId = UUID()
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

        let previewAttachment = PendingAttachment(
            id: placeholderId,
            kind: .image(data: previewData, contentType: "image/jpeg")
        )
        await MainActor.run {
            if attachments.count < 12 {
                Haptics.impact(.light)
                withAnimation(.spring(response: 0.34, dampingFraction: 0.82, blendDuration: 0)) {
                    attachments.append(previewAttachment)
                    selectedRecentAttachmentIdByAssetId[assetId] = placeholderId
                    selectedRecentAssetIdsInOrder.append(assetId)
                }
            }
        }

        // Full-quality replacement (keeps the same attachment id so the UI doesn't jump).
        guard let fullData = await recentPhotos.loadFullJPEGData(asset: asset, maxPixelSize: 3200) else { return }
        await MainActor.run {
            guard selectedRecentAttachmentIdByAssetId[assetId] == placeholderId else { return }
            guard let idx = attachments.firstIndex(where: { $0.id == placeholderId }) else { return }
            attachments[idx] = PendingAttachment(
                id: placeholderId,
                kind: .image(data: fullData, contentType: "image/jpeg")
            )
        }
    }

    @ViewBuilder
    private func recentThumb(_ asset: PHAsset) -> some View {
        Button(action: {
            Task { await toggleRecentAssetSelection(asset) }
        }) {
            ZStack {
                RoundedRectangle(cornerRadius: 18, style: .continuous)
                    .fill(.thinMaterial)
                    .frame(width: recentThumbSize, height: recentThumbSize)

                if let uiImage = recentPhotos.thumbnail(for: asset) {
                    Image(uiImage: uiImage)
                        .resizable()
                        .scaledToFill()
                        .frame(width: recentThumbSize, height: recentThumbSize)
                        .clipShape(RoundedRectangle(cornerRadius: 18, style: .continuous))
                } else {
                    // Lightweight skeleton while thumbnails load.
                    RoundedRectangle(cornerRadius: 18, style: .continuous)
                        .fill(Color.secondary.opacity(0.12))
                        .frame(width: recentThumbSize, height: recentThumbSize)
                        .overlay(
                            Image(systemName: "photo")
                                .font(.system(size: 18, weight: .semibold))
                                .foregroundColor(.secondary)
                        )
                }

                if let idx = selectedRecentAssetIdsInOrder.firstIndex(of: asset.localIdentifier) {
                    VStack {
                        HStack {
                            Spacer()
                            ZStack {
                                Circle()
                                    .fill(Color.black.opacity(0.55))
                                Text("\(idx + 1)")
                                    .font(.system(size: 13, weight: .semibold))
                                    .foregroundColor(.white)
                            }
                            .frame(width: 22, height: 22)
                        }
                        Spacer()
                    }
                    .padding(8)
                }
            }
        }
        .buttonStyle(.plain)
        .onAppear {
            // Request a high-res square thumbnail so the preview doesn't look like ~480px upscaled.
            // (PHImageManager targetSize is in pixels.)
            recentPhotos.prefetchThumbnail(
                asset: asset,
                targetSize: CGSize(width: 1200, height: 1200)
            )
        }
    }

    private func addCameraImage(_ image: UIImage) {
        let data = image.jpegData(compressionQuality: 0.9) ?? Data()
        guard !data.isEmpty else { return }
        let att = PendingAttachment(kind: .image(data: data, contentType: "image/jpeg"))
        if attachments.count < 12 {
            withAnimation(.spring(response: 0.34, dampingFraction: 0.82, blendDuration: 0)) {
                attachments.append(att)
            }
        }
    }

    private func addFiles(_ urls: [URL]) {
        for url in urls {
            if attachments.count >= 12 { break }
            let needsAccess = url.startAccessingSecurityScopedResource()
            defer { if needsAccess { url.stopAccessingSecurityScopedResource() } }

            guard let data = try? Data(contentsOf: url) else { continue }
            let filename = url.lastPathComponent
            let contentType = UTType(filenameExtension: url.pathExtension)?.preferredMIMEType ?? "application/octet-stream"
            if UTType(filenameExtension: url.pathExtension)?.conforms(to: .image) == true {
                // Normalize imported images to JPEG bytes for maximum downstream compatibility.
                let jpegData: Data? = {
                    guard let uiImage = UIImage(data: data) else { return nil }
                    return uiImage.jpegData(compressionQuality: 0.92)
                }()
                guard let jpegData, !jpegData.isEmpty else { continue }
                withAnimation(.spring(response: 0.34, dampingFraction: 0.82, blendDuration: 0)) {
                    attachments.append(PendingAttachment(kind: .image(data: jpegData, contentType: "image/jpeg")))
                }
            } else {
                withAnimation(.spring(response: 0.34, dampingFraction: 0.82, blendDuration: 0)) {
                    attachments.append(PendingAttachment(kind: .file(data: data, filename: filename, contentType: contentType)))
                }
            }
        }
    }
}

@MainActor
private final class RecentPhotosViewModel: ObservableObject {
    @Published private(set) var authStatus: PHAuthorizationStatus = PHPhotoLibrary.authorizationStatus(for: .readWrite)
    @Published private(set) var recentAssets: [PHAsset] = []
    @Published private var thumbnailsById: [String: UIImage] = [:]

    private let imageManager = PHCachingImageManager()
    private var inflightThumbs: Set<String> = []

    var canShowRecents: Bool {
        authStatus == .authorized || authStatus == .limited
    }

    func bootstrap(limit: Int) async {
        refreshAuthStatus()
        if canShowRecents {
            loadRecentAssets(limit: limit)
        }
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
        thumbnailsById[asset.localIdentifier]
    }

    func prefetchThumbnail(asset: PHAsset, targetSize: CGSize) {
        let id = asset.localIdentifier
        if thumbnailsById[id] != nil { return }
        if inflightThumbs.contains(id) { return }
        inflightThumbs.insert(id)

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
                self.inflightThumbs.remove(id)
                if let image {
                    self.thumbnailsById[id] = image
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

private struct TopRoundedRectangle: Shape {
    let radius: CGFloat

    func path(in rect: CGRect) -> Path {
        let bezier = UIBezierPath(
            roundedRect: rect,
            byRoundingCorners: [.topLeft, .topRight],
            cornerRadii: CGSize(width: radius, height: radius)
        )
        return Path(bezier.cgPath)
    }
}

#Preview {
    MediaPickerPanelView(attachments: .constant([]))
        .background(Color.black)
}


