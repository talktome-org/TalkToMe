import SwiftUI
import UIKit

// MARK: - Voices Cache

final class VoicesCache {
    static let shared = VoicesCache()

    private let cacheKey = "cachedAppVoices"
    private let defaultVoiceIdKey = "cachedDefaultVoiceId"
    private let cacheTimestampKey = "cachedAppVoicesTimestamp"
    private let cacheValiditySeconds: TimeInterval = 3600 // 1 hour

    private var memoryCache: [BackendService.AppVoiceDTO]?
    private var memoryDefaultVoiceId: String?

    private init() {
        // Load from disk on init
        loadFromDisk()
    }

    var cachedVoices: [BackendService.AppVoiceDTO]? {
        memoryCache
    }

    var cachedDefaultVoiceId: String? {
        memoryDefaultVoiceId
    }

    var isCacheValid: Bool {
        guard memoryCache != nil else { return false }
        let timestamp = UserDefaults.standard.double(forKey: cacheTimestampKey)
        guard timestamp > 0 else { return false }
        return Date().timeIntervalSince1970 - timestamp < cacheValiditySeconds
    }

    func cache(voices: [BackendService.AppVoiceDTO], defaultVoiceId: String) {
        memoryCache = voices
        memoryDefaultVoiceId = defaultVoiceId
        saveToDisk(voices: voices, defaultVoiceId: defaultVoiceId)
    }

    private func saveToDisk(voices: [BackendService.AppVoiceDTO], defaultVoiceId: String) {
        if let data = try? JSONEncoder().encode(voices) {
            UserDefaults.standard.set(data, forKey: cacheKey)
            UserDefaults.standard.set(defaultVoiceId, forKey: defaultVoiceIdKey)
            UserDefaults.standard.set(Date().timeIntervalSince1970, forKey: cacheTimestampKey)
        }
    }

    private func loadFromDisk() {
        if let data = UserDefaults.standard.data(forKey: cacheKey),
           let voices = try? JSONDecoder().decode([BackendService.AppVoiceDTO].self, from: data) {
            memoryCache = voices
            memoryDefaultVoiceId = UserDefaults.standard.string(forKey: defaultVoiceIdKey)
        }
    }
}

private struct BuddyDefinition: Identifiable {
    let key: String
    let name: String
    let description: String
    let imageName: String
    let videoName: String
    let aliases: Set<String>

    var id: String { key }
}

private struct BuddyOption: Identifiable {
    let definition: BuddyDefinition
    let configuredVoiceId: String?
    let resolvedDescription: String

    var id: String { definition.id }
}

// MARK: - View

struct ElevenLabsVoiceSuggestionsView: View {
    @State private var voices: [BackendService.AppVoiceDTO] = VoicesCache.shared.cachedVoices ?? []
    @State private var defaultVoiceId: String = VoicesCache.shared.cachedDefaultVoiceId ?? ""
    @State private var isLoading: Bool = false

    // Local-only selection state — reads/writes UserDefaults directly to avoid
    // @AppStorage re-rendering this view when the chat view processes the change.
    // Initialized directly from UserDefaults so state is correct immediately on
    // view creation (avoids the flash when the sheet recreates this view).
    @State private var localVoiceId: String = UserDefaults.standard.string(forKey: PreferenceKeys.elevenLabsVoiceId) ?? ""
    @State private var localVoiceName: String = UserDefaults.standard.string(forKey: PreferenceKeys.elevenLabsVoiceName) ?? ""

    @State private var currentPage: Int? = 0

    // Required buddy objects for MediaPickerPanelView. These are always shown even
    // when backend ElevenLabs voices are not configured in a dev environment.
    private static let requiredBuddies: [BuddyDefinition] = [
        BuddyDefinition(
            key: "mira",
            name: "Mira",
            description: "Bright, bubbly, and joyful",
            imageName: "mira",
            videoName: "mira",
            aliases: ["mira"]
        ),
        BuddyDefinition(
            key: "pax",
            name: "Pax",
            description: "Calm, clear, and wise",
            imageName: "pax",
            videoName: "pax",
            aliases: ["pax"]
        ),
        BuddyDefinition(
            key: "luma",
            name: "Luma",
            description: "Soft, warm, and caring",
            imageName: "luma",
            videoName: "luma",
            aliases: ["luma"]
        ),
        BuddyDefinition(
            key: "snow",
            name: "Snow",
            description: "Regal, poised, and grand",
            imageName: "snow",
            videoName: "snow",
            aliases: ["snow"]
        ),
        BuddyDefinition(
            key: "jay",
            name: "Jay",
            description: "Bold, quick, and driven",
            imageName: "jay",
            videoName: "jay",
            aliases: ["jay"]
        ),
        BuddyDefinition(
            key: "hex",
            name: "Hex",
            description: "Bookish, arcane, and curious",
            imageName: "hex",
            videoName: "hex",
            aliases: ["hex"]
        )
    ]

    /// Per-buddy start offsets so the first visible frame isn't a blank/awkward pose.
    static let ghostStartTimes: [String: Double] = [
        "jay": 0.1,
        "hex": 0.1,
        "snow": 0.1
    ]

    /// Pre-warm AVPlayers for all ghost videos so they display instantly in the media picker.
    static func preloadGhostVideos() {
        for buddy in requiredBuddies {
            guard Bundle.main.url(forResource: buddy.videoName, withExtension: "mp4") != nil else { continue }
            TransparentPlayerUIView.preload(
                videoName: buddy.videoName,
                extension: "mp4",
                loop: true,
                startTime: ghostStartTimes[buddy.videoName] ?? 0
            )
        }
    }

    static let ghostImageCache = NSCache<NSString, UIImage>()

    private static func normalizedGhostKey(_ rawName: String) -> String {
        rawName.lowercased().trimmingCharacters(in: .whitespacesAndNewlines)
    }

    private static func mappedBuddy(for voiceName: String) -> BuddyDefinition? {
        let key = normalizedGhostKey(voiceName)
        guard !key.isEmpty else { return nil }
        return requiredBuddies.first(where: { $0.aliases.contains(key) || $0.key == key })
    }

    private static func buddyKey(for voiceName: String) -> String {
        mappedBuddy(for: voiceName)?.key ?? normalizedGhostKey(voiceName)
    }

    static func ghostImageName(for voiceName: String) -> String? {
        if let mapped = mappedBuddy(for: voiceName) {
            return mapped.imageName
        }
        let fallback = normalizedGhostKey(voiceName)
        return fallback.isEmpty ? nil : fallback
    }

    static func ghostVideoName(for voiceName: String) -> String? {
        if let mapped = mappedBuddy(for: voiceName) {
            return mapped.videoName
        }
        let fallback = normalizedGhostKey(voiceName)
        return fallback.isEmpty ? nil : fallback
    }

    private var selectedBuddyKey: String {
        let keyFromName = Self.buddyKey(for: localVoiceName)
        if !keyFromName.isEmpty { return keyFromName }

        if let configured = voices.first(where: { $0.voice_id == localVoiceId }) {
            return Self.buddyKey(for: configured.name)
        }
        return ""
    }

    private var buddyOptions: [BuddyOption] {
        let configuredByAlias: [String: BackendService.AppVoiceDTO] = Dictionary(
            uniqueKeysWithValues: voices.map { (Self.normalizedGhostKey($0.name), $0) }
        )

        return Self.requiredBuddies.map { buddy in
            let matched = buddy.aliases.compactMap { configuredByAlias[$0] }.first
            let resolvedDescription = buddy.description
            return BuddyOption(
                definition: buddy,
                configuredVoiceId: matched?.voice_id,
                resolvedDescription: resolvedDescription
            )
        }
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text("Buddies")
                .font(.subheadline.weight(.semibold))
                .foregroundStyle(.secondary)

            voiceGrid

            if isLoading {
                HStack {
                    Spacer()
                    ProgressView()
                        .scaleEffect(0.85)
                    Spacer()
                }
            }
        }
        .task { await loadVoices() }
    }

    @ViewBuilder
    private var voiceGrid: some View {
        // Group buddies into pages of 4 (2x2 grid per page).
        let pages: [[BuddyOption]] = {
            var result: [[BuddyOption]] = []
            var remaining = buddyOptions
            while !remaining.isEmpty {
                let page = Array(remaining.prefix(4))
                result.append(page)
                remaining = Array(remaining.dropFirst(4))
            }
            return result
        }()

        VStack(spacing: 10) {
            GeometryReader { geo in
                let pageWidth = geo.size.width

                ScrollView(.horizontal, showsIndicators: false) {
                    HStack(spacing: 16) {
                        ForEach(Array(pages.enumerated()), id: \.offset) { index, pageVoices in
                            voicePage(voices: pageVoices, width: pageWidth)
                                .id(index)
                        }
                    }
                    .scrollTargetLayout()
                }
                .scrollPosition(id: $currentPage)
                .scrollTargetBehavior(.viewAligned)
            }
            .frame(height: 200) // 2 rows × 92pt + spacing

            // Page indicator dots
            if pages.count > 1 {
                HStack(spacing: 6) {
                    ForEach(0..<pages.count, id: \.self) { index in
                        Circle()
                            .fill(index == currentPage ? Color.primary.opacity(0.7) : Color.primary.opacity(0.2))
                            .frame(width: 6, height: 6)
                    }
                }
            }
        }
    }

    @ViewBuilder
    private func voicePage(voices pageVoices: [BuddyOption], width: CGFloat) -> some View {
        let topRow = Array(pageVoices.prefix(2))
        let bottomRow = Array(pageVoices.dropFirst(2).prefix(2))

        VStack(spacing: 10) {
            // First row
            HStack(spacing: 12) {
                ForEach(topRow) { buddy in
                    voiceCard(buddy)
                }
                // Fill empty space if only 1 in row
                if topRow.count == 1 {
                    Color.clear.frame(maxWidth: .infinity)
                }
            }

            // Second row
            if !bottomRow.isEmpty {
                HStack(spacing: 12) {
                    ForEach(bottomRow) { buddy in
                        voiceCard(buddy)
                    }
                    // Fill empty space if only 1 in row
                    if bottomRow.count == 1 {
                        Color.clear.frame(maxWidth: .infinity)
                    }
                }
            } else if pageVoices.count <= 2 {
                // Empty second row placeholder
                HStack(spacing: 12) {
                    Color.clear.frame(maxWidth: .infinity, minHeight: 92)
                }
            }
        }
        .frame(width: width)
        .frame(maxHeight: .infinity, alignment: .top)
    }

    static func ghostUIImage(for voiceName: String) -> UIImage? {
        guard let imageName = ghostImageName(for: voiceName) else { return nil }

        if let cached = ghostImageCache.object(forKey: imageName as NSString) {
            return cached
        }

        if let image = UIImage(named: imageName) {
            ghostImageCache.setObject(image, forKey: imageName as NSString)
            return image
        }
        guard let imageURL = Bundle.main.url(forResource: imageName, withExtension: "png") else {
            return nil
        }
        guard let image = UIImage(contentsOfFile: imageURL.path) else {
            return nil
        }
        ghostImageCache.setObject(image, forKey: imageName as NSString)
        return image
    }

    @ViewBuilder
    private func ghostPreview(for buddy: BuddyDefinition) -> some View {
        let videoExists = Bundle.main.url(forResource: buddy.videoName, withExtension: "mp4") != nil
        if videoExists {
            TransparentVideoPlayerView(
                videoName: buddy.videoName,
                videoExtension: "mp4",
                startTime: Self.ghostStartTimes[buddy.videoName] ?? 0
            )
        } else if let uiImage = Self.ghostUIImage(for: buddy.name) {
            Image(uiImage: uiImage)
                .resizable()
                .scaledToFit()
        } else {
            RoundedRectangle(cornerRadius: 12, style: .continuous)
                .fill(.ultraThinMaterial)
                .overlay {
                    Image(systemName: "sparkles")
                        .font(.system(size: 20, weight: .semibold))
                        .foregroundStyle(.secondary)
                }
        }
    }

    private func voiceCard(_ buddy: BuddyOption) -> some View {
        let isSelected = buddy.definition.key == selectedBuddyKey

        return HStack(alignment: .center, spacing: 12) {
            ghostPreview(for: buddy.definition)
                .frame(width: 72, height: 72)
                .clipShape(RoundedRectangle(cornerRadius: 10, style: .continuous))

            VStack(alignment: .leading, spacing: 4) {
                Text(buddy.definition.name)
                    .font(.system(size: 14, weight: .semibold))
                    .foregroundStyle(.primary)
                    .lineLimit(1)

                Text(buddy.resolvedDescription)
                    .font(.system(size: 12))
                    .foregroundStyle(.secondary)
                    .lineLimit(2)
            }
        }
        .padding(10)
        .frame(maxWidth: .infinity, minHeight: 92, alignment: .leading)
        .background(.ultraThinMaterial, in: RoundedRectangle(cornerRadius: 16, style: .continuous))
        .overlay(alignment: .topTrailing) {
            if isSelected {
                Image(systemName: "checkmark.circle.fill")
                    .font(.system(size: 22, weight: .semibold))
                    .foregroundStyle(.white, .blue)
                    .padding(8)
                    .transition(.scale.combined(with: .opacity))
            }
        }
        .animation(.spring(response: 0.3, dampingFraction: 0.8), value: isSelected)
        .overlay(
            RoundedRectangle(cornerRadius: 16, style: .continuous)
                .strokeBorder(
                    isSelected ? Color.blue.opacity(0.6) : Color.primary.opacity(0.08),
                    lineWidth: isSelected ? 2 : 1
                )
        )
        .contentShape(Rectangle())
        .onTapGesture {
            Haptics.impact(.light)
            localVoiceName = buddy.definition.name
            UserDefaults.standard.set(buddy.definition.name, forKey: PreferenceKeys.elevenLabsVoiceName)
            // Only write voiceId when a real backend ID is available so we don't
            // overwrite a previously-valid ID with "".  loadVoices() will back-fill
            // the ID once voices arrive from the network.
            if let voiceId = buddy.configuredVoiceId {
                localVoiceId = voiceId
                UserDefaults.standard.set(voiceId, forKey: PreferenceKeys.elevenLabsVoiceId)
            }
        }
    }

    /// Returns true when the user has not explicitly picked any buddy yet.
    private static func isSelectionEmpty() -> Bool {
        let id = (UserDefaults.standard.string(forKey: PreferenceKeys.elevenLabsVoiceId) ?? "")
            .trimmingCharacters(in: .whitespacesAndNewlines)
        let name = (UserDefaults.standard.string(forKey: PreferenceKeys.elevenLabsVoiceName) ?? "")
            .trimmingCharacters(in: .whitespacesAndNewlines)
        return id.isEmpty && name.isEmpty
    }

    /// If the user selected a buddy by name but we didn't have a backend voice ID
    /// yet, back-fill the ID now that voices are available.
    private func backfillVoiceIdIfNeeded(from voiceList: [BackendService.AppVoiceDTO]) {
        let storedId = (UserDefaults.standard.string(forKey: PreferenceKeys.elevenLabsVoiceId) ?? "")
            .trimmingCharacters(in: .whitespacesAndNewlines)
        guard storedId.isEmpty, !localVoiceName.isEmpty else { return }

        let key = Self.normalizedGhostKey(localVoiceName)
        if let matched = voiceList.first(where: { Self.normalizedGhostKey($0.name) == key }) {
            localVoiceId = matched.voice_id
            UserDefaults.standard.set(matched.voice_id, forKey: PreferenceKeys.elevenLabsVoiceId)
        }
    }

    private func loadVoices() async {
        let cache = VoicesCache.shared

        // If we have valid cached data, use it and refresh in background
        if let cached = cache.cachedVoices, !cached.isEmpty {
            voices = cached
            defaultVoiceId = cache.cachedDefaultVoiceId ?? ""

            // Set default selection only when the user has never picked a buddy
            if Self.isSelectionEmpty(),
               let defaultVoice = cached.first(where: { $0.voice_id == defaultVoiceId }) ?? cached.first {
                UserDefaults.standard.set(defaultVoice.voice_id, forKey: PreferenceKeys.elevenLabsVoiceId)
                UserDefaults.standard.set(defaultVoice.name, forKey: PreferenceKeys.elevenLabsVoiceName)
                localVoiceId = defaultVoice.voice_id
                localVoiceName = defaultVoice.name
            }

            backfillVoiceIdIfNeeded(from: cached)

            // If cache is still valid, skip network refresh
            if cache.isCacheValid { return }
        }

        guard !isLoading else { return }
        isLoading = true
        defer { isLoading = false }

        guard let token = await AuthService.shared.getAccessToken() else {
            return
        }

        do {
            let result = try await BackendService.shared.fetchAppVoices(accessToken: token)
            voices = result.voices
            defaultVoiceId = result.default_voice_id

            // Cache the result
            cache.cache(voices: result.voices, defaultVoiceId: result.default_voice_id)

            // Set default selection only when the user has never picked a buddy
            if Self.isSelectionEmpty(),
               let defaultVoice = result.voices.first(where: { $0.voice_id == result.default_voice_id }) ?? result.voices.first {
                UserDefaults.standard.set(defaultVoice.voice_id, forKey: PreferenceKeys.elevenLabsVoiceId)
                UserDefaults.standard.set(defaultVoice.name, forKey: PreferenceKeys.elevenLabsVoiceName)
                localVoiceId = defaultVoice.voice_id
                localVoiceName = defaultVoice.name
            }

            backfillVoiceIdIfNeeded(from: result.voices)
        } catch {
            // If we have cached data, silently ignore network errors
        }
    }
}

#Preview {
    ElevenLabsVoiceSuggestionsView()
        .padding()
}
