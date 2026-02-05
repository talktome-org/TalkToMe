import SwiftUI

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

// MARK: - View

struct ElevenLabsVoiceSuggestionsView: View {
    @State private var voices: [BackendService.AppVoiceDTO] = VoicesCache.shared.cachedVoices ?? []
    @State private var defaultVoiceId: String = VoicesCache.shared.cachedDefaultVoiceId ?? ""
    @State private var isLoading: Bool = false
    @State private var errorMessage: String?

    @AppStorage(PreferenceKeys.elevenLabsVoiceId) private var selectedVoiceId: String = ""
    @AppStorage(PreferenceKeys.elevenLabsVoiceName) private var selectedVoiceName: String = ""

    @State private var currentPage: Int? = 0

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text("Buddies")
                .font(.system(size: 15, weight: .semibold))
                .foregroundStyle(.secondary)

            if isLoading && voices.isEmpty {
                voiceSkeletonGrid
            } else if let msg = errorMessage, !msg.isEmpty, voices.isEmpty {
                Text(msg)
                    .font(.footnote)
                    .foregroundStyle(.secondary)
            } else {
                voiceGrid
            }
        }
        .task { await loadVoices() }
    }

    @ViewBuilder
    private var voiceSkeletonGrid: some View {
        VStack(spacing: 12) {
            HStack(spacing: 12) {
                ForEach(0..<2, id: \.self) { _ in
                    RoundedRectangle(cornerRadius: 16, style: .continuous)
                        .fill(Color.secondary.opacity(0.12))
                        .frame(height: 90)
                }
            }
            HStack(spacing: 12) {
                ForEach(0..<2, id: \.self) { _ in
                    RoundedRectangle(cornerRadius: 16, style: .continuous)
                        .fill(Color.secondary.opacity(0.12))
                        .frame(height: 90)
                }
            }
        }
    }

    @ViewBuilder
    private var voiceGrid: some View {
        // Group voices into pages of 4 (2x2 grid per page)
        let pages: [[BackendService.AppVoiceDTO]] = {
            var result: [[BackendService.AppVoiceDTO]] = []
            var remaining = voices
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
            .frame(height: 200) // 2 rows × 90pt + spacing

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
    private func voicePage(voices pageVoices: [BackendService.AppVoiceDTO], width: CGFloat) -> some View {
        let topRow = Array(pageVoices.prefix(2))
        let bottomRow = Array(pageVoices.dropFirst(2).prefix(2))

        VStack(spacing: 12) {
            // First row
            HStack(spacing: 12) {
                ForEach(topRow) { voice in
                    voiceCard(voice)
                }
                // Fill empty space if only 1 in row
                if topRow.count == 1 {
                    Color.clear.frame(maxWidth: .infinity)
                }
            }

            // Second row
            if !bottomRow.isEmpty {
                HStack(spacing: 12) {
                    ForEach(bottomRow) { voice in
                        voiceCard(voice)
                    }
                    // Fill empty space if only 1 in row
                    if bottomRow.count == 1 {
                        Color.clear.frame(maxWidth: .infinity)
                    }
                }
            } else if pageVoices.count <= 2 {
                // Empty second row placeholder
                HStack(spacing: 12) {
                    Color.clear.frame(maxWidth: .infinity, minHeight: 90)
                }
            }
        }
        .frame(width: width)
    }

    private func voiceCard(_ voice: BackendService.AppVoiceDTO) -> some View {
        let isSelected = voice.voice_id == selectedVoiceId
        return Button(action: {
            Haptics.impact(.light)
            selectedVoiceId = voice.voice_id
            selectedVoiceName = voice.name
        }) {
            VStack(alignment: .leading, spacing: 6) {
                HStack(alignment: .top) {
                    Text(voice.name)
                        .font(.system(size: 16, weight: .semibold))
                        .foregroundStyle(.primary)
                        .lineLimit(1)

                    Spacer(minLength: 4)

                    if isSelected {
                        Image(systemName: "checkmark.circle.fill")
                            .font(.system(size: 18))
                            .foregroundStyle(.white, .blue)
                    }
                }

                Text(voice.description)
                    .font(.system(size: 13))
                    .foregroundStyle(.secondary)
                    .lineLimit(3)
                    .multilineTextAlignment(.leading)
                    .fixedSize(horizontal: false, vertical: true)
            }
            .padding(14)
            .frame(maxWidth: .infinity, minHeight: 90, alignment: .topLeading)
            .background(.thinMaterial, in: RoundedRectangle(cornerRadius: 16, style: .continuous))
            .overlay(
                RoundedRectangle(cornerRadius: 16, style: .continuous)
                    .strokeBorder(
                        isSelected ? Color.blue.opacity(0.6) : Color.primary.opacity(0.08),
                        lineWidth: isSelected ? 2 : 1
                    )
            )
        }
        .buttonStyle(.plain)
    }

    private func loadVoices() async {
        let cache = VoicesCache.shared

        // If we have valid cached data, use it and refresh in background
        if let cached = cache.cachedVoices, !cached.isEmpty {
            voices = cached
            defaultVoiceId = cache.cachedDefaultVoiceId ?? ""

            // Set default selection if not already set
            if selectedVoiceId.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty,
               let defaultVoice = cached.first(where: { $0.voice_id == defaultVoiceId }) ?? cached.first {
                selectedVoiceId = defaultVoice.voice_id
                selectedVoiceName = defaultVoice.name
            }

            // If cache is still valid, skip network refresh
            if cache.isCacheValid { return }
        }

        guard !isLoading else { return }
        isLoading = voices.isEmpty // Only show loading if no cached data
        errorMessage = nil
        defer { isLoading = false }

        guard let token = await AuthService.shared.getAccessToken() else {
            if voices.isEmpty {
                errorMessage = "Sign in to load voices."
            }
            return
        }

        do {
            let result = try await BackendService.shared.fetchAppVoices(accessToken: token)
            voices = result.voices
            defaultVoiceId = result.default_voice_id

            // Cache the result
            cache.cache(voices: result.voices, defaultVoiceId: result.default_voice_id)

            // Set default selection if not already set
            if selectedVoiceId.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty,
               let defaultVoice = result.voices.first(where: { $0.voice_id == result.default_voice_id }) ?? result.voices.first {
                selectedVoiceId = defaultVoice.voice_id
                selectedVoiceName = defaultVoice.name
            }
        } catch {
            if voices.isEmpty {
                errorMessage = error.localizedDescription
            }
            // If we have cached data, silently ignore network errors
        }
    }
}

#Preview {
    ElevenLabsVoiceSuggestionsView()
        .padding()
}
