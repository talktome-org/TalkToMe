import SwiftUI

struct ElevenLabsVoiceSuggestionsView: View {
    @State private var voices: [BackendService.ElevenVoiceDTO] = []
    @State private var suggestedVoices: [BackendService.ElevenVoiceDTO] = []
    @State private var isLoading: Bool = false
    @State private var errorMessage: String?

    @AppStorage(PreferenceKeys.elevenLabsVoiceId) private var selectedVoiceId: String = ""
    @AppStorage(PreferenceKeys.elevenLabsVoiceName) private var selectedVoiceName: String = ""

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack(spacing: 10) {
                Text("Suggested voices")
                    .font(.system(size: 14, weight: .semibold))

                Spacer(minLength: 0)

                if !selectedVoiceName.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                    Text(selectedVoiceName)
                        .font(.system(size: 13, weight: .semibold))
                        .foregroundStyle(.secondary)
                        .lineLimit(1)
                }
            }

            if isLoading && suggestedVoices.isEmpty {
                voiceSkeletonRow
            } else if let msg = errorMessage, !msg.isEmpty {
                Text(msg)
                    .font(.footnote)
                    .foregroundStyle(.secondary)
            } else {
                voiceRow(suggestedVoices)
            }
        }
        .task { await loadVoices() }
    }

    @ViewBuilder
    private var voiceSkeletonRow: some View {
        ScrollView(.horizontal, showsIndicators: false) {
            HStack(spacing: 12) {
                ForEach(0..<5, id: \.self) { _ in
                    RoundedRectangle(cornerRadius: 16, style: .continuous)
                        .fill(Color.secondary.opacity(0.12))
                        .frame(width: 220, height: 78)
                }
            }
            .padding(.horizontal, 2)
        }
    }

    @ViewBuilder
    private func voiceRow(_ voices: [BackendService.ElevenVoiceDTO]) -> some View {
        ScrollView(.horizontal, showsIndicators: false) {
            HStack(spacing: 12) {
                ForEach(voices) { v in
                    voiceCard(v)
                }
            }
            .padding(.horizontal, 2)
        }
    }

    private func voiceCard(_ voice: BackendService.ElevenVoiceDTO) -> some View {
        let isSelected = voice.voice_id == selectedVoiceId
        return Button(action: {
            Haptics.impact(.light)
            selectedVoiceId = voice.voice_id
            selectedVoiceName = voice.name
        }) {
            VStack(alignment: .leading, spacing: 6) {
                HStack(spacing: 10) {
                    Text(voice.name)
                        .font(.system(size: 15, weight: .semibold))
                        .foregroundStyle(.primary)
                        .lineLimit(1)
                    Spacer(minLength: 0)
                    if isSelected {
                        Image(systemName: "checkmark.circle.fill")
                            .font(.system(size: 14, weight: .semibold))
                            .foregroundStyle(.green)
                    }
                }

                Text(description(for: voice))
                    .font(.footnote)
                    .foregroundStyle(.secondary)
                    .lineLimit(2)
                    .fixedSize(horizontal: false, vertical: true)
            }
            .padding(.horizontal, 12)
            .padding(.vertical, 11)
            .frame(width: 220, height: 78, alignment: .topLeading)
            .background(.thinMaterial, in: RoundedRectangle(cornerRadius: 16, style: .continuous))
            .overlay(
                RoundedRectangle(cornerRadius: 16, style: .continuous)
                    .strokeBorder(
                        isSelected ? Color.green.opacity(0.55) : Color.primary.opacity(0.08),
                        lineWidth: isSelected ? 1.5 : 1
                    )
            )
        }
        .buttonStyle(.plain)
    }

    private func loadVoices() async {
        guard !isLoading else { return }
        isLoading = true
        errorMessage = nil
        defer { isLoading = false }

        guard let token = await AuthService.shared.getAccessToken() else {
            errorMessage = "Sign in to load voices."
            return
        }

        do {
            let result = try await BackendService.shared.fetchElevenLabsVoices(accessToken: token)
            voices = result
            suggestedVoices = pickSuggestedVoices(from: result)

            if selectedVoiceId.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty,
               let first = suggestedVoices.first ?? result.first {
                selectedVoiceId = first.voice_id
                selectedVoiceName = first.name
            }
        } catch {
            errorMessage = error.localizedDescription
        }
    }

    private func pickSuggestedVoices(from voices: [BackendService.ElevenVoiceDTO]) -> [BackendService.ElevenVoiceDTO] {
        let preferredNames: [String] = ["Rachel", "Adam", "Antoni", "Bella", "Josh"]

        var out: [BackendService.ElevenVoiceDTO] = []
        var used: Set<String> = []

        for name in preferredNames {
            if let v = voices.first(where: { $0.name.compare(name, options: .caseInsensitive) == .orderedSame }) {
                if used.insert(v.voice_id).inserted {
                    out.append(v)
                }
            }
            if out.count >= 5 { break }
        }

        if out.count < 5 {
            let remainder = voices
                .filter { used.contains($0.voice_id) == false }
                .sorted {
                    let a = voiceScore($0)
                    let b = voiceScore($1)
                    if a != b { return a > b }
                    return $0.name.localizedCaseInsensitiveCompare($1.name) == .orderedAscending
                }
            for v in remainder {
                if used.insert(v.voice_id).inserted {
                    out.append(v)
                }
                if out.count >= 5 { break }
            }
        }

        return Array(out.prefix(5))
    }

    private func voiceScore(_ voice: BackendService.ElevenVoiceDTO) -> Int {
        var score = 0
        if let category = voice.category?.trimmingCharacters(in: .whitespacesAndNewlines).lowercased(),
           category.contains("premade") {
            score += 100
        }
        if let preview = voice.preview_url?.trimmingCharacters(in: .whitespacesAndNewlines),
           !preview.isEmpty {
            score += 10
        }
        if let gender = voice.labels?["gender"]?.trimmingCharacters(in: .whitespacesAndNewlines),
           !gender.isEmpty {
            score += 1
        }
        if let accent = voice.labels?["accent"]?.trimmingCharacters(in: .whitespacesAndNewlines),
           !accent.isEmpty {
            score += 1
        }
        return score
    }

    private func description(for voice: BackendService.ElevenVoiceDTO) -> String {
        switch voice.name.trimmingCharacters(in: .whitespacesAndNewlines).lowercased() {
        case "rachel":
            return "Warm, clear, and friendly—great for everyday conversation."
        case "adam":
            return "Deep and steady—confident, calm narrator energy."
        case "antoni":
            return "Upbeat and expressive—snappy, conversational delivery."
        case "bella":
            return "Bright and lively—warm tone with a little sparkle."
        case "josh":
            return "Soft and thoughtful—relaxed, reflective pacing."
        default:
            let accent = voice.labels?["accent"]?.trimmingCharacters(in: .whitespacesAndNewlines)
            let gender = voice.labels?["gender"]?.trimmingCharacters(in: .whitespacesAndNewlines)
            let meta = [accent, gender]
                .compactMap { $0 }
                .filter { !$0.isEmpty }
                .joined(separator: " • ")
            return meta.isEmpty
                ? "Natural, easy-to-listen voice for chat playback."
                : "\(meta) voice—natural and easy to listen to."
        }
    }
}

#Preview {
    ElevenLabsVoiceSuggestionsView()
        .padding()
}

