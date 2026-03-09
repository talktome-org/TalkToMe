import SwiftUI

/// Isolated view that owns `@AppStorage` for the selected buddy voice name.
/// Keeps re-renders contained to this view only when the buddy changes,
/// following the same pattern as `GhostVideoContentView` in InputAreaView.
struct SettingsBuddyDisplayView: View {
  @AppStorage(PreferenceKeys.elevenLabsVoiceName) private var selectedVoiceName: String = ""

  private var ghostVideoName: String {
    ElevenLabsVoiceSuggestionsView.ghostVideoName(for: selectedVoiceName)
      ?? selectedVoiceName.lowercased().trimmingCharacters(in: .whitespacesAndNewlines)
  }

  private var hasGhostVideo: Bool {
    guard !ghostVideoName.isEmpty else { return false }
    return Bundle.main.url(forResource: ghostVideoName, withExtension: "mp4") != nil
  }

  var buddyDisplayName: String {
    let name = selectedVoiceName.trimmingCharacters(in: .whitespacesAndNewlines)
    guard !name.isEmpty else { return "Buddy" }
    return name.prefix(1).uppercased() + name.dropFirst().lowercased()
  }

  var body: some View {
    VStack(spacing: 6) {
      ghostVideoView
      VStack(spacing: 4) {
        buddyNameText
        buddyLabel
      }
    }
  }

  var ghostVideoView: some View {
    Group {
      if hasGhostVideo {
        TransparentVideoPlayerView(
          videoName: ghostVideoName,
          videoExtension: "mp4",
          startTime: ElevenLabsVoiceSuggestionsView.ghostStartTimes[ghostVideoName] ?? 0
        )
      } else {
        ghostFallbackImage
      }
    }
    .frame(width: 120, height: 120)
  }

  var buddyNameText: some View {
    Text(buddyDisplayName)
      .font(.system(size: 18, weight: .semibold))
      .foregroundColor(.primary)
      .lineLimit(1)
  }

  var buddyLabel: some View {
    Text("Your Buddy")
      .font(.system(size: 13, weight: .medium))
      .foregroundColor(.secondary)
      .lineLimit(1)
      .padding(.horizontal, 14)
      .padding(.vertical, 6)
      .background(AppTheme.surface)
      .clipShape(RoundedRectangle(cornerRadius: 8, style: .continuous))
  }

  @ViewBuilder
  private var ghostFallbackImage: some View {
    if let uiImage = ElevenLabsVoiceSuggestionsView.ghostUIImage(for: selectedVoiceName) {
      Image(uiImage: uiImage)
        .resizable()
        .scaledToFit()
    } else {
      Circle()
        .fill(Color(.tertiarySystemFill))
        .overlay(
          Image(systemName: "sparkles")
            .font(.system(size: 30, weight: .semibold, design: .rounded))
            .foregroundColor(.secondary)
        )
    }
  }
}
