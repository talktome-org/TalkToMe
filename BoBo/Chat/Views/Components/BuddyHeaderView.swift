import SwiftUI

/// Compact buddy identity shown in the chat toolbar's `.principal` slot.
/// Shows ghost name + personality description, or voice status when speak mode is active.
/// When a partner friend is connected, shows "with [avatar] [first_name]" instead of the description.
struct BuddyHeaderView: View {
    @AppStorage(PreferenceKeys.elevenLabsVoiceName) private var selectedVoiceName: String = ""

    let isSpeakModeActive: Bool
    let speakModePhase: SpeakModePhase
    let connectedFriend: FriendSummary?
    let onTap: () -> Void

    private var buddyDisplayName: String {
        let name = selectedVoiceName.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !name.isEmpty else { return "Buddy" }
        return name.prefix(1).uppercased() + name.dropFirst().lowercased()
    }

    private static let shortDescriptions: [String: String] = [
        "snow": "Regal and grand",
        "mira": "Bright and uplifting",
        "pax": "Calm and wise",
        "luma": "Soft and warm",
        "jay": "Bold and driven",
        "hex": "Arcane and curious",
    ]

    private var buddyDescription: String {
        let key = selectedVoiceName.lowercased().trimmingCharacters(in: .whitespacesAndNewlines)
        return Self.shortDescriptions[key] ?? ""
    }

    private var friendFirstName: String? {
        guard let friend = connectedFriend else { return nil }
        let name = friend.fullName.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !name.isEmpty else { return nil }
        return String(name.split(separator: " ").first ?? Substring(name))
    }

    var body: some View {
        Button(action: {
            Haptics.impact(.light)
            onTap()
        }) {
            VStack(spacing: 2) {
                Text(buddyDisplayName)
                    .font(.system(size: 16, weight: .semibold))
                    .foregroundColor(.primary)
                    .lineLimit(1)

                if isSpeakModeActive {
                    voiceStatusView
                } else if let firstName = friendFirstName {
                    friendSubtitleView(firstName: firstName)
                } else if !buddyDescription.isEmpty {
                    Text(buddyDescription)
                        .font(.system(size: 11, weight: .medium))
                        .foregroundColor(.secondary)
                        .lineLimit(1)
                }
            }
            .padding(.horizontal, 24)
            .padding(.vertical, 6)
        }
        .buttonStyle(.plain)
        .modifier(GlassEffectModifier())
        .animation(.easeInOut(duration: 0.2), value: isSpeakModeActive)
        .animation(.easeInOut(duration: 0.2), value: speakModePhase)
        .animation(.smooth(duration: 0.45), value: connectedFriend?.id)
    }

    @ViewBuilder
    private func friendSubtitleView(firstName: String) -> some View {
        HStack(spacing: 4) {
            Text("with")
                .font(.system(size: 11, weight: .medium))
                .foregroundColor(.secondary)
                .padding(.trailing, 4)

            AvatarCacheManager.shared.cachedAsyncImage(
                urlString: connectedFriend?.avatarURL,
                placeholder: { AnyView(Color.clear.frame(width: 14, height: 14)) },
                fallback: {
                    AnyView(
                        Image(systemName: "person.circle.fill")
                            .font(.system(size: 12))
                            .foregroundColor(.secondary)
                    )
                }
            )
            .frame(width: 14, height: 14)
            .clipShape(Circle())

            Text(firstName)
                .font(.system(size: 11, weight: .semibold))
                .foregroundColor(.secondary)
        }
        .lineLimit(1)
    }

    @ViewBuilder
    private var voiceStatusView: some View {
        switch speakModePhase {
        case .answering:
            HStack(spacing: 4) {
                Circle()
                    .fill(.blue)
                    .frame(width: 6, height: 6)
                Text("Speaking")
                    .font(.system(size: 11, weight: .medium))
                    .foregroundColor(.secondary)
            }
        case .listening, .processing:
            HStack(spacing: 4) {
                Circle()
                    .fill(.yellow)
                    .frame(width: 6, height: 6)
                Text("Listening")
                    .font(.system(size: 11, weight: .medium))
                    .foregroundColor(.secondary)
            }
        default:
            // For .idle and .connecting — show the description instead
            if !buddyDescription.isEmpty {
                Text(buddyDescription)
                    .font(.system(size: 11, weight: .medium))
                    .foregroundColor(.secondary)
                    .lineLimit(1)
            }
        }
    }
}

private struct GlassEffectModifier: ViewModifier {
    func body(content: Content) -> some View {
        if #available(iOS 26.0, *) {
            content
                .glassEffect(.regular.interactive(), in: .capsule)
        } else {
            content
        }
    }
}

#if DEBUG
#Preview {
    NavigationStack {
        Color.clear
            .toolbar {
                ToolbarItem(placement: .principal) {
                    BuddyHeaderView(
                        isSpeakModeActive: false,
                        speakModePhase: .idle,
                        connectedFriend: nil,
                        onTap: {}
                    )
                }
            }
    }
}
#endif
