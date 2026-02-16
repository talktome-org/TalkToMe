import SwiftUI

struct CustomizeBuddiesView: View {

    @Environment(\.colorScheme) private var colorScheme
    @AppStorage(PreferenceKeys.elevenLabsVoiceName) private var selectedVoiceName: String = ""
    @State private var drafts: [String: String] = [:]
    @State private var saved: [String: String] = [:]
    @State private var displayedBuddyKey: String? = nil
    @State private var toastMessage: String? = nil
    @State private var toastWorkItem: DispatchWorkItem? = nil

    private let buddies = ElevenLabsVoiceSuggestionsView.requiredBuddies
    private let gridColumns = [GridItem(.flexible(), spacing: 12), GridItem(.flexible(), spacing: 12)]

    private var actualCurrentKey: String {
        selectedVoiceName.lowercased().trimmingCharacters(in: .whitespacesAndNewlines)
    }

    private var displayedBuddy: BuddyDefinition? {
        guard let key = displayedBuddyKey else { return nil }
        return buddies.first(where: { $0.key == key })
    }

    private var gridBuddies: [BuddyDefinition] {
        guard let displayed = displayedBuddy else { return buddies }
        return buddies.filter { $0.key != displayed.key }
    }

    var body: some View {
        ScrollView {
            VStack(spacing: 16) {
                // Displayed buddy — full width
                if let displayed = displayedBuddy {
                    expandedBuddyCard(displayed)
                        .id("expanded-\(displayed.key)")
                }

                // Others — 2-column grid
                if !gridBuddies.isEmpty {
                    LazyVGrid(columns: gridColumns, spacing: 12) {
                        ForEach(gridBuddies) { buddy in
                            gridBuddyCard(buddy)
                                .id("grid-\(buddy.key)")
                        }
                    }
                }
            }
            .padding(.horizontal, 16)
            .padding(.top, 8)
            .padding(.bottom, 32)
        }
        .scrollIndicators(.hidden)
        .scrollDismissesKeyboard(.interactively)
        .navigationTitle("Customize Buddies")
        .navigationBarTitleDisplayMode(.inline)
        .background(Color(.systemGroupedBackground))
        .overlay(alignment: .top) {
            if let toastMessage {
                ToastOverlayView(message: toastMessage, onDismiss: {
                    dismissToast()
                })
                .padding(.top, 8)
            }
        }
        .animation(.spring(response: 0.3, dampingFraction: 0.8), value: toastMessage)
        .onAppear {
            for buddy in buddies {
                let value = UserDefaults.standard.string(
                    forKey: PreferenceKeys.buddyCustomPromptKey(buddy.key)
                ) ?? ""
                drafts[buddy.key] = value
                saved[buddy.key] = value
            }
            if displayedBuddyKey == nil {
                displayedBuddyKey = buddies.first(where: { $0.key == actualCurrentKey })?.key
                    ?? buddies.first?.key
            }
        }
    }

    // MARK: - Expanded Buddy Card (Full Width)

    @ViewBuilder
    private func expandedBuddyCard(_ buddy: BuddyDefinition) -> some View {
        let draftText = drafts[buddy.key] ?? ""
        let isActualCurrent = buddy.key == actualCurrentKey
        let hasDraft = !draftText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
        let hasUnsavedChanges = draftText != (saved[buddy.key] ?? "")

        VStack(alignment: .leading, spacing: 18) {
            HStack(spacing: 16) {
                buddyVideo(buddy)
                    .frame(width: 80, height: 80)
                    .clipShape(RoundedRectangle(cornerRadius: 18, style: .continuous))

                VStack(alignment: .leading, spacing: 6) {
                    HStack(spacing: 6) {
                        Text(buddy.name)
                            .font(.system(size: 20, weight: .semibold, design: .rounded))
                            .foregroundStyle(.primary)

                        if isActualCurrent {
                            Text("Current")
                                .font(.system(size: 11, weight: .semibold))
                                .foregroundStyle(.secondary)
                                .padding(.horizontal, 7)
                                .padding(.vertical, 3)
                                .background(
                                    Capsule().fill(Color(.tertiarySystemFill))
                                )
                        }
                    }

                    Text(Self.overviews[buddy.key] ?? buddy.description)
                        .font(.system(size: 14))
                        .foregroundStyle(.secondary)
                        .lineSpacing(3)
                }
            }

            // Inline text field
            TextField(
                "Add custom instructions...",
                text: draftBinding(for: buddy.key),
                axis: .vertical
            )
            .font(.system(size: 15))
            .lineLimit(3...8)
            .padding(.horizontal, 16)
            .padding(.vertical, 14)
            .background(
                RoundedRectangle(cornerRadius: 14, style: .continuous)
                    .fill(colorScheme == .dark ? Color(.systemGray6) : Color(.systemBackground))
                    .overlay(
                        RoundedRectangle(cornerRadius: 14, style: .continuous)
                            .strokeBorder(Color.primary.opacity(0.08), lineWidth: 1)
                    )
            )

            // Reset + Submit buttons
            HStack {
                let savedText = (saved[buddy.key] ?? "").trimmingCharacters(in: .whitespacesAndNewlines)

                if !savedText.isEmpty {
                    Button {
                        resetToDefault(for: buddy)
                    } label: {
                        HStack(spacing: 4) {
                            Image(systemName: "arrow.counterclockwise")
                                .font(.system(size: 11))
                            Text("Reset to default")
                                .font(.system(size: 12, weight: .medium))
                        }
                        .foregroundStyle(.secondary)
                        .padding(.horizontal, 12)
                        .padding(.vertical, 7)
                        .background(
                            Capsule().fill(Color(.tertiarySystemFill))
                        )
                    }
                    .buttonStyle(.plain)
                    .transition(.opacity)
                }

                Spacer()

                if hasUnsavedChanges && hasDraft {
                    Button {
                        submitPrompt(for: buddy)
                    } label: {
                        Text("Submit")
                            .font(.system(size: 13, weight: .semibold))
                            .foregroundStyle(.white)
                            .padding(.horizontal, 16)
                            .padding(.vertical, 7)
                            .background(
                                Capsule().fill(Color.accentColor)
                            )
                    }
                    .buttonStyle(.plain)
                    .transition(.opacity)
                }
            }
            .animation(.easeInOut(duration: 0.25), value: hasUnsavedChanges)
            .animation(.easeInOut(duration: 0.25), value: (saved[buddy.key] ?? "").isEmpty)
        }
        .padding(16)
        .background(
            RoundedRectangle(cornerRadius: 20, style: .continuous)
                .fill(Color(.secondarySystemGroupedBackground))
        )
        .overlay(alignment: .topTrailing) {
            buddyTag(for: buddy)
                .padding(10)
        }
    }

    // MARK: - Grid Buddy Card

    @ViewBuilder
    private func gridBuddyCard(_ buddy: BuddyDefinition) -> some View {
        let savedText = saved[buddy.key] ?? ""
        let hasPrompt = !savedText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
        let isActualCurrent = buddy.key == actualCurrentKey

        Button {
            Haptics.selection()
            withAnimation(.spring(response: 0.35, dampingFraction: 0.85)) {
                displayedBuddyKey = buddy.key
            }
        } label: {
            VStack(spacing: 10) {
                buddyImage(buddy)
                    .frame(width: 72, height: 72)
                    .clipShape(RoundedRectangle(cornerRadius: 16, style: .continuous))

                VStack(spacing: 4) {
                    Text(buddy.name)
                        .font(.system(size: 15, weight: .semibold, design: .rounded))
                        .foregroundStyle(.primary)

                    if isActualCurrent {
                        Text("Current")
                            .font(.system(size: 10, weight: .semibold))
                            .foregroundStyle(.secondary)
                            .padding(.horizontal, 6)
                            .padding(.vertical, 2)
                            .background(
                                Capsule().fill(Color(.tertiarySystemFill))
                            )
                    }
                }

                Text(hasPrompt ? savedText : "Edit instructions")
                    .font(.system(size: 12))
                    .foregroundStyle(hasPrompt ? Color.secondary : Color.secondary.opacity(0.45))
                    .lineLimit(2)
                    .multilineTextAlignment(.center)
                    .frame(maxWidth: .infinity)
            }
            .padding(.horizontal, 12)
            .padding(.top, 16)
            .padding(.bottom, 14)
            .frame(maxWidth: .infinity)
            .background(
                RoundedRectangle(cornerRadius: 20, style: .continuous)
                    .fill(Color(.secondarySystemGroupedBackground))
            )
            .overlay(alignment: .topTrailing) {
                buddyTag(for: buddy)
                    .padding(8)
            }
        }
        .buttonStyle(BuddyCardButtonStyle())
    }

    // MARK: - Buddy Tag

    @ViewBuilder
    private func buddyTag(for buddy: BuddyDefinition) -> some View {
        switch buddy.key {
        case "snow":
            Text("Popular")
                .font(.system(size: 11, weight: .semibold))
                .foregroundStyle(.white)
                .padding(.horizontal, 7)
                .padding(.vertical, 3)
                .background(Capsule().fill(Color.orange))
        case "luma":
            Text("Most interactive")
                .font(.system(size: 11, weight: .semibold))
                .foregroundStyle(.white)
                .padding(.horizontal, 7)
                .padding(.vertical, 3)
                .background(Capsule().fill(Color.purple))
        default:
            EmptyView()
        }
    }

    // MARK: - Video (expanded card only)

    @ViewBuilder
    private func buddyVideo(_ buddy: BuddyDefinition) -> some View {
        let videoExists = Bundle.main.url(forResource: buddy.videoName, withExtension: "mp4") != nil
        if videoExists {
            TransparentVideoPlayerView(
                videoName: buddy.videoName,
                videoExtension: "mp4",
                startTime: ElevenLabsVoiceSuggestionsView.ghostStartTimes[buddy.videoName] ?? 0
            )
        } else {
            buddyImage(buddy)
        }
    }

    // MARK: - Buddy Image

    @ViewBuilder
    private func buddyImage(_ buddy: BuddyDefinition) -> some View {
        if let uiImage = ElevenLabsVoiceSuggestionsView.ghostUIImage(for: buddy.name) {
            Image(uiImage: uiImage)
                .resizable()
                .scaledToFit()
        } else {
            RoundedRectangle(cornerRadius: 16, style: .continuous)
                .fill(.ultraThinMaterial)
                .overlay {
                    Image(systemName: "sparkles")
                        .font(.system(size: 22, weight: .semibold))
                        .foregroundStyle(.secondary)
                }
        }
    }

    // MARK: - Helpers

    private func draftBinding(for key: String) -> Binding<String> {
        Binding(
            get: { drafts[key] ?? "" },
            set: { drafts[key] = $0 }
        )
    }

    private func resetToDefault(for buddy: BuddyDefinition) {
        UserDefaults.standard.removeObject(forKey: PreferenceKeys.buddyCustomPromptKey(buddy.key))
        drafts[buddy.key] = ""
        saved[buddy.key] = ""
        Haptics.impact(.medium)
        showToast("\(buddy.name) reset to default")
    }

    private func submitPrompt(for buddy: BuddyDefinition) {
        let text = drafts[buddy.key] ?? ""
        let trimmed = text.trimmingCharacters(in: .whitespacesAndNewlines)

        if trimmed.isEmpty {
            UserDefaults.standard.removeObject(forKey: PreferenceKeys.buddyCustomPromptKey(buddy.key))
        } else {
            UserDefaults.standard.set(text, forKey: PreferenceKeys.buddyCustomPromptKey(buddy.key))
        }
        saved[buddy.key] = text
        Haptics.impact(.medium)
        showToast("\(buddy.name)'s instructions updated")
    }

    private func showToast(_ message: String) {
        toastWorkItem?.cancel()
        withAnimation {
            toastMessage = message
        }
        let work = DispatchWorkItem {
            withAnimation {
                toastMessage = nil
            }
        }
        toastWorkItem = work
        DispatchQueue.main.asyncAfter(deadline: .now() + 4, execute: work)
    }

    private func dismissToast() {
        toastWorkItem?.cancel()
        withAnimation {
            toastMessage = nil
        }
    }
}

// MARK: - Button Style

private struct BuddyCardButtonStyle: ButtonStyle {
    func makeBody(configuration: Configuration) -> some View {
        configuration.label
            .scaleEffect(configuration.isPressed ? 0.97 : 1.0)
            .animation(.spring(response: 0.25, dampingFraction: 0.7), value: configuration.isPressed)
    }
}

// MARK: - Overviews

extension CustomizeBuddiesView {
    static let overviews: [String: String] = [
        "mira": "Mira brings bright energy and warmth to every conversation. She's encouraging, optimistic, and always finds the silver lining.",
        "pax": "Pax is your grounded companion. Thoughtful and measured, he helps you slow down and see things from a wider perspective.",
        "luma": "Luma speaks with gentle warmth and empathy. She creates a safe space where you feel heard and understood.",
        "snow": "Snow carries a quiet confidence and elegance. He offers insight with poise and a touch of grandeur.",
        "jay": "Jay is direct, motivated, and action-oriented. She pushes you forward and keeps the momentum going.",
        "hex": "Hex is endlessly curious and a little mysterious. He loves exploring ideas from unexpected angles."
    ]
}

#Preview {
    NavigationStack {
        CustomizeBuddiesView()
    }
}
