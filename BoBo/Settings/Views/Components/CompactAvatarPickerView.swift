import SwiftUI
import PhotosUI

struct ScaleButtonStyle: ButtonStyle {
    func makeBody(configuration: Configuration) -> some View {
        configuration.label
            .scaleEffect(configuration.isPressed ? 0.95 : 1.0)
            .animation(.easeInOut(duration: 0.1), value: configuration.isPressed)
    }
}

struct CompactAvatarPickerView: View {
    @ObservedObject var viewModel: SettingsViewModel
    @EnvironmentObject private var sessionsVM: ChatSessionsViewModel
    @State private var selection: PhotosPickerItem? = nil
    @Binding var isPresented: Bool
    @Binding var selectedEmoji: String?
    @Binding var uploadedImageData: Data?
    @Binding var showSaveButton: Bool
    @Binding var avatarSaved: Bool
    @Binding var isSavingAvatar: Bool
    let onSaveAvatar: () -> Void

    let emojiAvatars = ["🐻", "🦊", "🐨", "🦁", "🐼", "🦄"]

    private var selectedGradient: LinearGradient {
        LinearGradient(
            colors: [
                Color(red: 0.26, green: 0.58, blue: 1.00),
                Color(red: 0.63, green: 0.32, blue: 0.98)
            ],
            startPoint: .topLeading,
            endPoint: .bottomTrailing
        )
    }

    private var defaultGradient: LinearGradient {
        LinearGradient(
            colors: [Color(.systemGray5), Color(.systemGray5)],
            startPoint: .topLeading,
            endPoint: .bottomTrailing
        )
    }

    private var selectedShadowColor: Color {
        Color(red: 0.4, green: 0.2, blue: 0.6).opacity(0.2)
    }

    private var defaultShadowColor: Color {
        Color.black.opacity(0.04)
    }

    var currentAvatarURL: String? {
        sessionsVM.myAvatarURL
    }

    private var emojiGridColumns: [GridItem] {
        [
            GridItem(.flexible(), spacing: 12),
            GridItem(.flexible(), spacing: 12),
            GridItem(.flexible(), spacing: 12)
        ]
    }

    private var emojiGrid: some View {
        LazyVGrid(columns: emojiGridColumns, spacing: 12) {
            ForEach(emojiAvatars, id: \.self) { emoji in
                emojiButton(for: emoji)
            }
        }
        .padding(.horizontal, 4)
    }

    private func emojiButton(for emoji: String) -> some View {
        Button(action: {
            withAnimation(.spring(response: 0.3, dampingFraction: 0.7)) {
                selectedEmoji = emoji
                uploadedImageData = nil
                selection = nil
                showSaveButton = true
                avatarSaved = false  // Reset saved state when new emoji is selected
            }
        }) {
            ZStack {
                RoundedRectangle(cornerRadius: 16, style: .continuous)
                    .fill(Color(.systemBackground))
                    .overlay(
                        RoundedRectangle(cornerRadius: 16, style: .continuous)
                            .strokeBorder(
                                selectedEmoji == emoji ? selectedGradient : defaultGradient,
                                lineWidth: selectedEmoji == emoji ? 2.5 : 1.5
                            )
                    )
                    .frame(height: 70)
                    .shadow(
                        color: selectedEmoji == emoji ? selectedShadowColor : defaultShadowColor,
                        radius: selectedEmoji == emoji ? 10 : 3,
                        x: 0,
                        y: selectedEmoji == emoji ? 5 : 2
                    )

                Text(emoji)
                    .font(.system(size: 36))
            }
        }
        .buttonStyle(ScaleButtonStyle())
    }

    var body: some View {
        VStack(spacing: 20) {
            emojiGrid

            // Divider with "or"
            HStack(spacing: 10) {
                Rectangle()
                    .fill(Color(.systemGray5))
                    .frame(height: 1)

                Text("or")
                    .font(.system(size: 11, weight: .medium))
                    .foregroundColor(.secondary)

                Rectangle()
                    .fill(Color(.systemGray5))
                    .frame(height: 1)
            }
            .padding(.horizontal, 30)

            // Upload button
            PhotosPicker(selection: $selection, matching: .images) {
                Text("Set New Photo")
                    .font(.system(size: 13, weight: .semibold))
                .foregroundColor(Color(red: 0.4, green: 0.2, blue: 0.6))
                .frame(maxWidth: .infinity)
                .padding(.vertical, 12)
                .background(
                    RoundedRectangle(cornerRadius: 12, style: .continuous)
                        .fill(Color(.systemGray6))
                        .overlay(
                            RoundedRectangle(cornerRadius: 12, style: .continuous)
                                .strokeBorder(Color(red: 0.4, green: 0.2, blue: 0.6).opacity(0.15), lineWidth: 1)
                        )
                )
            }
            .buttonStyle(ScaleButtonStyle())
            .padding(.horizontal, 4)
            .onChange(of: selection, initial: false) { _, item in
                guard let item else { return }
                Task {
                    // Load as raw data to preserve original quality
                    if let data = try? await item.loadTransferable(type: Data.self) {
                        await MainActor.run {
                            uploadedImageData = data
                            selectedEmoji = nil
                            // Always show Save for uploaded photos
                            showSaveButton = true
                            avatarSaved = false  // Reset saved state when new photo is selected
                        }
                    }
                }
            }

            // Save button for avatar changes
            if showSaveButton || avatarSaved || isSavingAvatar {
                Button(action: {
                    if !avatarSaved && !isSavingAvatar {
                        Haptics.impact(.light)
                        onSaveAvatar()
                    }
                }) {
                    HStack {
                        if isSavingAvatar {
                            ProgressView()
                                .scaleEffect(0.9)
                                .tint(.white)
                        } else {
                            Image(systemName: avatarSaved ? "checkmark.circle.fill" : "checkmark.circle.fill")
                                .foregroundColor(.white)
                        }
                        Text(isSavingAvatar ? "Saving..." : (avatarSaved ? "Saved" : "Save Avatar"))
                            .font(.system(size: 14, weight: .semibold))
                            .foregroundColor(.white)
                    }
                    .frame(maxWidth: .infinity)
                    .padding(.vertical, 12)
                    .background(
                        RoundedRectangle(cornerRadius: 12)
                            .fill(avatarSaved ? Color.green : (isSavingAvatar ? Color.blue.opacity(0.7) : Color.blue))
                    )
                    .overlay(
                        RoundedRectangle(cornerRadius: 12)
                            .stroke((avatarSaved ? Color.green : Color.blue).opacity(0.3), lineWidth: 1)
                    )
                    .shadow(color: .black.opacity(0.1), radius: 4, x: 0, y: 2)
                }
                .buttonStyle(ScaleButtonStyle())
                .padding(.horizontal, 4)
                .padding(.top, 8)
                .transition(.opacity.combined(with: .scale(scale: 0.8)))
                .disabled(avatarSaved || isSavingAvatar)
            }
        }
        .padding(.vertical, 16)
        .padding(.horizontal, 12)
        .background(
            RoundedRectangle(cornerRadius: 20, style: .continuous)
                .fill(Color(.systemBackground))
                .overlay(
                    RoundedRectangle(cornerRadius: 20, style: .continuous)
                        .strokeBorder(Color(red: 0.4, green: 0.2, blue: 0.6).opacity(0.15), lineWidth: 1.5)
                )
                .shadow(color: .black.opacity(0.08), radius: 12, x: 0, y: 4)
        )
    }
}

