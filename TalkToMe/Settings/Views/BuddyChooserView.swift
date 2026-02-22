import SwiftUI

struct BuddyChooserView: View {

    @AppStorage(PreferenceKeys.elevenLabsVoiceName) private var selectedVoiceName: String = ""
    @State private var confirmedKey: String = ""

    private let buddies = ElevenLabsVoiceSuggestionsView.requiredBuddies

    private static let imageScales: [String: CGFloat] = [
        "pax": 1.08,
        "luma": 1.08,
        "snow": 1.08,
        "hex": 1.25,
    ]

    private var currentKey: String {
        selectedVoiceName.lowercased().trimmingCharacters(in: .whitespacesAndNewlines)
    }

    var body: some View {
        NavigationStack {
            ScrollView {
                VStack(spacing: 14) {
                    ForEach(buddies) { buddy in
                        buddyCard(buddy)
                    }
                }
                .padding(.horizontal, 16)
                .padding(.top, 8)
                .padding(.bottom, 32)
            }
            .scrollIndicators(.hidden)
            .navigationTitle("Change Buddy")
            .navigationBarTitleDisplayMode(.inline)
            .background(AppTheme.background)
        }
        .presentationDragIndicator(.visible)
        .onAppear {
            confirmedKey = currentKey
        }
    }

    @ViewBuilder
    private func buddyCard(_ buddy: BuddyDefinition) -> some View {
        let isSelected = buddy.key == confirmedKey

        Button {
            Haptics.impact(.medium)
            withAnimation(.spring(response: 0.3, dampingFraction: 0.7)) {
                confirmedKey = buddy.key
            }
            // Persist selection
            UserDefaults.standard.set(buddy.name, forKey: PreferenceKeys.elevenLabsVoiceName)
            UserDefaults.standard.set(true, forKey: PreferenceKeys.buddyExplicitlyChosen)
        } label: {
            HStack(spacing: 16) {
                let imageScale = Self.imageScales[buddy.key] ?? 1.0
                buddyImage(buddy)
                    .scaleEffect(imageScale)
                    .frame(width: 88, height: 88)
                    .clipShape(RoundedRectangle(cornerRadius: 18, style: .continuous))

                VStack(alignment: .leading, spacing: 5) {
                    HStack(spacing: 6) {
                        Text(buddy.name)
                            .font(.system(size: 19, weight: .semibold, design: .rounded))
                            .foregroundStyle(.primary)

                        buddyTag(for: buddy)
                    }

                    Text(CustomizeBuddiesView.overviews[buddy.key] ?? buddy.description)
                        .font(.system(size: 14))
                        .foregroundStyle(.secondary)
                        .lineSpacing(2)
                }

                Spacer(minLength: 0)

                ZStack {
                    Circle()
                        .strokeBorder(isSelected ? Color.accentColor : Color(.tertiarySystemFill), lineWidth: 2)
                        .frame(width: 28, height: 28)

                    if isSelected {
                        Circle()
                            .fill(Color.accentColor)
                            .frame(width: 28, height: 28)
                            .overlay(
                                Image(systemName: "checkmark")
                                    .font(.system(size: 13, weight: .bold))
                                    .foregroundStyle(.white)
                            )
                            .transition(.scale.combined(with: .opacity))
                    }
                }
                .animation(.spring(response: 0.3, dampingFraction: 0.7), value: isSelected)
            }
            .padding(16)
            .modifier(BuddyGlassModifier(shape: RoundedRectangle(cornerRadius: 22, style: .continuous)))
            .overlay(
                RoundedRectangle(cornerRadius: 22, style: .continuous)
                    .strokeBorder(isSelected ? Color.accentColor.opacity(0.4) : Color.clear, lineWidth: 1.5)
            )
        }
        .buttonStyle(BuddyChooserCardStyle())
    }

    @ViewBuilder
    private func buddyImage(_ buddy: BuddyDefinition) -> some View {
        if let uiImage = ElevenLabsVoiceSuggestionsView.ghostUIImage(for: buddy.name) {
            Image(uiImage: uiImage)
                .resizable()
                .scaledToFit()
        } else {
            RoundedRectangle(cornerRadius: 18, style: .continuous)
                .fill(.ultraThinMaterial)
                .overlay {
                    Image(systemName: "sparkles")
                        .font(.system(size: 24, weight: .semibold))
                        .foregroundStyle(.secondary)
                }
        }
    }

    @ViewBuilder
    private func buddyTag(for buddy: BuddyDefinition) -> some View {
        switch buddy.key {
        case "snow":
            Text("Popular")
                .font(.system(size: 10, weight: .semibold))
                .foregroundStyle(.white)
                .padding(.horizontal, 6)
                .padding(.vertical, 2)
                .background(Capsule().fill(Color.orange))
        case "luma":
            Text("Most interactive")
                .font(.system(size: 10, weight: .semibold))
                .foregroundStyle(.white)
                .padding(.horizontal, 6)
                .padding(.vertical, 2)
                .background(Capsule().fill(Color.purple))
        default:
            EmptyView()
        }
    }
}

private struct BuddyGlassModifier<S: Shape>: ViewModifier {
    let shape: S

    func body(content: Content) -> some View {
        if #available(iOS 26.0, *) {
            content
                .glassEffect(.regular.interactive(), in: shape)
        } else {
            content
                .background(shape.fill(Color(.secondarySystemGroupedBackground)))
                .overlay(shape.stroke(Color(.separator).opacity(0.2), lineWidth: 0.5))
        }
    }
}

private struct BuddyChooserCardStyle: ButtonStyle {
    func makeBody(configuration: Configuration) -> some View {
        configuration.label
            .scaleEffect(configuration.isPressed ? 0.97 : 1.0)
            .animation(.spring(response: 0.25, dampingFraction: 0.7), value: configuration.isPressed)
    }
}

#Preview {
    BuddyChooserView()
}
