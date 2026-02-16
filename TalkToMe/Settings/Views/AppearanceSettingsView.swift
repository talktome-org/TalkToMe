import SwiftUI

struct AppearanceSettingsView: View {

    @AppStorage(PreferenceKeys.appearancePreference) private var appearance: String = "System"
    @AppStorage(PreferenceKeys.fontSizePreference) private var fontSizeScale: Double = 1.0
    @Environment(\.colorScheme) private var colorScheme

    private let fontSizeSteps: [Double] = [0.85, 1.0, 1.15, 1.3]

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 28) {
                themeSectionView
                textSizeSectionView
                previewSectionView
            }
            .padding(.horizontal, 16)
            .padding(.top, 8)
            .padding(.bottom, 24)
        }
        .scrollIndicators(.hidden)
        .navigationTitle("Appearance")
        .navigationBarTitleDisplayMode(.inline)
        .background(Color(.systemGroupedBackground))
    }

    // MARK: - Theme

    private var themeSectionView: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text("Theme")
                .font(.system(size: 13))
                .foregroundStyle(.secondary)
                .textCase(.uppercase)
                .padding(.horizontal, 4)

            HStack(spacing: 12) {
                themeCard(title: "System", icon: "circle.lefthalf.filled", value: "System")
                themeCard(title: "Light", icon: "sun.max.fill", value: "Light")
                themeCard(title: "Dark", icon: "moon.fill", value: "Dark")
            }
        }
    }

    private func themeCard(title: String, icon: String, value: String) -> some View {
        let isSelected = appearance == value
        return Button {
            Haptics.selection()
            withAnimation(.easeInOut(duration: 0.2)) {
                appearance = value
            }
        } label: {
            VStack(spacing: 10) {
                ZStack {
                    Circle()
                        .fill(isSelected ? Color.primary.opacity(0.12) : Color(.tertiarySystemFill))
                        .frame(width: 44, height: 44)

                    Image(systemName: icon)
                        .font(.system(size: 20, weight: .medium))
                        .foregroundStyle(isSelected ? Color.primary : .secondary)
                }

                Text(title)
                    .font(.system(size: 14, weight: isSelected ? .semibold : .regular))
                    .foregroundStyle(isSelected ? Color.primary : .secondary)
            }
            .frame(maxWidth: .infinity)
            .padding(.vertical, 16)
            .background(
                RoundedRectangle(cornerRadius: 16, style: .continuous)
                    .fill(Color(.secondarySystemGroupedBackground))
            )
            .overlay(
                RoundedRectangle(cornerRadius: 16, style: .continuous)
                    .strokeBorder(
                        isSelected ? Color.primary.opacity(0.3) : Color.clear,
                        lineWidth: 1.5
                    )
            )
            .overlay(alignment: .topTrailing) {
                if isSelected {
                    Image(systemName: "checkmark.circle.fill")
                        .font(.system(size: 18))
                        .foregroundStyle(Color.primary)
                        .padding(8)
                        .transition(.scale.combined(with: .opacity))
                }
            }
        }
        .buttonStyle(.plain)
    }

    // MARK: - Text Size

    private var textSizeSectionView: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text("Text Size")
                .font(.system(size: 13))
                .foregroundStyle(.secondary)
                .textCase(.uppercase)
                .padding(.horizontal, 4)

            HStack(spacing: 14) {
                Text("A")
                    .font(.system(size: 14, weight: .medium))
                    .foregroundStyle(.secondary)

                Slider(
                    value: $fontSizeScale,
                    in: 0.85...1.3,
                    step: 0.15
                )
                .tint(.primary)
                .onChange(of: fontSizeScale) { _, newValue in
                    let snapped = fontSizeSteps.min(by: {
                        abs($0 - newValue) < abs($1 - newValue)
                    }) ?? 1.0
                    if snapped != newValue {
                        fontSizeScale = snapped
                    }
                    Haptics.selection()
                }

                Text("A")
                    .font(.system(size: 22, weight: .medium))
                    .foregroundStyle(.secondary)
            }
            .padding(.horizontal, 16)
            .padding(.vertical, 14)
            .background(Color(.secondarySystemGroupedBackground))
            .clipShape(RoundedRectangle(cornerRadius: 16, style: .continuous))
        }
    }

    // MARK: - Preview

    private var previewSectionView: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text("Preview")
                .font(.system(size: 13))
                .foregroundStyle(.secondary)
                .textCase(.uppercase)
                .padding(.horizontal, 4)

            VStack(spacing: 12) {
                // User message (right-aligned)
                HStack {
                    Spacer()
                    Text("How's your day going?")
                        .font(.system(size: 17 * fontSizeScale, weight: .regular))
                        .lineSpacing(2)
                        .padding(.horizontal, 16)
                        .padding(.vertical, 14)
                        .foregroundColor(.white)
                        .background(
                            RoundedRectangle(cornerRadius: 20, style: .continuous)
                                .fill(userBubbleGradient)
                        )
                }

                // Assistant message (left-aligned)
                HStack {
                    Text("It\u{2019}s been a good one! I\u{2019}ve been thinking about what makes conversations feel meaningful.")
                        .font(.system(size: 17 * fontSizeScale, weight: .regular))
                        .lineSpacing(2)
                        .padding(.horizontal, 12)
                        .padding(.vertical, 8)
                        .foregroundColor(.primary)
                        .background(
                            RoundedRectangle(cornerRadius: 18, style: .continuous)
                                .fill(Color(.secondarySystemBackground))
                        )
                    Spacer()
                }
            }
            .padding(16)
            .background(Color(.secondarySystemGroupedBackground))
            .clipShape(RoundedRectangle(cornerRadius: 16, style: .continuous))
            .animation(.easeInOut(duration: 0.2), value: fontSizeScale)
        }
    }

    private var userBubbleGradient: LinearGradient {
        let colors: [Color] = colorScheme == .dark
            ? [Color(white: 0.22), Color(white: 0.12)]
            : [Color(white: 0.25), Color(white: 0.15)]
        return LinearGradient(colors: colors, startPoint: .topLeading, endPoint: .bottomTrailing)
    }
}

#Preview {
    NavigationStack {
        AppearanceSettingsView()
    }
}
