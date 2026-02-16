import PhotosUI
import SwiftUI

// MARK: - Gradient Presets

struct GradientPreset: Identifiable {
  let id: String
  let name: String
  let colors: [Color]
  let startPoint: UnitPoint
  let endPoint: UnitPoint

  var gradient: LinearGradient {
    LinearGradient(colors: colors, startPoint: startPoint, endPoint: endPoint)
  }

  init(
    _ name: String, _ colors: [Color], start: UnitPoint = .topLeading,
    end: UnitPoint = .bottomTrailing
  ) {
    self.id = name
    self.name = name
    self.colors = colors
    self.startPoint = start
    self.endPoint = end
  }
}

let wallpaperPresets: [GradientPreset] = [
  GradientPreset(
    "Midnight",
    [Color(red: 0.05, green: 0.05, blue: 0.15), Color(red: 0.1, green: 0.08, blue: 0.3)]),
  GradientPreset(
    "Sunset", [Color(red: 0.2, green: 0.05, blue: 0.15), Color(red: 0.35, green: 0.1, blue: 0.25)],
    start: .top, end: .bottom),
  GradientPreset(
    "Ocean", [Color(red: 0.02, green: 0.1, blue: 0.18), Color(red: 0.05, green: 0.2, blue: 0.3)]),
  GradientPreset(
    "Forest", [Color(red: 0.03, green: 0.12, blue: 0.06), Color(red: 0.08, green: 0.2, blue: 0.1)],
    start: .top, end: .bottom),
  GradientPreset(
    "Berry", [Color(red: 0.18, green: 0.04, blue: 0.2), Color(red: 0.25, green: 0.08, blue: 0.15)]),
  GradientPreset(
    "Storm", [Color(red: 0.08, green: 0.08, blue: 0.1), Color(red: 0.15, green: 0.15, blue: 0.18)],
    start: .top, end: .bottom),
  GradientPreset(
    "Dawn", [Color(red: 0.2, green: 0.12, blue: 0.05), Color(red: 0.3, green: 0.18, blue: 0.08)],
    start: .top, end: .bottom),
  GradientPreset(
    "Arctic", [Color(red: 0.08, green: 0.12, blue: 0.18), Color(red: 0.15, green: 0.22, blue: 0.3)]),
  GradientPreset(
    "Ember", [Color(red: 0.2, green: 0.06, blue: 0.02), Color(red: 0.3, green: 0.1, blue: 0.05)],
    start: .top, end: .bottom),
  GradientPreset(
    "Lavender", [Color(red: 0.12, green: 0.08, blue: 0.2), Color(red: 0.2, green: 0.15, blue: 0.3)]),
  GradientPreset(
    "Moss", [Color(red: 0.06, green: 0.1, blue: 0.04), Color(red: 0.12, green: 0.18, blue: 0.08)]),
  GradientPreset(
    "Slate", [Color(red: 0.1, green: 0.1, blue: 0.12), Color(red: 0.18, green: 0.18, blue: 0.2)],
    start: .top, end: .bottom),
]

// MARK: - View

struct WallpapersSettingsView: View {

  @AppStorage(PreferenceKeys.chatWallpaperType) private var wallpaperType: String = "default"
  @AppStorage(PreferenceKeys.chatWallpaperValue) private var wallpaperValue: String = ""

<<<<<<< Updated upstream
    @State private var photoSelection: PhotosPickerItem?
    @State private var selectedColor: Color = .black
    @State private var showColorPicker = false
    @State private var toastMessage: String? = nil
    @State private var toastWorkItem: DispatchWorkItem? = nil
=======
  @State private var photoSelection: PhotosPickerItem?
  @State private var selectedColor: Color = .black
  @State private var showColorPicker = false
>>>>>>> Stashed changes

  private let columns = [
    GridItem(.flexible(), spacing: 10),
    GridItem(.flexible(), spacing: 10),
    GridItem(.flexible(), spacing: 10),
  ]

<<<<<<< Updated upstream
    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 24) {
                topActionsSection
                builtInSection
                resetSection
            }
            .padding(.horizontal, 16)
            .padding(.top, 8)
            .padding(.bottom, 24)
        }
        .scrollIndicators(.hidden)
        .navigationTitle("Wallpapers")
        .navigationBarTitleDisplayMode(.inline)
        .background(Color(.systemGroupedBackground))
        .overlay(alignment: .top) {
            if let toastMessage {
                ToastOverlayView(message: toastMessage, onDismiss: dismissToast)
                    .padding(.top, 8)
            }
        }
        .animation(.spring(response: 0.3, dampingFraction: 0.8), value: toastMessage)
        .onChange(of: photoSelection) { _, item in
            guard let item else { return }
            Task {
                if let data = try? await item.loadTransferable(type: Data.self) {
                    savePhotoWallpaper(data)
                }
            }
        }
=======
  var body: some View {
    ScrollView {
      VStack(alignment: .leading, spacing: 24) {
        topActionsSection
        builtInSection
        resetSection
      }
      .padding(.horizontal, 16)
      .padding(.top, 8)
      .padding(.bottom, 24)
>>>>>>> Stashed changes
    }
    .scrollIndicators(.hidden)
    .navigationTitle("Wallpapers")
    .navigationBarTitleDisplayMode(.inline)
    .background(AppTheme.background)
    .onChange(of: photoSelection) { _, item in
      guard let item else { return }
      Task {
        if let data = try? await item.loadTransferable(type: Data.self) {
          savePhotoWallpaper(data)
        }
      }
    }
  }

  // MARK: - Top Actions

  private var topActionsSection: some View {
    HStack(spacing: 12) {
      PhotosPicker(selection: $photoSelection, matching: .images) {
        actionCard(icon: "photo.on.rectangle", title: "Photos", isActive: wallpaperType == "photo")
      }
      .buttonStyle(.plain)

      Button {
        showColorPicker = true
      } label: {
        actionCard(icon: "paintpalette", title: "Solid Color", isActive: wallpaperType == "color")
      }
      .buttonStyle(.plain)
      .sheet(isPresented: $showColorPicker) {
        colorPickerSheet
      }
    }
  }

  private func actionCard(icon: String, title: String, isActive: Bool) -> some View {
    VStack(spacing: 8) {
      ZStack {
        Circle()
          .fill(isActive ? Color.primary.opacity(0.12) : Color(.tertiarySystemFill))
          .frame(width: 44, height: 44)

        Image(systemName: icon)
          .font(.system(size: 20, weight: .medium))
          .foregroundStyle(isActive ? Color.primary : .secondary)
      }

      Text(title)
        .font(.system(size: 13, weight: .medium))
        .foregroundStyle(isActive ? Color.primary : .secondary)
    }
    .frame(maxWidth: .infinity)
    .padding(.vertical, 16)
    .background(
      RoundedRectangle(cornerRadius: 16, style: .continuous)
        .fill(AppTheme.surface)
    )
    .overlay(
      RoundedRectangle(cornerRadius: 16, style: .continuous)
        .strokeBorder(isActive ? Color.primary.opacity(0.3) : .clear, lineWidth: 1.5)
    )
  }

  // MARK: - Color Picker Sheet

  private var colorPickerSheet: some View {
    NavigationStack {
      VStack(spacing: 24) {
        RoundedRectangle(cornerRadius: 16, style: .continuous)
          .fill(selectedColor)
          .frame(height: 120)
          .overlay(
            RoundedRectangle(cornerRadius: 16, style: .continuous)
              .strokeBorder(Color.primary.opacity(0.1), lineWidth: 1)
          )

        ColorPicker("Choose a color", selection: $selectedColor, supportsOpacity: false)
          .font(.system(size: 17))
          .padding(.horizontal, 16)
          .padding(.vertical, 12)
          .background(AppTheme.surface)
          .clipShape(RoundedRectangle(cornerRadius: 16, style: .continuous))

<<<<<<< Updated upstream
    private var colorPickerSheet: some View {
        NavigationStack {
            VStack(spacing: 24) {
                RoundedRectangle(cornerRadius: 16, style: .continuous)
                    .fill(selectedColor)
                    .frame(height: 120)
                    .overlay(
                        RoundedRectangle(cornerRadius: 16, style: .continuous)
                            .strokeBorder(Color.primary.opacity(0.1), lineWidth: 1)
                    )

                ColorPicker("Choose a color", selection: $selectedColor, supportsOpacity: false)
                    .font(.system(size: 17))
                    .padding(.horizontal, 16)
                    .padding(.vertical, 12)
                    .background(Color(.secondarySystemGroupedBackground))
                    .clipShape(RoundedRectangle(cornerRadius: 16, style: .continuous))

                Spacer()
            }
            .padding(16)
            .background(Color(.systemGroupedBackground))
            .navigationTitle("Solid Color")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Cancel") { showColorPicker = false }
                }
                ToolbarItem(placement: .confirmationAction) {
                    Button("Apply") {
                        wallpaperType = "color"
                        wallpaperValue = Self.hexFromColor(selectedColor)
                        Haptics.selection()
                        showColorPicker = false
                        showToast("Background color applied")
                    }
                    .fontWeight(.semibold)
                }
            }
        }
        .presentationDetents([.medium])
        .presentationDragIndicator(.visible)
    }

    // MARK: - Built-in Gradients

    private var builtInSection: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text("Built-in")
                .font(.system(size: 13))
                .foregroundStyle(.secondary)
                .textCase(.uppercase)
                .padding(.horizontal, 4)

            LazyVGrid(columns: columns, spacing: 10) {
                ForEach(wallpaperPresets) { preset in
                    Button {
                        Haptics.selection()
                        withAnimation(.easeInOut(duration: 0.2)) {
                            wallpaperType = "gradient"
                            wallpaperValue = preset.id
                        }
                        showToast("\(preset.name) wallpaper applied")
                    } label: {
                        gradientThumbnail(preset)
                    }
                    .buttonStyle(.plain)
                }
            }
=======
        Spacer()
      }
      .padding(16)
      .background(AppTheme.background)
      .navigationTitle("Solid Color")
      .navigationBarTitleDisplayMode(.inline)
      .toolbar {
        ToolbarItem(placement: .cancellationAction) {
          Button("Cancel") { showColorPicker = false }
        }
        ToolbarItem(placement: .confirmationAction) {
          Button("Apply") {
            wallpaperType = "color"
            wallpaperValue = Self.hexFromColor(selectedColor)
            Haptics.selection()
            showColorPicker = false
          }
          .fontWeight(.semibold)
>>>>>>> Stashed changes
        }
      }
    }
    .presentationDetents([.medium])
    .presentationDragIndicator(.visible)
  }

  // MARK: - Built-in Gradients

  private var builtInSection: some View {
    VStack(alignment: .leading, spacing: 8) {
      Text("Built-in")
        .font(.system(size: 13))
        .foregroundStyle(.secondary)
        .textCase(.uppercase)
        .padding(.horizontal, 4)

      LazyVGrid(columns: columns, spacing: 10) {
        ForEach(wallpaperPresets) { preset in
          Button {
            Haptics.selection()
            withAnimation(.easeInOut(duration: 0.2)) {
              wallpaperType = "gradient"
              wallpaperValue = preset.id
            }
<<<<<<< Updated upstream
            removePhotoWallpaper()
            showToast("Wallpaper reset to default")
        } label: {
            HStack {
                Image(systemName: "arrow.counterclockwise")
                    .font(.system(size: 16, weight: .medium))
                Text("Reset to Default")
                    .font(.system(size: 17))
            }
            .foregroundStyle(wallpaperType == "default" ? .secondary : .primary)
            .frame(maxWidth: .infinity)
            .padding(.vertical, 14)
            .background(Color(.secondarySystemGroupedBackground))
            .clipShape(RoundedRectangle(cornerRadius: 16, style: .continuous))
=======
          } label: {
            gradientThumbnail(preset)
          }
          .buttonStyle(.plain)
>>>>>>> Stashed changes
        }
      }
    }
  }

<<<<<<< Updated upstream
    // MARK: - Toast Helpers

    private func showToast(_ message: String) {
        toastWorkItem?.cancel()
        withAnimation { toastMessage = message }
        let work = DispatchWorkItem {
            withAnimation { toastMessage = nil }
        }
        toastWorkItem = work
        DispatchQueue.main.asyncAfter(deadline: .now() + 2.5, execute: work)
    }

    private func dismissToast() {
        toastWorkItem?.cancel()
        withAnimation { toastMessage = nil }
    }

    // MARK: - Photo Helpers

    private func savePhotoWallpaper(_ data: Data) {
        guard let image = UIImage(data: data),
              let jpeg = image.jpegData(compressionQuality: 0.85) else { return }

        let filename = "chat_wallpaper.jpg"
        let url = Self.wallpaperFileURL(filename: filename)
        try? jpeg.write(to: url)

        wallpaperType = "photo"
        wallpaperValue = filename
        Haptics.selection()
        showToast("Photo wallpaper applied")
=======
  private func gradientThumbnail(_ preset: GradientPreset) -> some View {
    let isSelected = wallpaperType == "gradient" && wallpaperValue == preset.id
    return ZStack(alignment: .bottomLeading) {
      RoundedRectangle(cornerRadius: 14, style: .continuous)
        .fill(preset.gradient)
        .aspectRatio(0.75, contentMode: .fit)

      Text(preset.name)
        .font(.system(size: 11, weight: .medium))
        .foregroundStyle(.white.opacity(0.7))
        .padding(8)
>>>>>>> Stashed changes
    }
    .overlay(
      RoundedRectangle(cornerRadius: 14, style: .continuous)
        .strokeBorder(isSelected ? Color.white.opacity(0.6) : .clear, lineWidth: 2)
    )
    .overlay(alignment: .topTrailing) {
      if isSelected {
        Image(systemName: "checkmark.circle.fill")
          .font(.system(size: 20))
          .foregroundStyle(.white)
          .shadow(color: .black.opacity(0.3), radius: 2, y: 1)
          .padding(6)
          .transition(.scale.combined(with: .opacity))
      }
    }
  }

  // MARK: - Reset

  private var resetSection: some View {
    Button {
      Haptics.selection()
      withAnimation(.easeInOut(duration: 0.2)) {
        wallpaperType = "default"
        wallpaperValue = ""
      }
      removePhotoWallpaper()
    } label: {
      HStack {
        Image(systemName: "arrow.counterclockwise")
          .font(.system(size: 16, weight: .medium))
        Text("Reset to Default")
          .font(.system(size: 17))
      }
      .foregroundStyle(wallpaperType == "default" ? .secondary : .primary)
      .frame(maxWidth: .infinity)
      .padding(.vertical, 14)
      .background(AppTheme.surface)
      .clipShape(RoundedRectangle(cornerRadius: 16, style: .continuous))
    }
    .buttonStyle(.plain)
    .disabled(wallpaperType == "default")
  }

  // MARK: - Photo Helpers

  private func savePhotoWallpaper(_ data: Data) {
    guard let image = UIImage(data: data),
      let jpeg = image.jpegData(compressionQuality: 0.85)
    else { return }

    let filename = "chat_wallpaper.jpg"
    let url = Self.wallpaperFileURL(filename: filename)
    try? jpeg.write(to: url)

    wallpaperType = "photo"
    wallpaperValue = filename
    Haptics.selection()
  }

  private func removePhotoWallpaper() {
    let url = Self.wallpaperFileURL(filename: "chat_wallpaper.jpg")
    try? FileManager.default.removeItem(at: url)
  }

  static func wallpaperFileURL(filename: String) -> URL {
    let docs = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask)[0]
    return docs.appendingPathComponent(filename)
  }

  static func colorFromHex(_ hex: String) -> Color {
    var s = hex.trimmingCharacters(in: CharacterSet(charactersIn: "#"))
    guard s.count == 6, let num = UInt64(s, radix: 16) else { return .black }
    return Color(
      red: Double((num >> 16) & 0xFF) / 255,
      green: Double((num >> 8) & 0xFF) / 255,
      blue: Double(num & 0xFF) / 255
    )
  }

  static func hexFromColor(_ color: Color) -> String {
    guard let components = UIColor(color).cgColor.components, components.count >= 3 else {
      return "000000"
    }
    let r = Int(components[0] * 255)
    let g = Int(components[1] * 255)
    let b = Int(components[2] * 255)
    return String(format: "%02X%02X%02X", r, g, b)
  }
}

#Preview {
  NavigationStack {
    WallpapersSettingsView()
  }
}
