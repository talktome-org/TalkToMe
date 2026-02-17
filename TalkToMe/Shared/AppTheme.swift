import SwiftUI
import UIKit

// MARK: - UIColor Extension (Hex Support)

extension UIColor {
  convenience init?(hex: String) {
    let hex = hex.trimmingCharacters(in: CharacterSet.alphanumerics.inverted)
    guard !hex.isEmpty else { return nil }
    var int: UInt64 = 0
    guard Scanner(string: hex).scanHexInt64(&int) else { return nil }
    let a: UInt64
    let r: UInt64
    let g: UInt64
    let b: UInt64
    switch hex.count {
    case 3:  // RGB (12-bit)
      (a, r, g, b) = (255, (int >> 8) * 17, (int >> 4 & 0xF) * 17, (int & 0xF) * 17)
    case 6:  // RGB (24-bit)
      (a, r, g, b) = (255, int >> 16, int >> 8 & 0xFF, int & 0xFF)
    case 8:  // ARGB (32-bit)
      (a, r, g, b) = (int >> 24, int >> 16 & 0xFF, int >> 8 & 0xFF, int & 0xFF)
    default:
      return nil
    }
    self.init(
      red: CGFloat(r) / 255,
      green: CGFloat(g) / 255,
      blue: CGFloat(b) / 255,
      alpha: CGFloat(a) / 255
    )
  }
}

// MARK: - Color Extension (Hex Support)

extension Color {
  init?(hex: String) {
    guard let uiColor = UIColor(hex: hex) else { return nil }
    self.init(uiColor: uiColor)
  }
}

// MARK: - Theme Definitions

extension Color {
  // Raw Palette (Static)
  static let paletteDarkNavy = Color(hex: "#0F1724")!
  static let paletteLightBlue = Color(hex: "#EAF6FF")!
  static let paletteSkyBlue = Color(hex: "#BFDFFF")!
  static let paletteClearBlue = Color(hex: "#6FB7FF")!
  static let paletteOffWhite = Color(hex: "#F5F7FA")!
  static let paletteCoolGray = Color(hex: "#A8B3C7")!
  static let paletteDarkGray = Color(hex: "#5F6B7C")!
  static let paletteSurfaceDark = Color(hex: "#1C2533")!  // Slightly lighter navy for cards
  static let paletteSurfaceLight = Color(hex: "#FFFFFF")!
  static let palettePartner = Color(hex: "#F6E6EC")!

  // Legacy Static Aliases (Deprecated but kept for compatibility)
  static let talkToMeBrand = paletteSkyBlue
  static let talkToMeAccent = paletteClearBlue
  static let talkToMeDarkBackground = paletteDarkNavy
  static let talkToMeLightSurface = paletteLightBlue
  static let talkToMePartnerHighlight = palettePartner
  static let talkToMePrimaryText = paletteOffWhite
  static let talkToMeSecondaryText = paletteCoolGray

  // Semantic Aliases
  static let talkToMeBackground = talkToMeDarkBackground
  static var talkToMeBubbleAI: Color {
    Color(
      uiColor: UIColor { trait in
        trait.userInterfaceStyle == .dark ? UIColor(hex: "#1C2533")! : UIColor(hex: "#EAF6FF")!
      })
  }
  static let talkToMeBubbleUser = talkToMeBrand
}

// MARK: - AppTheme (Dynamic)

struct AppTheme {

  // Dynamic Providers

  static var background: Color {
    Color(
      uiColor: UIColor { trait in
        trait.userInterfaceStyle == .dark ? UIColor(hex: "#0F1724")! : UIColor(hex: "#EAF6FF")!
      })
  }

  static var surface: Color {
    Color(
      uiColor: UIColor { trait in
        trait.userInterfaceStyle == .dark ? UIColor(hex: "#1C2533")! : UIColor(hex: "#FFFFFF")!
      })
  }

  static var brand: Color {
    Color(
      uiColor: UIColor { trait in
        trait.userInterfaceStyle == .dark ? UIColor(hex: "#BFDFFF")! : UIColor(hex: "#4DAAFC")!
      })
  }

  static var accent: Color {
    Color(
      uiColor: UIColor { trait in
        trait.userInterfaceStyle == .dark ? UIColor(hex: "#6FB7FF")! : UIColor(hex: "#007AFF")!
      })
  }

  static var textPrimary: Color {
    Color(
      uiColor: UIColor { trait in
        trait.userInterfaceStyle == .dark ? UIColor(hex: "#F5F7FA")! : UIColor(hex: "#0F1724")!
      })
  }

  static var textSecondary: Color {
    Color(
      uiColor: UIColor { trait in
        trait.userInterfaceStyle == .dark ? UIColor(hex: "#A8B3C7")! : UIColor(hex: "#5F6B7C")!
      })
  }

  static var partner: Color {
    Color.talkToMePartnerHighlight  // Usually constant, but could be dynamic
  }

  // Compatibility
  static let darkBackground = Color.talkToMeDarkBackground
  static let talkToMeBubbleAI = Color.talkToMeBubbleAI
  static let talkToMeDarkBackground = Color.talkToMeDarkBackground
  static let talkToMePrimaryText = Color.talkToMePrimaryText
  static let talkToMeBackground = Color.talkToMeBackground
}
