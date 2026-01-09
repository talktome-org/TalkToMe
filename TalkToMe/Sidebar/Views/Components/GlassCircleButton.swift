import SwiftUI

struct GlassCircleButton: View {
    let systemName: String
    let action: () -> Void

    var body: some View {
        Button(action: action) {
            Image(systemName: systemName)
                .font(.system(size: 18, weight: .semibold))
                .foregroundStyle(.primary)
                .frame(width: 44, height: 44)
                .background {
                    if #available(iOS 26.0, *) {
                        Circle()
                            .fill(.clear)
                            .glassEffect()
                    } else {
                        Circle()
                            .fill(.ultraThinMaterial)
                    }
                }
                .clipShape(Circle())
                .overlay(
                    Circle()
                        .stroke(Color.white.opacity(0.16), lineWidth: 1)
                )
        }
        .buttonStyle(.plain)
    }
}


