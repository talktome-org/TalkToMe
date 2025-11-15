import SwiftUI

struct QuickSuggestion: Identifiable, Hashable {
    let id = UUID()
    let title: String
    let subtitle: String
}

struct QuickSuggestionsView: View {

    let suggestions: [QuickSuggestion]
    let onTap: (String) -> Void

    var body: some View {
        ScrollView(.horizontal, showsIndicators: false) {
            HStack(spacing: 12) {
                ForEach(suggestions) { item in
                    Button(action: {
                        onTap("\(item.title) \(item.subtitle)")
                    }) {
                        VStack(alignment: .leading, spacing: 2) {
                            Text(item.title)
                                .font(.system(size: 15, weight: .semibold))
                                .foregroundColor(.primary)
                            Text(item.subtitle)
                                .font(.system(size: 13, weight: .regular))
                                .foregroundColor(.secondary)
                                .lineLimit(2)
                                .multilineTextAlignment(.leading)
                        }
                        .padding(.horizontal, 14)
                        .padding(.vertical, 10)
                        .background {
                            if #available(iOS 26.0, *) {
                                Color.clear
                                    .glassEffect(.regular, in: RoundedRectangle(cornerRadius: 14, style: .continuous))
                            } else {
                                RoundedRectangle(cornerRadius: 14, style: .continuous)
                                    .fill(.ultraThinMaterial)
                            }
                        }
                        .overlay {
                            if #available(iOS 26.0, *) {
                                Color.clear
                            } else {
                                RoundedRectangle(cornerRadius: 14, style: .continuous)
                                    .strokeBorder(Color.black.opacity(0.06), lineWidth: 1)
                            }
                        }
                        .clipShape(RoundedRectangle(cornerRadius: 14, style: .continuous))
                    }
                    .buttonStyle(.plain)
                }
            }
            .padding(.horizontal, 16)
            .padding(.vertical, 6)
        }
        .accessibilityIdentifier("quick-suggestions-scroll")
    }
}

#Preview {
    QuickSuggestionsView(
        suggestions: [
            QuickSuggestion(title: "How to", subtitle: "make a skincare routine effective"),
            QuickSuggestion(title: "Best places", subtitle: "to visit in Copenhagen"),
            QuickSuggestion(title: "Explain", subtitle: "quantum computing in simple terms")
        ],
        onTap: { _ in }
    )
}


