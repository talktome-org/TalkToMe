import SwiftUI

struct SidebarAvatarView: View {
    let avatarURL: String?
    var name: String? = nil

    @State private var image: UIImage?
    @State private var isLoading = false
    @State private var refreshKey = UUID()

    private let avatarCacheManager = AvatarCacheManager.shared

    private var placeholderView: some View {
        Circle()
            .fill(Color(.tertiarySystemFill))
            .overlay(
                Group {
                    if let name, let first = name.first {
                        Text(String(first).uppercased())
                            .font(.system(size: 16, weight: .semibold, design: .rounded))
                            .foregroundStyle(.secondary)
                    } else {
                        Image(systemName: "person.fill")
                            .font(.system(size: 16, weight: .medium))
                            .foregroundStyle(.secondary)
                    }
                }
            )
    }

    var body: some View {
        Group {
            if let image = image {
                Image(uiImage: image)
                    .resizable()
                    .scaledToFill()
            } else if isLoading {
                placeholderView
            } else {
                placeholderView
            }
        }
        .id(refreshKey)
        .onAppear {
            loadAvatar()
        }
        .onChange(of: avatarURL) { _, newURL in
            // Force refresh when avatar URL changes
            refreshKey = UUID()
            loadAvatar()
        }
        .onReceive(NotificationCenter.default.publisher(for: .avatarChanged)) { _ in
            // Force refresh when avatar changes notification is received
            refreshKey = UUID()
            loadAvatar()
        }
    }

    private func loadAvatar() {
        guard let urlString = avatarURL, !urlString.isEmpty else {
            image = nil
            isLoading = false
            return
        }

        // First try to get cached image immediately
        if let cachedImage = avatarCacheManager.getImageIfCached(urlString: urlString) {
            image = cachedImage
            isLoading = false
            return
        }

        // If not cached, show loading and fetch
        isLoading = true
        image = nil

        Task {
            let fetchedImage = await avatarCacheManager.getCachedImage(urlString: urlString)
            await MainActor.run {
                image = fetchedImage
                isLoading = false
            }
        }
    }
}

#Preview {
    SidebarAvatarView(avatarURL: nil)
        .frame(width: 36, height: 36)
}
