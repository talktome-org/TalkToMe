import SwiftUI
import PhotosUI

struct PersonalizationEditView: View {
    @Binding var isPresented: Bool
    let profileNamespace: Namespace.ID

    @StateObject private var viewModel = SettingsViewModel()
    @EnvironmentObject private var sessionsVM: ChatSessionsViewModel
    @Environment(\.colorScheme) private var colorScheme

    @State private var showingAvatarSelection = false
    @State private var previewEmoji: String? = nil
    @State private var previewImageData: Data? = nil
    @State private var showSaveButton = false

    // Personal info fields
    @State private var fullName: String = ""
    @State private var birthday: Date = Date()
    @State private var personalInfo: String = ""

    // Cached avatar image
    @State private var avatarRefreshKey = UUID()

    // Save state
    @State private var isSaving: Bool = false
    @State private var showSaveSuccess: Bool = false
    @State private var avatarSaved: Bool = false
    @State private var isSavingAvatar: Bool = false

    var body: some View {
        ZStack {
            NavigationStack {
                ScrollView {
                    VStack(spacing: 0) {
                        // Top gap
                        Color.clear
                            .frame(height: 40)

                        // Current profile picture
                        ZStack {
                            // Background: Show preview if available, otherwise show saved avatar
                            if let previewData = previewImageData, let previewImage = UIImage(data: previewData) {
                                // Show preview image immediately
                                Image(uiImage: previewImage)
                                    .resizable()
                                    .scaledToFill()
                                    .frame(width: 120, height: 120)
                                    .clipShape(Circle())
                                    .matchedGeometryEffect(id: "personalizationAvatar", in: profileNamespace)
                            } else if let previewEmoji = previewEmoji {
                                // Show preview emoji immediately
                                ZStack {
                                    Circle()
                                        .fill(
                                            LinearGradient(
                                                colors: [
                                                    Color(red: 0.26, green: 0.58, blue: 1.00),
                                                    Color(red: 0.63, green: 0.32, blue: 0.98)
                                                ],
                                                startPoint: .topLeading,
                                                endPoint: .bottomTrailing
                                            )
                                        )
                                        .frame(width: 120, height: 120)
                                        .overlay(
                                            Circle()
                                                .stroke(Color.white.opacity(0.9), lineWidth: 2)
                                        )

                                    Text(previewEmoji)
                                        .font(.system(size: 64))
                                }
                                .matchedGeometryEffect(id: "personalizationAvatar", in: profileNamespace)
                            } else {
                                // Show saved avatar using cache manager
                                AvatarCacheManager.shared.cachedAsyncImage(
                                    urlString: sessionsVM.myAvatarURL,
                                    placeholder: {
                                        AnyView(
                                            ProgressView()
                                                .scaleEffect(0.8)
                                                .frame(width: 120, height: 120)
                                        )
                                    },
                                    fallback: {
                                        AnyView(
                                            Circle()
                                                .fill(
                                                    LinearGradient(
                                                        colors: [
                                                            Color(red: 0.26, green: 0.58, blue: 1.00),
                                                            Color(red: 0.63, green: 0.32, blue: 0.98)
                                                        ],
                                                        startPoint: .topLeading,
                                                        endPoint: .bottomTrailing
                                                    )
                                                )
                                                .frame(width: 120, height: 120)
                                                .overlay(
                                                    Circle()
                                                        .stroke(Color.white.opacity(0.9), lineWidth: 2)
                                                )
                                                .overlay(
                                                    Image(systemName: "person.fill")
                                                        .font(.system(size: 48, weight: .semibold))
                                                        .foregroundColor(.white)
                                                )
                                        )
                                    }
                                )
                                .frame(width: 120, height: 120)
                                .clipShape(Circle())
                                .matchedGeometryEffect(id: "personalizationAvatar", in: profileNamespace)
                                .id(avatarRefreshKey)
                            }

                        }
                        .animation(.spring(response: 0.35, dampingFraction: 0.7), value: previewEmoji)
                        .animation(.spring(response: 0.35, dampingFraction: 0.7), value: previewImageData)
                        .shadow(color: .black.opacity(0.08), radius: 10, x: 0, y: 6)
                        .transition(.move(edge: .bottom).combined(with: .opacity))
                        .padding(.bottom, 20)

                        // Edit Avatar button
                        Button(action: {
                            if showingAvatarSelection {
                                // Clear unsaved selections when closing
                                previewEmoji = nil
                                previewImageData = nil
                                showSaveButton = false
                            }
                            Haptics.impact(.light)
                            withAnimation(.spring(response: 0.25, dampingFraction: 0.8)) {
                                showingAvatarSelection.toggle()
                            }
                        }) {
                            HStack(spacing: 8) {
                                Image(systemName: showingAvatarSelection ? "chevron.up.circle.fill" : "person.2.circle")
                                    .foregroundColor(Color(red: 0.4, green: 0.2, blue: 0.6))
                                    .font(.system(size: 14, weight: .semibold))
                                Text("Edit Avatar")
                                    .font(.system(size: 14, weight: .semibold))
                                    .foregroundColor(Color(red: 0.4, green: 0.2, blue: 0.6))
                            }
                            .padding(.horizontal, 16)
                            .padding(.vertical, 10)
                            .background(
                                Capsule()
                                    .fill(showingAvatarSelection ? Color(red: 0.4, green: 0.2, blue: 0.6).opacity(0.1) : Color(.systemGray6))
                                    .overlay(
                                        Capsule()
                                            .stroke(Color(red: 0.4, green: 0.2, blue: 0.6).opacity(showingAvatarSelection ? 0.4 : 0.12), lineWidth: showingAvatarSelection ? 2 : 1)
                                    )
                                    .shadow(color: .black.opacity(0.05), radius: 6, x: 0, y: 3)
                            )
                        }
                        .buttonStyle(PlainButtonStyle())
                        .padding(.bottom, 40)

                        // Personal Information Section
                        VStack(spacing: 16) {
                            // Full Name Field
                            VStack(spacing: 0) {
                                TextField("Full Name", text: $fullName)
                                    .font(.system(size: 16, weight: .medium))
                                    .padding(.horizontal, 16)
                                    .padding(.vertical, 12)
                                    .background(
                                        RoundedRectangle(cornerRadius: 12)
                                            .fill(colorScheme == .dark ? Color(.systemGray6) : Color.white)
                                            .overlay(
                                                RoundedRectangle(cornerRadius: 12)
                                                    .stroke(colorScheme == .dark ? Color.white.opacity(0.1) : Color.gray.opacity(0.1), lineWidth: 1)
                                            )
                                            .shadow(color: colorScheme == .dark ? .black.opacity(0.2) : .black.opacity(0.05), radius: 2, x: 0, y: 1)
                                    )
                                    .onChange(of: fullName) { _, newValue in
                                        let limit = 22
                                        if newValue.count > limit {
                                            fullName = String(newValue.prefix(limit))
                                        }
                                    }

                                // Helper text
                                HStack {
                                    Text("Enter your name and add an optional profile photo.")
                                        .font(.system(size: 13))
                                        .foregroundColor(.secondary)
                                        .frame(maxWidth: .infinity, alignment: .leading)
                                    Text("\(fullName.count)/22")
                                        .font(.system(size: 12, weight: .semibold))
                                        .foregroundColor(fullName.count > 22 ? .red : .secondary)
                                }
                                .padding(.horizontal, 4)
                                .padding(.top, 8)
                            }

                            // Bio Section
                            VStack(spacing: 12) {
                                TextField("Bio", text: $personalInfo, axis: .vertical)
                                    .font(.system(size: 16, weight: .medium))
                                    .padding(.horizontal, 16)
                                    .padding(.vertical, 12)
                                    .background(
                                        RoundedRectangle(cornerRadius: 12)
                                            .fill(colorScheme == .dark ? Color(.systemGray6) : Color.white)
                                            .overlay(
                                                RoundedRectangle(cornerRadius: 12)
                                                    .stroke(colorScheme == .dark ? Color.white.opacity(0.1) : Color.gray.opacity(0.1), lineWidth: 1)
                                            )
                                            .shadow(color: colorScheme == .dark ? .black.opacity(0.2) : .black.opacity(0.05), radius: 2, x: 0, y: 1)
                                    )
                                    .lineLimit(3...6)

                                // Helper text
                                Text("This will help TalkToMe understand you better and provide more personalized responses.")
                                    .font(.system(size: 13))
                                    .foregroundColor(.secondary)
                                    .frame(maxWidth: .infinity, alignment: .leading)
                                    .padding(.horizontal, 4)
                            }

                        }
                        .padding(.horizontal, 8)
                        .padding(.bottom, 40)

                        // (Replaced by top-left toolbar Save button)
                    }
                    .padding(.horizontal, 16)
                    .padding(.bottom, 24)
                }
                .scrollIndicators(.hidden)
                .scrollDisabled(showingAvatarSelection)
                .background(Color.clear)
                .transition(.opacity.combined(with: .move(edge: .bottom)))
                .animation(.easeInOut(duration: 0.2), value: showingAvatarSelection)
                .onTapGesture {
                    // Dismiss keyboard when tapping anywhere on the screen
                    UIApplication.shared.sendAction(#selector(UIResponder.resignFirstResponder), to: nil, from: nil, for: nil)
                }
                .onAppear {
                    loadProfileData()
                    // Only fetch from backend if not already loaded from cache
                    if !viewModel.isProfileLoaded {
                        viewModel.loadProfileInfo()
                    }
                }
                .onChange(of: viewModel.isProfileLoaded, initial: false) { _, loaded in
                    if loaded { loadProfileData() }
                }
            }
        }
        .background(Color(.systemGray6).ignoresSafeArea())
        .animation(.spring(response: 0.45, dampingFraction: 0.9, blendDuration: 0), value: isPresented)
        .overlay(alignment: .top) {
            GeometryReader { proxy in
                let topInset: CGFloat = proxy.safeAreaInsets.top

                Color(.systemGray6)
                    .frame(height: topInset)
                    .ignoresSafeArea(edges: .top)
                    .overlay(
                        Group {
                            EmptyView()
                        }, alignment: .bottom
                    )
                    .allowsHitTesting(false)
                    .accessibilityHidden(true)
            }
            .frame(height: 0)
        }
        .overlay(alignment: .top) {
            ZStack(alignment: .top) {
                if showingAvatarSelection {
                    // Tap to dismiss background
                    Color.clear
                        .contentShape(Rectangle())
                        .onTapGesture {
                            // Clear unsaved selections
                            previewEmoji = nil
                            previewImageData = nil
                            showSaveButton = false
                            Haptics.impact(.light)
                            withAnimation(.spring(response: 0.25, dampingFraction: 0.8)) {
                                showingAvatarSelection = false
                            }
                        }
                }

                VStack(spacing: 0) {
                    // Spacer to position avatar picker slightly higher
                    Color.clear
                        .frame(height: 170)

                        CompactAvatarPickerView(
                            viewModel: viewModel,
                            isPresented: $showingAvatarSelection,
                            selectedEmoji: $previewEmoji,
                            uploadedImageData: $previewImageData,
                            showSaveButton: $showSaveButton,
                            avatarSaved: $avatarSaved,
                            isSavingAvatar: $isSavingAvatar,
                            onSaveAvatar: {
                                Task {
                                    await saveAvatarOnly()
                                }
                            }
                        )
                        .padding(.horizontal, 16)
                        .scaleEffect(showingAvatarSelection ? 1.0 : 0.5)

                    Spacer()
                }
                .allowsHitTesting(showingAvatarSelection)

            }
            .animation(.spring(response: 0.25, dampingFraction: 0.8), value: showingAvatarSelection)
            .zIndex(showingAvatarSelection ? 1 : -1)
            .opacity(showingAvatarSelection ? 1.0 : 0.0)
        }
        .overlay(alignment: .topTrailing) {
            // Close button in top right
            Button(action: {
                Haptics.impact(.light)
                isPresented = false
            }) {
                Image(systemName: "xmark")
                    .font(.system(size: 20, weight: .semibold))
                    .foregroundColor(Color(red: 0.4, green: 0.2, blue: 0.6))
                    .frame(width: 44, height: 44)
            }
            .background(
                Group {
                    if #available(iOS 26.0, *) {
                        // iOS 26+ Glass effect
                        Color.clear
                            .glassEffect(.regular)
                    } else {
                        // Fallback for older iOS versions
                        Color(.systemGray6)
                            .opacity(0.8)
                    }
                }
            )
            .clipShape(Circle())
            .buttonStyle(.plain)
            .contentShape(Circle())
            .padding(.top, 8)
            .padding(.trailing, 16)
            .zIndex(10)
        }
        .overlay(alignment: .topLeading) {
            // Save button in top left
            Button(action: {
                Haptics.impact(.light)
                Task {
                    await saveAllChanges()
                }
            }) {
                if isSaving {
                    ProgressView()
                        .scaleEffect(0.9)
                        .tint(Color(red: 0.4, green: 0.2, blue: 0.6))
                        .frame(height: 44)
                        .padding(.horizontal, 18)
                } else {
                    Text("Save")
                        .font(.system(size: 17, weight: .semibold))
                        .foregroundColor(Color(red: 0.4, green: 0.2, blue: 0.6))
                        .padding(.horizontal, 18)
                        .frame(height: 44)
                }
            }
            .background(
                Group {
                    if #available(iOS 26.0, *) {
                        Color.clear
                            .glassEffect(.regular)
                    } else {
                        Color(.systemGray6)
                            .opacity(0.8)
                    }
                }
            )
            .clipShape(Capsule())
            .buttonStyle(.plain)
            .contentShape(Capsule())
            .padding(.top, 8)
            .padding(.leading, 16)
            .opacity(isSaving && showSaveSuccess == false ? 0.8 : 1.0)
            .disabled(isSaving)
            .zIndex(10)
        }
        .onAppear {
            // Clear any preview states when view appears
            previewEmoji = nil
            previewImageData = nil
            showSaveButton = false
        }
        .onReceive(NotificationCenter.default.publisher(for: .avatarChanged)) { _ in
            // Force refresh of the avatar display by changing the ID
            avatarRefreshKey = UUID()
        }
    }

    // Helper to generate image from emoji
    private func generateEmojiImage(emoji: String) -> Data? {
        let size: CGFloat = 512
        let renderer = UIGraphicsImageRenderer(size: CGSize(width: size, height: size))
        let image = renderer.image { context in
            // Draw gradient background
            let gradient = CGGradient(
                colorsSpace: CGColorSpaceCreateDeviceRGB(),
                colors: [
                    UIColor(red: 0.26, green: 0.58, blue: 1.00, alpha: 1.0).cgColor,
                    UIColor(red: 0.63, green: 0.32, blue: 0.98, alpha: 1.0).cgColor
                ] as CFArray,
                locations: [0.0, 1.0]
            )!
            context.cgContext.drawLinearGradient(
                gradient,
                start: CGPoint(x: 0, y: 0),
                end: CGPoint(x: size, y: size),
                options: []
            )

            // Draw emoji
            let paragraphStyle = NSMutableParagraphStyle()
            paragraphStyle.alignment = .center
            let attributes: [NSAttributedString.Key: Any] = [
                .font: UIFont.systemFont(ofSize: size * 0.6),
                .paragraphStyle: paragraphStyle
            ]
            let emojiSize = (emoji as NSString).size(withAttributes: attributes)
            let rect = CGRect(
                x: (size - emojiSize.width) / 2,
                y: (size - emojiSize.height) / 2,
                width: emojiSize.width,
                height: emojiSize.height
            )
            (emoji as NSString).draw(in: rect, withAttributes: attributes)
        }
        return image.jpegData(compressionQuality: 1.0)
    }

    // Load profile data from ViewModel
    private func loadProfileData() {
        fullName = viewModel.fullName
        personalInfo = viewModel.bio
    }

    // Save avatar only
    private func saveAvatarOnly() async {
        if showSaveButton {
            // Store old avatar URL to clear from cache
            let oldAvatarURL = sessionsVM.myAvatarURL

            // Store the current selection before clearing
            let emojiToSave = previewEmoji
            let imageDataToSave = previewImageData

            await MainActor.run { isSavingAvatar = true }
            // Update frontend immediately with new avatar
            await updateFrontendImmediately(emoji: emojiToSave, imageData: imageDataToSave, oldURL: oldAvatarURL)

            // Show success state immediately - KEEP the preview visible
            await MainActor.run {
                // Don't clear previewEmoji and previewImageData - keep them visible
                showSaveButton = false
                // Do not close the picker; wait until backend refresh confirms
            }

            // Upload to backend in background
            Task {
                if let emoji = emojiToSave {
                    if let emojiImage = generateEmojiImage(emoji: emoji) {
                        await viewModel.uploadAvatar(data: emojiImage)
                        await refreshAvatarAfterUpload(oldURL: oldAvatarURL)
                    }
                } else if let data = imageDataToSave {
                    await viewModel.uploadAvatar(data: data)
                    await refreshAvatarAfterUpload(oldURL: oldAvatarURL)
                }
                await MainActor.run {
                    self.avatarSaved = true
                    self.isSavingAvatar = false
                    // Keep the picker open; auto-clear saved state after a short delay
                    DispatchQueue.main.asyncAfter(deadline: .now() + 2) {
                        self.avatarSaved = false
                    }
                }
            }
        }
    }

    // Update frontend immediately with new avatar
    private func updateFrontendImmediately(emoji: String?, imageData: Data?, oldURL: String?) async {
        // Clear old avatar from cache immediately
        if let oldURL = oldURL {
            await AvatarCacheManager.shared.clearSpecificImage(urlString: oldURL)
        }

        // Generate new avatar URL for immediate display
        var newAvatarURL: String?

        if let emoji = emoji {
            // For emoji, create a temporary URL that we'll use until backend responds
            newAvatarURL = "emoji://\(emoji)"

            // Generate emoji image and cache it immediately
            if let emojiImage = generateEmojiImage(emoji: emoji) {
                await AvatarCacheManager.shared.cacheImageImmediately(urlString: newAvatarURL!, image: UIImage(data: emojiImage)!)
            }
        } else if let imageData = imageData {
            // For uploaded image, create a temporary URL
            newAvatarURL = "temp://uploaded-image-\(UUID().uuidString)"

            // Cache the uploaded image immediately
            if let image = UIImage(data: imageData) {
                await AvatarCacheManager.shared.cacheImageImmediately(urlString: newAvatarURL!, image: image)
            }
        }

        // Update the sessions view model with new avatar URL immediately
        await MainActor.run {
            if let newURL = newAvatarURL {
                sessionsVM.myAvatarURL = newURL
            }

            // Force refresh the sidebar avatar immediately
            sessionsVM.objectWillChange.send()

            // Send notification to update all UI components immediately
            NotificationCenter.default.post(name: .avatarChanged, object: nil)
        }
    }

    // Helper to refresh avatar after upload
    private func refreshAvatarAfterUpload(oldURL: String?) async {
        // Clear old avatar from cache
        if let oldURL = oldURL {
            await AvatarCacheManager.shared.clearSpecificImage(urlString: oldURL)
        }

        // Reload avatar URLs from backend
        await sessionsVM.loadPairedAvatars()

        // Force refresh all avatar displays across the entire app
        await AvatarCacheManager.shared.forceRefreshAllAvatars()

        // Preload new avatar immediately and ensure it's cached
        await sessionsVM.preloadAvatars()
        await sessionsVM.ensureProfilePictureCached()

        // Force UI refresh by updating the sessionsVM state
        await MainActor.run {
            // Trigger a state change to force UI refresh
            sessionsVM.objectWillChange.send()

            // Send notification to all components that display avatars
            NotificationCenter.default.post(name: .avatarChanged, object: nil)

            // Now clear the preview since the official avatar is saved and loaded
            previewEmoji = nil
            previewImageData = nil
        }
    }

    // Save all changes (profile info + avatar)
    private func saveAllChanges() async {
        await MainActor.run {
            isSaving = true
        }

        // Save profile information
        let profileSaved = await viewModel.saveProfileInfo(
            fullName: fullName,
            bio: personalInfo
        )

        // Save avatar if there are pending changes
        if showSaveButton {
            // Store old avatar URL to clear from cache
            let oldAvatarURL = sessionsVM.myAvatarURL

            // Update frontend immediately with new avatar
            await updateFrontendImmediately(emoji: previewEmoji, imageData: previewImageData, oldURL: oldAvatarURL)

            // Clear avatar selection state and show success - KEEP the preview visible
            await MainActor.run {
                // Don't clear previewEmoji and previewImageData - keep them visible
                showSaveButton = false
                showingAvatarSelection = false
                avatarSaved = true

                // Reset avatarSaved after 2 seconds
                DispatchQueue.main.asyncAfter(deadline: .now() + 2) {
                    avatarSaved = false
                }
            }

            // Upload to backend in background
            Task {
                if let emoji = previewEmoji {
                    if let emojiImage = generateEmojiImage(emoji: emoji) {
                        await viewModel.uploadAvatar(data: emojiImage)
                        await refreshAvatarAfterUpload(oldURL: oldAvatarURL)
                    }
                } else if let data = previewImageData {
                    await viewModel.uploadAvatar(data: data)
                    await refreshAvatarAfterUpload(oldURL: oldAvatarURL)
                }
            }
        }

        await MainActor.run {
            isSaving = false
            if profileSaved {
                showSaveSuccess = true
                viewModel.fullName = fullName
                viewModel.bio = personalInfo
                NotificationCenter.default.post(name: .profileChanged, object: nil)
                // Auto-hide success message after 2 seconds
                DispatchQueue.main.asyncAfter(deadline: .now() + 2) {
                    showSaveSuccess = false
                }
            }
        }
    }
}

#Preview {
    @Previewable @State var isPresented = true
    @Previewable @Namespace var namespace

    PersonalizationEditView(
        isPresented: $isPresented,
        profileNamespace: namespace
    )
    .environmentObject(ChatSessionsViewModel())
}
