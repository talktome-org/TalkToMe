import SwiftUI

struct ChatView: View {

    @EnvironmentObject private var navigationViewModel: SidebarNavigationViewModel
    @EnvironmentObject private var sessionsViewModel: ChatSessionsViewModel

    @StateObject private var viewModel: ChatViewModel

    @State private var showNotLinkedAlert: Bool = false
    @State private var showPartnerAddedBanner: Bool = false
    @State private var partnerAddedName: String = ""

    @FocusState private var isInputFocused: Bool

    private var isPartnerLinked: Bool {
        sessionsViewModel.partnerInfo?.linked == true ||
        UserDefaults.standard.bool(forKey: PreferenceKeys.partnerConnected) == true
    }

    init(sessionId: UUID? = nil) {
        _viewModel = StateObject(wrappedValue: ChatViewModel(sessionId: sessionId))
    }

    var body: some View {
        NavigationStack {
            ChatScreenView(
                chatViewModel: viewModel,
                isInputFocused: $isInputFocused
            )
            .overlay(alignment: .top) { partnerAddedBannerOverlay }
            .toolbar {
                ToolbarItem(placement: .topBarLeading) {
                    Button(action: {
                        Haptics.impact(.medium)
                        UIApplication.shared.sendAction(#selector(UIResponder.resignFirstResponder), to: nil, from: nil, for: nil)
                        navigationViewModel.openSidebar()
                    }) {
                        ZStack(alignment: .topTrailing) {
                            Image(systemName: "line.3.horizontal")
                                .font(.system(size: 20, weight: .medium))
                                .foregroundColor(Color(red: 0.4, green: 0.2, blue: 0.6))
                                .frame(width: 44, height: 44)

                            let unreadCount = sessionsViewModel.unreadPartnerSessionIds.count + sessionsViewModel.pendingRequests.count
                            if unreadCount > 0 {
                                ZStack {
                                    Circle()
                                        .fill(Color(red: 0.4, green: 0.2, blue: 0.6))
                                    Text("\(min(unreadCount, 99))")
                                        .font(.system(size: 10, weight: .bold))
                                        .foregroundColor(.white)
                                        .minimumScaleFactor(0.7)
                                        .lineLimit(1)
                                }
                                .frame(width: 16, height: 16)
                                .offset(x: -5, y: 5)
                                .transition(.scale.combined(with: .opacity))
                            }
                        }
                    }
                }
            }
            .navigationTitle("")
            .navigationBarTitleDisplayMode(.inline)
        }
        .onAppear {
            Task { await sessionsViewModel.loadPendingRequests() }
            sessionsViewModel.chatViewModel = viewModel

            if navigationViewModel.isOpen {
                isInputFocused = false
            } else {
                DispatchQueue.main.asyncAfter(deadline: .now() + 0.15) {
                    if sessionsViewModel.activeSessionId == nil && !navigationViewModel.isOpen {
                        isInputFocused = true
                    }
                }
            }
        }
        .onChange(of: navigationViewModel.isOpen, initial: false) { _, newValue in
            if newValue { isInputFocused = false }
        }
        .onReceive(NotificationCenter.default.publisher(for: .sendPartnerMessageFromBubble)) { note in
            if let text = note.userInfo?["content"] as? String {
                let trimmed = text.trimmingCharacters(in: .whitespacesAndNewlines)
                guard !trimmed.isEmpty else { return }

                if isPartnerLinked {
                    Task {
                        await viewModel.sendToPartner(sessionsViewModel: sessionsViewModel, customMessage: trimmed)
                        viewModel.partnerDrafts.markPartnerDraftAsSent(sessionId: viewModel.sessionId, messageContent: trimmed)
                    }
                } else {
                    Haptics.notification(.error)
                    showNotLinkedAlert = true
                }
            }
        }
        .onReceive(NotificationCenter.default.publisher(for: .partnerLinkOpened)) { _ in
            partnerAddedName = PreferenceKeys.getPartnerDisplayName()
            withAnimation(.spring(response: 0.3, dampingFraction: 0.85)) {
                showPartnerAddedBanner = true
            }
            DispatchQueue.main.asyncAfter(deadline: .now() + 3) {
                if showPartnerAddedBanner {
                    withAnimation(.easeInOut(duration: 0.35)) { showPartnerAddedBanner = false }
                }
            }
        }
        .onChange(of: sessionsViewModel.partnerInfo?.partner?.name ?? "", initial: false) { _, _ in
            if showPartnerAddedBanner && partnerAddedName.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                partnerAddedName = PreferenceKeys.getPartnerDisplayName()
            }
        }
        .onChange(of: sessionsViewModel.chatViewKey, initial: false) { _, _ in
            if sessionsViewModel.activeSessionId == nil {
                viewModel.sessionId = nil
                Task { await viewModel.loadHistory() }
            } else {
                if let sessionId = sessionsViewModel.activeSessionId {
                    Task { await viewModel.presentSession(sessionId) }
                }
            }
        }
        .animation(nil, value: viewModel.messages.isEmpty)
        .alert("Not connected", isPresented: $showNotLinkedAlert) {
            Button("OK", role: .cancel) { }
        } message: {
            Text("Your account is not connected to a partner.")
        }
    }

    @ViewBuilder
    private var partnerAddedBannerOverlay: some View {
        if showPartnerAddedBanner {
            HStack(alignment: .center, spacing: 12) {
                Image(systemName: "person.2.fill")
                    .font(.system(size: 16, weight: .semibold))
                    .foregroundColor(.primary)
                    .frame(width: 34, height: 34)
                    .background {
                        Circle().fill(.ultraThinMaterial).overlay(Circle().stroke(Color.primary.opacity(0.12), lineWidth: 1))
                    }
                    .shadow(color: Color.black.opacity(0.06), radius: 6, x: 0, y: 3)

                VStack(alignment: .leading, spacing: 2) {
                    Text("You’ve been added as a partner to " + (partnerAddedName.isEmpty ? "your partner" : partnerAddedName))
                        .font(.system(size: 16, weight: .medium))
                        .foregroundColor(.primary)
                }
                Spacer(minLength: 8)
                Button(action: {
                    withAnimation(.easeInOut(duration: 0.35)) { showPartnerAddedBanner = false }
                }) {
                    Image(systemName: "xmark")
                        .font(.system(size: 14, weight: .semibold))
                        .foregroundColor(.primary)
                        .frame(width: 30, height: 30)
                        .background {
                            Circle().fill(.ultraThinMaterial).overlay(Circle().stroke(Color.primary.opacity(0.10), lineWidth: 1))
                        }
                }
                .buttonStyle(.plain)
            }
            .padding(14)
            .background(
                RoundedRectangle(cornerRadius: 18, style: .continuous)
                    .fill(.ultraThinMaterial)
                    .overlay(
                        RoundedRectangle(cornerRadius: 18, style: .continuous)
                            .stroke(Color.primary.opacity(0.08), lineWidth: 1)
                    )
                    .shadow(color: Color.black.opacity(0.08), radius: 12, x: 0, y: 6)
            )
            .padding(.horizontal, 20)
            .padding(.top, 10)
            .transition(
                .asymmetric(
                    insertion: .scale(scale: 0.95, anchor: .top).combined(with: .opacity),
                    removal: .scale(scale: 0.98, anchor: .top).combined(with: .opacity)
                )
            )
            .animation(.easeInOut(duration: 0.35), value: showPartnerAddedBanner)
            .zIndex(20)
        }
    }
}

#Preview {
    ChatView()
        .environmentObject(SidebarNavigationViewModel())
        .environmentObject(ChatSessionsViewModel())
}

