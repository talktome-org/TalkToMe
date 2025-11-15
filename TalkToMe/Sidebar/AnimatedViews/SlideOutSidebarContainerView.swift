import SwiftUI
import PhotosUI
import UIKit

struct SlideOutSidebarContainerView<Content: View>: View {

    @EnvironmentObject private var navigationViewModel: SidebarNavigationViewModel
    @EnvironmentObject private var sessionsViewModel: ChatSessionsViewModel

    @Namespace private var profileNamespace

    @Environment(\.scenePhase) private var scenePhase

    @EnvironmentObject private var linkVM: LinkViewModel

    let content: Content

    init(@ViewBuilder content: () -> Content) {
        self.content = content()
    }

    private var linkedMonthYear: String? {
        switch linkVM.state {
        case .linked:
            if let date = linkVM.linkedAt {
                let formatter = DateFormatter()
                formatter.dateFormat = "MMMM yyyy"
                return formatter.string(from: date)
            }
            return nil
        default:
            return nil
        }
    }

    var body: some View {
        GeometryReader { proxy in
            let width: CGFloat = proxy.size.width
            let blurIntensity: CGFloat = {
                let maxBlur: CGFloat = 6
                let w = max(width, 1)
                if navigationViewModel.isOpen {
                    // Fully open => 1, closing (dragging left) reduces toward 0 as content returns
                    let progress = 1 - min(1, max(0, abs(navigationViewModel.dragOffset) / w))
                    return maxBlur * progress
                } else {
                    // Closed => 0, opening (dragging right) increases toward 1 as content departs
                    let progress = min(1, max(0, navigationViewModel.dragOffset / w))
                    return maxBlur * progress
                }
            }()

            ZStack {
                // Main Content - slides completely off screen when sidebar is open
                content
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
                    .offset(x: navigationViewModel.isOpen ? width + navigationViewModel.dragOffset : max(0, navigationViewModel.dragOffset))
                    .blur(radius: blurIntensity)
                    // Bouncier response when toggling open/close, and lively interactive spring while dragging
                    .animation(.spring(response: 0.34, dampingFraction: 0.72, blendDuration: 0), value: navigationViewModel.isOpen)
                    .animation(.interactiveSpring(response: 0.26, dampingFraction: 0.78, blendDuration: 0), value: navigationViewModel.dragOffset)

                // Slide-out Sidebar - slides in from left to fully replace main content
                // Compute sidebar offset once so we can position the edge blur exactly at the visible edge
                let sidebarOffsetX: CGFloat = navigationViewModel.isOpen ? navigationViewModel.dragOffset : (-width + max(0, navigationViewModel.dragOffset))

                SlidebarView(
                    isOpen: $navigationViewModel.isOpen,
                    profileNamespace: profileNamespace
                )
                .offset(x: sidebarOffsetX)
                .animation(.spring(response: 0.34, dampingFraction: 0.72, blendDuration: 0), value: navigationViewModel.isOpen)
                .animation(.interactiveSpring(response: 0.26, dampingFraction: 0.78, blendDuration: 0), value: navigationViewModel.dragOffset)
            }
            .sheet(isPresented: $navigationViewModel.showSettingsSheet) {
                SettingsView(
                    profileNamespace: profileNamespace,
                    isPresented: $navigationViewModel.showSettingsSheet
                )
                .environmentObject(sessionsViewModel)
                .environmentObject(linkVM)
                .presentationDetents([.large])
                .presentationDragIndicator(.visible)
            }
            .contentShape(Rectangle())
            .gesture(
                DragGesture(minimumDistance: 5)
                    .onChanged { value in
                        let dx = value.translation.width
                        let dy = value.translation.height

                        guard abs(dx) > abs(dy) else { return }
                        let clamped = max(min(dx, width), -width)
                        navigationViewModel.handleDragGesture(clamped, width: width)
                    }
                    .onEnded { value in
                        let dx = value.translation.width
                        let dy = value.translation.height
                        guard abs(dx) > abs(dy) else {
                            withAnimation(.spring(response: 0.34, dampingFraction: 0.76, blendDuration: 0)) {
                                navigationViewModel.dragOffset = 0
                            }
                            return
                        }
                        navigationViewModel.handleSwipeGesture(dx, velocity: value.velocity.width, width: width)
                    }
            )
        }
        .onAppear {
            sessionsViewModel.setNavigationViewModel(navigationViewModel)
            sessionsViewModel.setLinkViewModel(linkVM)
            sessionsViewModel.startObserving()
            navigationViewModel.dragOffset = 0
        }
        .onChange(of: scenePhase, initial: false) { _, newPhase in
            switch newPhase {
            case .inactive, .background:
                withAnimation(nil) { navigationViewModel.dragOffset = 0 }
            case .active:
                withAnimation(nil) { navigationViewModel.dragOffset = 0 }
                Task { await sessionsViewModel.refreshSessions() }
            @unknown default:
                withAnimation(nil) { navigationViewModel.dragOffset = 0 }
            }
        }
        // If the sidebar is open, ensure the chat input isn't focused to prevent the keyboard from appearing underneath
        .onChange(of: navigationViewModel.isOpen, initial: false) { _, open in
            if open {
                // Broadcast a notification to clear any chat input focus if needed
                NotificationCenter.default.post(name: .init("TalToMe_ClearChatInputFocus"), object: nil)
            }
        }
    }
}

#Preview {
    SlideOutSidebarContainerView {
        Text("Main Content")
            .frame(maxWidth: .infinity, maxHeight: .infinity)
            .background(Color.blue.opacity(0.1))
    }
    .environmentObject(LinkViewModel(accessTokenProvider: {
        return "mock-access-token"
    }))
}