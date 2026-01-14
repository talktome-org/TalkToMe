import SwiftUI

@MainActor
final class SidebarContainerViewModel: ObservableObject {
    private let edgeActivationWidth: CGFloat = 26

    func blurIntensity(width: CGFloat, isOpen: Bool, dragOffset: CGFloat) -> CGFloat {
        let maxBlur: CGFloat = 6
        let w = max(width, 1)

        if isOpen {
            let progress = 1 - min(1, max(0, abs(dragOffset) / w))
            return maxBlur * progress
        } else {
            let progress = min(1, max(0, dragOffset / w))
            return maxBlur * progress
        }
    }

    func sidebarOffsetX(width: CGFloat, isOpen: Bool, dragOffset: CGFloat) -> CGFloat {
        isOpen ? dragOffset : (-width + max(0, dragOffset))
    }

    func handleDragChanged(
        dx: CGFloat,
        dy: CGFloat,
        startX: CGFloat,
        width: CGFloat,
        navigationViewModel: SidebarNavigationViewModel
    ) {
        guard abs(dx) > abs(dy) else { return }
        // When closed, only allow opening from the screen edge so horizontal scroll views (e.g. photo strip)
        // don't accidentally trigger sidebar navigation.
        if !navigationViewModel.isOpen, startX > edgeActivationWidth { return }
        let clamped = max(min(dx, width), -width)
        navigationViewModel.handleDragGesture(clamped, width: width)
    }

    func handleDragEnded(
        dx: CGFloat,
        dy: CGFloat,
        velocityX: CGFloat,
        startX: CGFloat,
        width: CGFloat,
        navigationViewModel: SidebarNavigationViewModel
    ) {
        guard abs(dx) > abs(dy) else {
            withAnimation(.spring(response: 0.34, dampingFraction: 0.76, blendDuration: 0)) {
                navigationViewModel.dragOffset = 0
            }
            return
        }
        if !navigationViewModel.isOpen, startX > edgeActivationWidth {
            withAnimation(.spring(response: 0.34, dampingFraction: 0.76, blendDuration: 0)) {
                navigationViewModel.dragOffset = 0
            }
            return
        }
        navigationViewModel.handleSwipeGesture(dx, velocity: velocityX, width: width)
    }

    func onAppear(
        navigationViewModel: SidebarNavigationViewModel,
        sessionsViewModel: ChatSessionsViewModel,
        linkViewModel: LinkViewModel
    ) {
        sessionsViewModel.setNavigationViewModel(navigationViewModel)
        sessionsViewModel.setLinkViewModel(linkViewModel)
        navigationViewModel.dragOffset = 0
    }

    func onScenePhaseChange(
        _ newPhase: ScenePhase,
        navigationViewModel: SidebarNavigationViewModel,
        sessionsViewModel: ChatSessionsViewModel
    ) {
        withAnimation(nil) { navigationViewModel.dragOffset = 0 }
        if newPhase == .active {
            Task { await sessionsViewModel.refreshSessions() }
        }
    }
}