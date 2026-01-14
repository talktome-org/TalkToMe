import SwiftUI
import UIKit

enum SidebarTab: String, CaseIterable, Identifiable {
    case chat = "Chat"

    var id: String { self.rawValue }
}

class SidebarNavigationViewModel: ObservableObject {

    // Telegram-like default: land on the conversations list (sidebar) first.
    @Published var isOpen = true
    @Published var selectedTab: SidebarTab = .chat
    @Published var dragOffset: CGFloat = 0
    @Published var showSettingsSheet: Bool = false
    @Published var isNotificationsExpanded: Bool = false
    @Published var isChatsExpanded: Bool = false

    func openSidebar() {
        Haptics.impact(.light)
        withAnimation(.spring(response: 0.46, dampingFraction: 0.7, blendDuration: 0)) {
            isOpen = true
            dragOffset = 0
        }
    }

    func closeSidebar() {
        Haptics.impact(.light)
        withAnimation(.spring(response: 0.46, dampingFraction: 0.7, blendDuration: 0)) {
            isOpen = false
            dragOffset = 0
        }
    }

    func toggleSidebar() {
        withAnimation(.spring(response: 0.46, dampingFraction: 0.7, blendDuration: 0)) {
            isOpen.toggle()
            dragOffset = 0
        }
    }

    func selectTab(_ tab: SidebarTab) {
        withAnimation(.spring(response: 0.46, dampingFraction: 0.7, blendDuration: 0)) {
            selectedTab = tab
            isOpen = false
            dragOffset = 0
        }
    }

    func handleDragGesture(_ translation: CGFloat, width: CGFloat) {
        // Allow interactive dragging from both states.
        // Positive translation (dragging right) begins opening; negative begins closing.
        let clamped = max(-width, min(width, translation))
        if isOpen {
            // When open, offset moves left from 0 towards -width
            dragOffset = max(-width, min(0, clamped))
        } else {
            // When closed, offset moves right from 0 towards +width to preview opening
            dragOffset = max(0, min(width, clamped))
        }
    }

    func handleSwipeGesture(_ translation: CGFloat, velocity: CGFloat, width: CGFloat) {
        // Lower thresholds to improve responsiveness
        let threshold: CGFloat = width * 0.18
        let velocityThreshold: CGFloat = 350

        if isOpen {
            if translation < -threshold || velocity < -velocityThreshold {
                closeSidebar()
            } else {
                withAnimation(.spring(response: 0.36, dampingFraction: 0.74, blendDuration: 0)) {
                    dragOffset = 0
                }
            }
        } else {
            // Decide based on direction and distance
            if translation > threshold || velocity > velocityThreshold {
                openSidebar()
            } else {
                withAnimation(.spring(response: 0.36, dampingFraction: 0.74, blendDuration: 0)) {
                    dragOffset = 0
                }
            }
        }
    }
}
