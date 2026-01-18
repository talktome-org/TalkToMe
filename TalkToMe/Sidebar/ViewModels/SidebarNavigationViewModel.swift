import SwiftUI
import UIKit

enum SidebarTab: String, CaseIterable, Identifiable {
    case chat = "Chat"

    var id: String { self.rawValue }
}

class SidebarNavigationViewModel: ObservableObject {
    @Published var isOpen = true
    @Published var selectedTab: SidebarTab = .chat
    @Published var showSettingsSheet: Bool = false
    @Published var isNotificationsExpanded: Bool = false
    @Published var isChatsExpanded: Bool = false

    func openSidebar() {
        Haptics.impact(.light)
        withAnimation(.spring(response: 0.46, dampingFraction: 0.7, blendDuration: 0)) {
            isOpen = true
        }
    }

    func closeSidebar() {
        Haptics.impact(.light)
        withAnimation(.spring(response: 0.46, dampingFraction: 0.7, blendDuration: 0)) {
            isOpen = false
        }
    }

    func toggleSidebar() {
        withAnimation(.spring(response: 0.46, dampingFraction: 0.7, blendDuration: 0)) {
            isOpen.toggle()
        }
    }

    func selectTab(_ tab: SidebarTab) {
        withAnimation(.spring(response: 0.46, dampingFraction: 0.7, blendDuration: 0)) {
            selectedTab = tab
            isOpen = false
        }
    }
}
