import SwiftUI

struct SidebarPartnerInviteOverlayView: View {
    let isVisible: Bool

    var body: some View {
        if isVisible {
            PartnerInviteBannerView()
                .padding(.horizontal, 20)
                .padding(.bottom, 20)
                .transition(.opacity.combined(with: .scale(scale: 0.95)))
                .animation(.spring(response: 0.3, dampingFraction: 0.8), value: isVisible)
                .ignoresSafeArea(.keyboard, edges: .bottom)
                .ignoresSafeArea(.container, edges: .bottom)
                .zIndex(10)
        }
    }
}


