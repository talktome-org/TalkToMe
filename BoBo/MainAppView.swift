//
//  MainAppView.swift
//  BoBo
//
//  Created by Stephan  on 29.08.2025.
//

import SwiftUI

struct MainAppView: View {
    let onOpenChat: (UUID) -> Void
    let onStartNewChat: () -> Void
    var hideHeader: Bool = false

    var body: some View {
        SidebarView(
            onOpenChat: onOpenChat,
            onStartNewChat: onStartNewChat,
            hideHeader: hideHeader
        )
    }
}

#Preview {
    MainAppView(onOpenChat: { _ in }, onStartNewChat: { })
        .environmentObject(SidebarNavigationViewModel())
        .environmentObject(ChatSessionsViewModel())
        .environmentObject(FriendsViewModel(accessTokenProvider: { "" }))
}
