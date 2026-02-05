//
//  MainAppView.swift
//  TalkToMe
//
//  Created by Stephan  on 29.08.2025.
//

import SwiftUI

struct MainAppView: View {
    let onOpenChat: (UUID) -> Void
    let onStartNewChat: () -> Void
    var searchText: String = ""
    var hideHeader: Bool = false

    var body: some View {
        SidebarView(
            onOpenChat: onOpenChat,
            onStartNewChat: onStartNewChat,
            searchText: searchText,
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
