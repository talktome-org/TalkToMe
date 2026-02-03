//
//  MainTabView.swift
//  TalkToMe
//
//  Created by Stephan on 02.02.2026.
//

import SwiftUI

struct MainTabView: View {
    @EnvironmentObject private var sessionsViewModel: ChatSessionsViewModel

    @State private var selectedTab: Int = 2
    @State private var showChat: Bool = false

    private func openChat(sessionId: UUID?) {
        if let sid = sessionId {
            sessionsViewModel.openSession(sid)
        } else {
            sessionsViewModel.startNewChat()
        }
        withAnimation(.easeInOut(duration: 0.3)) {
            showChat = true
        }
    }

    var body: some View {
        ZStack {
            TabView(selection: $selectedTab) {
                HomeView()
                    .tabItem {
                        Label("Home", systemImage: "house")
                    }
                    .tag(0)

                DiaryView()
                    .tabItem {
                        Label("Diary", systemImage: "book")
                    }
                    .tag(1)

                MainAppView(
                    onOpenChat: { sid in openChat(sessionId: sid) },
                    onStartNewChat: { openChat(sessionId: nil) }
                )
                    .tabItem {
                        Label("Chat", systemImage: "bubble.left.and.bubble.right")
                    }
                    .tag(2)
            }

            if showChat {
                let viewId: String = {
                    if let sid = sessionsViewModel.activeSessionId { return "session_\(sid.uuidString)" }
                    return "new_\(sessionsViewModel.chatViewKey.uuidString)"
                }()
                ChatView(sessionId: sessionsViewModel.activeSessionId, onBack: {
                    showChat = false
                })
                .id(viewId)
                .frame(maxWidth: .infinity, maxHeight: .infinity)
                .background(Color(.systemBackground))
                .ignoresSafeArea()
                .zIndex(1)
            }
        }
    }
}

#Preview {
    MainTabView()
        .environmentObject(SidebarNavigationViewModel())
        .environmentObject(ChatSessionsViewModel())
        .environmentObject(FriendsViewModel(accessTokenProvider: { "" }))
}
