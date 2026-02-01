//
//  MainAppView.swift
//  TalkToMe
//
//  Created by Stephan  on 29.08.2025.
//

import SwiftUI

struct MainAppView: View {
    @EnvironmentObject private var sessionsViewModel: ChatSessionsViewModel

    private enum Route: Hashable {
        case chat
    }

    @State private var path: [Route] = []

    private func navigateToChat(sessionId: UUID?) {
        var tx = Transaction(animation: nil)
        tx.disablesAnimations = true
        withTransaction(tx) {
            if let sid = sessionId {
                sessionsViewModel.openSession(sid)
            } else {
                sessionsViewModel.startNewChat()
            }
            path = [.chat]
        }
    }

    var body: some View {
        NavigationStack(path: $path) {
            SidebarView(
                onOpenChat: { sid in
                    navigateToChat(sessionId: sid)
                },
                onStartNewChat: {
                    navigateToChat(sessionId: nil)
                }
            )
            .navigationDestination(for: Route.self) { route in
                switch route {
                case .chat:
                    let viewId: String = {
                        if let sid = sessionsViewModel.activeSessionId { return "session_\(sid.uuidString)" }
                        return "new_\(sessionsViewModel.chatViewKey.uuidString)"
                    }()
                    ChatView(sessionId: sessionsViewModel.activeSessionId)
                        .id(viewId)
                }
            }
        }
    }
}

#Preview {
    MainAppView()
        .environmentObject(SidebarNavigationViewModel())
        .environmentObject(ChatSessionsViewModel())
        .environmentObject(FriendsViewModel(accessTokenProvider: { "" }))
}
