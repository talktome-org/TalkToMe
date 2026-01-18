//
//  MainAppView.swift
//  TalkToMe
//
//  Created by Stephan  on 29.08.2025.
//

import SwiftUI

struct MainAppView: View {
    @EnvironmentObject private var sessionsViewModel: ChatSessionsViewModel
    @EnvironmentObject private var navigationViewModel: SidebarNavigationViewModel

    var body: some View {
        // If we have any conversations but none is selected, don't show the chat screen at all.
        // This makes "landing in main chat view" impossible on launch — the user starts in the sidebar list.
        if sessionsViewModel.activeSessionId == nil,
           !sessionsViewModel.sessions.isEmpty,
           navigationViewModel.isOpen {
            ZStack {
                Color(.systemBackground).ignoresSafeArea()
                VStack(spacing: 10) {
                    Image(systemName: "bubble.left.and.bubble.right")
                        .font(.system(size: 26, weight: .semibold))
                        .foregroundStyle(.secondary)
                    Text("Select a conversation")
                        .font(.system(size: 16, weight: .semibold))
                        .foregroundStyle(.secondary)
                }
                .padding(.top, 40)
            }
            .onAppear {
                withAnimation(nil) {
                    navigationViewModel.isOpen = true
                }
            }
        } else {
            let viewId: String = {
                if let sid = sessionsViewModel.activeSessionId { return "session_\(sid.uuidString)" }
                return "new_\(sessionsViewModel.chatViewKey.uuidString)"
            }()
            ChatView(sessionId: sessionsViewModel.activeSessionId)
                .id(viewId)
        }
    }
}

#Preview {
    MainAppView()
        .environmentObject(SidebarNavigationViewModel())
        .environmentObject(ChatSessionsViewModel())
}
