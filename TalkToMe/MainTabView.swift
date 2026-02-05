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
    @State private var chatDragOffset: CGFloat = 0

    private let dismissThreshold: CGFloat = 100

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

    private func dismissChat() {
        withAnimation(.easeOut(duration: 0.25)) {
            showChat = false
        }
        // Reset offset after animation completes
        DispatchQueue.main.asyncAfter(deadline: .now() + 0.25) {
            chatDragOffset = 0
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
                    dismissChat()
                })
                .id(viewId)
                .frame(maxWidth: .infinity, maxHeight: .infinity)
                .background(Color(.systemBackground))
                .ignoresSafeArea()
                .offset(x: chatDragOffset)
                .overlay(alignment: .leading) {
                    // Edge-swipe to dismiss so inner horizontal drags remain usable.
                    Color.clear
                        .frame(width: 24)
                        .contentShape(Rectangle())
                        .gesture(
                            DragGesture()
                                .onChanged { value in
                                    // Only allow dragging to the right (positive x translation)
                                    if value.translation.width > 0 {
                                        chatDragOffset = value.translation.width
                                    }
                                }
                                .onEnded { value in
                                    if value.translation.width > dismissThreshold || value.predictedEndTranslation.width > dismissThreshold * 2 {
                                        // Dismiss with a slide-out animation
                                        withAnimation(.easeOut(duration: 0.2)) {
                                            chatDragOffset = UIScreen.main.bounds.width
                                        }
                                        DispatchQueue.main.asyncAfter(deadline: .now() + 0.2) {
                                            showChat = false
                                            chatDragOffset = 0
                                        }
                                    } else {
                                        // Snap back
                                        withAnimation(.spring(response: 0.3, dampingFraction: 0.7)) {
                                            chatDragOffset = 0
                                        }
                                    }
                                }
                        )
                }
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
