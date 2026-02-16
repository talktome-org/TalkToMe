//
//  MainTabView.swift
//  TalkToMe
//
//  Created by Stephan on 02.02.2026.
//

import SwiftUI

enum AppTab: Hashable {
    case home
    case diary
    case chat
    case settings
    case search
}

struct MainTabView: View {
    @EnvironmentObject private var sessionsViewModel: ChatSessionsViewModel
    @EnvironmentObject private var navigationViewModel: SidebarNavigationViewModel

    @State private var selectedTab: AppTab = .chat
    @State private var showChat: Bool = false
    @State private var chatDragOffset: CGFloat = 0
    @State private var searchString: String = ""

    private let dismissThreshold: CGFloat = 100

    private func openChat(sessionId: UUID?) {
        if let sid = sessionId {
            sessionsViewModel.openSession(sid)
        } else {
            sessionsViewModel.startNewChat()
        }
        // Start off-screen to the right
        chatDragOffset = UIScreen.main.bounds.width
        showChat = true
        // Slide in from the right
        withAnimation(.easeOut(duration: 0.20)) {
            chatDragOffset = 0
        }
    }

    private func dismissChat() {
        UIApplication.shared.sendAction(#selector(UIResponder.resignFirstResponder), to: nil, from: nil, for: nil)
        withAnimation(.easeOut(duration: 0.25)) {
            chatDragOffset = UIScreen.main.bounds.width
        }
        DispatchQueue.main.asyncAfter(deadline: .now() + 0.25) {
            showChat = false
            chatDragOffset = 0
        }
    }

    var body: some View {
        ZStack {
            TabView(selection: $selectedTab) {
                Tab("Home", systemImage: "house", value: .home) {
                    HomeView()
                }

                Tab("Diary", systemImage: "book", value: .diary) {
                    DiaryView()
                }

                Tab("Chat", systemImage: "bubble.left.and.bubble.right", value: .chat) {
                    MainAppView(
                        onOpenChat: { sid in openChat(sessionId: sid) },
                        onStartNewChat: { openChat(sessionId: nil) }
                    )
                }

                Tab("Settings", systemImage: "gearshape", value: .settings) {
                    SettingsTabWrapper()
                }

                Tab(value: .search, role: .search) {
                    NavigationStack {
                        MainAppView(
                            onOpenChat: { sid in openChat(sessionId: sid) },
                            onStartNewChat: { openChat(sessionId: nil) },
                            searchText: searchString
                        )
                        .searchable(text: $searchString)
                        .navigationBarHidden(true)
                    }
                }
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
                .clipShape(RoundedRectangle(cornerRadius: 44, style: .continuous))
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
                                        // Dismiss keyboard
                                        UIApplication.shared.sendAction(#selector(UIResponder.resignFirstResponder), to: nil, from: nil, for: nil)
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
        .onReceive(NotificationCenter.default.publisher(for: .openChatSession)) { note in
            if let sid = note.userInfo?["sessionId"] as? UUID {
                selectedTab = .chat
                openChat(sessionId: sid)
            }
        }
    }
}

/// Wraps SettingsView so it can live inside a tab instead of a sheet.
private struct SettingsTabWrapper: View {
    @Namespace private var profileNamespace
    @State private var isPresented: Bool = true

    var body: some View {
        SettingsView(
            profileNamespace: profileNamespace,
            isPresented: $isPresented
        )
        .onChange(of: isPresented) { _, newValue in
            // In a tab, we can't dismiss — keep it presented.
            if !newValue { isPresented = true }
        }
    }
}

#Preview {
    MainTabView()
        .environmentObject(SidebarNavigationViewModel())
        .environmentObject(ChatSessionsViewModel())
        .environmentObject(FriendsViewModel(accessTokenProvider: { "" }))
        .environmentObject(SettingsViewModel())
}
