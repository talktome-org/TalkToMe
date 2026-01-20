//
//  ContentView.swift
//  TalkToMe
//
//  Created by Stephan  on 29.08.2025.
//

import SwiftUI

struct ContentView: View {
    @ObservedObject private var authService = AuthService.shared
    @EnvironmentObject private var sessionsViewModel: ChatSessionsViewModel
    @EnvironmentObject private var navigationViewModel: SidebarNavigationViewModel
    @EnvironmentObject private var friendsVM: FriendsViewModel

    @Namespace private var profileNamespace
    @Environment(\.scenePhase) private var scenePhase

    var body: some View {
        ZStack {
            Group {
                if authService.isCheckingAuth {
                    // Telegram-like: show cached UI immediately; otherwise a minimal spinner.
                    // Only show cached UI if we have an effective user id (avoids flashing stale state across account switches).
                    if authService.currentUserId?.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty == false,
                       !sessionsViewModel.sessions.isEmpty {
                        sidebarContainer(content: MainAppView())
                            .transition(.opacity)
                    } else {
                        // No spinner before cached UI: just show the app background.
                        Color(.systemBackground)
                            .ignoresSafeArea()
                        .transition(.opacity)
                    }
                } else if authService.isAuthenticated {
                    sidebarContainer(content: MainAppView())
                        .transition(.opacity)
                } else {
                    AuthView()
                        .transition(.opacity)
                }
            }
        }
        .animation(.easeInOut(duration: 0.3), value: authService.isAuthenticated)
        .animation(.easeInOut(duration: 0.3), value: authService.isCheckingAuth)
    }

    @ViewBuilder
    private func sidebarContainer<Content: View>(content: Content) -> some View {
        GeometryReader { proxy in
            // If the sidebar is open and nothing is selected, never show ChatView "behind" it.
            // This prevents the brief ChatView flash during login / account switches while sessions are still loading.
            let showSidebarOnly: Bool = navigationViewModel.isOpen
                && sessionsViewModel.activeSessionId == nil

            ZStack(alignment: .leading) {
                if showSidebarOnly {
                    Color(.systemBackground)
                        .ignoresSafeArea()
                } else {
                    content
                        .frame(maxWidth: .infinity, maxHeight: .infinity)
                        .zIndex(0)
                }

                SidebarView(
                    isOpen: $navigationViewModel.isOpen,
                    profileNamespace: profileNamespace
                )
                .frame(width: proxy.size.width)
                .offset(x: navigationViewModel.isOpen ? 0 : -proxy.size.width)
                .zIndex(2)
            }
            .sheet(isPresented: $navigationViewModel.showSettingsSheet) {
                SettingsView(
                    profileNamespace: profileNamespace,
                    isPresented: $navigationViewModel.showSettingsSheet
                )
                .environmentObject(sessionsViewModel)
                .environmentObject(friendsVM)
                .presentationDetents([.large])
                .presentationDragIndicator(.visible)
            }
        }
        .onAppear {
            sessionsViewModel.setNavigationViewModel(navigationViewModel)
        }
        .onChange(of: scenePhase, initial: false) { _, newPhase in
            if newPhase == .active {
                Task { await sessionsViewModel.refreshSessions() }
            }
        }
    }
}

#Preview {
    ContentView()
}
