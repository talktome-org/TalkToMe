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
        Group {
            if authService.isCheckingAuth {
                // Telegram-like: show cached UI immediately; otherwise a minimal spinner.
                // Only show cached UI if we have an effective user id (avoids flashing stale state across account switches).
                if authService.currentUserId?.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty == false,
                   !sessionsViewModel.sessions.isEmpty {
                    MainAppView()
                        .transition(.opacity)
                } else {
                    // No spinner before cached UI: just show the app background.
                    Color(.systemBackground)
                        .ignoresSafeArea()
                        .transition(.opacity)
                }
            } else if authService.isAuthenticated {
                MainAppView()
                    .transition(.opacity)
            } else {
                AuthView()
                    .transition(.opacity)
            }
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
        .onChange(of: scenePhase, initial: false) { _, newPhase in
            if newPhase == .active {
                Task { await sessionsViewModel.refreshSessions() }
            }
        }
        .animation(.easeInOut(duration: 0.3), value: authService.isAuthenticated)
        .animation(.easeInOut(duration: 0.3), value: authService.isCheckingAuth)
    }
}

#Preview {
    ContentView()
}
