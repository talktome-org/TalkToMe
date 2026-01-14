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

    var body: some View {
        ZStack {
            Group {
                if authService.isCheckingAuth {
                    // Telegram-like: show cached UI immediately; otherwise a minimal spinner.
                    if !sessionsViewModel.sessions.isEmpty {
                        SlideOutSidebarContainerView {
                            MainAppView()
                        }
                        .transition(.opacity)
                    } else {
                        // No spinner before cached UI: just show the app background.
                        Color(.systemBackground)
                            .ignoresSafeArea()
                        .transition(.opacity)
                    }
                } else if authService.isAuthenticated {
                    SlideOutSidebarContainerView {
                        MainAppView()
                    }
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
}

#Preview {
    ContentView()
}
