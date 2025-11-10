//
//  ContentView.swift
//  TalkToMe
//
//  Created by Stephan  on 29.08.2025.
//

import SwiftUI

struct ContentView: View {
    @ObservedObject private var authService = AuthService.shared
    @EnvironmentObject private var navigationViewModel: SidebarNavigationViewModel
    @EnvironmentObject private var sessionsViewModel: ChatSessionsViewModel

    var body: some View {
        ZStack {
            Group {
                if authService.isAuthenticated {
                    if authService.isLoadingInitialData {
                        LoadingView()
                            .transition(.opacity)
                    } else {
                        SlideOutSidebarContainerView {
                            MainAppView()
                        }
                        .transition(.opacity)
                    }
                } else {
                    AuthView()
                        .transition(.opacity)
                }
            }
        }
        .animation(.easeInOut(duration: 0.3), value: authService.isAuthenticated)
        .animation(.easeInOut(duration: 0.3), value: authService.isLoadingInitialData)
    }
}

#Preview {
    ContentView()
}
