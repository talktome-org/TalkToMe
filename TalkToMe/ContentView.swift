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

    @StateObject private var onboardingVM = OnboardingViewModel()
    @State private var onboardingLoaded = false

    @Namespace private var profileNamespace
    @Environment(\.scenePhase) private var scenePhase

    var body: some View {
        Group {
            if authService.isCheckingAuth {
                if authService.currentUserId?.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty == false {
                    MainTabView()
                        .transition(.opacity)
                } else {
                    AppTheme.background
                        .ignoresSafeArea()
                        .transition(.opacity)
                }
            } else if authService.isAuthenticated {
                if onboardingLoaded, onboardingVM.step != .completed {
                    OnboardingFlowView(viewModel: onboardingVM)
                        .transition(.opacity)
                } else {
                    MainTabView()
                        .transition(.opacity)
                }
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
        .onChange(of: authService.isAuthenticated, initial: true) { _, isAuthed in
            if !isAuthed {
                onboardingLoaded = false
                onboardingVM.reset()
                return
            }
            Task {
                await onboardingVM.load()
                await MainActor.run { onboardingLoaded = true }
            }
        }
        .animation(.easeInOut(duration: 0.3), value: authService.isAuthenticated)
        .animation(.easeInOut(duration: 0.3), value: authService.isCheckingAuth)
    }
}

#Preview {
    ContentView()
}
