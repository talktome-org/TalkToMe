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
    @State private var onboardingLoadTask: Task<Void, Never>? = nil

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
                if !onboardingLoaded {
                    onboardingLoadingView
                        .transition(.opacity)
                } else if onboardingVM.step != .completed {
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
            onboardingLoadTask?.cancel()
            onboardingLoadTask = nil

            if !isAuthed {
                onboardingLoaded = false
                onboardingVM.reset()
                return
            }

            onboardingLoaded = false
            onboardingLoadTask = Task {
                await onboardingVM.load()
                await MainActor.run {
                    guard authService.isAuthenticated else { return }
                    onboardingLoaded = true
                }
            }
        }
        .animation(.easeInOut(duration: 0.3), value: authService.isAuthenticated)
        .animation(.easeInOut(duration: 0.3), value: authService.isCheckingAuth)
    }

    private var onboardingLoadingView: some View {
        ZStack {
            AppTheme.background.ignoresSafeArea()
            ProgressView("Preparing your account...")
                .foregroundStyle(AppTheme.textSecondary)
        }
    }
}

#Preview {
    ContentView()
}
