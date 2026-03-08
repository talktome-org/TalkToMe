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

    @Environment(\.scenePhase) private var scenePhase

    /// Show the main tab view when we have a cached user (during auth check)
    /// OR when fully authenticated with onboarding complete. Keeping this in a
    /// single computed property ensures SwiftUI sees one `MainTabView` branch,
    /// avoiding a fade-out/fade-in flash on launch.
    private var shouldShowMainTab: Bool {
        if authService.isCheckingAuth {
            return authService.currentUserId?.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty == false
        }
        return authService.isAuthenticated
            && onboardingLoaded && onboardingVM.step == .completed
    }

    var body: some View {
        Group {
            if shouldShowMainTab {
                MainTabView()
                    .transition(.opacity)
            } else if !authService.isCheckingAuth && authService.isAuthenticated
                        && onboardingLoaded && onboardingVM.step != .completed {
                OnboardingFlowView(viewModel: onboardingVM)
                    .transition(.opacity)
            } else if !authService.isCheckingAuth && !authService.isAuthenticated {
                AuthView()
                    .transition(.opacity)
            } else {
                AppTheme.background
                    .ignoresSafeArea()
                    .transition(.opacity)
            }
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

            // Skip the loading screen for returning users who already completed onboarding.
            let userId = authService.currentUserId ?? ""
            let cacheKey = "onboarding_completed_\(userId)"
            if !userId.isEmpty && UserDefaults.standard.bool(forKey: cacheKey) {
                onboardingVM.step = .completed
                onboardingLoaded = true
            } else {
                onboardingLoaded = false
            }

            onboardingLoadTask = Task {
                await onboardingVM.load()
                await MainActor.run {
                    guard authService.isAuthenticated else { return }
                    // Update the cache based on the server response.
                    if onboardingVM.step == .completed && !userId.isEmpty {
                        UserDefaults.standard.set(true, forKey: cacheKey)
                    } else if !userId.isEmpty {
                        UserDefaults.standard.removeObject(forKey: cacheKey)
                    }
                    onboardingLoaded = true
                }
            }
        }
        .animation(.easeInOut(duration: 0.3), value: authService.isAuthenticated)
        .animation(.easeInOut(duration: 0.3), value: authService.isCheckingAuth)
        .animation(.easeInOut(duration: 0.3), value: onboardingLoaded)
    }

}

#Preview {
    ContentView()
}
