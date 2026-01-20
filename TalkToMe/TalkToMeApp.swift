//
//  TalkToMeApp.swift
//  TalkToMe
//
//  Created by Stephan  on 29.08.2025.
//

import SwiftUI
import UIKit
import Supabase
import BackgroundTasks

@main
struct TalkToMeApp: App {

    @UIApplicationDelegateAdaptor(AppDelegate.self) var appDelegate

    @StateObject private var auth = AuthService.shared
    @StateObject private var friendsVM = FriendsViewModel(accessTokenProvider: {
        let session = try await AuthService.shared.client.auth.session
        return session.accessToken
    }, userIdProvider: {
        AuthService.shared.currentUserId
    })
    @StateObject private var navigationViewModel = SidebarNavigationViewModel()
    @StateObject private var sessionsViewModel = ChatSessionsViewModel()
    @StateObject private var settingsViewModel = SettingsViewModel()

    @AppStorage(PreferenceKeys.appearancePreference) private var appearance: String = "System"
    @Environment(\.scenePhase) private var scenePhase

    init() {
        // Register BGTask handlers before app finishes launching
    }

    var body: some Scene {
        WindowGroup {
            ContentView()
                .environmentObject(friendsVM)
                .environmentObject(navigationViewModel)
                .environmentObject(sessionsViewModel)
                .environmentObject(settingsViewModel)
                .preferredColorScheme(
                    appearance == "Light" ? .light : appearance == "Dark" ? .dark : nil
                )
                .onOpenURL { url in
                    print("[URL] onOpenURL received: \(url.absoluteString)")
                    AuthService.shared.client.auth.handle(url)
                    if url.scheme?.hasPrefix("supabase-") == true && url.path == "/auth/callback" {
                        print("[URL] Supabase auth callback detected")
                    }
                }
                .onChange(of: auth.isAuthenticated, initial: true) { oldValue, isAuthed in
                    // With `initial: true`, SwiftUI calls this once on first render.
                    // We must NOT treat "initial false" as a logout event.
                    let isInitialCallback = (oldValue == isAuthed)

                    // If Settings was open before logout, `showSettingsSheet` can remain true while the auth view is shown.
                    // On next login, the sidebar container reappears and immediately presents the sheet again.
                    if !isInitialCallback {
                        navigationViewModel.showSettingsSheet = false
                    }

                    if !isAuthed {
                        if !isInitialCallback {
                            // User logged out - reset session view model for fresh login
                            sessionsViewModel.resetForLogout()

                            // Always reopen the sidebar on logout to avoid flashing ChatView on next login.
                            withAnimation(nil) {
                                navigationViewModel.isOpen = true
                                navigationViewModel.selectedTab = .chat
                            }
                        }
                        return
                    }

                    // On login, force the sidebar open immediately (same-runloop) so ChatView can't flash.
                    withAnimation(nil) {
                        navigationViewModel.isOpen = true
                        navigationViewModel.selectedTab = .chat
                    }

#if DEBUG
                    print("[Auth] isAuthenticated -> true; userId=\(auth.currentUserId ?? "nil") isOpen=\(navigationViewModel.isOpen) activeSessionId=\(sessionsViewModel.activeSessionId?.uuidString ?? "nil") sessions=\(sessionsViewModel.sessions.count)")
#endif
                    Task {
                        // Load all initial data in parallel
                        await withTaskGroup(of: Void.self) { group in
                            group.addTask {
                                await MainActor.run {
                                    sessionsViewModel.startObserving()
                                }
                            }
                            group.addTask {
                                await MainActor.run {
                                    settingsViewModel.loadProfileInfo()
                                    settingsViewModel.preloadAvatar()
                                }
                            }
                            group.addTask {
                                // Ensure the sidebar "Friends +" sheet can show a code immediately.
                                await friendsVM.refreshMyCode()
                            }
                            group.addTask {
                                // Warm avatar URL + image cache so Settings opens with the picture already loaded.
                                await sessionsViewModel.ensureProfilePictureCached()
                            }
                        }

                        // All initial data loaded, hide loading screen
                        // Handle push notifications
                        APNSService.shared.tryUploadIfAuthenticated()
                        APNSService.shared.consumePendingIfReady()
                    }
                }
                .onChange(of: auth.currentUserId, initial: true) { oldValue, newValue in
                    // Handle account switching where `isAuthenticated` may remain true.
                    let oldKey = oldValue?.trimmingCharacters(in: .whitespacesAndNewlines)
                    let newKey = newValue?.trimmingCharacters(in: .whitespacesAndNewlines)
                    if oldKey == newKey { return }

                    // If we're switching to a different user (or restoring cached auth), reset volatile UI state.
                    if let newKey, !newKey.isEmpty {
                        navigationViewModel.showSettingsSheet = false
                        withAnimation(nil) {
                            navigationViewModel.isOpen = true
                            navigationViewModel.selectedTab = .chat
                        }
                        sessionsViewModel.resetForAccountSwitch()

#if DEBUG
                        print("[Auth] currentUserId changed \(oldKey ?? "nil") -> \(newKey); forcing sidebar open; activeSessionId=\(sessionsViewModel.activeSessionId?.uuidString ?? "nil") sessions=\(sessionsViewModel.sessions.count)")
#endif
                        Task {
                            // Re-bootstrap app data for the new user.
                            await withTaskGroup(of: Void.self) { group in
                                group.addTask { @MainActor in
                                    sessionsViewModel.startObserving()
                                }
                                group.addTask { @MainActor in
                                    settingsViewModel.loadProfileInfo()
                                    settingsViewModel.preloadAvatar()
                                }
                                group.addTask {
                                    await friendsVM.refreshMyCode()
                                }
                                group.addTask {
                                    await sessionsViewModel.ensureProfilePictureCached()
                                }
                            }
                            APNSService.shared.tryUploadIfAuthenticated()
                            APNSService.shared.consumePendingIfReady()
                        }
                    } else {
                        // User id cleared (logout). The `isAuthenticated` onChange handles reset.
                    }
                }
                .task {
                    _ = NetworkMonitor.shared
                    ChatOutboxProcessor.shared.start()
                    // Telegram-like boot: show cached UI immediately while auth restores.
                    Task { @MainActor in
                        await sessionsViewModel.preloadCachedSessionsIfNeeded()
                    }
                    if auth.isAuthenticated {
                        sessionsViewModel.startObserving()
                    }
                    APNSService.shared.requestAuthorizationAndRegister()
                }
                .onChange(of: scenePhase, initial: false) { _, phase in
                    if phase == .active {
                        DispatchQueue.main.async {
                            APNSService.shared.consumePendingIfReady()
                        }
                    }
                }
        }
    }
}
