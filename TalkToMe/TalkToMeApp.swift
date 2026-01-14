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
    @StateObject private var linkVM = LinkViewModel(accessTokenProvider: {
        let session = try await AuthService.shared.client.auth.session
        return session.accessToken
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
                .environmentObject(linkVM)
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
                    let base = AuthService.getInfoPlistValue(for: "SHARE_LINK_BASE_URL") as? String
                    let configuredHost = base.flatMap { URL(string: $0)?.host }
                    if url.host == configuredHost || url.path.hasPrefix("/link") {
                        if let components = URLComponents(url: url, resolvingAgainstBaseURL: false),
                           let token = components.queryItems?.first(where: { $0.name == "code" })?.value,
                           !token.isEmpty {
                            // Prefer inviter display name from deep link if present
                            var partnerName = components.queryItems?.first(where: { $0.name == "name" })?.value ?? ""
                            partnerName = partnerName.trimmingCharacters(in: .whitespacesAndNewlines)
                            if partnerName.isEmpty {
                                partnerName = UserDefaults.standard.string(forKey: PreferenceKeys.partnerName) ?? ""
                            } else {
                                UserDefaults.standard.set(partnerName, forKey: PreferenceKeys.partnerName)
                            }
                            // Fire an immediate notification so UI can react instantly
                            NotificationCenter.default.post(name: .partnerLinkOpened, object: nil, userInfo: ["partnerName": partnerName])
                            if auth.isAuthenticated {
                                Task {
                                    await linkVM.acceptInvite(using: token)
                                    await sessionsViewModel.loadPartnerInfo()
                                    await sessionsViewModel.loadPairedAvatars()
                                }
                            } else {
                                linkVM.captureIncomingInviteToken(token)
                            }
                        }
                    }
                }
                .onChange(of: auth.isAuthenticated, initial: true) { oldValue, isAuthed in
                    // With `initial: true`, SwiftUI calls this once on first render.
                    // We must NOT treat "initial false" as a logout event.
                    let isInitialCallback = (oldValue == isAuthed)

                    if !isAuthed {
                        if !isInitialCallback {
                            // User logged out - reset session view model for fresh login
                            sessionsViewModel.resetForLogout()
                        }
                        return
                    }

                    if let token = linkVM.pendingInviteToken, !token.isEmpty {  // If user just signed in and we have a pending invite token, accept it
                        Task {
                            await linkVM.acceptInvite(using: token)
                            linkVM.pendingInviteToken = nil
                        }
                    }

                    Task {
                        // Load all initial data in parallel
                        await withTaskGroup(of: Void.self) { group in
                            group.addTask {
                                await linkVM.ensureInviteReady()
                            }
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
                        }

                        // All initial data loaded, hide loading screen
                        // Handle push notifications
                        PushNotificationManager.shared.tryUploadIfAuthenticated()
                        PushNotificationManager.shared.consumePendingIfReady()
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
                        await linkVM.ensureInviteReady()
                        sessionsViewModel.startObserving()
                    }
                    PushNotificationManager.shared.requestAuthorizationAndRegister()
                }
                .onChange(of: scenePhase, initial: false) { _, phase in
                    if phase == .active {
                        DispatchQueue.main.async {
                            PushNotificationManager.shared.consumePendingIfReady()
                        }
                    }
                }
        }
    }
}
