//
//  TherAIApp.swift
//  TherAI
//
//  Created by Stephan  on 29.08.2025.
//

import SwiftUI
import UIKit
import Supabase
import BackgroundTasks

@main
struct TherAIApp: App {

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
                    AuthService.shared.client.auth.handle(url)
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
                                    // Immediately mark onboarding as completed in backend to suppress the overlay
                                    if let access = try? await AuthService.shared.client.auth.session.accessToken {
                                        _ = try? await BackendService.shared.updateOnboarding(
                                            accessToken: access,
                                            update: .init(partner_display_name: nil, onboarding_step: "completed")
                                        )
                                    }
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
                .onChange(of: auth.isAuthenticated, initial: false) { _, isAuthed in
                    if !isAuthed {
                        // User logged out - reset session view model for fresh login
                        sessionsViewModel.resetForLogout()
                    }
                    if isAuthed, let token = linkVM.pendingInviteToken, !token.isEmpty {  // If user just signed in and we have a pending invite token, accept it
                        Task {
                            await linkVM.acceptInvite(using: token)
                            linkVM.pendingInviteToken = nil
                        }
                    }
                    if isAuthed {
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
                                    await sessionsViewModel.bootstrapInitialData()
                                    // Ensure my avatar image is cached before dismissing loading screen
                                    await sessionsViewModel.ensureProfilePictureCached()
                                }
                                group.addTask {
                                    await MainActor.run {
                                        settingsViewModel.loadProfileInfo()
                                        settingsViewModel.preloadAvatar()
                                    }
                                }
                            }

                            // All initial data loaded, hide loading screen
                            auth.setInitialDataLoaded()

                            // Handle push notifications
                            PushNotificationManager.shared.tryUploadIfAuthenticated()
                            PushNotificationManager.shared.consumePendingIfReady()
                        }
                    }
                }
                .task {
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
