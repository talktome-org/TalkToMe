//
//  TalkToMeApp.swift
//  TalkToMe
//
//  Created by Stephan  on 29.08.2025.
//

import BackgroundTasks
import Supabase
import SwiftUI
import UIKit

#if DEBUG
  import MachO
#endif

#if DEBUG
  private enum DyldDebug {
    static func logTalkToMeImages() {
      let count = _dyld_image_count()
      for i in 0..<count {
        guard let cName = _dyld_get_image_name(i) else { continue }
        let path = String(cString: cName)
        guard path.contains("TalkToMe") else { continue }
        let slide = _dyld_get_image_vmaddr_slide(i)
        let slideHex = String(format: "0x%llx", Int64(slide))
        print("[DYLD] \(path) slide=\(slideHex)")
      }
    }
  }
#endif

@main
struct TalkToMeApp: App {

  @UIApplicationDelegateAdaptor(AppDelegate.self) var appDelegate

  @StateObject private var auth = AuthService.shared
  @StateObject private var friendsVM = FriendsViewModel(
    accessTokenProvider: {
      let session = try await AuthService.shared.client.auth.session
      return session.accessToken
    },
    userIdProvider: {
      AuthService.shared.currentUserId
    })
  @StateObject private var navigationViewModel = SidebarNavigationViewModel()
  @StateObject private var sessionsViewModel = ChatSessionsViewModel()
  @StateObject private var settingsViewModel = SettingsViewModel()
  @StateObject private var contactsVM = ContactsViewModel()

  @AppStorage(PreferenceKeys.appearancePreference) private var appearance: String = "System"
  @Environment(\.scenePhase) private var scenePhase

  init() {
    #if DEBUG
      DyldDebug.logTalkToMeImages()
    #endif
    // Register BGTask handlers before app finishes launching
  }

  var body: some Scene {
    WindowGroup {
      ContentView()
        .environmentObject(friendsVM)
        .environmentObject(navigationViewModel)
        .environmentObject(sessionsViewModel)
        .environmentObject(settingsViewModel)
        .environmentObject(contactsVM)
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
            }
            return
          }

          #if DEBUG
            print(
              "[Auth] isAuthenticated -> true; userId=\(auth.currentUserId ?? "nil") activeSessionId=\(sessionsViewModel.activeSessionId?.uuidString ?? "nil") sessions=\(sessionsViewModel.sessions.count)"
            )
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
                // Warm friends list + avatars so the Friends sheet is instant.
                try? await friendsVM.loadFriends()
              }
              group.addTask {
                // Warm avatar URL + image cache so Settings opens with the picture already loaded.
                await sessionsViewModel.ensureProfilePictureCached()
              }
              group.addTask {
                // Pre-warm device contacts so Friends and Contacts opens instantly.
                await contactsVM.preloadIfAuthorized()
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
            sessionsViewModel.resetForAccountSwitch()

            #if DEBUG
              print(
                "[Auth] currentUserId changed \(oldKey ?? "nil") -> \(newKey); activeSessionId=\(sessionsViewModel.activeSessionId?.uuidString ?? "nil") sessions=\(sessionsViewModel.sessions.count)"
              )
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
                  try? await friendsVM.loadFriends()
                }
                group.addTask {
                  await sessionsViewModel.ensureProfilePictureCached()
                }
                group.addTask {
                  await contactsVM.preloadIfAuthorized()
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
          // Keep decorative media from interrupting other app audio until voice mode is used.
          SharedAudioEngine.shared.ensureIdleAudioSession()
          // Pre-warm ghost video players so the media picker is instant.
          ElevenLabsVoiceSuggestionsView.preloadGhostVideos()
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
            if auth.isAuthenticated {
              Task {
                guard let token = try? await AuthService.shared.client.auth.session.accessToken else { return }
                try? await BackendService.shared.updatePresence(online: true, accessToken: token)
              }
            }
          } else if phase == .background {
            if auth.isAuthenticated {
              let bgTaskId = UIApplication.shared.beginBackgroundTask(expirationHandler: nil)
              Task {
                defer { UIApplication.shared.endBackgroundTask(bgTaskId) }
                guard let token = try? await AuthService.shared.client.auth.session.accessToken else { return }
                try? await BackendService.shared.updatePresence(online: false, accessToken: token)
              }
            }
          }
        }
    }
  }
}
