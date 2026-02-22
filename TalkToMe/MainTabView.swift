//
//  MainTabView.swift
//  TalkToMe
//
//  Created by Stephan on 02.02.2026.
//

import SwiftUI

enum AppTab: Hashable {
  case home
  case diary
  case chat
  case buddy
}

struct MainTabView: View {
  @EnvironmentObject private var sessionsViewModel: ChatSessionsViewModel
  @EnvironmentObject private var navigationViewModel: SidebarNavigationViewModel

  @AppStorage(PreferenceKeys.elevenLabsVoiceName) private var buddyVoiceName: String = ""
  @State private var selectedTab: AppTab = .home
  @State private var showChat: Bool = false
  @State private var chatDragOffset: CGFloat = 0
  private let dismissThreshold: CGFloat = 100

  private struct BuddyTabConfig {
    let pointSize: CGFloat
    let offsetX: CGFloat  // positive = right, negative = left
    let offsetY: CGFloat  // positive = down, negative = up
    init(pointSize: CGFloat, offsetX: CGFloat = 0, offsetY: CGFloat) {
      self.pointSize = pointSize
      self.offsetX = offsetX
      self.offsetY = offsetY
    }
  }

  private static let buddyTabConfigs: [String: BuddyTabConfig] = [
    "jay":  BuddyTabConfig(pointSize: 24, offsetY: 4),
    "hex":  BuddyTabConfig(pointSize: 32, offsetY: 2.5),
    "mira": BuddyTabConfig(pointSize: 23, offsetX: -1, offsetY: 3),
    "pax":  BuddyTabConfig(pointSize: 26, offsetX: 1, offsetY: 4),
    "luma": BuddyTabConfig(pointSize: 25, offsetX: 1, offsetY: 2),
    "snow": BuddyTabConfig(pointSize: 26, offsetY: 2),
  ]

  private static let defaultTabConfig = BuddyTabConfig(pointSize: 24, offsetY: 3)

  private var buddyTabImage: UIImage? {
    guard let source = ElevenLabsVoiceSuggestionsView.ghostUIImage(for: buddyVoiceName) else { return nil }
    let key = buddyVoiceName.lowercased().trimmingCharacters(in: .whitespacesAndNewlines)
    let config = Self.buddyTabConfigs[key] ?? Self.defaultTabConfig
    let scale = UIScreen.main.scale
    let canvasWidth = (config.pointSize + abs(config.offsetX)) * scale
    let canvasHeight = (config.pointSize + abs(config.offsetY)) * scale
    let canvasSize = CGSize(width: canvasWidth, height: canvasHeight)
    let imageSize = CGSize(width: config.pointSize * scale, height: config.pointSize * scale)
    let xOrigin = max(config.offsetX, 0) * scale
    let yOrigin = max(config.offsetY, 0) * scale
    let renderer = UIGraphicsImageRenderer(size: canvasSize)
    let resized = renderer.image { _ in
      source.draw(in: CGRect(origin: CGPoint(x: xOrigin, y: yOrigin), size: imageSize))
    }
    return resized.withRenderingMode(.alwaysOriginal)
  }

  private func openChat(sessionId: UUID?) {
    if let sid = sessionId {
      sessionsViewModel.openSession(sid)
    } else {
      sessionsViewModel.startNewChat()
    }
    // Start off-screen to the right
    chatDragOffset = UIScreen.main.bounds.width
    showChat = true
    // Slide in from the right
    withAnimation(.easeOut(duration: 0.20)) {
      chatDragOffset = 0
    }
  }

  private func dismissChat() {
    UIApplication.shared.sendAction(
      #selector(UIResponder.resignFirstResponder), to: nil, from: nil, for: nil)
    withAnimation(.easeOut(duration: 0.25)) {
      chatDragOffset = UIScreen.main.bounds.width
    }
    DispatchQueue.main.asyncAfter(deadline: .now() + 0.25) {
      showChat = false
      chatDragOffset = 0
    }
  }

  private var tabSelection: Binding<AppTab> {
    Binding(
      get: { selectedTab },
      set: { newTab in
        if newTab == .buddy {
          Haptics.impact(.light)
          openChat(sessionId: nil)
        } else {
          selectedTab = newTab
        }
      }
    )
  }

  var body: some View {
    ZStack {
      TabView(selection: tabSelection) {
        Tab("Home", systemImage: "house", value: .home) {
          HomeView(
            selectedTab: $selectedTab,
            onStartNewChat: { openChat(sessionId: nil) },
            onOpenChat: { sid in openChat(sessionId: sid) }
          )
        }

        Tab("Diary", systemImage: "book", value: .diary) {
          DiaryView()
        }

        Tab("Chat", systemImage: "bubble.left.and.bubble.right", value: .chat) {
          NavigationStack {
            MainAppView(
              onOpenChat: { sid in openChat(sessionId: sid) },
              onStartNewChat: { openChat(sessionId: nil) }
            )
          }
        }

        Tab(value: .buddy, role: .search) {
          Color.clear
        } label: {
          if let img = buddyTabImage {
            Image(uiImage: img)
          } else {
            Image(systemName: "bubble.left.fill")
          }
        }

      }

      if showChat {
        let viewId: String = {
          if let sid = sessionsViewModel.activeSessionId { return "session_\(sid.uuidString)" }
          return "new_\(sessionsViewModel.chatViewKey.uuidString)"
        }()
        ChatView(
          sessionId: sessionsViewModel.activeSessionId,
          onBack: {
            dismissChat()
          }
        )
        .id(viewId)
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .background(AppTheme.background)
        .clipShape(RoundedRectangle(cornerRadius: 44, style: .continuous))
        .ignoresSafeArea()
        .offset(x: chatDragOffset)
        .overlay(alignment: .leading) {
          // Edge-swipe to dismiss so inner horizontal drags remain usable.
          Color.clear
            .frame(width: 24)
            .contentShape(Rectangle())
            .gesture(
              DragGesture()
                .onChanged { value in
                  // Only allow dragging to the right (positive x translation)
                  if value.translation.width > 0 {
                    chatDragOffset = value.translation.width
                  }
                }
                .onEnded { value in
                  if value.translation.width > dismissThreshold
                    || value.predictedEndTranslation.width > dismissThreshold * 2
                  {
                    // Dismiss keyboard
                    UIApplication.shared.sendAction(
                      #selector(UIResponder.resignFirstResponder), to: nil, from: nil, for: nil)
                    // Dismiss with a slide-out animation
                    withAnimation(.easeOut(duration: 0.2)) {
                      chatDragOffset = UIScreen.main.bounds.width
                    }
                    DispatchQueue.main.asyncAfter(deadline: .now() + 0.2) {
                      showChat = false
                      chatDragOffset = 0
                    }
                  } else {
                    // Snap back
                    withAnimation(.spring(response: 0.3, dampingFraction: 0.7)) {
                      chatDragOffset = 0
                    }
                  }
                }
            )
        }
        .onReceive(NotificationCenter.default.publisher(for: .openChatSession)) { note in
          if let sid = note.userInfo?["sessionId"] as? UUID {
            selectedTab = .chat
            openChat(sessionId: sid)
          }
        }
        .zIndex(1)
      }
    }
  }
}

#Preview {
  MainTabView()
    .environmentObject(SidebarNavigationViewModel())
    .environmentObject(ChatSessionsViewModel())
    .environmentObject(FriendsViewModel(accessTokenProvider: { "" }))
    .environmentObject(SettingsViewModel())
    .environmentObject(ContactsViewModel())
}
