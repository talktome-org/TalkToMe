//
//  HomeView.swift
//  TalkToMe
//
//  Created by Stephan on 02.02.2026.
//

import SwiftUI

// MARK: - Feature Model

private enum HomeFeature: Int, CaseIterable, Identifiable {
  case voiceMode
  case customize
  case capturePhoto
  case lateNight
  case weekReview

  var id: Int { rawValue }

  var title: String {
    switch self {
    case .voiceMode: return "Talk, Don\u{2019}t Type"
    case .customize: return "Make It Yours"
    case .capturePhoto: return "Capture Moments"
    case .lateNight: return "Late Night Thoughts"
    case .weekReview: return "Your Week in Review"
    }
  }

  var subtitle: String {
    switch self {
    case .voiceMode:
      return "Have a real conversation using your voice. Your buddy speaks back — each one sounds different."
    case .customize:
      return "Wallpapers, themes, and colors — design your perfect chat space."
    case .capturePhoto:
      return "Add photos to your diary entries. Snap a moment, write a thought, look back anytime."
    case .lateNight:
      return "Can\u{2019}t sleep? Your buddy is always awake. No judgment, just good conversation at any hour."
    case .weekReview:
      return "Your buddy remembers everything you\u{2019}ve talked about. Ask for a recap of your week\u{2019}s highlights."
    }
  }

  var actionLabel: String {
    switch self {
    case .voiceMode: return "Try Voice Mode"
    case .customize: return "Customize"
    case .capturePhoto: return "Open Diary"
    case .lateNight: return "Start Chat"
    case .weekReview: return "Open Diary"
    }
  }

  var iconName: String {
    switch self {
    case .voiceMode: return "waveform"
    case .customize: return "paintpalette.fill"
    case .capturePhoto: return "camera.fill"
    case .lateNight: return "moon.stars.fill"
    case .weekReview: return "calendar.badge.clock"
    }
  }

  var gradientColors: [Color] {
    switch self {
    case .voiceMode:
      return [Color(red: 0.95, green: 0.45, blue: 0.25), Color(red: 0.98, green: 0.65, blue: 0.30)]
    case .customize:
      return [Color(red: 0.63, green: 0.32, blue: 0.98), Color(red: 0.90, green: 0.40, blue: 0.65)]
    case .capturePhoto:
      return [Color(red: 0.25, green: 0.72, blue: 0.68), Color(red: 0.40, green: 0.85, blue: 0.55)]
    case .lateNight:
      return [Color(red: 0.20, green: 0.22, blue: 0.55), Color(red: 0.40, green: 0.30, blue: 0.75)]
    case .weekReview:
      return [Color(red: 0.26, green: 0.58, blue: 1.00), Color(red: 0.30, green: 0.78, blue: 0.95)]
    }
  }
}

// MARK: - Get Started Model

private enum GetStartedStep: Int, CaseIterable, Identifiable {
  case sayHi
  case connectFriend
  case writeDiary
  case tellStory

  var id: Int { rawValue }

  func title(buddyName: String) -> String {
    switch self {
    case .sayHi: return "Say Hi to \(buddyName)"
    case .connectFriend: return "Connect with a Friend"
    case .writeDiary: return "Write Your First Entry"
    case .tellStory: return "Tell Your Story"
    }
  }

  func subtitleView(buddyName: String) -> Text {
    switch self {
    case .sayHi:
      return Text("Start your first chat and ") + Text("get to know \(buddyName)").bold()
    case .connectFriend:
      return Text("Share your friend code to pair up, ") + Text("at no extra cost").bold()
    case .writeDiary:
      return Text("Start a daily diary — ") + Text("\(buddyName) will help you reflect").bold()
    case .tellStory:
      return Text("Share about yourself so ") + Text("\(buddyName) knows you better").bold()
    }
  }

  var iconName: String {
    switch self {
    case .sayHi: return "bubble.left.and.text.bubble.right.fill"
    case .connectFriend: return "person.2.fill"
    case .writeDiary: return "book.fill"
    case .tellStory: return "heart.text.clipboard.fill"
    }
  }

  var gradientColors: [Color] {
    switch self {
    case .sayHi:
      return [Color(red: 0.26, green: 0.58, blue: 1.00), Color(red: 0.30, green: 0.78, blue: 0.95)]
    case .connectFriend:
      return [Color(red: 0.63, green: 0.32, blue: 0.98), Color(red: 0.90, green: 0.40, blue: 0.65)]
    case .writeDiary:
      return [Color(red: 0.25, green: 0.72, blue: 0.68), Color(red: 0.40, green: 0.85, blue: 0.55)]
    case .tellStory:
      return [Color(red: 0.95, green: 0.55, blue: 0.30), Color(red: 0.98, green: 0.75, blue: 0.35)]
    }
  }
}

// MARK: - Get Started Step Card

private struct GetStartedStepCardView: View {
  let step: GetStartedStep
  let buddyName: String
  var index: Int = 0
  var total: Int = 1

  var body: some View {
    VStack(alignment: .leading, spacing: 0) {
      ZStack {
        LinearGradient(
          colors: step.gradientColors,
          startPoint: .topLeading,
          endPoint: .bottomTrailing
        )

        Circle()
          .fill(.white.opacity(0.1))
          .frame(width: 60, height: 60)
          .offset(x: -40, y: -15)

        Circle()
          .fill(.white.opacity(0.08))
          .frame(width: 40, height: 40)
          .offset(x: 50, y: 15)

        Image(systemName: step.iconName)
          .font(.system(size: 24, weight: .medium))
          .foregroundStyle(.white)
          .shadow(color: .black.opacity(0.15), radius: 4, x: 0, y: 2)

        VStack {
          HStack {
            Spacer()
            Text("\(index + 1)/\(total)")
              .font(.system(size: 10, weight: .semibold, design: .rounded))
              .foregroundStyle(.white.opacity(0.7))
              .padding(.horizontal, 7)
              .padding(.vertical, 3)
              .background(.white.opacity(0.15))
              .clipShape(Capsule())
          }
          Spacer()
        }
        .padding(8)
      }
      .frame(height: 76)

      VStack(alignment: .leading, spacing: 2) {
        Text(step.title(buddyName: buddyName))
          .font(.system(size: 13, weight: .semibold))
          .foregroundStyle(Color(.label))
          .lineLimit(2)
          .fixedSize(horizontal: false, vertical: true)

        step.subtitleView(buddyName: buddyName)
          .font(.system(size: 11))
          .foregroundStyle(Color(.secondaryLabel))
          .lineLimit(3)
          .fixedSize(horizontal: false, vertical: true)
      }
      .padding(.horizontal, 10)
      .padding(.top, 8)
      .padding(.bottom, 10)
      .frame(maxHeight: .infinity, alignment: .top)
    }
    .frame(width: 148, height: 170)
    .background(Color(.tertiarySystemBackground))
    .clipShape(RoundedRectangle(cornerRadius: 14, style: .continuous))
    .overlay(
      RoundedRectangle(cornerRadius: 14, style: .continuous)
        .stroke(Color(.separator).opacity(0.2), lineWidth: 0.5)
    )
  }
}

// MARK: - Home Card View (from main)

struct HomeCardView: View {
  let title: String
  let overview: String
  let gradientColors: [Color]

  var body: some View {
    VStack(alignment: .leading, spacing: 0) {
      // Image placeholder
      ZStack {
        LinearGradient(
          colors: gradientColors,
          startPoint: .topLeading,
          endPoint: .bottomTrailing
        )

        Image(systemName: "sparkles")
          .font(.system(size: 32, weight: .light))
          .foregroundStyle(.white.opacity(0.5))
      }
      .frame(height: 160)
      .clipped()

      // Text content
      VStack(alignment: .leading, spacing: 6) {
        Text(title)
          .font(.system(size: 18, weight: .semibold))
          .foregroundStyle(AppTheme.textPrimary)

        Text(overview)
          .font(.system(size: 14))
          .foregroundStyle(AppTheme.textSecondary)
          .lineLimit(2)
      }
      .padding(.horizontal, 14)
      .padding(.vertical, 12)
    }
    .background(AppTheme.surface)
    .clipShape(RoundedRectangle(cornerRadius: 16, style: .continuous))
    //.environment(\.colorScheme, .light)
    .overlay(
      RoundedRectangle(cornerRadius: 16, style: .continuous)
        .stroke(Color.primary.opacity(0.1), lineWidth: 0.5)
    )
  }
}

// MARK: - Quarter-fill Progress Ring

private struct QuarterProgressRing: View {
  let completed: Int
  let total: Int

  var body: some View {
    ZStack {
      Circle()
        .stroke(Color(.separator).opacity(0.25), lineWidth: 2.5)

      ForEach(0..<completed, id: \.self) { i in
        Circle()
          .trim(
            from: CGFloat(i) / CGFloat(total),
            to: CGFloat(i + 1) / CGFloat(total) - 0.02
          )
          .stroke(Color(.label), style: StrokeStyle(lineWidth: 2.5, lineCap: .round))
          .rotationEffect(.degrees(-90))
      }

      Text("\(completed)/\(total)")
        .font(.system(size: 11, weight: .semibold, design: .rounded))
        .foregroundStyle(Color(.label))
    }
    .frame(width: 34, height: 34)
  }
}

// MARK: - Get Started Card

private struct GetStartedCardView: View {
  let buddyName: String
  let completedCount: Int
  let totalCount: Int
  let steps: [GetStartedStep]
  let completedStepIds: Set<Int>
  let onStepTap: (GetStartedStep) -> Void
  var onReset: (() -> Void)? = nil

  var body: some View {
    VStack(alignment: .leading, spacing: 0) {
      // Header
      HStack(spacing: 10) {
        QuarterProgressRing(completed: completedCount, total: totalCount)

        Text("Get started")
          .font(.system(size: 18, weight: .bold))
          .foregroundStyle(Color(.label))

        Spacer()
      }
      .simultaneousGesture(
        LongPressGesture(minimumDuration: 1.0).onEnded { _ in
          Haptics.notification(.warning)
          withAnimation(.spring(response: 0.4, dampingFraction: 0.8)) {
            onReset?()
          }
        }
      )
      .padding(.horizontal, 14)
      .padding(.top, 4)
      .padding(.bottom, 10)

      // Arc carousel
      ArcCarouselView(
        steps: steps,
        completedStepIds: completedStepIds,
        buddyName: buddyName,
        onStepTap: onStepTap
      )
    }
  }
}

// MARK: - Arc Carousel

private struct ArcCarouselView: View {
  let steps: [GetStartedStep]
  let completedStepIds: Set<Int>
  let buddyName: String
  let onStepTap: (GetStartedStep) -> Void

  private let maxYDrop: CGFloat = 20
  private let maxRotation: Double = 5

  private var visibleSteps: [GetStartedStep] {
    steps.filter { !completedStepIds.contains($0.id) }
  }

  var body: some View {
    if !visibleSteps.isEmpty {
      ScrollView(.horizontal, showsIndicators: false) {
        HStack(alignment: .top, spacing: 12) {
          ForEach(Array(visibleSteps.enumerated()), id: \.element.id) { index, step in
            Button { onStepTap(step) } label: {
              GetStartedStepCardView(step: step, buddyName: buddyName, index: index, total: visibleSteps.count)
            }
            .buttonStyle(SpringPressStyle())
            .visualEffect { content, proxy in
              let scrollBounds = proxy.bounds(of: .scrollView(axis: .horizontal)) ?? .zero
              let scrollCenter = scrollBounds.width / 2
              let cardMid = proxy.frame(in: .scrollView(axis: .horizontal)).midX
              let distance = cardMid - scrollCenter
              let normalized = scrollCenter > 0 ? distance / scrollCenter : 0
              let clamped = max(-1.5, min(1.5, normalized))

              return content
                .offset(y: clamped * clamped * maxYDrop)
                .rotationEffect(.degrees(-Double(clamped) * maxRotation))
            }
          }
        }
        .padding(.horizontal, 14)
        .padding(.bottom, maxYDrop)
      }
      .scrollClipDisabled()
      .padding(.bottom, 8)
    }
  }
}

// MARK: - Button Style

private struct SpringPressStyle: ButtonStyle {
  func makeBody(configuration: Configuration) -> some View {
    configuration.label
      .scaleEffect(configuration.isPressed ? 0.97 : 1.0)
      .animation(.spring(response: 0.3, dampingFraction: 0.8), value: configuration.isPressed)
  }
}

// MARK: - Feature Card

private struct HomeFeatureCardView: View {
  let feature: HomeFeature

  var body: some View {
    VStack(alignment: .leading, spacing: 0) {
      // Gradient header with icon
      ZStack {
        LinearGradient(
          colors: feature.gradientColors,
          startPoint: .topLeading,
          endPoint: .bottomTrailing
        )

        // Decorative circles
        Circle()
          .fill(.white.opacity(0.08))
          .frame(width: 120, height: 120)
          .offset(x: -80, y: -40)

        Circle()
          .fill(.white.opacity(0.06))
          .frame(width: 80, height: 80)
          .offset(x: 100, y: 30)

        // Main icon
        Image(systemName: feature.iconName)
          .font(.system(size: 38, weight: .medium))
          .foregroundStyle(.white)
          .shadow(color: .black.opacity(0.15), radius: 4, x: 0, y: 2)

        // Feature number badge
        VStack {
          HStack {
            Spacer()
            Text("\(feature.rawValue + 1) / \(HomeFeature.allCases.count)")
              .font(.system(size: 12, weight: .semibold, design: .rounded))
              .foregroundStyle(.white.opacity(0.7))
              .padding(.horizontal, 10)
              .padding(.vertical, 5)
              .background(.white.opacity(0.15))
              .clipShape(Capsule())
          }
          Spacer()
        }
        .padding(14)
      }
      .frame(height: 170)
      .clipped()

      // Content area
      VStack(alignment: .leading, spacing: 10) {
        Text(feature.title)
          .font(.system(size: 20, weight: .semibold))
          .foregroundStyle(Color(.label))

        Text(feature.subtitle)
          .font(.system(size: 14))
          .foregroundStyle(Color(.secondaryLabel))
          .lineLimit(2)

        // CTA button
        HStack(spacing: 6) {
          Text(feature.actionLabel)
            .font(.system(size: 14, weight: .semibold))
          Image(systemName: "arrow.right")
            .font(.system(size: 12, weight: .semibold))
        }
        .foregroundStyle(.white)
        .padding(.horizontal, 16)
        .padding(.vertical, 10)
        .background(
          LinearGradient(
            colors: feature.gradientColors,
            startPoint: .leading,
            endPoint: .trailing
          )
        )
        .clipShape(Capsule())
        .padding(.top, 4)
      }
      .padding(.horizontal, 16)
      .padding(.vertical, 14)
    }
    .background(AppTheme.surface)
    .clipShape(RoundedRectangle(cornerRadius: 26, style: .continuous))
    .overlay(
      RoundedRectangle(cornerRadius: 26, style: .continuous)
        .stroke(Color(.separator).opacity(0.2), lineWidth: 0.5)
    )
    .shadow(color: Color.black.opacity(0.06), radius: 8, x: 0, y: 4)
  }
}

// MARK: - Notifications View (pushed via navigationDestination)

private struct NotificationItem: Identifiable {
  let id: UUID
  let title: String
  let body: String
  let date: Date
  let icon: String

  init(title: String, body: String, date: Date, icon: String) {
    self.id = UUID()
    self.title = title
    self.body = body
    self.date = date
    self.icon = icon
  }
}

private enum NotificationDateGroup: String, CaseIterable {
  case today = "Today"
  case yesterday = "Yesterday"
  case last7Days = "Last 7 Days"
}

private struct NotificationsView: View {
  let sessions: [ChatSession]
  let onTap: (UUID) -> Void

  @ObservedObject private var apns = APNSService.shared
  @State private var notifications: [NotificationItem] = []

  private var groupedNotifications: [(group: NotificationDateGroup, items: [NotificationItem])] {
    let calendar = Calendar.current
    let now = Date()
    let startOfToday = calendar.startOfDay(for: now)
    let startOfYesterday = calendar.date(byAdding: .day, value: -1, to: startOfToday)!
    let startOfLast7 = calendar.date(byAdding: .day, value: -7, to: startOfToday)!

    var today: [NotificationItem] = []
    var yesterday: [NotificationItem] = []
    var last7: [NotificationItem] = []

    for item in notifications.sorted(by: { $0.date > $1.date }) {
      if item.date >= startOfToday {
        today.append(item)
      } else if item.date >= startOfYesterday {
        yesterday.append(item)
      } else if item.date >= startOfLast7 {
        last7.append(item)
      }
    }

    var result: [(group: NotificationDateGroup, items: [NotificationItem])] = []
    if !today.isEmpty { result.append((.today, today)) }
    if !yesterday.isEmpty { result.append((.yesterday, yesterday)) }
    if !last7.isEmpty { result.append((.last7Days, last7)) }
    return result
  }

  var body: some View {
    ZStack {
      AppTheme.background.ignoresSafeArea()

      ScrollView {
        VStack(spacing: 20) {
          if !apns.isPushEnabled {
            VStack(spacing: 14) {
              ZStack(alignment: .topTrailing) {
                RoundedRectangle(cornerRadius: 16, style: .continuous)
                  .fill(Color(.secondarySystemFill))
                  .frame(width: 60, height: 60)
                  .overlay(
                    Image(systemName: "bell.fill")
                      .font(.system(size: 26))
                      .foregroundStyle(Color(.label))
                  )

                Circle()
                  .fill(Color.accentColor)
                  .frame(width: 18, height: 18)
                  .offset(x: 4, y: -4)
              }
              .padding(.top, 8)

              Text("Don\u{2019}t Miss a Thing")
                .font(.system(size: 19, weight: .bold))
                .foregroundStyle(Color(.label))

              Text("Stay in the loop with messages from your buddy, diary reminders, and friend updates — all in one place.")
                .font(.system(size: 14))
                .foregroundStyle(Color(.secondaryLabel))
                .multilineTextAlignment(.center)
                .padding(.horizontal, 16)

              HStack(spacing: 10) {
                Button {
                  Haptics.impact(.light)
                  APNSService.shared.setPushEnabled(true)
                  APNSService.shared.requestAuthorizationAndRegister()
                } label: {
                  Text("Allow Notifications")
                    .font(.system(size: 14, weight: .semibold))
                    .foregroundStyle(AppTheme.background)
                    .padding(.horizontal, 20)
                    .padding(.vertical, 11)
                    .background(Color(.label))
                    .clipShape(Capsule())
                }

                Button {
                  Haptics.impact(.light)
                  APNSService.shared.setPushEnabled(true)
                } label: {
                  Text("Later")
                    .font(.system(size: 14, weight: .semibold))
                    .foregroundStyle(Color(.label))
                    .padding(.horizontal, 20)
                    .padding(.vertical, 11)
                    .background(Color(.secondarySystemFill))
                    .clipShape(Capsule())
                }
              }
              .padding(.top, 4)
            }
            .frame(maxWidth: .infinity)
            .padding(.bottom, 0)

            Divider()
              .padding(.horizontal, 0)
              .padding(.top, 4)
              .padding(.bottom, 4)
          }

          // Notification sections
          ForEach(groupedNotifications, id: \.group) { section in
            VStack(alignment: .leading, spacing: 10) {
              Text(section.group.rawValue)
                .font(.system(size: 17, weight: .semibold))
                .foregroundColor(.primary)
                .padding(.horizontal, 4)

              ForEach(section.items) { item in
                notificationRow(item)
              }
            }
          }

          if groupedNotifications.isEmpty && apns.isPushEnabled {
            VStack(spacing: 8) {
              Image(systemName: "bell.slash")
                .font(.system(size: 28))
                .foregroundStyle(Color(.tertiaryLabel))
              Text("No notifications yet")
                .font(.system(size: 15, weight: .medium))
                .foregroundStyle(Color(.secondaryLabel))
            }
            .frame(maxWidth: .infinity)
            .padding(.top, 40)
          }
        }
        .padding(.horizontal, 16)
        .padding(.top, 8)
        .padding(.bottom, 24)
      }
    }
    .navigationTitle("Notifications")
  }

  @ViewBuilder
  private func notificationRow(_ item: NotificationItem) -> some View {
    HStack(alignment: .top, spacing: 10) {
      Image(systemName: item.icon)
        .font(.system(size: 16))
        .foregroundColor(.secondary)
        .frame(width: 24, height: 24)
        .padding(.top, 10)

      VStack(alignment: .leading, spacing: 2) {
        HStack {
          Text(item.title)
            .font(.system(size: 15, weight: .regular))
            .foregroundColor(.primary)
            .lineLimit(2)

          Spacer()

          Text(timeAgoShort(item.date))
            .font(.system(size: 12, weight: .regular))
            .foregroundColor(Color(.tertiaryLabel))
        }

        if !item.body.isEmpty {
          Text(item.body)
            .font(.system(size: 13, weight: .regular))
            .foregroundColor(.secondary)
            .lineLimit(2)
        }
      }
      .padding(.horizontal, 14)
      .padding(.vertical, 10)
      .background(AppTheme.surface)
      .clipShape(RoundedRectangle(cornerRadius: 16, style: .continuous))
    }
    .padding(.leading, 8)
  }

  private func timeAgoShort(_ date: Date) -> String {
    let interval = Date().timeIntervalSince(date)
    let minutes = Int(interval / 60)
    if minutes < 1 { return "now" }
    if minutes < 60 { return "\(minutes)m" }
    let hours = minutes / 60
    if hours < 24 { return "\(hours)h" }
    let days = hours / 24
    return "\(days)d"
  }
}

// MARK: - Home View

struct HomeView: View {
  @Binding var selectedTab: AppTab
  let onStartNewChat: () -> Void
  var onOpenChat: ((UUID) -> Void)? = nil

  @EnvironmentObject private var settingsViewModel: SettingsViewModel
  @EnvironmentObject private var sessionsVM: ChatSessionsViewModel

  @State private var showNotifications: Bool = false

  @AppStorage("get_started_dismissed") private var getStartedDismissed: Bool = false
  @AppStorage("get_started_say_hi") private var sayHiDone: Bool = false
  @AppStorage("get_started_connect_friend") private var connectFriendDone: Bool = false
  @AppStorage("get_started_write_diary") private var writeDiaryDone: Bool = false
  @AppStorage("get_started_tell_story") private var tellStoryDone: Bool = false
  @AppStorage(PreferenceKeys.elevenLabsVoiceName) private var selectedVoiceName: String = ""

  private var buddyDisplayName: String {
    let name = selectedVoiceName.trimmingCharacters(in: .whitespacesAndNewlines)
    guard !name.isEmpty else { return "Buddy" }
    return name.prefix(1).uppercased() + name.dropFirst().lowercased()
  }

  private var completedStepCount: Int {
    [sayHiDone, connectFriendDone, writeDiaryDone, tellStoryDone].filter { $0 }.count
  }

  private var completedStepIds: Set<Int> {
    var ids = Set<Int>()
    if sayHiDone { ids.insert(GetStartedStep.sayHi.id) }
    if connectFriendDone { ids.insert(GetStartedStep.connectFriend.id) }
    if writeDiaryDone { ids.insert(GetStartedStep.writeDiary.id) }
    if tellStoryDone { ids.insert(GetStartedStep.tellStory.id) }
    return ids
  }

  private var shouldShowGetStarted: Bool {
    !getStartedDismissed && completedStepCount < GetStartedStep.allCases.count
  }

  private var displayName: String {
    let name = settingsViewModel.fullName.trimmingCharacters(in: .whitespacesAndNewlines)
    if !name.isEmpty { return name.components(separatedBy: " ").first ?? name }
    if let user = AuthService.shared.currentUser {
      if let n = user.userMetadata["full_name"]?.stringValue, !n.isEmpty {
        return n.components(separatedBy: " ").first ?? n
      }
    }
    return "there"
  }

  private var unreadSessions: [ChatSession] {
    sessionsVM.sessions.filter { $0.unreadCount > 0 }
  }

  private var totalUnread: Int {
    unreadSessions.reduce(0) { $0 + $1.unreadCount }
  }

  private var avatarFallback: some View {
    Circle()
      .fill(
        LinearGradient(
          colors: [
            Color(red: 0.26, green: 0.58, blue: 1.00),
            Color(red: 0.63, green: 0.32, blue: 0.98)
          ],
          startPoint: .topLeading,
          endPoint: .bottomTrailing
        )
      )
      .overlay(
        Text(displayName.prefix(1).uppercased())
          .font(.system(size: 12, weight: .semibold, design: .rounded))
          .foregroundStyle(.white)
      )
  }

  var body: some View {
    NavigationStack {
      ZStack {
        AppTheme.background.ignoresSafeArea()

        ScrollView {
          VStack(spacing: 16) {
            if shouldShowGetStarted {
              GetStartedCardView(
                buddyName: buddyDisplayName,
                completedCount: completedStepCount,
                totalCount: GetStartedStep.allCases.count,
                steps: GetStartedStep.allCases,
                completedStepIds: completedStepIds,
                onStepTap: handleGetStartedTap,
                onReset: resetGetStarted
              )
              .transition(.opacity.combined(with: .move(edge: .top)))
            }

            Spacer().frame(height: 12)

            ForEach(HomeFeature.allCases) { feature in
              Button { handleCardTap(feature) } label: {
                HomeFeatureCardView(feature: feature)
              }
              .buttonStyle(SpringPressStyle())
            }
          }
          .padding(.horizontal, 16)
          .padding(.top, 8)
          .padding(.bottom, 24)
        }
      }
      .navigationBarTitleDisplayMode(.inline)
      .navigationDestination(isPresented: $showNotifications) {
        NotificationsView(
          sessions: unreadSessions,
          onTap: { sessionId in
            showNotifications = false
            onOpenChat?(sessionId)
          }
        )
      }
      .toolbar {
        ToolbarItem(placement: .topBarLeading) {
          Button {
            Haptics.impact(.light)
            selectedTab = .settings
          } label: {
            if let url = sessionsVM.myAvatarURL, !url.isEmpty {
              AvatarCacheManager.shared.cachedAsyncImage(
                urlString: url,
                placeholder: { AnyView(avatarFallback) },
                fallback: { AnyView(avatarFallback) }
              )
              .frame(width: 36, height: 36)
              .clipShape(Circle())
            } else {
              avatarFallback
                .frame(width: 36, height: 36)
            }
          }
        }

        if #available(iOS 26.0, *) {
          ToolbarSpacer(.fixed, placement: .topBarLeading)
        }

        ToolbarItem(placement: .topBarLeading) {
          Text(" \(displayName) ")
            .font(.system(size: 15, weight: .semibold))
            .foregroundStyle(Color(.label))
            .fixedSize()
        }

        ToolbarItem(placement: .topBarTrailing) {
          Button {
            Haptics.impact(.light)
            showNotifications = true
          } label: {
            ZStack(alignment: .topTrailing) {
              Image(systemName: "bell.fill")
                .font(.system(size: 16))
                .foregroundStyle(.primary)

              if totalUnread > 0 {
                Circle()
                  .fill(Color.accentColor)
                  .frame(width: 12, height: 12)
                  .offset(x: 4, y: -4)
              }
            }
          }
        }
      }
    }
  }

  private func handleCardTap(_ feature: HomeFeature) {
    if feature != .customize { Haptics.impact(.light) }
    switch feature {
    case .voiceMode:
      sessionsVM.shouldStartInVoiceMode = true
      onStartNewChat()
    case .customize:
      selectedTab = .settings
      DispatchQueue.main.asyncAfter(deadline: .now() + 0.3) {
        settingsViewModel.shouldHighlightCustomization = true
      }
    case .capturePhoto:
      selectedTab = .diary
    case .lateNight:
      onStartNewChat()
    case .weekReview:
      selectedTab = .diary
    }
  }

  private func resetGetStarted() {
    sayHiDone = false
    connectFriendDone = false
    writeDiaryDone = false
    tellStoryDone = false
    getStartedDismissed = false
  }

  private func handleGetStartedTap(_ step: GetStartedStep) {
    Haptics.impact(.light)

    // Navigate to the relevant feature
    switch step {
    case .sayHi:
      onStartNewChat()
    case .connectFriend:
      selectedTab = .settings
      DispatchQueue.main.asyncAfter(deadline: .now() + 0.3) {
        settingsViewModel.shouldNavigateToContacts = true
      }
    case .writeDiary:
      selectedTab = .diary
    case .tellStory:
      selectedTab = .settings
      DispatchQueue.main.asyncAfter(deadline: .now() + 0.3) {
        settingsViewModel.showPersonalizationEdit = true
      }
    }

    // Mark step as completed
    withAnimation(.spring(response: 0.4, dampingFraction: 0.8)) {
      switch step {
      case .sayHi: sayHiDone = true
      case .connectFriend: connectFriendDone = true
      case .writeDiary: writeDiaryDone = true
      case .tellStory: tellStoryDone = true
      }
    }

    // Auto-dismiss card when all steps are done
    if sayHiDone && connectFriendDone && writeDiaryDone && tellStoryDone {
      DispatchQueue.main.asyncAfter(deadline: .now() + 0.8) {
        withAnimation(.easeOut(duration: 0.3)) {
          getStartedDismissed = true
        }
      }
    }
  }
}

#Preview {
  @Previewable @State var tab: AppTab = .home
  HomeView(selectedTab: $tab, onStartNewChat: {})
    .environmentObject(SettingsViewModel())
    .environmentObject(ChatSessionsViewModel())
}
