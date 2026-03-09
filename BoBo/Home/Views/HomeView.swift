//
//  HomeView.swift
//  BoBo
//
//  Created by Stephan on 02.02.2026.
//

import SwiftUI
import UserNotifications

// MARK: - Feature Model

private enum HomeFeature: Int, CaseIterable, Identifiable {
  case chat
  case voiceMode
  case capturePhoto
  case customize

  var id: Int { rawValue }

  var title: String {
    switch self {
    case .chat: return "Someone Who Gets You"
    case .voiceMode: return "Talk, Don\u{2019}t Type"
    case .capturePhoto: return "Snap It, Keep It"
    case .customize: return "Make It Yours"
    }
  }

  var subtitle: String {
    switch self {
    case .chat:
      return "A buddy who remembers, listens, and actually gets you."
    case .voiceMode:
      return "Talk out loud — your buddy talks back."
    case .capturePhoto:
      return "Snap a photo and pair it with your entries."
    case .customize:
      return "Pick colors, themes, and wallpapers you love."
    }
  }

  var actionLabel: String {
    switch self {
    case .chat: return "Start Chat"
    case .voiceMode: return "Try Voice Mode"
    case .capturePhoto: return "New Entry"
    case .customize: return "Customize"
    }
  }

  var iconName: String {
    switch self {
    case .chat: return "bubble.left.and.bubble.right.fill"
    case .voiceMode: return "waveform"
    case .capturePhoto: return "camera.fill"
    case .customize: return "paintpalette.fill"
    }
  }

  var imageName: String {
    switch self {
    case .chat: return "HomeChat"
    case .voiceMode: return "HomeVoice"
    case .capturePhoto: return "HomePhoto"
    case .customize: return "HomeCustomize"
    }
  }

  var gradientColors: [Color] {
    switch self {
    case .chat:
      return [Color(red: 0.38, green: 0.30, blue: 0.92), Color(red: 0.58, green: 0.40, blue: 1.00)]
    case .voiceMode:
      return [Color(red: 0.95, green: 0.45, blue: 0.25), Color(red: 0.98, green: 0.65, blue: 0.30)]
    case .capturePhoto:
      return [Color(red: 0.20, green: 0.65, blue: 0.75), Color(red: 0.30, green: 0.80, blue: 0.70)]
    case .customize:
      return [Color(red: 0.63, green: 0.32, blue: 0.98), Color(red: 0.90, green: 0.40, blue: 0.65)]
    }
  }
}

// MARK: - Get Started Model

private enum GetStartedStep: Int, CaseIterable, Identifiable {
  case sayHi
  case connectFriend
  case writeDiary
  case customizeBuddy

  var id: Int { rawValue }

  func title(buddyName: String) -> String {
    switch self {
    case .sayHi: return "Break the Ice"
    case .connectFriend: return "Bring a Friend"
    case .writeDiary: return "Your First Entry"
    case .customizeBuddy: return "Make It Yours"
    }
  }

  func subtitleView(buddyName: String) -> Text {
    switch self {
    case .sayHi:
      return Text("Start your first chat and ") + Text("get to know your buddy").bold()
    case .connectFriend:
      return Text("Share your friend code to pair up, ") + Text("at no extra cost").bold()
    case .writeDiary:
      return Text("Start a daily diary — ") + Text("your buddy will help you reflect").bold()
    case .customizeBuddy:
      return Text("Pick a voice and personality — ") + Text("make them uniquely yours").bold()
    }
  }

  var iconName: String {
    switch self {
    case .sayHi: return "bubble.left.and.text.bubble.right.fill"
    case .connectFriend: return "person.2.fill"
    case .writeDiary: return "book.fill"
    case .customizeBuddy: return "wand.and.stars"
    }
  }

  var imageName: String {
    switch self {
    case .sayHi: return "GetStartedSayHi"
    case .connectFriend: return "GetStartedConnect"
    case .writeDiary: return "GetStartedDiary"
    case .customizeBuddy: return "GetStartedCustomize"
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
    case .customizeBuddy:
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
  var isCompleted: Bool = false

  var body: some View {
    VStack(alignment: .leading, spacing: 0) {
      ZStack {
        Image(step.imageName)
          .resizable()
          .aspectRatio(contentMode: .fill)

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
      .clipped()

      VStack(alignment: .leading, spacing: 4) {
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
    .overlay {
      if isCompleted {
        RoundedRectangle(cornerRadius: 14, style: .continuous)
          .fill(Color(.systemBackground).opacity(0.55))

        Circle()
          .fill(
            LinearGradient(
              colors: step.gradientColors,
              startPoint: .topLeading,
              endPoint: .bottomTrailing
            )
          )
          .frame(width: 28, height: 28)
          .overlay(
            Image(systemName: "checkmark")
              .font(.system(size: 12, weight: .bold))
              .foregroundStyle(.white)
          )
          .shadow(color: (step.gradientColors.first ?? .accentColor).opacity(0.4), radius: 6, x: 0, y: 2)
          .transition(.scale.combined(with: .opacity))
      }
    }
    .animation(.spring(response: 0.3, dampingFraction: 0.8), value: isCompleted)
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

// MARK: - Get Started Card

private struct GetStartedCardView: View {
  let buddyName: String
  let completedCount: Int
  let totalCount: Int
  let steps: [GetStartedStep]
  let completedStepIds: Set<Int>
  let onStepTap: (GetStartedStep) -> Void
  var onStepNavigate: ((GetStartedStep) -> Void)? = nil
  var onStepReset: ((GetStartedStep) -> Void)? = nil
  var onReset: (() -> Void)? = nil

  @State private var dashPhase: CGFloat = 0
  @State private var animatingStepId: Int? = nil
  @State private var lineFillProgress: CGFloat = 0
  @State private var checkpointVisible: Set<Int> = []
  @State private var showInfoTip: Bool = false

  private func handleCardTap(_ step: GetStartedStep) {
    guard !completedStepIds.contains(step.id) else { return }

    // Phase 1: Start line fill animation (dot by dot, fast & smooth)
    animatingStepId = step.id
    lineFillProgress = 0
    withAnimation(.easeInOut(duration: 0.5)) {
      lineFillProgress = 1
    }

    // Phase 2: After line fills, reveal checkpoint + mark complete
    DispatchQueue.main.asyncAfter(deadline: .now() + 0.5) {
      checkpointVisible.insert(step.id)
      onStepTap(step)
    }

    // Phase 3: After checkpoint animates, navigate
    DispatchQueue.main.asyncAfter(deadline: .now() + 0.95) {
      animatingStepId = nil
      lineFillProgress = 0
      onStepNavigate?(step)
    }
  }

  private let containerShape = RoundedRectangle(cornerRadius: 26, style: .continuous)

  var body: some View {
    VStack(alignment: .leading, spacing: 0) {
      // Milestone roadmap
      VStack(alignment: .leading, spacing: 12) {
        HStack(spacing: 0) {
          ForEach(Array(steps.enumerated()), id: \.element.id) { index, step in
            let done = completedStepIds.contains(step.id)
            let isAnimatingIn = animatingStepId == step.id
            let showFilled = done || checkpointVisible.contains(step.id)

            // Milestone node
            ZStack {
              if showFilled {
                Circle()
                  .fill((step.gradientColors.first ?? .accentColor).opacity(0.2))
                  .frame(width: 34, height: 34)
                  .transition(.scale.combined(with: .opacity))

                Circle()
                  .fill(
                    LinearGradient(
                      colors: step.gradientColors,
                      startPoint: .topLeading,
                      endPoint: .bottomTrailing
                    )
                  )
                  .frame(width: 24, height: 24)
                  .transition(.scale.combined(with: .opacity))

                Image(systemName: "checkmark")
                  .font(.system(size: 11, weight: .bold))
                  .foregroundStyle(.white)
                  .transition(.scale.combined(with: .opacity))
              } else {
                Circle()
                  .stroke(Color(.separator).opacity(0.5), lineWidth: 1.5)
                  .frame(width: 24, height: 24)
              }
            }
            .scaleEffect(isAnimatingIn && !showFilled ? 0.8 : 1.0)
            .animation(.spring(response: 0.3, dampingFraction: 0.65), value: showFilled)
            .frame(width: 34, height: 34)

            // Connector line
            if index < steps.count - 1 {
              let nextStep = steps[index + 1]
              let nextDone = completedStepIds.contains(nextStep.id)
              let bothDone = done && nextDone
              let isFillingToNext = animatingStepId == nextStep.id
              let targetColor = nextStep.gradientColors.first ?? .accentColor

              GeometryReader { geo in
                let midY = geo.size.height / 2

                // Base dotted line (always visible, dim)
                Path { path in
                  path.move(to: CGPoint(x: 0, y: midY))
                  path.addLine(to: CGPoint(x: geo.size.width, y: midY))
                }
                .stroke(
                  Color(.label).opacity(0.12),
                  style: StrokeStyle(
                    lineWidth: 2,
                    lineCap: .round,
                    dash: [2, 6],
                    dashPhase: dashPhase
                  )
                )

                // Colored dotted overlay, trimmed to reveal dot-by-dot
                Path { path in
                  path.move(to: CGPoint(x: 0, y: midY))
                  path.addLine(to: CGPoint(x: geo.size.width, y: midY))
                }
                .trim(from: 0, to: bothDone ? 1 : (isFillingToNext ? lineFillProgress : 0))
                .stroke(
                  targetColor.opacity(bothDone || isFillingToNext ? 0.8 : 0),
                  style: StrokeStyle(
                    lineWidth: 2,
                    lineCap: .round,
                    dash: [2, 6],
                    dashPhase: dashPhase
                  )
                )
              }
              .frame(height: 4)
            }
          }
        }
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
      .padding(.top, 8)
      .padding(.bottom, 12)

      // Arc carousel
      ArcCarouselView(
        steps: steps,
        completedStepIds: completedStepIds,
        buddyName: buddyName,
        onStepTap: handleCardTap,
        onStepReset: onStepReset
      )
      .padding(.bottom, 14)
    }
    .overlay(alignment: .bottomLeading) {
      Button {
        Haptics.impact(.light)
        withAnimation(.spring(response: 0.3, dampingFraction: 0.8)) {
          showInfoTip.toggle()
        }
        if showInfoTip {
          DispatchQueue.main.asyncAfter(deadline: .now() + 5.0) {
            withAnimation(.easeOut(duration: 0.3)) {
              showInfoTip = false
            }
          }
        }
      } label: {
        Image(systemName: "info.circle")
          .font(.system(size: 15, weight: .medium))
          .foregroundStyle(Color(.systemGray))
      }
      .padding(12)
      .overlay(alignment: .leading) {
        if showInfoTip {
          Text("Long press a card to undo")
            .font(.system(size: 12, weight: .medium))
            .foregroundStyle(Color(.systemGray))
            .padding(.horizontal, 10)
            .padding(.vertical, 6)
            .background(.ultraThinMaterial, in: Capsule())
            .fixedSize()
            .offset(x: 38)
            .transition(.opacity.combined(with: .scale(scale: 0.8, anchor: .leading)))
        }
      }
    }
    .clipShape(containerShape)
    .modifier(GlassContainerModifier(shape: containerShape))
    .onAppear {
      // Seed checkpointVisible with already-completed steps
      checkpointVisible = completedStepIds
      // Reset before starting to avoid stacking repeat-forever drivers
      dashPhase = 0
      DispatchQueue.main.async {
        withAnimation(.linear(duration: 1.0).repeatForever(autoreverses: false)) {
          dashPhase = -8
        }
      }
    }
    .onDisappear {
      // Cancel the repeating animation to free the main thread
      withAnimation(nil) {
        dashPhase = 0
      }
    }
    .onChange(of: completedStepIds) { _, newIds in
      // Remove checkpoints for steps that were reset
      let removed = checkpointVisible.subtracting(newIds)
      if !removed.isEmpty {
        withAnimation(.spring(response: 0.3, dampingFraction: 0.8)) {
          checkpointVisible.subtract(removed)
        }
      }
    }
  }
}

// MARK: - Glass Container Modifier

private struct GlassContainerModifier<S: Shape>: ViewModifier {
  let shape: S

  func body(content: Content) -> some View {
    if #available(iOS 26.0, *) {
      content
        .glassEffect(.regular.interactive(), in: shape)
    } else {
      content
        .background(AppTheme.surface)
        .overlay(
          shape
            .stroke(Color(.separator).opacity(0.2), lineWidth: 0.5)
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
  var onStepReset: ((GetStartedStep) -> Void)? = nil

  private let maxYDrop: CGFloat = 20
  private let maxRotation: Double = 5

  var body: some View {
    if !steps.isEmpty {
      ScrollView(.horizontal, showsIndicators: false) {
        HStack(alignment: .top, spacing: 12) {
          ForEach(Array(steps.enumerated()), id: \.element.id) { index, step in
            let done = completedStepIds.contains(step.id)
            Button { if !done { onStepTap(step) } } label: {
              GetStartedStepCardView(step: step, buddyName: buddyName, index: index, total: steps.count, isCompleted: done)
            }
            .buttonStyle(SpringPressStyle())
            .disabled(done)
            .overlay {
              if done {
                Color.clear
                  .contentShape(Rectangle())
                  .onLongPressGesture(minimumDuration: 0.5) {
                    Haptics.notification(.warning)
                    withAnimation(.spring(response: 0.4, dampingFraction: 0.8)) {
                      onStepReset?(step)
                    }
                  }
              }
            }
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
    ZStack {
      Image(feature.imageName)
        .resizable()
        .aspectRatio(contentMode: .fill)

      VStack {
        // Double chevron circle — top right
        HStack {
          Spacer()
          Image(systemName: "chevron.right.2")
            .font(.system(size: 14, weight: .bold))
            .foregroundStyle(.white)
            .offset(x: 1)
            .padding(10)
            .background {
              Circle()
                .fill(.ultraThinMaterial)
                .environment(\.colorScheme, .dark)
            }
        }

        Spacer()

        // Title + overview in glass pill — bottom left
        VStack(alignment: .leading, spacing: 3) {
          Text(feature.title)
            .font(.system(size: 16, weight: .bold))
            .foregroundStyle(.white)
          Text(feature.subtitle)
            .font(.system(size: 13, weight: .medium))
            .foregroundStyle(.white.opacity(0.85))
            .lineLimit(2)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .padding(.horizontal, 12)
        .padding(.vertical, 10)
        .background {
          RoundedRectangle(cornerRadius: 14, style: .continuous)
            .fill(.ultraThinMaterial)
            .environment(\.colorScheme, .dark)
        }
      }
      .padding(10)
    }
    .frame(height: 210)
    .clipShape(RoundedRectangle(cornerRadius: 24, style: .continuous))
    .overlay(
      RoundedRectangle(cornerRadius: 24, style: .continuous)
        .stroke(.white.opacity(0.12), lineWidth: 0.5)
    )
    .shadow(color: feature.gradientColors.first?.opacity(0.3) ?? .clear, radius: 12, x: 0, y: 6)
  }
}

// MARK: - Notifications View (pushed via navigationDestination)

private struct NotificationItem: Identifiable, Codable {
  let id: UUID
  let title: String
  let body: String
  let date: Date
  let icon: String
  let sessionId: UUID?
  let avatarURL: String?
  var isViewed: Bool

  init(title: String, body: String, date: Date, icon: String, sessionId: UUID? = nil, avatarURL: String? = nil, isViewed: Bool = false) {
    self.id = UUID()
    self.title = title
    self.body = body
    self.date = date
    self.icon = icon
    self.sessionId = sessionId
    self.avatarURL = avatarURL
    self.isViewed = isViewed
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

  @EnvironmentObject private var friendsViewModel: FriendsViewModel
  @ObservedObject private var apns = APNSService.shared
  @State private var notifications: [NotificationItem] = {
    guard let data = UserDefaults.standard.data(forKey: "cached_notifications"),
          let items = try? JSONDecoder().decode([NotificationItem].self, from: data) else { return [] }
    return items
  }()
  @State private var searchText: String = ""
  @State private var viewedIds: Set<String> = {
    Set(UserDefaults.standard.stringArray(forKey: "viewed_notification_ids") ?? [])
  }()

  private var groupedNotifications: [(group: NotificationDateGroup, items: [NotificationItem])] {
    let calendar = Calendar.current
    let now = Date()
    let startOfToday = calendar.startOfDay(for: now)
    let startOfYesterday = calendar.date(byAdding: .day, value: -1, to: startOfToday)!
    let startOfLast7 = calendar.date(byAdding: .day, value: -7, to: startOfToday)!

    var today: [NotificationItem] = []
    var yesterday: [NotificationItem] = []
    var last7: [NotificationItem] = []

    let filtered = notifications.filter { item in
      guard !searchText.isEmpty else { return true }
      let query = searchText.lowercased()
      return item.title.lowercased().contains(query) || item.body.lowercased().contains(query)
    }

    for item in filtered.sorted(by: { $0.date > $1.date }) {
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
              .padding(.horizontal, -16)
              .padding(.top, 4)
              .padding(.bottom, 4)
          }

          // Notification sections
          ForEach(groupedNotifications, id: \.group) { section in
            VStack(alignment: .leading, spacing: 0) {
              Text(section.group.rawValue)
                .font(.system(size: 13, weight: .semibold))
                .foregroundColor(Color(.tertiaryLabel))
                .textCase(.uppercase)
                .padding(.horizontal, 4)
                .padding(.bottom, 12)

              ForEach(Array(section.items.enumerated()), id: \.element.id) { index, item in
                if let sessionId = item.sessionId {
                  Button {
                    markAsViewed(item)
                    onTap(sessionId)
                  } label: {
                    notificationRow(item)
                  }
                  .buttonStyle(.plain)
                } else {
                  notificationRow(item)
                }

                Divider()
                  .padding(.leading, avatarSize + 12)
              }
            }
            .padding(.bottom, 8)
          }

        }
        .padding(.horizontal, 16)
        .padding(.top, 8)
        .padding(.bottom, 24)
      }

      if groupedNotifications.isEmpty && apns.isPushEnabled {
        VStack(spacing: 12) {
          if !searchText.isEmpty {
            Image(systemName: "magnifyingglass")
              .font(.system(size: 56))
              .foregroundStyle(Color(.tertiaryLabel))
            Text("No results for \"\(searchText)\"")
              .font(.system(size: 15, weight: .medium))
              .foregroundStyle(Color(.secondaryLabel))
          } else {
            Image(systemName: "bell.slash")
              .font(.system(size: 56))
              .foregroundStyle(Color(.tertiaryLabel))
            Text("No notifications yet")
              .font(.system(size: 15, weight: .medium))
              .foregroundStyle(Color(.secondaryLabel))
          }
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
      }
    }
    .navigationTitle("Notifications")
    .searchable(text: $searchText, prompt: "Search notifications")
    .onAppear { loadNotifications() }
    .onReceive(NotificationCenter.default.publisher(for: .partnerMessageReceived)) { _ in
      loadNotifications()
    }
    .onReceive(NotificationCenter.default.publisher(for: .partnerMessageUnsent)) { _ in
      loadNotifications()
    }
    .onReceive(NotificationCenter.default.publisher(for: .friendAdded)) { _ in
      loadNotifications()
    }
  }

  private func loadNotifications() {
    // 1. Build items from delivered OS push notifications.
    let friends = friendsViewModel.friends
    let allSessions = sessions
    let unreadSessionIds = Set(allSessions.filter { $0.unreadCount > 0 }.map(\.id))
    let previousItems = self.notifications

    UNUserNotificationCenter.current().getDeliveredNotifications { delivered in
      var items: [NotificationItem] = []
      var coveredSessionIds = Set<UUID>()

      for note in delivered {
        let content = note.request.content
        let userInfo = content.userInfo
        let date = note.date

        if let type = userInfo["type"] as? String, type == "friend_added" {
          items.append(NotificationItem(
            title: content.title,
            body: content.body,
            date: date,
            icon: "person.badge.plus"
          ))
        } else if let sidString = userInfo["session_id"] as? String,
                  let sid = UUID(uuidString: sidString) {
          let avatar = Self.resolveAvatarURL(forSessionId: sid, friends: friends)
          items.append(NotificationItem(
            title: content.title,
            body: content.body,
            date: date,
            icon: "bubble.left.fill",
            sessionId: sid,
            avatarURL: avatar
          ))
          coveredSessionIds.insert(sid)
        }
      }

      // 2. Add unread sessions that have no matching delivered notification.
      for session in allSessions where session.unreadCount > 0 && !coveredSessionIds.contains(session.id) {
        let date = Self.parseISO8601(session.lastUsedISO8601) ?? Date()
        let key = "session_friend_user_id_\(session.id.uuidString)"
        let friendId: UUID? = {
          guard let raw = UserDefaults.standard.string(forKey: key) else { return nil }
          return UUID(uuidString: raw)
        }()
        let friend = friendId.flatMap { fid in friends.first(where: { $0.id == fid }) }
        let friendName: String = {
          if let name = friend?.fullName.trimmingCharacters(in: .whitespacesAndNewlines), !name.isEmpty { return name }
          return UserDefaults.standard.string(forKey: PreferenceKeys.partnerName) ?? "Partner Message"
        }()
        items.append(NotificationItem(
          title: friendName,
          body: session.lastMessageContent ?? "",
          date: date,
          icon: "bubble.left.fill",
          sessionId: session.id,
          avatarURL: friend?.avatarURL
        ))
        coveredSessionIds.insert(session.id)
      }

      // 3. Preserve previously shown items whose sessions are now read — keep as viewed.
      for prev in previousItems {
        guard let sid = prev.sessionId, !coveredSessionIds.contains(sid) else { continue }
        var kept = prev
        kept.isViewed = true
        items.append(kept)
      }

      DispatchQueue.main.async {
        // Restore viewed state from persisted IDs + auto-mark read sessions
        for i in items.indices {
          if let sid = items[i].sessionId {
            if self.viewedIds.contains(sid.uuidString) || !unreadSessionIds.contains(sid) {
              items[i].isViewed = true
            }
          }
        }
        self.notifications = items
        if let data = try? JSONEncoder().encode(items) {
          UserDefaults.standard.set(data, forKey: "cached_notifications")
        }
      }
    }
  }

  private func markAsViewed(_ item: NotificationItem) {
    guard let sid = item.sessionId else { return }
    viewedIds.insert(sid.uuidString)
    UserDefaults.standard.set(Array(viewedIds), forKey: "viewed_notification_ids")
    if let idx = notifications.firstIndex(where: { $0.id == item.id }) {
      notifications[idx].isViewed = true
      if let data = try? JSONEncoder().encode(notifications) {
        UserDefaults.standard.set(data, forKey: "cached_notifications")
      }
    }
  }

  private static func resolveAvatarURL(forSessionId sid: UUID, friends: [FriendSummary]) -> String? {
    let key = "session_friend_user_id_\(sid.uuidString)"
    guard let raw = UserDefaults.standard.string(forKey: key),
          let friendId = UUID(uuidString: raw),
          let friend = friends.first(where: { $0.id == friendId }) else {
      // Fall back to default partner avatar
      return UserDefaults.standard.string(forKey: PreferenceKeys.partnerAvatarURL)
    }
    return friend.avatarURL
  }

  private static func parseISO8601(_ iso: String?) -> Date? {
    guard let iso, !iso.isEmpty else { return nil }
    let f1 = ISO8601DateFormatter()
    f1.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
    if let d = f1.date(from: iso) { return d }
    let f2 = ISO8601DateFormatter()
    f2.formatOptions = [.withInternetDateTime]
    return f2.date(from: iso)
  }

  private let avatarSize: CGFloat = 60

  @ViewBuilder
  private func notificationRow(_ item: NotificationItem) -> some View {
    HStack(alignment: .center, spacing: 12) {
      notificationAvatar(item)

      VStack(alignment: .leading, spacing: 6) {
        HStack(spacing: 6) {
          Text(item.title)
            .font(.system(size: 17, weight: .semibold))
            .foregroundColor(item.isViewed ? Color(.secondaryLabel) : .primary)
            .lineLimit(1)

          Spacer()

          if item.isViewed {
            HStack(spacing: 4) {
              Text("Viewed")
                .font(.system(size: 11, weight: .semibold))
                .foregroundColor(Color(.secondaryLabel))

              Text("·")
                .font(.system(size: 11, weight: .bold))
                .foregroundColor(Color(.tertiaryLabel))

              Text(timeAgoShort(item.date))
                .font(.system(size: 11, weight: .medium))
                .foregroundColor(Color(.tertiaryLabel))
            }
            .padding(.horizontal, 8)
            .padding(.vertical, 4)
            .background(Color(.tertiarySystemFill))
            .clipShape(RoundedRectangle(cornerRadius: 6, style: .continuous))
          } else {
            HStack(spacing: 4) {
              Text("Unread")
                .font(.system(size: 11, weight: .semibold))
                .foregroundColor(AppTheme.brand)

              Text("·")
                .font(.system(size: 11, weight: .bold))
                .foregroundColor(Color(.tertiaryLabel))

              Text(timeAgoShort(item.date))
                .font(.system(size: 11, weight: .medium))
                .foregroundColor(Color(.tertiaryLabel))
            }
            .padding(.horizontal, 8)
            .padding(.vertical, 4)
            .background(AppTheme.brand.opacity(0.12))
            .clipShape(RoundedRectangle(cornerRadius: 6, style: .continuous))
          }
        }

        if !item.body.isEmpty {
          Text(item.body)
            .font(.system(size: 15))
            .foregroundColor(item.isViewed ? Color(.tertiaryLabel) : Color(.secondaryLabel))
            .lineSpacing(4)
            .lineLimit(2)
            .truncationMode(.tail)
            .frame(maxWidth: .infinity, alignment: .leading)
        }
      }
    }
    .padding(.vertical, 10)
  }

  @ViewBuilder
  private func notificationAvatar(_ item: NotificationItem) -> some View {
    if let avatarURLString = item.avatarURL, !avatarURLString.isEmpty {
      AvatarCacheManager.shared.cachedAsyncImage(
        urlString: avatarURLString,
        placeholder: { AnyView(avatarPlaceholder(item)) },
        fallback: { AnyView(avatarPlaceholder(item)) }
      )
      .frame(width: avatarSize, height: avatarSize)
      .clipShape(Circle())
      .opacity(item.isViewed ? 0.5 : 1)
    } else {
      avatarPlaceholder(item)
        .opacity(item.isViewed ? 0.5 : 1)
    }
  }

  private func avatarPlaceholder(_ item: NotificationItem) -> some View {
    ZStack {
      Circle()
        .fill(Color(.tertiarySystemFill))
        .frame(width: avatarSize, height: avatarSize)

      if item.sessionId != nil {
        Text(String(item.title.prefix(1)).uppercased())
          .font(.system(size: 22, weight: .semibold, design: .rounded))
          .foregroundStyle(.secondary)
      } else {
        Image(systemName: item.icon)
          .font(.system(size: 20))
          .foregroundStyle(Color(.secondaryLabel))
      }
    }
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

  @State private var showSettings: Bool = false
  @State private var showNotifications: Bool = false
  @State private var showContacts: Bool = false
  @State private var showCustomizeBuddies: Bool = false
  @State private var showDiary: Bool = false

  @AppStorage("get_started_dismissed") private var getStartedDismissed: Bool = false
  @AppStorage("get_started_say_hi") private var sayHiDone: Bool = false
  @AppStorage("get_started_connect_friend") private var connectFriendDone: Bool = false
  @AppStorage("get_started_write_diary") private var writeDiaryDone: Bool = false
  @AppStorage("get_started_customize_buddy") private var customizeBuddyDone: Bool = false
  @AppStorage(PreferenceKeys.elevenLabsVoiceName) private var selectedVoiceName: String = ""

  private var buddyDisplayName: String {
    let name = selectedVoiceName.trimmingCharacters(in: .whitespacesAndNewlines)
    guard !name.isEmpty else { return "Buddy" }
    return name.prefix(1).uppercased() + name.dropFirst().lowercased()
  }

  private var completedStepCount: Int {
    [sayHiDone, connectFriendDone, writeDiaryDone, customizeBuddyDone].filter { $0 }.count
  }

  private var completedStepIds: Set<Int> {
    var ids = Set<Int>()
    if sayHiDone { ids.insert(GetStartedStep.sayHi.id) }
    if connectFriendDone { ids.insert(GetStartedStep.connectFriend.id) }
    if writeDiaryDone { ids.insert(GetStartedStep.writeDiary.id) }
    if customizeBuddyDone { ids.insert(GetStartedStep.customizeBuddy.id) }
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
      .fill(Color(.tertiarySystemFill))
      .overlay(
        Text(displayName.prefix(1).uppercased())
          .font(.system(size: 12, weight: .semibold, design: .rounded))
          .foregroundStyle(.secondary)
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
                onStepTap: markStepComplete,
                onStepNavigate: navigateToStep,
                onStepReset: resetStep,
                onReset: resetGetStarted
              )
              .transition(.opacity.combined(with: .move(edge: .top)))
            }

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
      .navigationDestination(isPresented: $showSettings) {
        SettingsView(
          embedded: true,
          isPresented: $showSettings
        )
      }
      .navigationDestination(isPresented: $showNotifications) {
        NotificationsView(
          sessions: sessionsVM.sessions,
          onTap: { sessionId in
            onOpenChat?(sessionId)
            // Pop notifications after chat overlay fully covers the screen
            DispatchQueue.main.asyncAfter(deadline: .now() + 0.25) {
              showNotifications = false
            }
          }
        )
      }
      .navigationDestination(isPresented: $showContacts) {
        FriendsAndContactsSectionView()
      }
      .navigationDestination(isPresented: $showCustomizeBuddies) {
        CustomizeBuddiesView()
      }
      .navigationDestination(isPresented: $showDiary) {
        DiaryView(embedded: true)
      }
      .toolbar {
        ToolbarItem(placement: .topBarLeading) {
          Button {
            Haptics.impact(.light)
            showSettings = true
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
          Button {
            Haptics.impact(.light)
            showSettings = true
          } label: {
            Text(displayName)
              .font(.system(size: 15, weight: .semibold))
              .foregroundStyle(Color(.label))
              .fixedSize()
          }
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
    case .chat:
      onStartNewChat()
    case .voiceMode:
      sessionsVM.shouldStartInVoiceMode = true
      onStartNewChat()
    case .capturePhoto:
      selectedTab = .diary
      DispatchQueue.main.asyncAfter(deadline: .now() + 0.3) {
        NotificationCenter.default.post(name: .openNewDiaryEntry, object: nil)
      }
    case .customize:
      showSettings = true
      DispatchQueue.main.asyncAfter(deadline: .now() + 0.3) {
        settingsViewModel.shouldHighlightCustomization = true
      }
    }
  }

  private func resetStep(_ step: GetStartedStep) {
    withAnimation(.spring(response: 0.4, dampingFraction: 0.8)) {
      switch step {
      case .sayHi: sayHiDone = false
      case .connectFriend: connectFriendDone = false
      case .writeDiary: writeDiaryDone = false
      case .customizeBuddy: customizeBuddyDone = false
      }
    }
  }

  private func resetGetStarted() {
    sayHiDone = false
    connectFriendDone = false
    writeDiaryDone = false
    customizeBuddyDone = false
    getStartedDismissed = false
  }

  private func markStepComplete(_ step: GetStartedStep) {
    Haptics.impact(.light)

    withAnimation(.spring(response: 0.3, dampingFraction: 0.8)) {
      switch step {
      case .sayHi: sayHiDone = true
      case .connectFriend: connectFriendDone = true
      case .writeDiary: writeDiaryDone = true
      case .customizeBuddy: customizeBuddyDone = true
      }
    }

    // Auto-dismiss card when all steps are done
    if sayHiDone && connectFriendDone && writeDiaryDone && customizeBuddyDone {
      DispatchQueue.main.asyncAfter(deadline: .now() + 1.2) {
        withAnimation(.easeOut(duration: 0.3)) {
          getStartedDismissed = true
        }
      }
    }
  }

  private func navigateToStep(_ step: GetStartedStep) {
    switch step {
    case .sayHi:
      onStartNewChat()
    case .connectFriend:
      showContacts = true
    case .writeDiary:
      showDiary = true
    case .customizeBuddy:
      showCustomizeBuddies = true
    }
  }
}

#Preview {
  @Previewable @State var tab: AppTab = .home
  HomeView(selectedTab: $tab, onStartNewChat: {})
    .environmentObject(SettingsViewModel())
    .environmentObject(ChatSessionsViewModel())
}
