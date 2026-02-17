//
//  DiaryView.swift
//  TalkToMe
//
//  Created by Stephan on 02.02.2026.
//

import PhotosUI
import SwiftUI
import UIKit
import UniformTypeIdentifiers

struct DiaryView: View {
  @StateObject private var viewModel = DiaryViewModel()
  @State private var tab: JournalTab = .overview
  @State private var newEntrySession: NewEntrySession?
  @State private var showDiaryEditor: Bool = false
  @State private var editEntrySession: EditEntrySession?
  @State private var draftTitle: String = ""
  @State private var draftBody: String = ""
  @State private var deleteErrorMessage: String?
  @State private var isKeyboardVisible: Bool = false

  var body: some View {
    NavigationStack {
      ZStack(alignment: .bottomTrailing) {
        AppTheme.background.ignoresSafeArea()

        VStack(spacing: 0) {
          DiaryHeroCardView(
            title: diaryDisplayTitle,
            subtitle: "\(viewModel.displayTodayString) • \(viewModel.displayYearString)",
            gradientColors: heroGradientColors
          )

          JournalSheetView(
            tab: $tab,
            description: viewModel.diaryDescription,
            stats: viewModel.stats,
            entries: $viewModel.entries,
            accentColor: viewModel.diaryColor,
            onEditDiary: { showDiaryEditor = true },
            onAddEntryForDate: { date in
              newEntrySession = NewEntrySession(initialDate: date)
            },
            onSelectEntry: { entry in
              editEntrySession = EditEntrySession(entry: entry)
            },
            onDeleteEntry: { entry in
              deleteEntry(entry)
            }
          )
          .frame(maxWidth: .infinity, maxHeight: .infinity)
          .padding(.top, -24)
        }
        .padding(.top, 0)
        .padding(.bottom, 0)
        .ignoresSafeArea(edges: [.top, .bottom])

        GlassFloatingActionButton(systemName: "plus") {
          Haptics.impact(.light)
          newEntrySession = NewEntrySession(initialDate: Date())
        }
        .opacity(isKeyboardVisible ? 0 : 1)
        .scaleEffect(isKeyboardVisible ? 0.92 : 1)
        .offset(y: isKeyboardVisible ? 22 : 0)
        .allowsHitTesting(!isKeyboardVisible)
        .accessibilityHidden(isKeyboardVisible)
        .animation(.easeInOut(duration: 0.2), value: isKeyboardVisible)
        .padding(.trailing, 18)
        .padding(.bottom, 50)
      }
      .navigationTitle("")
      .navigationBarTitleDisplayMode(.inline)
      .toolbar {
        ToolbarItem(placement: .topBarTrailing) {
          Button("Edit") {
            Haptics.impact(.light)
            showDiaryEditor = true
          }
          .font(.system(size: 17, weight: .semibold))
        }
      }
    }
    .sheet(isPresented: $showDiaryEditor) {
      DiaryEditorSheet(
        name: viewModel.diaryName,
        description: viewModel.diaryDescription,
        color: viewModel.diaryColor,
        onSave: { name, description, color in
          viewModel.saveSettings(name: name, description: description, color: color)
        }
      )
      .presentationDetents([.medium, .large])
      .presentationDragIndicator(.visible)
    }
    .sheet(item: $newEntrySession) { session in
      NewDiaryNoteSheet(
        accentColor: viewModel.diaryColor,
        initialDate: session.initialDate,
        draftTitle: draftTitle,
        draftBody: draftBody,
        onAdd: { draft in
          viewModel.addEntry(draft)
          draftTitle = ""
          draftBody = ""
        },
        onSaveDraft: { title, body in
          draftTitle = title
          draftBody = body
        }
      )
      .presentationDetents([.large])
      .presentationDragIndicator(.visible)
    }
    .sheet(item: $editEntrySession) { session in
      EditDiaryNoteSheet(
        entry: session.entry,
        accentColor: viewModel.diaryColor,
        onSave: { updated in
          if let idx = viewModel.entries.firstIndex(where: { $0.id == updated.id }) {
            viewModel.entries[idx] = updated
          }
          viewModel.loadIfNeeded()
          editEntrySession = nil
        },
        onDelete: {
          viewModel.entries.removeAll { $0.id == session.entry.id }
          editEntrySession = nil
        }
      )
      .presentationDetents([.large])
      .presentationDragIndicator(.visible)
    }
    .onAppear { viewModel.loadIfNeeded() }
    .onChange(of: AuthService.shared.currentUserId) { _, _ in viewModel.loadIfNeeded() }
    .onReceive(NotificationCenter.default.publisher(for: UIResponder.keyboardWillShowNotification)) { _ in
      isKeyboardVisible = true
    }
    .onReceive(NotificationCenter.default.publisher(for: UIResponder.keyboardWillHideNotification)) { _ in
      isKeyboardVisible = false
    }
    .alert(
      "Couldn't delete note",
      isPresented: Binding(
        get: { deleteErrorMessage != nil },
        set: { if !$0 { deleteErrorMessage = nil } }
      )
    ) {
      Button("OK", role: .cancel) { deleteErrorMessage = nil }
    } message: {
      if let msg = deleteErrorMessage { Text(msg) }
    }
  }

  private var diaryDisplayTitle: String {
    let trimmed = viewModel.diaryName.trimmingCharacters(in: .whitespacesAndNewlines)
    return trimmed.isEmpty ? "My Diary" : trimmed
  }

  private var heroGradientColors: [Color] {
    return [
      viewModel.diaryColor.opacity(0.98),
      viewModel.diaryColor.opacity(0.94),
      viewModel.diaryColor.opacity(0.88),
      AppTheme.brand.opacity(0.86),
      AppTheme.brand.opacity(0.80),
    ]
  }

  private func deleteEntry(_ entry: JournalEntry) {
    Task {
      guard let userId = AuthService.shared.currentUserId, let uid = UUID(uuidString: userId) else {
        await MainActor.run {
          deleteErrorMessage = "Sign in to delete this note."
        }
        return
      }
      do {
        try await DiaryService.shared.deleteEntry(userId: uid, entryId: entry.id)
        await MainActor.run {
          viewModel.entries.removeAll { $0.id == entry.id }
          if editEntrySession?.entry.id == entry.id {
            editEntrySession = nil
          }
        }
      } catch {
        await MainActor.run {
          deleteErrorMessage = DiaryService.userFacingMessage(from: error)
        }
      }
    }
  }
}

#Preview {
  DiaryView()
}

private extension Color {
  func blended(with other: Color, amount: CGFloat) -> Color {
    let t = min(max(amount, 0), 1)

    let a = UIColor(self)
    let b = UIColor(other)

    var ar: CGFloat = 0
    var ag: CGFloat = 0
    var ab: CGFloat = 0
    var aa: CGFloat = 0
    var br: CGFloat = 0
    var bg: CGFloat = 0
    var bb: CGFloat = 0
    var ba: CGFloat = 0

    guard a.getRed(&ar, green: &ag, blue: &ab, alpha: &aa),
      b.getRed(&br, green: &bg, blue: &bb, alpha: &ba)
    else {
      return self
    }

    return Color(
      uiColor: UIColor(
        red: ar + (br - ar) * t,
        green: ag + (bg - ag) * t,
        blue: ab + (bb - ab) * t,
        alpha: aa + (ba - aa) * t
      )
    )
  }
}

private struct DiaryHeroCardView: View {
  @Environment(\.colorScheme) private var colorScheme
  let title: String
  let subtitle: String
  let gradientColors: [Color]

  private var baseGradientColors: [Color] {
    [
      AppTheme.brand.opacity(0.98),
      AppTheme.brand.opacity(0.94),
      AppTheme.brand.opacity(0.88),
      AppTheme.accent.opacity(0.86),
      AppTheme.accent.opacity(0.80),
    ]
  }

  private var styledGradientColors: [Color] {
    let source = gradientColors.isEmpty ? baseGradientColors : gradientColors
    if colorScheme == .dark {
      return [
        source[0].blended(with: .black, amount: 0.34),
        source[1].blended(with: AppTheme.accent, amount: 0.32),
        source[2].blended(with: .black, amount: 0.20),
        source[3].blended(with: Color(red: 0.20, green: 0.32, blue: 0.50), amount: 0.44),
        source[4].blended(with: .black, amount: 0.30),
      ]
    }

    return [
      source[0].blended(with: Color(red: 0.99, green: 0.88, blue: 0.74), amount: 0.24),
      source[1].blended(with: Color(red: 0.70, green: 0.86, blue: 1.0), amount: 0.24),
      source[2].blended(with: Color(red: 0.96, green: 0.74, blue: 0.80), amount: 0.22),
      source[3].blended(with: .white, amount: 0.12),
      source[4].blended(with: Color(red: 0.63, green: 0.81, blue: 0.99), amount: 0.20),
    ]
  }

  var body: some View {
    let source = styledGradientColors
    let c0 = source[0]
    let c1 = source[min(1, source.count - 1)]
    let c2 = source[min(2, source.count - 1)]
    let c3 = source[min(3, source.count - 1)]
    let c4 = source[min(4, source.count - 1)]
    let m12a = c1.blended(with: c2, amount: 0.25)
    let m12b = c1.blended(with: c2, amount: 0.70)
    let m23 = c2.blended(with: c3, amount: 0.50)

    ZStack {
      LinearGradient(
        gradient: Gradient(stops: [
          .init(color: c0, location: 0.0),
          .init(color: c1, location: 0.22),
          .init(color: m12a, location: 0.36),
          .init(color: m12b, location: 0.52),
          .init(color: m23, location: 0.68),
          .init(color: c3, location: 0.84),
          .init(color: c4, location: 1.0),
        ]),
        startPoint: .leading,
        endPoint: .trailing
      )

      LinearGradient(
        colors: [
          .white.opacity(colorScheme == .dark ? 0.04 : 0.16),
          .clear,
          .black.opacity(colorScheme == .dark ? 0.14 : 0.05),
        ],
        startPoint: .topLeading,
        endPoint: .bottomTrailing
      )

      FloatingOrb(
        size: 190,
        color: .white.opacity(colorScheme == .dark ? 0.22 : 0.34),
        startOffset: CGSize(width: -118, height: -36),
        drift: CGSize(width: 20, height: 18),
        blurRadius: 1.5,
        duration: 8.0
      )

      FloatingOrb(
        size: 120,
        color: AppTheme.brand.opacity(colorScheme == .dark ? 0.18 : 0.28),
        startOffset: CGSize(width: -18, height: 68),
        drift: CGSize(width: -14, height: -18),
        blurRadius: 0.8,
        duration: 6.7
      )

      FloatingOrb(
        size: 108,
        color: .white.opacity(colorScheme == .dark ? 0.18 : 0.30),
        startOffset: CGSize(width: 126, height: 54),
        drift: CGSize(width: -18, height: -20),
        blurRadius: 1.0,
        duration: 7.4
      )

      VStack(alignment: .leading, spacing: 12) {
        HStack(spacing: 10) {
          Image(systemName: "book.closed.fill")
            .font(.system(size: 22, weight: .semibold))
            .foregroundStyle(.white.opacity(0.95))
          Text(title)
            .font(.system(size: 32, weight: .bold))
            .lineLimit(1)
            .foregroundStyle(.white)
        }

        Text(subtitle)
          .font(.system(size: 14, weight: .semibold))
          .foregroundStyle(.white.opacity(0.94))
      }
      .padding(.horizontal, 18)
      .padding(.top, 92)
      .padding(.bottom, 22)
      .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }
    .frame(height: 260)
    .ignoresSafeArea(edges: .top)
  }
}

private struct FloatingOrb: View {
  let size: CGFloat
  let color: Color
  let startOffset: CGSize
  let drift: CGSize
  let blurRadius: CGFloat
  let duration: Double

  @State private var animate: Bool = false

  var body: some View {
    Circle()
      .fill(
        RadialGradient(
          colors: [color, color.opacity(0.5), .clear],
          center: .center,
          startRadius: 0,
          endRadius: size * 0.62
        )
      )
      .frame(width: size, height: size)
      .blur(radius: blurRadius)
      .offset(
        x: startOffset.width + (animate ? drift.width : 0),
        y: startOffset.height + (animate ? drift.height : 0)
      )
      .onAppear { animate = true }
      .animation(.easeInOut(duration: duration).repeatForever(autoreverses: true), value: animate)
      .allowsHitTesting(false)
  }
}

// MARK: - View model + models

private enum JournalTab: Hashable {
  case overview
  case list
  case calendar
  case todo
}

private struct JournalEntry: Identifiable, Hashable {
  let id: UUID
  var date: Date
  var title: String
  var body: String
  var createdAt: Date
  var timezoneAbbreviation: String
  /// Number of photo blocks in this entry (from body_blocks).
  var photoCount: Int
  /// First photo URL in this entry (signed by backend), if available.
  var firstPhotoURL: String?

  var excerpt: String {
    let trimmed = body.trimmingCharacters(in: .whitespacesAndNewlines)
    if trimmed.isEmpty { return "—" }
    let singleLine =
      trimmed
      .replacingOccurrences(of: "\n", with: " ")
      .replacingOccurrences(of: "\t", with: " ")
    return String(singleLine.prefix(120))
  }
}

private struct JournalStats: Hashable {
  var streakDays: Int
  var entriesCount: Int
  var mediaCount: Int
  var uniqueDaysCount: Int
  var onThisDayCount: Int
}

private struct DiaryTodoItem: Identifiable, Codable, Hashable {
  var id: UUID
  var title: String
  var isCompleted: Bool

  init(id: UUID = UUID(), title: String, isCompleted: Bool = false) {
    self.id = id
    self.title = title
    self.isCompleted = isCompleted
  }
}

@MainActor
private final class DiaryViewModel: ObservableObject {
  @Published var diaryName: String = "My Diary"
  @Published var diaryDescription: String = ""
  @Published var diaryColor: Color = AppTheme.brand
  @Published var entries: [JournalEntry] = []
  @Published var isLoading: Bool = false
  @Published var loadError: String?

  init() {}

  func loadIfNeeded() {
    guard let userId = AuthService.shared.currentUserId, let uid = UUID(uuidString: userId) else {
      entries = []
      return
    }
    Task {
      await loadSettings(userId: uid)
      await loadEntries(userId: uid)
    }
  }

  private func loadSettings(userId: UUID) async {
    do {
      let (name, desc, hex) = try await DiaryService.shared.fetchSettings(userId: userId)
      await MainActor.run {
        diaryName = name
        diaryDescription = desc
        diaryColor = Color(hex: hex) ?? AppTheme.brand
      }
    } catch {
      await MainActor.run { loadError = error.localizedDescription }
    }
  }

  private func loadEntries(userId: UUID) async {
    await MainActor.run { isLoading = true }
    defer { Task { @MainActor in isLoading = false } }
    do {
      let rows = try await DiaryService.shared.fetchEntries(userId: userId)
      let list = rows.compactMap { row -> JournalEntry? in
        guard let date = DiaryService.date(from: row.date) else { return nil }
        let body = DiaryService.textContentFromBodyBlocks(row.body_blocks)
        let createdAt = (row.created_at).flatMap { ISO8601DateFormatter().date(from: $0) } ?? Date()
        let photoCount = row.body_blocks.filter { $0["type"] == "image" }.count
        let firstPhotoURL = row.body_blocks.first { block in
          guard block["type"] == "image" else { return false }
          let trimmed = (block["url"] ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
          return !trimmed.isEmpty
        }?["url"]
        return JournalEntry(
          id: row.id,
          date: date,
          title: row.title,
          body: body,
          createdAt: createdAt,
          timezoneAbbreviation: row.timezone_abbreviation,
          photoCount: photoCount,
          firstPhotoURL: firstPhotoURL
        )
      }
      await MainActor.run {
        entries = list
        loadError = nil
      }
    } catch {
      await MainActor.run {
        loadError = error.localizedDescription
        entries = []
      }
    }
  }

  func saveSettings(name: String, description: String, color: Color) {
    guard let userId = AuthService.shared.currentUserId, let uid = UUID(uuidString: userId) else {
      return
    }
    let hex = color.hexString
    Task {
      do {
        try await DiaryService.shared.upsertSettings(
          userId: uid, name: name, description: description, headerColorHex: hex)
        await MainActor.run {
          diaryName = name
          diaryDescription = description
          diaryColor = color
        }
      } catch {
        await MainActor.run { loadError = error.localizedDescription }
      }
    }
  }

  var displayYearString: String {
    let year = Calendar.current.component(.year, from: Date())
    return "\(year)"
  }

  var displayTodayString: String {
    let f = DateFormatter()
    f.dateFormat = "EEEE, MMM d"
    return f.string(from: Date())
  }

  var stats: JournalStats {
    let entriesCount = entries.count

    let cal = Calendar.current
    let uniqueDays = Set(entries.map { cal.startOfDay(for: $0.date) })

    let today = cal.startOfDay(for: Date())
    let onThisDayCount = entries.filter { entry in
      let d = entry.date
      return cal.component(.month, from: d) == cal.component(.month, from: today)
        && cal.component(.day, from: d) == cal.component(.day, from: today)
        && cal.component(.year, from: d) != cal.component(.year, from: today)
    }.count

    // Simple local streak: consecutive days ending today with >= 1 entry/day.
    var streak = 0
    var cursor = today
    while uniqueDays.contains(cursor) {
      streak += 1
      guard let prev = cal.date(byAdding: .day, value: -1, to: cursor) else { break }
      cursor = prev
    }

    let mediaCount = entries.reduce(0) { $0 + $1.photoCount }

    return JournalStats(
      streakDays: streak,
      entriesCount: entriesCount,
      mediaCount: mediaCount,
      uniqueDaysCount: uniqueDays.count,
      onThisDayCount: onThisDayCount
    )
  }

  func addEntry(_ draft: NewJournalEntryDraft) {
    guard let userId = AuthService.shared.currentUserId, let uid = UUID(uuidString: userId) else {
      return
    }
    let tz = TimeZone.current.abbreviation() ?? "UTC"
    let title =
      draft.title.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty ? "Untitled" : draft.title

    // If the entry was already saved to Supabase (e.g. with image blocks), insert locally only.
    if let existingId = draft.entryId {
      let entry = JournalEntry(
        id: existingId,
        date: draft.date,
        title: title,
        body: draft.body,
        createdAt: Date(),
        timezoneAbbreviation: tz,
        photoCount: draft.photoCount,
        firstPhotoURL: nil
      )
      entries.insert(entry, at: 0)
      Task { await loadEntries(userId: uid) }
      return
    }

    // Text-only fallback path.
    let payload: [DiaryBlockPayload] = [.text(id: UUID(), content: draft.body)]
    Task {
      do {
        let entryId = try await DiaryService.shared.saveEntry(
          userId: uid,
          entryId: nil,
          date: draft.date,
          title: title,
          bodyBlocks: payload,
          timezoneAbbreviation: tz
        )
        let entry = JournalEntry(
          id: entryId,
          date: draft.date,
          title: title,
          body: draft.body,
          createdAt: Date(),
          timezoneAbbreviation: tz,
          photoCount: 0,
          firstPhotoURL: nil
        )
        await MainActor.run { entries.insert(entry, at: 0) }
        await loadEntries(userId: uid)
      } catch {
        await MainActor.run { loadError = error.localizedDescription }
      }
    }
  }
}

// MARK: - Journal sheet + tabs

private struct JournalSheetView: View {
  @Binding var tab: JournalTab
  let description: String
  let stats: JournalStats
  @Binding var entries: [JournalEntry]
  let accentColor: Color
  let onEditDiary: () -> Void
  let onAddEntryForDate: (Date) -> Void
  let onSelectEntry: (JournalEntry) -> Void
  let onDeleteEntry: (JournalEntry) -> Void

  @State private var selectedCalendarDay: Date? = Calendar.current.startOfDay(for: Date())

  var body: some View {
    VStack(spacing: 0) {
      JournalTabBar(tab: $tab)

      Divider()
        .opacity(0.22)

      Group {
        switch tab {
        case .overview:
          JournalOverviewTab(
            description: description,
            stats: stats,
            accentColor: accentColor,
            onEditDiary: onEditDiary
          )
        case .list:
          JournalListTab(
            entries: entries,
            onSelectEntry: onSelectEntry,
            onDeleteEntry: onDeleteEntry
          )
        case .calendar:
          JournalCalendarTab(
            entries: $entries,
            accentColor: accentColor,
            selectedDay: $selectedCalendarDay,
            onAddEntryForDate: onAddEntryForDate,
            onSelectEntry: onSelectEntry
          )
        case .todo:
          JournalTodoTab()
        }
      }
      .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .top)
    }
    .background(AppTheme.surface)
    .clipShape(
      UnevenRoundedRectangle(
        topLeadingRadius: 30,
        bottomLeadingRadius: 0,
        bottomTrailingRadius: 0,
        topTrailingRadius: 30,
        style: .continuous
      )
    )
    .overlay(
      UnevenRoundedRectangle(
        topLeadingRadius: 30,
        bottomLeadingRadius: 0,
        bottomTrailingRadius: 0,
        topTrailingRadius: 30,
        style: .continuous
      )
        .stroke(Color(.separator).opacity(0.2), lineWidth: 0.5)
    )
    .shadow(color: Color.black.opacity(0.04), radius: 6, x: 0, y: -1)
  }
}

private struct JournalTabBar: View {
  @Binding var tab: JournalTab
  @State private var isKeyboardVisible: Bool = false

  var body: some View {
    HStack(spacing: 8) {
      tabButton(.overview, title: "Overview")
      tabButton(.list, title: "List")
      tabButton(.calendar, title: "Calendar")
      tabButton(.todo, title: "To-Do")
    }
    .padding(.horizontal, 10)
    .padding(.top, 18)
    .padding(.bottom, 10)
    .onReceive(NotificationCenter.default.publisher(for: UIResponder.keyboardWillShowNotification)) { _ in
      isKeyboardVisible = true
    }
    .onReceive(NotificationCenter.default.publisher(for: UIResponder.keyboardWillHideNotification)) { _ in
      isKeyboardVisible = false
    }
  }

  private func tabButton(_ value: JournalTab, title: String) -> some View {
    let isActive = tab == value

    return Button {
      guard tab != value else { return }
      Haptics.impact(.light)

      if isKeyboardVisible {
        UIApplication.shared.sendAction(
          #selector(UIResponder.resignFirstResponder),
          to: nil,
          from: nil,
          for: nil
        )
        DispatchQueue.main.asyncAfter(deadline: .now() + 0.16) {
          withAnimation(.spring(response: 0.26, dampingFraction: 0.78)) {
            tab = value
          }
        }
      } else {
        withAnimation(.spring(response: 0.26, dampingFraction: 0.78)) {
          tab = value
        }
      }
    } label: {
      Text(title)
        .font(.system(size: 11, weight: .semibold))
        .lineLimit(1)
        .minimumScaleFactor(0.76)
      .foregroundStyle(isActive ? Color.white : Color(.secondaryLabel))
      .padding(.horizontal, 8)
      .padding(.vertical, 9)
      .frame(maxWidth: .infinity)
      .background(
        Group {
          if isActive {
            LinearGradient(
              colors: [AppTheme.brand, AppTheme.accent],
              startPoint: .leading,
              endPoint: .trailing
            )
          } else {
            Color(.tertiarySystemBackground)
          }
        }
      )
      .clipShape(Capsule())
      .overlay(
        Capsule()
          .stroke(Color(.separator).opacity(isActive ? 0.0 : 0.35), lineWidth: 0.5)
      )
    }
    .buttonStyle(.plain)
  }
}

// MARK: - Overview tab

private struct JournalOverviewTab: View {
  let description: String
  let stats: JournalStats
  let accentColor: Color
  let onEditDiary: () -> Void

  var body: some View {
    ScrollView {
      VStack(alignment: .leading, spacing: 14) {
        JournalDescriptionCard(
          description: description,
          accentColor: accentColor,
          onEditDiary: onEditDiary
        )

        VStack(alignment: .leading, spacing: 10) {
          Text("Statistics")
            .font(.system(size: 20, weight: .bold))
            .foregroundStyle(.primary)
            .padding(.horizontal, 4)

          JournalStatisticsCards(stats: stats, accentColor: accentColor)
        }

        Spacer().frame(height: 120)
      }
      .padding(.horizontal, 16)
      .padding(.top, 16)
    }
    .scrollIndicators(.hidden)
  }
}

private struct JournalDescriptionCard: View {
  let description: String
  let accentColor: Color
  let onEditDiary: () -> Void

  private var trimmedDescription: String {
    description.trimmingCharacters(in: .whitespacesAndNewlines)
  }

  var body: some View {
    Button {
      Haptics.impact(.light)
      onEditDiary()
    } label: {
      HStack(alignment: .top, spacing: 14) {
        ZStack {
          RoundedRectangle(cornerRadius: 12, style: .continuous)
            .fill(
              LinearGradient(
                colors: [accentColor.opacity(0.95), AppTheme.brand.opacity(0.8)],
                startPoint: .topLeading,
                endPoint: .bottomTrailing
              )
            )
          Image(systemName: "quote.bubble.fill")
            .font(.system(size: 14, weight: .semibold))
            .foregroundStyle(.white)
        }
        .frame(width: 34, height: 34)
        .padding(.top, 2)

        VStack(alignment: .leading, spacing: 6) {
          Text("Description")
            .font(.system(size: 16, weight: .semibold))
            .foregroundStyle(.primary)

          Text(
            trimmedDescription.isEmpty
              ? "Add a short description for your diary."
              : trimmedDescription
          )
          .font(.system(size: 16, weight: .regular))
          .lineSpacing(2)
          .foregroundStyle(trimmedDescription.isEmpty ? .secondary : .primary)
          .lineLimit(nil)
          .fixedSize(horizontal: false, vertical: true)
          .frame(maxWidth: .infinity, alignment: .leading)
        }

        Image(systemName: "chevron.right")
          .font(.system(size: 13, weight: .bold))
          .foregroundStyle(.tertiary)
          .padding(.top, 5)
      }
      .padding(.horizontal, 16)
      .padding(.vertical, 15)
      .background(AppTheme.surface)
      .clipShape(RoundedRectangle(cornerRadius: 18, style: .continuous))
      .overlay(
        RoundedRectangle(cornerRadius: 18, style: .continuous)
          .stroke(Color(.separator).opacity(0.2), lineWidth: 0.5)
      )
    }
    .buttonStyle(.plain)
  }
}

private struct JournalStatisticsCards: View {
  let stats: JournalStats
  let accentColor: Color

  private let spacing: CGFloat = 12
  private let smallCardHeight: CGFloat = 78

  var body: some View {
    GeometryReader { proxy in
      let availableWidth = max(proxy.size.width, 0)
      let bigCardWidth = min(max(availableWidth * 0.38, 122), 148)
      let bigCardHeight = smallCardHeight * 2 + spacing

      HStack(alignment: .top, spacing: spacing) {
        GlassStatBigCard(
          title: "STREAK",
          value: "\(stats.streakDays)",
          subtitle: "Days",
          accentColor: accentColor
        )
        .frame(width: bigCardWidth, height: bigCardHeight)

        VStack(spacing: spacing) {
          HStack(spacing: spacing) {
            GlassStatSmallCard(
              title: "ENTRIES", value: "\(stats.entriesCount)", accentColor: accentColor)
            GlassStatSmallCard(
              title: "MEDIA", value: "\(stats.mediaCount)", accentColor: accentColor)
          }
          HStack(spacing: spacing) {
            GlassStatSmallCard(
              title: "DAYS", value: "\(stats.uniqueDaysCount)", accentColor: accentColor)
            GlassStatSmallCard(
              title: "ON THIS DAY", value: "\(stats.onThisDayCount)", accentColor: accentColor)
          }
        }
        .frame(maxWidth: .infinity)
      }
      .frame(width: availableWidth, alignment: .leading)
    }
    .frame(height: smallCardHeight * 2 + spacing)
  }
}

private struct StatCardButtonStyle: ButtonStyle {
  func makeBody(configuration: Configuration) -> some View {
    configuration.label
      .scaleEffect(configuration.isPressed ? 0.96 : 1.0)
      .opacity(configuration.isPressed ? 0.92 : 1.0)
      .animation(.easeInOut(duration: 0.2), value: configuration.isPressed)
  }
}

private struct GlassStatBigCard: View {
  let title: String
  let value: String
  let subtitle: String
  let accentColor: Color

  var body: some View {
    let shape = RoundedRectangle(cornerRadius: 18, style: .continuous)
    Button {
    } label: {
      VStack(spacing: 0) {
        HStack(spacing: 6) {
          Image(systemName: "flame.fill")
            .font(.system(size: 11, weight: .bold))
            .foregroundStyle(accentColor)

          Text(title)
            .font(.system(size: 11, weight: .semibold))
            .foregroundStyle(.secondary)
        }

        Spacer(minLength: 0)

        Text(value)
          .font(.system(size: 44, weight: .bold, design: .rounded))
          .monospacedDigit()
          .foregroundStyle(.primary)

        Text(subtitle)
          .font(.system(size: 15, weight: .semibold))
          .foregroundStyle(.secondary)

        Spacer(minLength: 0)
      }
      .padding(16)
      .frame(maxWidth: .infinity, maxHeight: .infinity)
      .background { GlassCardBackground(shape: shape) }
      .overlay { shape.stroke(accentColor.opacity(0.2), lineWidth: 1) }
      .clipShape(shape)
      .shadow(color: Color.black.opacity(0.05), radius: 6, x: 0, y: 3)
    }
    .buttonStyle(StatCardButtonStyle())
  }
}

private struct GlassStatSmallCard: View {
  let title: String
  let value: String
  let accentColor: Color

  var body: some View {
    let shape = RoundedRectangle(cornerRadius: 18, style: .continuous)
    Button {
    } label: {
      VStack(alignment: .leading, spacing: 8) {
        Text(title)
          .font(.system(size: 10, weight: .semibold))
          .lineLimit(2)
          .minimumScaleFactor(0.75)
          .foregroundStyle(.secondary)
          .frame(maxWidth: .infinity, alignment: .leading)

        Text(value)
          .font(.system(size: 22, weight: .bold, design: .rounded))
          .monospacedDigit()
          .foregroundStyle(.primary)

        Spacer(minLength: 0)
      }
      .padding(14)
      .frame(maxWidth: .infinity, maxHeight: .infinity)
      .background { GlassCardBackground(shape: shape) }
      .overlay { shape.stroke(Color(.separator).opacity(0.2), lineWidth: 0.5) }
      .clipShape(shape)
    }
    .buttonStyle(StatCardButtonStyle())
  }
}

private struct GlassCardBackground<S: Shape>: View {
  let shape: S
  var interactive: Bool = true

  var body: some View {
    if #available(iOS 26.0, *) {
      Color.clear
        .glassEffect(interactive ? .regular.interactive() : .regular, in: shape)
    } else {
      shape
        .fill(.ultraThinMaterial)
    }
  }
}

// MARK: - List tab

private struct JournalListTab: View {
  let entries: [JournalEntry]
  let onSelectEntry: (JournalEntry) -> Void
  let onDeleteEntry: (JournalEntry) -> Void
  @State private var pendingDeleteEntry: JournalEntry?
  @State private var showDeleteEntryConfirm: Bool = false

  private var grouped: [(key: String, value: [JournalEntry])] {
    let formatter = DateFormatter()
    formatter.dateFormat = "LLLL yyyy"
    let dict = Dictionary(grouping: entries) { entry in
      formatter.string(from: entry.date)
    }
    // Keep stable chronological ordering (newest month first).
    let monthFormatter = DateFormatter()
    monthFormatter.dateFormat = "LLLL yyyy"
    let sortedKeys = dict.keys.sorted { a, b in
      guard let da = monthFormatter.date(from: a), let db = monthFormatter.date(from: b) else {
        return a > b
      }
      return da > db
    }
    return sortedKeys.map { ($0, (dict[$0] ?? []).sorted { $0.date > $1.date }) }
  }

  var body: some View {
    ScrollView {
      if entries.isEmpty {
        VStack(spacing: 10) {
          Spacer().frame(height: 36)
          Image(systemName: "book.closed")
            .font(.system(size: 28, weight: .semibold))
            .foregroundStyle(.secondary)
          Text("No entries yet")
            .font(.system(size: 16, weight: .semibold))
          Text("Tap + to add your first entry.")
            .font(.system(size: 14))
            .foregroundStyle(.secondary)
        }
        .frame(maxWidth: .infinity)
      } else {
        LazyVStack(alignment: .leading, spacing: 10) {
          ForEach(grouped, id: \.key) { section in
            Text(section.key)
              .font(.system(size: 14, weight: .semibold))
              .foregroundStyle(.secondary)
              .padding(.horizontal, 18)
              .padding(.top, 16)

            ForEach(section.value) { entry in
              JournalEntryRowLink(
                entry: entry,
                onSelect: onSelectEntry,
                onLongPress: {
                  Haptics.impact(.light)
                  pendingDeleteEntry = entry
                  showDeleteEntryConfirm = true
                },
                onDelete: onDeleteEntry
              )
                .padding(.horizontal, 18)
            }
          }
          Spacer().frame(height: 120)
        }
      }
    }
    .scrollIndicators(.hidden)
    .confirmationDialog(
      "Delete note?",
      isPresented: $showDeleteEntryConfirm,
      titleVisibility: .visible
    ) {
      Button("Delete", role: .destructive) {
        guard let entry = pendingDeleteEntry else { return }
        onDeleteEntry(entry)
        pendingDeleteEntry = nil
      }
      Button("Cancel", role: .cancel) {
        pendingDeleteEntry = nil
      }
    } message: {
      Text("This will permanently delete the note.")
    }
  }
}

// MARK: - ToDo list tab

private let diaryTodoStorageKey = "diary_todo_items"
private let diaryDefaultTodoItems = [
  DiaryTodoItem(title: "Plan tomorrow's top priority")
]

private struct JournalTodoTab: View {
  @State private var todoItems: [DiaryTodoItem] = []
  @State private var newTodoTitle: String = ""
  @State private var activeReorderItemId: UUID?
  @State private var activeReorderTranslationY: CGFloat = 0
  @State private var reorderReferenceTranslationY: CGFloat = 0
  @FocusState private var isAddFieldFocused: Bool

  // Swap only after the dragged row has passed roughly one full row+gap distance.
  private let todoReorderSwapDistance: CGFloat = 72

  private var completedCount: Int {
    todoItems.filter(\.isCompleted).count
  }

  private var pendingCount: Int {
    max(0, todoItems.count - completedCount)
  }

  private var completionRatio: CGFloat {
    guard !todoItems.isEmpty else { return 0 }
    return CGFloat(completedCount) / CGFloat(todoItems.count)
  }

  var body: some View {
    ScrollView {
      VStack(alignment: .leading, spacing: 12) {
        todoSummaryCard
        addTaskCard

        if todoItems.isEmpty {
          emptyStateCard
        } else {
          LazyVStack(spacing: 10) {
            ForEach($todoItems) { $item in
              TodoRowView(
                item: $item,
                isBeingReordered: activeReorderItemId == item.id,
                onDelete: {
                  removeTodo(id: item.id)
                }
              )
              .frame(maxWidth: .infinity, alignment: .leading)
              .contentShape(Rectangle())
              .offset(y: activeReorderItemId == item.id ? activeReorderTranslationY : 0)
              .zIndex(activeReorderItemId == item.id ? 1 : 0)
              .simultaneousGesture(reorderGesture(for: item.id))
            }
          }
          .padding(.top, 2)
        }

        Spacer().frame(height: 120)
      }
      .padding(.horizontal, 16)
      .padding(.top, 14)
    }
    .scrollIndicators(.hidden)
    .background(AppTheme.background)
    .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .top)
    .onAppear { loadTodos() }
    .onChange(of: todoItems) { _, _ in saveTodos() }
  }

  private var todoSummaryCard: some View {
    let shape = RoundedRectangle(cornerRadius: 20, style: .continuous)

    return ZStack(alignment: .topLeading) {
      LinearGradient(
        colors: [AppTheme.brand.opacity(0.24), AppTheme.accent.opacity(0.2), Color.white.opacity(0.06)],
        startPoint: .topLeading,
        endPoint: .bottomTrailing
      )

      Circle()
        .fill(.white.opacity(0.22))
        .frame(width: 138, height: 138)
        .offset(x: -45, y: -64)

      Circle()
        .fill(.white.opacity(0.12))
        .frame(width: 92, height: 92)
        .offset(x: 260, y: 20)

      VStack(alignment: .leading, spacing: 14) {
        HStack(alignment: .top, spacing: 12) {
          VStack(alignment: .leading, spacing: 3) {
            Text("To-Do")
              .font(.system(size: 24, weight: .bold))
              .foregroundStyle(.primary)

            Text(
              pendingCount == 0
                ? "All tasks are done."
                : "\(pendingCount) \(pendingCount == 1 ? "task" : "tasks") pending"
            )
            .font(.system(size: 14, weight: .semibold))
            .foregroundStyle(.secondary)
          }

          Spacer(minLength: 8)

          ZStack {
            Circle()
              .stroke(Color.primary.opacity(0.12), lineWidth: 5)
            Circle()
              .trim(from: 0, to: completionRatio)
              .stroke(
                AngularGradient(
                  colors: [AppTheme.accent, AppTheme.brand, AppTheme.accent],
                  center: .center
                ),
                style: StrokeStyle(lineWidth: 5, lineCap: .round)
              )
              .rotationEffect(.degrees(-90))
          }
          .frame(width: 46, height: 46)
          .accessibilityLabel("Task completion")
          .accessibilityValue("\(Int((completionRatio * 100).rounded())) percent")
        }

        HStack(spacing: 8) {
          todoMetricPill(
            icon: "circle",
            title: "Pending",
            value: "\(pendingCount)",
            tint: AppTheme.accent
          )
          todoMetricPill(
            icon: "checkmark.circle.fill",
            title: "Done",
            value: "\(completedCount)",
            tint: Color.green.opacity(0.85)
          )
        }
      }
      .padding(16)
    }
    .frame(maxWidth: .infinity)
    .clipShape(shape)
    .overlay(
      shape
        .stroke(Color(.separator).opacity(0.2), lineWidth: 0.6)
    )
    .shadow(color: .black.opacity(0.04), radius: 8, x: 0, y: 2)
  }

  private func todoMetricPill(icon: String, title: String, value: String, tint: Color) -> some View {
    HStack(spacing: 8) {
      Image(systemName: icon)
        .font(.system(size: 11, weight: .semibold))
        .foregroundStyle(tint)

      Text(title)
        .font(.system(size: 12, weight: .semibold))
        .foregroundStyle(.secondary)

      Text(value)
        .font(.system(size: 12, weight: .bold, design: .rounded))
        .foregroundStyle(.primary)
        .monospacedDigit()
    }
    .padding(.horizontal, 10)
    .padding(.vertical, 8)
    .background(Color(.systemBackground).opacity(0.7))
    .clipShape(Capsule())
    .overlay(
      Capsule()
        .stroke(Color(.separator).opacity(0.18), lineWidth: 0.5)
    )
  }

  private var addTaskCard: some View {
    let trimmed = newTodoTitle.trimmingCharacters(in: .whitespacesAndNewlines)
    let canAdd = !trimmed.isEmpty
    let shape = RoundedRectangle(cornerRadius: 18, style: .continuous)

    return HStack(spacing: 10) {
      ZStack {
        Circle()
          .fill(
            LinearGradient(
              colors: [AppTheme.brand, AppTheme.accent],
              startPoint: .topLeading,
              endPoint: .bottomTrailing
            )
          )
        Image(systemName: "sparkles")
          .font(.system(size: 13, weight: .semibold))
          .foregroundStyle(.white)
      }
      .frame(width: 30, height: 30)

      TextField("Add a task", text: $newTodoTitle)
        .font(.system(size: 16, weight: .regular))
        .focused($isAddFieldFocused)
        .submitLabel(.done)
        .onSubmit { addTodo() }

      Button {
        addTodo()
      } label: {
        Image(systemName: "plus")
          .font(.system(size: 15, weight: .bold))
          .foregroundStyle(canAdd ? .white : .secondary)
          .frame(width: 34, height: 34)
          .background(
            Group {
              if canAdd {
                LinearGradient(
                  colors: [AppTheme.brand, AppTheme.accent],
                  startPoint: .topLeading,
                  endPoint: .bottomTrailing
                )
              } else {
                Color(.tertiarySystemFill)
              }
            }
          )
          .clipShape(Circle())
      }
      .buttonStyle(.plain)
      .disabled(!canAdd)
    }
    .padding(.horizontal, 14)
    .padding(.vertical, 13)
    .background { GlassCardBackground(shape: shape) }
    .clipShape(shape)
    .overlay(
      shape
        .stroke(Color(.separator).opacity(0.22), lineWidth: 0.6)
    )
  }

  private var emptyStateCard: some View {
    let shape = RoundedRectangle(cornerRadius: 20, style: .continuous)

    return VStack(spacing: 8) {
      Spacer().frame(height: 40)
      Image(systemName: "checklist.checked")
        .font(.system(size: 26, weight: .medium))
        .foregroundStyle(AppTheme.accent.opacity(0.8))
      Text("No tasks yet")
        .font(.system(size: 17, weight: .semibold))
        .foregroundStyle(.primary)
      Text("Add your first task to start a clean, focused plan.")
        .font(.system(size: 14, weight: .medium))
        .multilineTextAlignment(.center)
        .foregroundStyle(.secondary)
        .padding(.horizontal, 22)
      Spacer().frame(height: 34)
    }
    .frame(maxWidth: .infinity)
    .background {
      LinearGradient(
        colors: [AppTheme.surface, AppTheme.brand.opacity(0.07)],
        startPoint: .topLeading,
        endPoint: .bottomTrailing
      )
    }
    .clipShape(shape)
    .overlay(
      shape
        .stroke(Color(.separator).opacity(0.2), lineWidth: 0.6)
    )
  }

  private func addTodo() {
    let trimmed = newTodoTitle.trimmingCharacters(in: .whitespacesAndNewlines)
    guard !trimmed.isEmpty else { return }
    Haptics.impact(.light)
    withAnimation(.spring(response: 0.28, dampingFraction: 0.82)) {
      todoItems.insert(DiaryTodoItem(title: trimmed), at: 0)
    }
    newTodoTitle = ""
  }

  private func removeTodo(id: UUID) {
    Haptics.impact(.light)
    withAnimation(.easeInOut(duration: 0.2)) {
      todoItems.removeAll { $0.id == id }
    }
  }

  private func loadTodos() {
    let defaults = UserDefaults.standard
    guard let data = defaults.data(forKey: diaryTodoStorageKey),
      let decoded = try? JSONDecoder().decode([DiaryTodoItem].self, from: data)
    else {
      if defaults.object(forKey: diaryTodoStorageKey) == nil {
        todoItems = diaryDefaultTodoItems
      }
      return
    }
    todoItems = decoded
  }

  private func saveTodos() {
    guard let data = try? JSONEncoder().encode(todoItems) else { return }
    UserDefaults.standard.set(data, forKey: diaryTodoStorageKey)
  }

  private func reorderGesture(for itemId: UUID) -> some Gesture {
    LongPressGesture(minimumDuration: 0.25)
      .sequenced(before: DragGesture(minimumDistance: 0, coordinateSpace: .global))
      .onChanged { value in
        switch value {
        case .first(true):
          beginReorderIfNeeded(for: itemId)
        case .second(true, let drag?):
          beginReorderIfNeeded(for: itemId)
          updateReorder(translationY: drag.translation.height)
        default:
          break
        }
      }
      .onEnded { _ in
        endReorder()
      }
  }

  private func beginReorderIfNeeded(for itemId: UUID) {
    guard activeReorderItemId == nil else { return }
    guard todoItems.firstIndex(where: { $0.id == itemId }) != nil else { return }

    activeReorderItemId = itemId
    reorderReferenceTranslationY = 0
    activeReorderTranslationY = 0
    Haptics.impact(.light)
  }

  private func updateReorder(translationY: CGFloat) {
    guard let activeId = activeReorderItemId else { return }
    guard todoItems.firstIndex(where: { $0.id == activeId }) != nil else { return }

    // Move one slot only after crossing a full swap distance.
    var deltaFromReference = translationY - reorderReferenceTranslationY
    while deltaFromReference >= todoReorderSwapDistance {
      guard moveActiveTodo(by: 1) else { break }
      reorderReferenceTranslationY += todoReorderSwapDistance
      deltaFromReference = translationY - reorderReferenceTranslationY
    }
    while deltaFromReference <= -todoReorderSwapDistance {
      guard moveActiveTodo(by: -1) else { break }
      reorderReferenceTranslationY -= todoReorderSwapDistance
      deltaFromReference = translationY - reorderReferenceTranslationY
    }

    // Keep only the residual movement after each swap so dragging feels responsive.
    activeReorderTranslationY = translationY - reorderReferenceTranslationY
  }

  private func moveActiveTodo(by direction: Int) -> Bool {
    guard (direction == 1 || direction == -1), let activeId = activeReorderItemId else { return false }
    guard let fromIndex = todoItems.firstIndex(where: { $0.id == activeId }) else { return false }
    let toIndex = fromIndex + direction
    guard (0..<todoItems.count).contains(toIndex) else { return false }

    withAnimation(.easeInOut(duration: 0.1)) {
      todoItems.move(
        fromOffsets: IndexSet(integer: fromIndex),
        toOffset: toIndex > fromIndex ? toIndex + 1 : toIndex
      )
    }
    return true
  }

  private func endReorder() {
    guard activeReorderItemId != nil else { return }

    withAnimation(.spring(response: 0.22, dampingFraction: 0.9)) {
      activeReorderTranslationY = 0
    }

    DispatchQueue.main.asyncAfter(deadline: .now() + 0.12) {
      activeReorderItemId = nil
      reorderReferenceTranslationY = 0
      activeReorderTranslationY = 0
    }
  }
}

private struct TodoRowView: View {
  @Binding var item: DiaryTodoItem
  var isBeingReordered: Bool = false
  var onDelete: (() -> Void)?

  @State private var isSlidingOut: Bool = false
  private let slideOutDuration: Double = 0.28

  var body: some View {
    let shape = RoundedRectangle(cornerRadius: 16, style: .continuous)

    HStack(alignment: .center, spacing: 12) {
      Button {
        Haptics.impact(.light)
        withAnimation(.spring(response: 0.25, dampingFraction: 0.82)) {
          item.isCompleted.toggle()
        }
      } label: {
        ZStack {
          Circle()
            .fill(item.isCompleted ? Color.green.opacity(0.18) : Color.clear)
            .frame(width: 28, height: 28)

          Image(systemName: item.isCompleted ? "checkmark.circle.fill" : "circle")
            .font(.system(size: 20, weight: .semibold))
            .foregroundStyle(item.isCompleted ? Color.green.opacity(0.9) : Color.primary.opacity(0.26))
            .symbolRenderingMode(.hierarchical)
        }
      }
      .buttonStyle(.plain)

      Text(item.title)
        .font(.system(size: 16, weight: .medium))
        .strikethrough(item.isCompleted, color: item.isCompleted ? Color.secondary : nil)
        .foregroundStyle(item.isCompleted ? .secondary : .primary)
        .frame(maxWidth: .infinity, alignment: .leading)
        .multilineTextAlignment(.leading)

      if let onDelete = onDelete {
        if item.isCompleted {
          Button {
            withAnimation(.easeIn(duration: slideOutDuration)) {
              isSlidingOut = true
            }
            DispatchQueue.main.asyncAfter(deadline: .now() + slideOutDuration) {
              onDelete()
            }
          } label: {
            Image(systemName: "trash.fill")
              .font(.system(size: 14, weight: .semibold))
              .foregroundStyle(Color.red.opacity(0.9))
              .frame(width: 28, height: 28)
              .background(Color.red.opacity(0.1))
              .clipShape(Circle())
          }
          .buttonStyle(.plain)
          .disabled(isBeingReordered)
        } else {
          Menu {
            Button(role: .destructive) {
              onDelete()
            } label: {
              Label("Delete Task", systemImage: "trash")
            }
          } label: {
            Image(systemName: "ellipsis.circle")
              .font(.system(size: 18, weight: .semibold))
              .foregroundStyle(.secondary.opacity(0.8))
          }
          .disabled(isBeingReordered)
        }
      }
    }
    .padding(.horizontal, 14)
    .padding(.vertical, 12)
    .background { GlassCardBackground(shape: shape) }
    .clipShape(shape)
    .overlay(
      shape
        .stroke(
          isBeingReordered
            ? AppTheme.accent.opacity(0.78)
            : (item.isCompleted ? Color.green.opacity(0.2) : Color(.separator).opacity(0.22)),
          lineWidth: isBeingReordered ? 1.3 : 0.6
        )
    )
    .shadow(
      color: isBeingReordered ? AppTheme.accent.opacity(0.24) : .black.opacity(0.03),
      radius: isBeingReordered ? 12 : 4,
      x: 0,
      y: isBeingReordered ? 6 : 1
    )
    .scaleEffect(isBeingReordered ? 1.015 : 1)
    .frame(maxWidth: .infinity, alignment: .leading)
    .contentShape(Rectangle())
    .offset(x: isSlidingOut ? -UIScreen.main.bounds.width : 0)
    .opacity(isSlidingOut ? 0 : 1)
    .animation(.easeIn(duration: slideOutDuration), value: isSlidingOut)
  }
}

private struct JournalEntryRowLink: View {
  let entry: JournalEntry
  let onSelect: (JournalEntry) -> Void
  let onLongPress: (() -> Void)?
  let onDelete: ((JournalEntry) -> Void)?

  init(
    entry: JournalEntry,
    onSelect: @escaping (JournalEntry) -> Void,
    onLongPress: (() -> Void)? = nil,
    onDelete: ((JournalEntry) -> Void)? = nil
  ) {
    self.entry = entry
    self.onSelect = onSelect
    self.onLongPress = onLongPress
    self.onDelete = onDelete
  }

  var body: some View {
    JournalListRowContent(entry: entry)
      .contentShape(Rectangle())
      .onTapGesture {
        Haptics.impact(.light)
        onSelect(entry)
      }
      .onLongPressGesture(minimumDuration: 0.4) {
        onLongPress?()
      }
      .contextMenu {
        if let onDelete {
          Button(role: .destructive) {
            Haptics.impact(.light)
            onDelete(entry)
          } label: {
            Label("Delete", systemImage: "trash")
          }
        }
      }
  }
}

private struct JournalListRowContent: View {
  let entry: JournalEntry

  private var weekdayText: String {
    let f = DateFormatter()
    f.dateFormat = "EEE"
    return f.string(from: entry.date).uppercased()
  }

  private var dayNumberText: String {
    let day = Calendar.current.component(.day, from: entry.date)
    return "\(day)"
  }

  private var timeText: String {
    let f = DateFormatter()
    f.dateFormat = "HH:mm"
    return "\(f.string(from: entry.createdAt)) \(entry.timezoneAbbreviation)"
  }

  var body: some View {
    HStack(alignment: .top, spacing: 14) {
      VStack(spacing: 6) {
        Text(weekdayText)
          .font(.system(size: 12, weight: .semibold))
          .foregroundStyle(.secondary)
        Text(dayNumberText)
          .font(.system(size: 20, weight: .bold, design: .rounded))
          .foregroundStyle(.primary)
          .monospacedDigit()
      }
      .frame(width: 44, alignment: .leading)

      VStack(alignment: .leading, spacing: 6) {
        Text(entry.title)
          .font(.system(size: 16, weight: .semibold))
          .foregroundStyle(.primary)
          .lineLimit(1)

        Text(entry.excerpt + (entry.excerpt.count < entry.body.count ? "…" : ""))
          .font(.system(size: 14))
          .foregroundStyle(.secondary)
          .lineLimit(2)

        Text(timeText)
          .font(.system(size: 12, weight: .regular))
          .foregroundStyle(.secondary)
          .padding(.top, 2)
      }

      Spacer(minLength: 0)

      Image(systemName: "chevron.right")
        .font(.system(size: 13, weight: .semibold))
        .foregroundStyle(.tertiary)
        .padding(.top, 4)
    }
    .padding(.horizontal, 14)
    .padding(.vertical, 12)
    .background(AppTheme.surface)
    .clipShape(RoundedRectangle(cornerRadius: 16, style: .continuous))
    .overlay(
      RoundedRectangle(cornerRadius: 16, style: .continuous)
        .stroke(Color(.separator).opacity(0.2), lineWidth: 0.5)
    )
    .contentShape(Rectangle())
  }
}

// MARK: - Calendar tab

private struct JournalCalendarTab: View {
  @Binding var entries: [JournalEntry]
  let accentColor: Color
  @Binding var selectedDay: Date?
  let onAddEntryForDate: (Date) -> Void
  let onSelectEntry: (JournalEntry) -> Void

  @State private var showDaySheet: Bool = false
  @State private var sheetDay: Date?

  private let cal = Calendar.current

  private var dayVisuals: [Date: CalendarDayVisual] {
    var grouped: [Date: [JournalEntry]] = [:]
    for entry in entries {
      let day = cal.startOfDay(for: entry.date)
      grouped[day, default: []].append(entry)
    }

    var result: [Date: CalendarDayVisual] = [:]
    for (day, dayEntries) in grouped {
      let sorted = dayEntries.sorted { $0.createdAt < $1.createdAt }
      let firstPhotoURL = sorted.compactMap { entry -> String? in
        guard let url = entry.firstPhotoURL?.trimmingCharacters(in: .whitespacesAndNewlines),
          !url.isEmpty
        else { return nil }
        return url
      }.first
      let hasPhoto = sorted.contains { $0.photoCount > 0 || (($0.firstPhotoURL ?? "").isEmpty == false) }
      result[day] = CalendarDayVisual(
        notesCount: dayEntries.count,
        hasPhoto: hasPhoto,
        firstPhotoURL: firstPhotoURL
      )
    }
    return result
  }

  private var daysWithEntries: Set<Date> {
    Set(dayVisuals.keys)
  }

  private func entries(on day: Date) -> [JournalEntry] {
    let s = cal.startOfDay(for: day)
    return
      entries
      .filter { cal.startOfDay(for: $0.date) == s }
      .sorted { $0.createdAt > $1.createdAt }
  }

  private var selectedDayText: String {
    guard let selectedDay else { return "Select a day" }
    let f = DateFormatter()
    f.dateFormat = "EEEE, MMM d"
    return f.string(from: selectedDay)
  }

  var body: some View {
    ScrollView {
      VStack(spacing: 12) {
        calendarSummaryCard

        MonthCalendarScrollView(
          dayVisuals: dayVisuals,
          accentColor: accentColor,
          selectedDay: $selectedDay,
          onDayTapped: { date in
            let normalizedDay = cal.startOfDay(for: date)
            sheetDay = normalizedDay
            showDaySheet = true
          }
        )
        .padding(.top, 2)

        if let selectedDay {
          let dayEntries = entries(on: selectedDay)
          if dayEntries.isEmpty {
            selectedDayEmptyCard(for: selectedDay)
          } else {
            selectedDayEntriesCard(day: selectedDay, dayEntries: dayEntries)
          }
        }

        Spacer().frame(height: 120)
      }
      .padding(.horizontal, 16)
      .padding(.top, 14)
    }
    .scrollIndicators(.hidden)
    .background(AppTheme.background)
    .sheet(isPresented: $showDaySheet) {
      if let day = sheetDay {
        CalendarDaySheet(
          date: day,
          entries: $entries,
          accentColor: accentColor,
          onAddEntry: {
            showDaySheet = false
            DispatchQueue.main.asyncAfter(deadline: .now() + 0.35) {
              onAddEntryForDate(day)
            }
          },
          onSelectEntry: { entry in
            showDaySheet = false
            DispatchQueue.main.asyncAfter(deadline: .now() + 0.35) {
              onSelectEntry(entry)
            }
          }
        )
        .presentationDetents([.medium, .large])
        .presentationDragIndicator(.visible)
      }
    }
  }

  private var calendarSummaryCard: some View {
    let shape = RoundedRectangle(cornerRadius: 20, style: .continuous)
    let selectedCount = selectedDay.map { entries(on: $0).count } ?? 0

    return ZStack(alignment: .topLeading) {
      LinearGradient(
        colors: [AppTheme.brand.opacity(0.22), AppTheme.accent.opacity(0.18), Color.white.opacity(0.05)],
        startPoint: .topLeading,
        endPoint: .bottomTrailing
      )

      Circle()
        .fill(.white.opacity(0.2))
        .frame(width: 132, height: 132)
        .offset(x: -45, y: -58)

      Circle()
        .fill(.white.opacity(0.1))
        .frame(width: 90, height: 90)
        .offset(x: 255, y: 24)

      VStack(alignment: .leading, spacing: 14) {
        HStack(alignment: .top, spacing: 12) {
          VStack(alignment: .leading, spacing: 3) {
            Text("Calendar")
              .font(.system(size: 24, weight: .bold))
              .foregroundStyle(.primary)

            Text(selectedDayText)
              .font(.system(size: 14, weight: .semibold))
              .foregroundStyle(.secondary)
          }

          Spacer(minLength: 8)

          ZStack {
            Circle()
              .fill(
                LinearGradient(
                  colors: [AppTheme.brand, AppTheme.accent],
                  startPoint: .topLeading,
                  endPoint: .bottomTrailing
                )
              )
            Image(systemName: "calendar")
              .font(.system(size: 15, weight: .semibold))
              .foregroundStyle(.white)
          }
          .frame(width: 42, height: 42)
        }

        HStack(spacing: 8) {
          calendarMetricPill(
            icon: "checklist.checked",
            title: "Logged Days",
            value: "\(daysWithEntries.count)",
            tint: AppTheme.accent
          )
          calendarMetricPill(
            icon: "book.closed.fill",
            title: "Selected",
            value: "\(selectedCount)",
            tint: accentColor
          )
        }
      }
      .padding(16)
    }
    .frame(maxWidth: .infinity)
    .clipShape(shape)
    .overlay(
      shape
        .stroke(Color(.separator).opacity(0.2), lineWidth: 0.6)
    )
    .shadow(color: .black.opacity(0.04), radius: 8, x: 0, y: 2)
  }

  private func calendarMetricPill(icon: String, title: String, value: String, tint: Color) -> some View {
    HStack(spacing: 8) {
      Image(systemName: icon)
        .font(.system(size: 11, weight: .semibold))
        .foregroundStyle(tint)

      Text(title)
        .font(.system(size: 12, weight: .semibold))
        .foregroundStyle(.secondary)

      Text(value)
        .font(.system(size: 12, weight: .bold, design: .rounded))
        .foregroundStyle(.primary)
        .monospacedDigit()
    }
    .padding(.horizontal, 10)
    .padding(.vertical, 8)
    .background(Color(.systemBackground).opacity(0.7))
    .clipShape(Capsule())
    .overlay(
      Capsule()
        .stroke(Color(.separator).opacity(0.18), lineWidth: 0.5)
    )
  }

  private func selectedDayEntriesCard(day: Date, dayEntries: [JournalEntry]) -> some View {
    let shape = RoundedRectangle(cornerRadius: 20, style: .continuous)
    let f = DateFormatter()
    f.dateFormat = "MMMM d"

    return VStack(alignment: .leading, spacing: 10) {
      HStack(alignment: .center, spacing: 8) {
        Text("On this day")
          .font(.system(size: 16, weight: .semibold))
          .foregroundStyle(.primary)

        Text(f.string(from: day))
          .font(.system(size: 13, weight: .semibold))
          .foregroundStyle(.secondary)

        Spacer(minLength: 8)

        Text("\(dayEntries.count)")
          .font(.system(size: 12, weight: .bold, design: .rounded))
          .monospacedDigit()
          .foregroundStyle(.primary)
          .padding(.horizontal, 10)
          .padding(.vertical, 6)
          .background(Color(.tertiarySystemFill))
          .clipShape(Capsule())
      }
      .padding(.horizontal, 14)
      .padding(.top, 14)

      VStack(spacing: 10) {
        ForEach(dayEntries) { entry in
          JournalEntryRowLink(entry: entry, onSelect: onSelectEntry)
        }
      }
      .padding(.horizontal, 12)
      .padding(.bottom, 12)
    }
    .background { GlassCardBackground(shape: shape) }
    .clipShape(shape)
    .overlay(
      shape
        .stroke(Color(.separator).opacity(0.2), lineWidth: 0.6)
    )
  }

  private func selectedDayEmptyCard(for day: Date) -> some View {
    let shape = RoundedRectangle(cornerRadius: 20, style: .continuous)
    let f = DateFormatter()
    f.dateFormat = "MMMM d"
    let isFutureDay = cal.startOfDay(for: day) > cal.startOfDay(for: Date())

    return VStack(spacing: 8) {
      Spacer().frame(height: 28)
      Image(systemName: isFutureDay ? "calendar.badge.exclamationmark" : "calendar.badge.plus")
        .font(.system(size: 24, weight: .medium))
        .foregroundStyle((isFutureDay ? Color.orange : AppTheme.accent).opacity(0.86))
      Text("No entries on \(f.string(from: day))")
        .font(.system(size: 16, weight: .semibold))
        .foregroundStyle(.primary)
      Text(
        isFutureDay
          ? "You can't add a note for this date yet because that day hasn't happened."
          : "Tap a date to add a note and start building your timeline."
      )
        .font(.system(size: 14, weight: .medium))
        .multilineTextAlignment(.center)
        .foregroundStyle(.secondary)
        .padding(.horizontal, 20)
      Spacer().frame(height: 24)
    }
    .frame(maxWidth: .infinity)
    .background {
      LinearGradient(
        colors: [AppTheme.surface, AppTheme.brand.opacity(0.07)],
        startPoint: .topLeading,
        endPoint: .bottomTrailing
      )
    }
    .clipShape(shape)
    .overlay(
      shape
        .stroke(Color(.separator).opacity(0.2), lineWidth: 0.6)
    )
  }
}

private struct CalendarDayVisual {
  let notesCount: Int
  let hasPhoto: Bool
  let firstPhotoURL: String?
}

private struct MonthCalendarScrollView: View {
  let dayVisuals: [Date: CalendarDayVisual]
  let accentColor: Color
  @Binding var selectedDay: Date?
  var onDayTapped: ((Date) -> Void)?

  private let cal = Calendar.current

  private var monthsToShow: [Date] {
    let now = Date()
    let start = cal.date(byAdding: .month, value: -2, to: now) ?? now
    let end = cal.date(byAdding: .month, value: 6, to: now) ?? now

    var months: [Date] = []
    var cursor = cal.date(from: cal.dateComponents([.year, .month], from: start)) ?? start
    let endMonth = cal.date(from: cal.dateComponents([.year, .month], from: end)) ?? end
    while cursor <= endMonth {
      months.append(cursor)
      cursor = cal.date(byAdding: .month, value: 1, to: cursor) ?? cursor
    }
    return months
  }

  var body: some View {
    LazyVStack(spacing: 12) {
      ForEach(monthsToShow, id: \.self) { monthStart in
        MonthGridView(
          monthStart: monthStart,
          dayVisuals: dayVisuals,
          accentColor: accentColor,
          selectedDay: $selectedDay,
          onDayTapped: onDayTapped
        )
      }
    }
  }
}

private struct MonthGridView: View {
  let monthStart: Date
  let dayVisuals: [Date: CalendarDayVisual]
  let accentColor: Color
  @Binding var selectedDay: Date?
  var onDayTapped: ((Date) -> Void)?

  private let cal = Calendar.current

  private var title: String {
    let f = DateFormatter()
    f.dateFormat = "LLLL yyyy"
    return f.string(from: monthStart)
  }

  private var weekdaySymbols: [String] {
    let symbols = cal.shortWeekdaySymbols  // e.g. Sun, Mon
    // Reorder to match calendar's firstWeekday
    let firstIndex = cal.firstWeekday - 1
    return Array(symbols[firstIndex...] + symbols[..<firstIndex]).map {
      String($0.prefix(1)).uppercased()
    }
  }

  private var days: [Date?] {
    let comps = cal.dateComponents([.year, .month], from: monthStart)
    guard let firstOfMonth = cal.date(from: comps),
      let range = cal.range(of: .day, in: .month, for: firstOfMonth)
    else { return [] }

    let weekday = cal.component(.weekday, from: firstOfMonth)
    let leadingBlanks = (weekday - cal.firstWeekday + 7) % 7

    var result: [Date?] = Array(repeating: nil, count: leadingBlanks)
    for day in range {
      if let date = cal.date(byAdding: .day, value: day - 1, to: firstOfMonth) {
        result.append(date)
      }
    }
    // Pad to full weeks for consistent spacing
    while result.count % 7 != 0 {
      result.append(nil)
    }
    return result
  }

  private var monthEntryDaysCount: Int {
    dayVisuals.keys.filter { date in
      cal.isDate(date, equalTo: monthStart, toGranularity: .month)
    }.count
  }

  var body: some View {
    let shape = RoundedRectangle(cornerRadius: 20, style: .continuous)

    VStack(alignment: .leading, spacing: 12) {
      HStack(alignment: .center, spacing: 8) {
        Text(title)
          .font(.system(size: 16, weight: .bold))
          .foregroundStyle(.primary)

        Spacer(minLength: 8)

        HStack(spacing: 6) {
          Image(systemName: "record.circle")
            .font(.system(size: 10, weight: .semibold))
          Text("\(monthEntryDaysCount) logged")
            .font(.system(size: 11, weight: .semibold))
            .monospacedDigit()
        }
        .foregroundStyle(.secondary)
        .padding(.horizontal, 10)
        .padding(.vertical, 6)
        .background(Color(.tertiarySystemFill))
        .clipShape(Capsule())
      }

      LazyVGrid(columns: Array(repeating: GridItem(.flexible(), spacing: 6), count: 7), spacing: 8) {
        ForEach(weekdaySymbols, id: \.self) { sym in
          Text(sym)
            .font(.system(size: 10, weight: .semibold))
            .foregroundStyle(.secondary.opacity(0.88))
            .frame(maxWidth: .infinity)
            .padding(.vertical, 3)
        }

        ForEach(Array(days.enumerated()), id: \.offset) { _, date in
          DayCell(
            date: date,
            dayVisual: date.flatMap { dayVisuals[cal.startOfDay(for: $0)] },
            accentColor: accentColor,
            selectedDay: $selectedDay,
            onDayTapped: onDayTapped
          )
        }
      }
    }
    .padding(14)
    .background { GlassCardBackground(shape: shape) }
    .clipShape(shape)
    .overlay(
      shape
        .stroke(Color(.separator).opacity(0.2), lineWidth: 0.6)
    )
  }
}

private struct DayCell: View {
  let date: Date?
  let dayVisual: CalendarDayVisual?
  let accentColor: Color
  @Binding var selectedDay: Date?
  var onDayTapped: ((Date) -> Void)?

  private let cal = Calendar.current

  private var isToday: Bool {
    guard let date else { return false }
    return cal.isDateInToday(date)
  }

  private var isSelected: Bool {
    guard let date, let selectedDay else { return false }
    return cal.startOfDay(for: date) == cal.startOfDay(for: selectedDay)
  }

  private var hasEntry: Bool {
    dayVisual != nil
  }

  private var hasPhoto: Bool {
    dayVisual?.hasPhoto == true
  }

  private var noteCount: Int {
    dayVisual?.notesCount ?? 0
  }

  private var indicatorCount: Int {
    min(max(noteCount, 1), 3)
  }

  private var firstPhotoURL: URL? {
    guard let raw = dayVisual?.firstPhotoURL else { return nil }
    return URL(string: raw)
  }

  var body: some View {
    Group {
      if let date {
        Button {
          Haptics.impact(.light)
          withAnimation(.spring(response: 0.24, dampingFraction: 0.8)) {
            selectedDay = cal.startOfDay(for: date)
          }
          onDayTapped?(date)
        } label: {
          let shape = RoundedRectangle(cornerRadius: 12, style: .continuous)

          ZStack {
            shape
              .fill(Color(.secondarySystemBackground).opacity(0.32))

            if let firstPhotoURL {
              AsyncImage(url: firstPhotoURL) { phase in
                switch phase {
                case .success(let image):
                  image
                    .resizable()
                    .scaledToFill()
                case .empty, .failure:
                  LinearGradient(
                    colors: [accentColor.opacity(0.38), accentColor.opacity(0.18)],
                    startPoint: .topLeading,
                    endPoint: .bottomTrailing
                  )
                @unknown default:
                  LinearGradient(
                    colors: [accentColor.opacity(0.38), accentColor.opacity(0.18)],
                    startPoint: .topLeading,
                    endPoint: .bottomTrailing
                  )
                }
              }
              .frame(maxWidth: .infinity, maxHeight: .infinity)
              .clipped()
              .clipShape(shape)
            } else if hasPhoto {
              LinearGradient(
                colors: [accentColor.opacity(0.34), AppTheme.accent.opacity(0.24)],
                startPoint: .topLeading,
                endPoint: .bottomTrailing
              )
            } else if hasEntry {
              shape
                .fill(accentColor.opacity(0.15))
            }

            if hasPhoto {
              shape
                .fill(
                  LinearGradient(
                    colors: [.black.opacity(0.24), .clear, .black.opacity(0.14)],
                    startPoint: .top,
                    endPoint: .bottom
                  )
                )
            }

            Text("\(cal.component(.day, from: date))")
              .font(
                .system(
                  size: 16, weight: isToday ? .bold : .semibold, design: .rounded)
              )
              .foregroundStyle(textColor)
              .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .center)

            if hasEntry {
              HStack(spacing: 3) {
                ForEach(0..<indicatorCount, id: \.self) { idx in
                  Capsule()
                    .fill(indicatorColor.opacity(idx == 0 ? 1.0 : 0.78))
                    .frame(width: idx == 0 ? 10 : 5, height: 4)
                }
              }
              .padding(.bottom, 5)
              .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .bottom)
            }

            shape
              .stroke(borderColor, lineWidth: isSelected || isToday ? 1.1 : 0.55)
          }
          .frame(height: 48)
        }
        .buttonStyle(.plain)
      } else {
        Color.clear
          .frame(height: 48)
      }
    }
  }

  private var textColor: Color {
    if hasPhoto { return .white }
    if isToday { return accentColor.opacity(0.95) }
    return .primary
  }

  private var indicatorColor: Color {
    if hasPhoto { return .white }
    return accentColor.opacity(0.95)
  }

  private var borderColor: Color {
    if isSelected { return Color(.label).opacity(0.38) }
    if hasPhoto { return .white.opacity(0.26) }
    if isToday { return accentColor.opacity(0.55) }
    return Color(.separator).opacity(0.25)
  }
}

// MARK: - Calendar day sheet

private struct CalendarDaySheet: View {
  @Environment(\.dismiss) private var dismiss
  let date: Date
  @Binding var entries: [JournalEntry]
  let accentColor: Color
  let onAddEntry: () -> Void
  let onSelectEntry: (JournalEntry) -> Void

  private let cal = Calendar.current

  private var formattedDate: String {
    let f = DateFormatter()
    f.dateFormat = "EEEE, MMMM d"
    return f.string(from: date)
  }

  private var dayEntries: [JournalEntry] {
    let s = cal.startOfDay(for: date)
    return
      entries
      .filter { cal.startOfDay(for: $0.date) == s }
      .sorted { $0.createdAt > $1.createdAt }
  }

  private var isFutureDay: Bool {
    cal.startOfDay(for: date) > cal.startOfDay(for: Date())
  }

  var body: some View {
    NavigationStack {
      ScrollView {
        VStack(alignment: .leading, spacing: 20) {
          Text(formattedDate)
            .font(.system(size: 22, weight: .bold))
            .foregroundStyle(.primary)
            .frame(maxWidth: .infinity, alignment: .leading)
            .padding(.horizontal, 20)
            .padding(.top, 8)

          if dayEntries.isEmpty {
            VStack(spacing: 12) {
              Image(systemName: isFutureDay ? "calendar.badge.exclamationmark" : "book.closed")
                .font(.system(size: 36))
                .foregroundStyle(isFutureDay ? .orange : .secondary)
              Text("No entries on this day")
                .font(.system(size: 16, weight: .medium))
                .foregroundStyle(.secondary)
              Text(
                isFutureDay
                  ? "You can't add a note for a future date yet. Come back on this date."
                  : "Tap the button below to add your first entry."
              )
                .font(.system(size: 14))
                .foregroundStyle(.tertiary)
                .multilineTextAlignment(.center)
            }
            .frame(maxWidth: .infinity)
            .padding(.vertical, 40)
          } else {
            VStack(alignment: .leading, spacing: 0) {
              Text("Entries")
                .font(.system(size: 13, weight: .semibold))
                .foregroundStyle(.secondary)
                .padding(.horizontal, 20)
                .padding(.bottom, 12)

              ForEach(dayEntries) { entry in
                CalendarDayEntryRow(
                  entry: entry,
                  accentColor: accentColor,
                  onSelect: onSelectEntry
                )
              }
            }
          }

          Button {
            guard !isFutureDay else { return }
            Haptics.impact(.light)
            onAddEntry()
          } label: {
            HStack(spacing: 10) {
              Image(systemName: "plus.circle.fill")
                .font(.system(size: 20))
              Text(isFutureDay ? "Date not reached yet" : "Add entry")
                .font(.system(size: 17, weight: .semibold))
            }
            .foregroundStyle(.white)
            .frame(maxWidth: .infinity)
            .padding(.vertical, 16)
            .background(isFutureDay ? Color(.systemGray3) : accentColor)
            .clipShape(RoundedRectangle(cornerRadius: 14, style: .continuous))
          }
          .disabled(isFutureDay)
          .padding(.horizontal, 20)
          .padding(.top, 8)
          .padding(.bottom, 24)
        }
      }
      .background(AppTheme.background)
      .navigationBarTitleDisplayMode(.inline)
      .toolbar {
        ToolbarItem(placement: .topBarTrailing) {
          Button("Done") {
            Haptics.impact(.light)
            dismiss()
          }
          .fontWeight(.semibold)
        }
      }
    }
  }
}

private struct CalendarDayEntryRow: View {
  let entry: JournalEntry
  let accentColor: Color
  let onSelect: (JournalEntry) -> Void

  var body: some View {
    Button {
      Haptics.impact(.light)
      onSelect(entry)
    } label: {
      JournalListRowContent(entry: entry)
    }
    .buttonStyle(.plain)
    .padding(.horizontal, 20)
    .padding(.vertical, 12)
  }
}

// MARK: - Glass FAB

private struct GlassFloatingActionButton: View {
  let systemName: String
  let action: () -> Void

  var body: some View {
    Button(action: action) {
      Image(systemName: systemName)
        .font(.system(size: 20, weight: .semibold))
        .foregroundStyle(.primary)
        .frame(width: 58, height: 58)
    }
    .buttonStyle(.plain)
    .background {
      if #available(iOS 26.0, *) {
        Circle()
          .glassEffect(.clear.interactive(), in: Circle())
      } else {
        Circle()
          .fill(Color(.secondarySystemBackground))
      }
    }
    .accessibilityLabel("Add entry")
  }
}

// MARK: - Sheets

private struct NewJournalEntryDraft: Hashable {
  var date: Date
  var title: String
  var body: String
  var photoCount: Int = 0
  /// If set, entry is already persisted remotely and should not be re-saved as text-only.
  var entryId: UUID? = nil
}

private struct NewEntrySession: Identifiable {
  let id = UUID()
  let initialDate: Date
}

private struct EditEntrySession: Identifiable {
  let id = UUID()
  let entry: JournalEntry
}

// Replaced with a full-page editor style sheet (`NewDiaryNoteSheet`).

private struct DiaryEditorSheet: View {
  @Environment(\.dismiss) private var dismiss
  @Environment(\.colorScheme) private var colorScheme

  @State private var name: String
  @State private var description: String
  @State private var color: Color
  @FocusState private var focusedField: Field?
  private let descriptionMaxLength: Int = 10_000

  let onSave: (String, String, Color) -> Void

  private enum Field: Hashable {
    case name
    case description
  }

  init(
    name: String,
    description: String,
    color: Color,
    onSave: @escaping (String, String, Color) -> Void
  ) {
    _name = State(initialValue: name)
    _description = State(initialValue: description)
    _color = State(initialValue: color)
    self.onSave = onSave
  }

  var body: some View {
    NavigationStack {
      ZStack {
        DiaryEditorBackdrop(accentColor: color)

        ScrollView {
          VStack(alignment: .leading, spacing: 14) {
            VStack(alignment: .leading, spacing: 6) {
              Text("Customize your diary")
                .font(.system(size: 22, weight: .bold))
                .foregroundStyle(.primary)
              Text("Update the title, description, and header color.")
                .font(.system(size: 14, weight: .medium))
                .foregroundStyle(.secondary)
            }
            .padding(.horizontal, 4)
            .padding(.bottom, 2)

            VStack(alignment: .leading, spacing: 10) {
              Text("Diary name")
                .font(.system(size: 13, weight: .semibold))
                .foregroundStyle(.secondary)

              TextField("My Diary", text: $name)
                .textContentType(.none)
                .textInputAutocapitalization(.words)
                .autocorrectionDisabled()
                .focused($focusedField, equals: .name)
                .font(.system(size: 17, weight: .semibold))
                .padding(.horizontal, 14)
                .padding(.vertical, 12)
                .background(Color(.systemBackground))
                .clipShape(RoundedRectangle(cornerRadius: 14, style: .continuous))
                .overlay(
                  RoundedRectangle(cornerRadius: 14, style: .continuous)
                    .stroke(
                      focusedField == .name
                        ? AppTheme.accent.opacity(0.95)
                        : Color(.separator).opacity(0.34),
                      lineWidth: focusedField == .name ? 1.6 : 1
                    )
                )
            }
            .padding(14)
            .background(editorCardBackground)
            .clipShape(RoundedRectangle(cornerRadius: 18, style: .continuous))
            .overlay(
              RoundedRectangle(cornerRadius: 18, style: .continuous)
                .stroke(Color(.separator).opacity(0.2), lineWidth: 0.7)
            )

            VStack(alignment: .leading, spacing: 10) {
              Text("Description")
                .font(.system(size: 13, weight: .semibold))
                .foregroundStyle(.secondary)

              ZStack(alignment: .topLeading) {
                if description.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                  Text("Add a short description for your diary.")
                    .font(.system(size: 16))
                    .foregroundStyle(.tertiary)
                    .padding(.horizontal, 16)
                    .padding(.vertical, 14)
                    .allowsHitTesting(false)
                }

                TextEditor(text: $description)
                  .focused($focusedField, equals: .description)
                  .font(.system(size: 16))
                  .frame(minHeight: 170)
                  .padding(.horizontal, 10)
                  .padding(.vertical, 8)
                  .scrollContentBackground(.hidden)
                  .onChange(of: description) { _, newValue in
                    if newValue.count > descriptionMaxLength {
                      description = String(newValue.prefix(descriptionMaxLength))
                    }
                  }
              }
              .background(Color(.systemBackground))
              .clipShape(RoundedRectangle(cornerRadius: 14, style: .continuous))
              .overlay(
                RoundedRectangle(cornerRadius: 14, style: .continuous)
                  .stroke(
                    focusedField == .description
                      ? AppTheme.accent.opacity(0.95)
                      : Color(.separator).opacity(0.34),
                    lineWidth: focusedField == .description ? 1.6 : 1
                  )
              )

              Text("\(description.count)/\(descriptionMaxLength)")
                .font(.system(size: 12, weight: .semibold))
                .foregroundStyle(.secondary)
                .frame(maxWidth: .infinity, alignment: .trailing)
            }
            .padding(14)
            .background(editorCardBackground)
            .clipShape(RoundedRectangle(cornerRadius: 18, style: .continuous))
            .overlay(
              RoundedRectangle(cornerRadius: 18, style: .continuous)
                .stroke(Color(.separator).opacity(0.2), lineWidth: 0.7)
            )

            VStack(alignment: .leading, spacing: 10) {
              Text("Header color")
                .font(.system(size: 13, weight: .semibold))
                .foregroundStyle(.secondary)

              Text("Changes the top section tint above the tabs.")
                .font(.system(size: 12))
                .foregroundStyle(.tertiary)

              DiaryColorPicker(selected: $color)
            }
            .padding(14)
            .background(editorCardBackground)
            .clipShape(RoundedRectangle(cornerRadius: 18, style: .continuous))
            .overlay(
              RoundedRectangle(cornerRadius: 18, style: .continuous)
                .stroke(Color(.separator).opacity(0.2), lineWidth: 0.7)
            )

            Button(role: .destructive) {
              // Placeholder: backend functionality later
              Haptics.impact(.light)
            } label: {
              HStack(spacing: 10) {
                Image(systemName: "trash")
                Text("Delete diary")
              }
              .font(.system(size: 16, weight: .semibold))
              .frame(maxWidth: .infinity)
              .padding(.vertical, 12)
            }
            .buttonStyle(.bordered)
            .tint(.red)
            .padding(.top, 2)

            Spacer(minLength: 10)
          }
          .padding(16)
        }
      }
      .navigationTitle("Edit Diary")
      .navigationBarTitleDisplayMode(.inline)
      .toolbar {
        ToolbarItem(placement: .topBarLeading) {
          Button("Close") { dismiss() }
        }
        ToolbarItem(placement: .topBarTrailing) {
          Button("Save") {
            onSave(
              name.trimmingCharacters(in: .whitespacesAndNewlines),
              description.trimmingCharacters(in: .whitespacesAndNewlines),
              color
            )
            dismiss()
          }
          .fontWeight(.semibold)
        }
      }
    }
  }

  private var editorCardBackground: Color {
    if colorScheme == .dark {
      return AppTheme.surface.blended(with: Color.black, amount: 0.12).opacity(0.96)
    }
    return Color.white.opacity(0.82)
  }
}

private struct DiaryEditorBackdrop: View {
  @Environment(\.colorScheme) private var colorScheme
  let accentColor: Color

  var body: some View {
    let top = accentColor.blended(
      with: colorScheme == .dark ? AppTheme.background : Color(red: 0.97, green: 0.99, blue: 1.0),
      amount: colorScheme == .dark ? 0.74 : 0.56
    )
    let mid =
      colorScheme == .dark
      ? AppTheme.background.blended(with: .black, amount: 0.18)
      : AppTheme.background.blended(with: .white, amount: 0.30)
    let bottom =
      colorScheme == .dark
      ? AppTheme.surface.blended(with: .black, amount: 0.10)
      : AppTheme.surface.blended(with: .white, amount: 0.04)

    ZStack {
      LinearGradient(
        colors: [top, mid, bottom],
        startPoint: .topLeading,
        endPoint: .bottomTrailing
      )

      FloatingOrb(
        size: 260,
        color: accentColor.opacity(colorScheme == .dark ? 0.20 : 0.24),
        startOffset: CGSize(width: -180, height: -210),
        drift: CGSize(width: 34, height: 30),
        blurRadius: 2.4,
        duration: 9.0
      )

      FloatingOrb(
        size: 180,
        color: .white.opacity(colorScheme == .dark ? 0.16 : 0.26),
        startOffset: CGSize(width: 130, height: -120),
        drift: CGSize(width: -22, height: 26),
        blurRadius: 2.0,
        duration: 7.6
      )

      FloatingOrb(
        size: 150,
        color: AppTheme.brand.opacity(colorScheme == .dark ? 0.14 : 0.22),
        startOffset: CGSize(width: 120, height: 270),
        drift: CGSize(width: -24, height: -20),
        blurRadius: 2.0,
        duration: 8.4
      )
    }
    .ignoresSafeArea()
  }
}

private struct DiaryColorPicker: View {
  @Binding var selected: Color

  private struct Option: Hashable {
    let name: String
    let color: Color
  }

  private let options: [Option] = [
    .init(name: "Pink", color: Color(red: 1.0, green: 0.82, blue: 0.88)),
    .init(name: "Lavender", color: Color(red: 0.84, green: 0.82, blue: 1.0)),
    .init(name: "Sky", color: Color(red: 0.72, green: 0.87, blue: 1.0)),
    .init(name: "Mint", color: Color(red: 0.73, green: 0.94, blue: 0.85)),
    .init(name: "Sand", color: Color(red: 0.96, green: 0.91, blue: 0.78)),
    .init(name: "Graphite", color: Color(red: 0.78, green: 0.78, blue: 0.82)),
  ]

  private let columns: [GridItem] = Array(repeating: GridItem(.flexible(), spacing: 12), count: 6)

  var body: some View {
    VStack(alignment: .leading, spacing: 12) {
      LazyVGrid(columns: columns, spacing: 12) {
        ForEach(options, id: \.self) { option in
          Button {
            Haptics.impact(.light)
            selected = option.color
          } label: {
            ZStack {
              Circle()
                .fill(option.color)
                .frame(width: 34, height: 34)
                .overlay(
                  Circle()
                    .stroke(Color.white.opacity(0.92), lineWidth: 1)
                )

              if isSelected(option.color) {
                Image(systemName: "checkmark")
                  .font(.system(size: 14, weight: .bold))
                  .foregroundStyle(Color.black.opacity(0.75))
              }
            }
            .frame(maxWidth: .infinity)
          }
          .buttonStyle(.plain)
          .accessibilityLabel(option.name)
        }
      }
      .padding(12)
      .background(Color(.tertiarySystemBackground))
      .clipShape(RoundedRectangle(cornerRadius: 14, style: .continuous))
      .overlay(
        RoundedRectangle(cornerRadius: 14, style: .continuous)
          .stroke(Color(.separator).opacity(0.24), lineWidth: 0.7)
      )

      // Still allow custom colors, but keep it secondary.
      ColorPicker("Custom", selection: $selected, supportsOpacity: false)
        .font(.system(size: 14, weight: .semibold))
        .padding(12)
        .background(Color(.tertiarySystemBackground))
        .clipShape(RoundedRectangle(cornerRadius: 14, style: .continuous))
        .overlay(
          RoundedRectangle(cornerRadius: 14, style: .continuous)
            .stroke(Color(.separator).opacity(0.24), lineWidth: 0.7)
        )
    }
  }

  private func isSelected(_ candidate: Color) -> Bool {
    // Best-effort: SwiftUI Color doesn't compare reliably; compare by description.
    String(describing: candidate) == String(describing: selected)
  }
}

// MARK: - Diary blocks (list of text + image blocks, looks like one page)

private struct DiaryBlock: Identifiable {
  let id: UUID
  var content: Content

  enum Content {
    case text(String)
    case image(UIImage?, storagePath: String?)
  }

  static func text(_ string: String) -> DiaryBlock {
    DiaryBlock(id: UUID(), content: .text(string))
  }

  static func image(_ image: UIImage, storagePath: String? = nil) -> DiaryBlock {
    DiaryBlock(id: UUID(), content: .image(image, storagePath: storagePath))
  }
}

extension Array where Element == DiaryBlock {
  fileprivate var textContent: String {
    map { block in
      if case .text(let s) = block.content { return s }
      return ""
    }.joined(separator: "\n\n")
  }

  fileprivate var photoCount: Int {
    filter {
      if case .image = $0.content { return true }
      return false
    }.count
  }
}

private struct DiaryBlockListView: View {
  @Binding var blocks: [DiaryBlock]
  var insertImageAfterBlockId: UUID?
  var imagesToInsert: [UIImage]
  var isVoiceRecording: Bool
  var displayedRecordingText: String
  var onImagesInserted: () -> Void
  var onBodyChanged: (() -> Void)?
  var onFocusBlock: (UUID) -> Void
  @Binding var imageForViewer: UIImage?
  @Binding var blockIdForViewer: UUID?

  private let placeholder = "Start writing…"

  var body: some View {
    LazyVStack(alignment: .leading, spacing: 12) {
      if blocks.isEmpty && !isVoiceRecording {
        ZStack(alignment: .topLeading) {
          Text(placeholder)
            .font(.system(size: 17))
            .foregroundStyle(.secondary)
            .padding(.top, 8)
            .padding(.leading, 4)
          TextEditor(text: .constant(""))
            .font(.system(size: 17))
            .scrollContentBackground(.hidden)
            .padding(.leading, -4)
            .frame(minHeight: 120)
            .frame(maxWidth: .infinity, alignment: .topLeading)
            .disabled(true)
        }
        .contentShape(Rectangle())
        .onTapGesture {
          let newBlock = DiaryBlock.text("")
          blocks.insert(newBlock, at: 0)
          onFocusBlock(newBlock.id)
          onBodyChanged?()
        }
      }
      ForEach(blocks) { block in
        switch block.content {
        case .text(let s):
          TextBlockRow(
            textBinding: isVoiceRecording && block.id == lastTextBlockId
              ? nil : binding(for: block),
            placeholder: placeholder,
            showPlaceholder: s.isEmpty && blocks.count == 1,
            isVoiceRecording: isVoiceRecording,
            displayedRecordingText: displayedRecordingText,
            onFocus: { onFocusBlock(block.id) }
          )
          .id(block.id)

        case .image(let image, _):
          ImageBlockRow(
            image: image,
            onTap: {
              if let image {
                imageForViewer = image
                blockIdForViewer = block.id
              }
            }
          )
        }
      }
      if isVoiceRecording && blocks.isEmpty {
        TextBlockRow(
          textBinding: nil,
          placeholder: placeholder,
          showPlaceholder: true,
          isVoiceRecording: true,
          displayedRecordingText: displayedRecordingText,
          onFocus: {}
        )
        .disabled(true)
      }
    }
    .onChange(of: imagesToInsert.count) { _, _ in
      guard !imagesToInsert.isEmpty else { return }
      let insertIndex: Int = {
        if let id = insertImageAfterBlockId, let idx = blocks.firstIndex(where: { $0.id == id }) {
          return idx + 1
        }
        return blocks.count
      }()
      for image in imagesToInsert.reversed() {
        blocks.insert(.text(""), at: min(insertIndex + 1, blocks.count))
        blocks.insert(.image(image, storagePath: nil), at: min(insertIndex, blocks.count))
      }
      onImagesInserted()
      onBodyChanged?()
    }
  }

  private var lastTextBlockId: UUID? {
    blocks.last {
      if case .text = $0.content { return true }
      return false
    }?.id
  }

  private func binding(for block: DiaryBlock) -> Binding<String> {
    Binding(
      get: {
        guard let i = blocks.firstIndex(where: { $0.id == block.id }),
          case .text(let s) = blocks[i].content
        else { return "" }
        return s
      },
      set: { newValue in
        guard let i = blocks.firstIndex(where: { $0.id == block.id }) else { return }
        blocks[i].content = .text(newValue)
        onBodyChanged?()
      }
    )
  }
}

private struct TextBlockRow: View {
  var textBinding: Binding<String>?
  var placeholder: String
  var showPlaceholder: Bool = true
  var isVoiceRecording: Bool
  var displayedRecordingText: String
  var onFocus: () -> Void

  private var effectiveText: String {
    if textBinding == nil, isVoiceRecording { return displayedRecordingText }
    return textBinding?.wrappedValue ?? ""
  }

  var body: some View {
    ZStack(alignment: .topLeading) {
      if showPlaceholder && effectiveText.isEmpty && !isVoiceRecording {
        Text(placeholder)
          .font(.system(size: 17))
          .foregroundStyle(.secondary)
          .padding(.top, 8)
          .padding(.leading, 4)
      }
      TextEditor(text: textBinding ?? .constant(displayedRecordingText))
        .font(.system(size: 17))
        .scrollContentBackground(.hidden)
        .padding(.leading, -4)
        .frame(minHeight: 44)
        .frame(maxWidth: .infinity, alignment: .topLeading)
        .disabled(isVoiceRecording)
    }
    .contentShape(Rectangle())
    .onTapGesture { if !isVoiceRecording { onFocus() } }
  }
}

private struct ImageBlockRow: View {
  let image: UIImage?
  var onTap: () -> Void

  var body: some View {
    Group {
      if let image {
        Image(uiImage: image)
          .resizable()
          .scaledToFit()
          .frame(maxWidth: .infinity)
      } else {
        ZStack {
          RoundedRectangle(cornerRadius: 12, style: .continuous)
            .fill(Color(.secondarySystemBackground))
          VStack(spacing: 8) {
            Image(systemName: "photo")
              .font(.system(size: 24, weight: .semibold))
              .foregroundStyle(.secondary)
            Text("Image unavailable")
              .font(.system(size: 13, weight: .medium))
              .foregroundStyle(.secondary)
          }
          .padding(.vertical, 28)
        }
        .frame(maxWidth: .infinity, minHeight: 160)
      }
    }
    .clipShape(RoundedRectangle(cornerRadius: 12, style: .continuous))
    .onTapGesture { onTap() }
  }
}

private struct DiaryImageViewer: View {
  let image: UIImage
  var onDismiss: () -> Void
  var onDelete: (() -> Void)?

  var body: some View {
    ZStack {
      Color.black.ignoresSafeArea()
      Image(uiImage: image)
        .resizable()
        .scaledToFit()
    }
    .onTapGesture { onDismiss() }
    .overlay(alignment: .topTrailing) {
      Button("Done") { onDismiss() }
        .font(.system(size: 17, weight: .semibold))
        .foregroundStyle(.white)
        .padding()
    }
    .overlay(alignment: .bottom) {
      if let onDelete = onDelete {
        Button {
          Haptics.impact(.light)
          onDelete()
          onDismiss()
        } label: {
          Label("Delete", systemImage: "trash")
            .font(.system(size: 17, weight: .medium))
            .foregroundStyle(.white)
        }
        .padding(.bottom, 40)
      }
    }
  }
}

// MARK: - Note editor

private struct DiaryNoteEditorView: View {
  @Environment(\.dismiss) private var dismiss

  @Binding var entry: JournalEntry
  let accentColor: Color
  var onDeleteEntry: (() -> Void)?
  @State private var blocks: [DiaryBlock] = []
  @State private var didLoadBlocksFromApi: Bool = false
  @State private var isLoadingBlocksFromApi: Bool = false
  @State private var isVoiceRecording: Bool = false
  @State private var bodyBeforeRecording: String = ""
  @State private var voiceErrorMessage: String?
  @State private var showPhotoLibrarySheet: Bool = false
  @State private var photoLibrarySelection: [PhotosPickerItem] = []
  @State private var showCamera: Bool = false
  @State private var showFileImporter: Bool = false
  @State private var isSavingEntry: Bool = false
  @State private var saveErrorMessage: String?
  @State private var imagesToInsert: [UIImage] = []
  @State private var attachedFileURLs: [URL] = []
  @State private var imageForViewer: UIImage?
  @State private var blockIdForViewer: UUID?
  @StateObject private var diarySTTService = DeepgramStreamingSTTService()

  @FocusState private var focusedField: Field?
  @FocusState private var focusedBlockId: UUID?

  private enum Field: Hashable {
    case title
    case body
  }

  private var displayedTextForRecording: String {
    if isVoiceRecording {
      let transcript = diarySTTService.userTranscript.trimmingCharacters(
        in: .whitespacesAndNewlines)
      return transcript.isEmpty
        ? bodyBeforeRecording
        : bodyBeforeRecording + (bodyBeforeRecording.isEmpty ? "" : " ") + transcript
    }
    return blocks.textContent
  }

  private var toolbarDateText: String {
    let f = DateFormatter()
    f.dateFormat = "EEE, MMM d, yyyy"
    return f.string(from: entry.date)
  }

  private var editorContent: some View {
    VStack(alignment: .leading, spacing: 10) {
      Text(toolbarDateText)
        .font(.system(size: 13, weight: .medium))
        .foregroundStyle(.secondary)
        .padding(.top, 4)
        .padding(.bottom, 2)

      TextField("Title", text: $entry.title)
        .textFieldStyle(.plain)
        .font(.system(size: 34, weight: .bold))
        .textInputAutocapitalization(.sentences)
        .disableAutocorrection(false)
        .focused($focusedField, equals: .title)
        .submitLabel(.next)
        .onSubmit {
          focusedBlockId =
            blocks.first(where: {
              if case .text = $0.content { return true }
              return false
            })?.id
        }
        .padding(.top, 10)
        .disabled(isVoiceRecording)

      ScrollView {
        if isLoadingBlocksFromApi && blocks.isEmpty {
          VStack(spacing: 12) {
            ProgressView()
              .scaleEffect(1.1)
              .padding(.top, 40)
            Text("Loading note…")
              .font(.system(size: 15, weight: .medium))
              .foregroundStyle(.secondary)
          }
          .frame(maxWidth: .infinity, maxHeight: .infinity)
        } else {
          DiaryBlockListView(
            blocks: $blocks,
            insertImageAfterBlockId: focusedBlockId,
            imagesToInsert: imagesToInsert,
            isVoiceRecording: isVoiceRecording,
            displayedRecordingText: displayedTextForRecording,
            onImagesInserted: { imagesToInsert = [] },
            onBodyChanged: { entry.body = blocks.textContent },
            onFocusBlock: { focusedBlockId = $0 },
            imageForViewer: $imageForViewer,
            blockIdForViewer: $blockIdForViewer
          )
          .frame(maxWidth: .infinity, alignment: .leading)
          .padding(.top, 2)
        }
      }
      .frame(maxWidth: .infinity, maxHeight: .infinity)
      .fullScreenCover(
        isPresented: Binding(
          get: { imageForViewer != nil },
          set: {
            if !$0 {
              imageForViewer = nil
              blockIdForViewer = nil
            }
          })
      ) {
        if let img = imageForViewer {
          DiaryImageViewer(
            image: img,
            onDismiss: {
              imageForViewer = nil
              blockIdForViewer = nil
            },
            onDelete: {
              if let id = blockIdForViewer {
                blocks.removeAll { $0.id == id }
                entry.body = blocks.textContent
              }
              blockIdForViewer = nil
              imageForViewer = nil
            }
          )
        }
      }

      if !attachedFileURLs.isEmpty {
        editorFileList
      }
    }
    .padding(.horizontal, 18)
    .padding(.bottom, 14)
  }

  private var editorFileList: some View {
    VStack(alignment: .leading, spacing: 6) {
      ForEach(Array(attachedFileURLs.enumerated()), id: \.offset) { idx, url in
        HStack(spacing: 8) {
          Image(systemName: "doc.fill")
            .font(.system(size: 14))
            .foregroundStyle(.secondary)
          Text(url.lastPathComponent)
            .font(.system(size: 15))
            .lineLimit(1)
            .truncationMode(.middle)
          Spacer(minLength: 8)
          Button {
            Haptics.impact(.light)
            attachedFileURLs.remove(at: idx)
          } label: {
            Image(systemName: "xmark.circle.fill")
              .font(.system(size: 20))
              .symbolRenderingMode(.hierarchical)
              .foregroundStyle(.secondary)
          }
        }
        .padding(.vertical, 6)
        .padding(.horizontal, 10)
        .background(Color(.secondarySystemBackground))
        .clipShape(RoundedRectangle(cornerRadius: 8, style: .continuous))
      }
    }
    .frame(maxWidth: .infinity, alignment: .leading)
  }

  private var noteEditorBase: some View {
    VStack(alignment: .leading, spacing: 10) {
      editorContent
    }
    .background(AppTheme.background)
    .navigationBarTitleDisplayMode(.inline)
    .toolbarBackground(.hidden, for: .navigationBar)
    .toolbar {
      ToolbarItemGroup(placement: .topBarTrailing) {
        Menu {
          Button {
            let text = [entry.title, entry.body].joined(separator: "\n\n").trimmingCharacters(
              in: .whitespacesAndNewlines)
            if !text.isEmpty { UIPasteboard.general.string = text }
          } label: {
            Label("Copy", systemImage: "doc.on.doc")
          }
          Button {
            entry.title = ""
          } label: {
            Label("Remove Title", systemImage: "character.cursor.ibeam")
          }
          Divider()
          Button(role: .destructive) {
            Haptics.impact(.light)
            Task {
              if let userId = AuthService.shared.currentUserId, let uid = UUID(uuidString: userId) {
                try? await DiaryService.shared.deleteEntry(userId: uid, entryId: entry.id)
              }
              await MainActor.run {
                onDeleteEntry?()
                dismiss()
              }
            }
          } label: {
            Label("Move to Trash", systemImage: "trash")
          }
        } label: {
          Image(systemName: "ellipsis.circle")
            .font(.system(size: 18, weight: .semibold))
        }
        .accessibilityLabel("More")
        .disabled(isVoiceRecording || isSavingEntry)

        Button("Done") {
          Haptics.impact(.light)
          Task {
            await MainActor.run { isSavingEntry = true }
            let didSave = await saveEntryToSupabase()
            await MainActor.run {
              isSavingEntry = false
              if didSave {
                dismiss()
              }
            }
          }
        }
        .fontWeight(.semibold)
        .disabled(isVoiceRecording || isSavingEntry)
      }
    }
    .safeAreaInset(edge: .bottom, spacing: 0) {
      NoteAccessoryTray(
        accentColor: accentColor,
        isVoiceRecording: isVoiceRecording,
        onAudioTap: { startVoiceRecording() },
        onEndRecording: { endVoiceRecording() },
        onPhotoLibrary: { showPhotoLibrarySheet = true },
        onCamera: {
          if UIImagePickerController.isSourceTypeAvailable(.camera) {
            showCamera = true
          }
        },
        onFiles: { showFileImporter = true }
      )
    }
  }

  var body: some View {
    noteEditorBase
    .fileImporter(
      isPresented: $showFileImporter, allowedContentTypes: [.item, .pdf, .plainText, .image],
      allowsMultipleSelection: true
    ) { result in
      switch result {
      case .success(let urls):
        attachedFileURLs.append(contentsOf: urls)
      case .failure:
        break
      }
    }
    .photosPicker(
      isPresented: $showPhotoLibrarySheet, selection: $photoLibrarySelection, maxSelectionCount: 5,
      matching: .images
    )
    .onChange(of: photoLibrarySelection) { _, new in
      guard !new.isEmpty else { return }
      Task {
        let images = await loadImagesFromPickerItems(new)
        await MainActor.run {
          imagesToInsert.append(contentsOf: images)
          photoLibrarySelection = []
        }
      }
    }
    .fullScreenCover(isPresented: $showCamera) {
      CameraPickerView(
        onImage: { image in
          imagesToInsert.append(image)
          showCamera = false
        },
        onCancel: { showCamera = false }
      )
      .ignoresSafeArea()
    }
    .alert(
      "Voice input",
      isPresented: Binding(
        get: { voiceErrorMessage != nil },
        set: { if !$0 { voiceErrorMessage = nil } }
      )
    ) {
      Button("OK", role: .cancel) { voiceErrorMessage = nil }
    } message: {
      if let msg = voiceErrorMessage { Text(msg) }
    }
    .alert(
      "Couldn't save note",
      isPresented: Binding(
        get: { saveErrorMessage != nil },
        set: { if !$0 { saveErrorMessage = nil } }
      )
    ) {
      Button("OK", role: .cancel) { saveErrorMessage = nil }
    } message: {
      if let msg = saveErrorMessage { Text(msg) }
    }
    .onAppear {
      if blocks.isEmpty, !didLoadBlocksFromApi {
        didLoadBlocksFromApi = true
        isLoadingBlocksFromApi = true
        guard let userId = AuthService.shared.currentUserId, let uid = UUID(uuidString: userId)
        else {
          blocks = entry.body.isEmpty ? [.text("")] : [.text(entry.body)]
          isLoadingBlocksFromApi = false
          return
        }
        Task {
          defer { Task { @MainActor in isLoadingBlocksFromApi = false } }
          do {
            guard
              let row = try await DiaryService.shared.fetchEntry(userId: uid, entryId: entry.id),
              !row.body_blocks.isEmpty
            else {
              await MainActor.run {
                blocks = entry.body.isEmpty ? [.text("")] : [.text(entry.body)]
                DispatchQueue.main.asyncAfter(deadline: .now() + 0.25) {
                  focusedBlockId =
                    blocks.first(where: {
                      if case .text = $0.content { return true }
                      return false
                    })?.id
                }
              }
              return
            }
            let decoded = DiaryService.decodeBodyBlocks(row.body_blocks)
            var loaded: [DiaryBlock] = []
            for d in decoded {
              switch d.content {
              case .text(let s):
                loaded.append(DiaryBlock(id: d.id, content: .text(s)))
              case .imageURL(let url, let path):
                let img = await DiaryService.shared.loadImageFromURL(url)
                loaded.append(DiaryBlock(id: d.id, content: .image(img, storagePath: path)))
              }
            }
            await MainActor.run {
              blocks =
                loaded.isEmpty ? (entry.body.isEmpty ? [.text("")] : [.text(entry.body)]) : loaded
              entry.body = blocks.textContent
              DispatchQueue.main.asyncAfter(deadline: .now() + 0.25) {
                focusedBlockId =
                  blocks.first(where: {
                    if case .text = $0.content { return true }
                    return false
                  })?.id
              }
            }
          } catch {
            await MainActor.run {
              blocks = entry.body.isEmpty ? [.text("")] : [.text(entry.body)]
              DispatchQueue.main.asyncAfter(deadline: .now() + 0.25) {
                focusedBlockId =
                  blocks.first(where: {
                    if case .text = $0.content { return true }
                    return false
                  })?.id
              }
            }
          }
        }
      }
    }
  }

  private func saveEntryToSupabase() async -> Bool {
    let pendingSelection = await MainActor.run { photoLibrarySelection }
    if !pendingSelection.isEmpty {
      let images = await loadImagesFromPickerItems(pendingSelection)
      await MainActor.run {
        imagesToInsert.append(contentsOf: images)
        photoLibrarySelection = []
      }
    }
    await MainActor.run { flushPendingImagesIntoBlocksEditor() }

    guard let userId = AuthService.shared.currentUserId, let uid = UUID(uuidString: userId) else {
      await MainActor.run { saveErrorMessage = "Sign in to save this note." }
      return false
    }
    let payload: [DiaryBlockPayload] = await MainActor.run {
      blocks.compactMap { block in
        switch block.content {
        case .text(let s):
          return .text(id: block.id, content: s)
        case .image(_, let path?) where path.isEmpty == false:
          return .imageRemote(id: block.id, remotePath: path)
        case .image(let img?, _):
          return .imageLocal(id: block.id, image: img)
        case .image(nil, _):
          return nil
        }
      }
    }
    let tz = TimeZone.current.abbreviation() ?? "UTC"
    do {
      _ = try await DiaryService.shared.saveEntry(
        userId: uid,
        entryId: entry.id,
        date: entry.date,
        title: entry.title,
        bodyBlocks: payload,
        timezoneAbbreviation: tz
      )
      await MainActor.run {
        entry.body = blocks.textContent
        saveErrorMessage = nil
      }
      return true
    } catch {
      await MainActor.run { saveErrorMessage = DiaryService.userFacingMessage(from: error) }
      return false
    }
  }

  private func loadImagesFromPickerItems(_ items: [PhotosPickerItem]) async -> [UIImage] {
    var result: [UIImage] = []
    for item in items {
      do {
        if let data = try await item.loadTransferable(type: Data.self),
          let image = UIImage(data: data)
        {
          result.append(image)
        }
      } catch {}
    }
    return result
  }

  private func flushPendingImagesIntoBlocksEditor() {
    guard !imagesToInsert.isEmpty else { return }
    var insertIndex = blocks.count
    if let id = focusedBlockId, let idx = blocks.firstIndex(where: { $0.id == id }) {
      insertIndex = idx + 1
    }
    for image in imagesToInsert.reversed() {
      blocks.insert(.text(""), at: min(insertIndex + 1, blocks.count))
      blocks.insert(.image(image, storagePath: nil), at: min(insertIndex, blocks.count))
    }
    imagesToInsert = []
    entry.body = blocks.textContent
  }

  private func startVoiceRecording() {
    guard !diarySTTService.isRecording else { return }
    if !UserDefaults.standard.bool(forKey: PreferenceKeys.dictationEnabled) {
      Haptics.notification(.warning)
      voiceErrorMessage = "Turn on Dictation in Settings to use voice input."
      return
    }
    if !NetworkMonitor.shared.isOnline {
      voiceErrorMessage = "Voice input requires internet."
      return
    }
    Task { @MainActor in
      guard await AuthService.shared.getAccessToken() != nil else {
        voiceErrorMessage = "Sign in to use the microphone."
        return
      }
      if !diarySTTService.isConnected {
        await diarySTTService.connect()
      }
      guard diarySTTService.isConnected else {
        voiceErrorMessage = diarySTTService.lastError ?? "Could not connect. Try again."
        return
      }
      bodyBeforeRecording = blocks.textContent
      diarySTTService.startRecording()
      if diarySTTService.lastError != nil {
        voiceErrorMessage = diarySTTService.lastError
        return
      }
      withAnimation(.spring(response: 0.4, dampingFraction: 0.82)) {
        isVoiceRecording = true
      }
    }
  }

  private func endVoiceRecording() {
    diarySTTService.stopRecording()
    Task { @MainActor in
      try? await Task.sleep(nanoseconds: 500_000_000)
      let finalText = diarySTTService.userTranscript.trimmingCharacters(in: .whitespacesAndNewlines)
      let newText =
        finalText.isEmpty
        ? bodyBeforeRecording
        : bodyBeforeRecording + (bodyBeforeRecording.isEmpty ? "" : " ") + finalText
      if let lastIdx = blocks.indices.reversed().first(where: {
        if case .text = blocks[$0].content { return true }
        return false
      }) {
        blocks[lastIdx].content = .text(newText)
      } else {
        blocks.append(.text(newText))
      }
      entry.body = blocks.textContent
      withAnimation(.spring(response: 0.4, dampingFraction: 0.82)) {
        isVoiceRecording = false
        bodyBeforeRecording = ""
      }
      diarySTTService.disconnect()
    }
  }
}

// MARK: - New note (full-page editor sheet)

private struct NewDiaryNoteSheet: View {
  @Environment(\.dismiss) private var dismiss
  let accentColor: Color
  let initialDate: Date
  let draftTitle: String
  let draftBody: String
  let onAdd: (NewJournalEntryDraft) -> Void
  let onSaveDraft: (String, String) -> Void

  @State private var title: String
  @State private var blocks: [DiaryBlock] = []
  @State private var imagesToInsert: [UIImage] = []
  @State private var attachedFileURLs: [URL] = []
  @State private var imageForViewer: UIImage?
  @State private var blockIdForViewer: UUID?
  @State private var isVoiceRecording: Bool = false
  @State private var bodyBeforeRecording: String = ""
  @State private var voiceErrorMessage: String?
  @State private var showPhotoLibrarySheet: Bool = false
  @State private var photoLibrarySelection: [PhotosPickerItem] = []
  @State private var showCamera: Bool = false
  @State private var showFileImporter: Bool = false
  @State private var isSavingEntry: Bool = false
  @State private var saveErrorMessage: String?
  @StateObject private var diarySTTService = DeepgramStreamingSTTService()
  @FocusState private var focusedField: Field?
  @FocusState private var focusedBlockId: UUID?

  private enum Field: Hashable {
    case title
    case body
  }

  private var displayedTextForRecording: String {
    if isVoiceRecording {
      let transcript = diarySTTService.userTranscript.trimmingCharacters(
        in: .whitespacesAndNewlines)
      return transcript.isEmpty
        ? bodyBeforeRecording
        : bodyBeforeRecording + (bodyBeforeRecording.isEmpty ? "" : " ") + transcript
    }
    return blocks.textContent
  }

  private var canAddNote: Bool {
    let hasTitle = !title.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
    let hasText = !blocks.textContent.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
    let hasPhotos = blocks.photoCount > 0 || !imagesToInsert.isEmpty || !photoLibrarySelection.isEmpty
    return hasTitle || hasText || hasPhotos
  }

  init(
    accentColor: Color, initialDate: Date, draftTitle: String, draftBody: String,
    onAdd: @escaping (NewJournalEntryDraft) -> Void, onSaveDraft: @escaping (String, String) -> Void
  ) {
    self.accentColor = accentColor
    self.initialDate = initialDate
    self.draftTitle = draftTitle
    self.draftBody = draftBody
    self.onAdd = onAdd
    self.onSaveDraft = onSaveDraft
    _title = State(initialValue: draftTitle)
    _blocks = State(initialValue: draftBody.isEmpty ? [.text("")] : [.text(draftBody)])
  }

  private var toolbarDateText: String {
    let f = DateFormatter()
    f.dateFormat = "EEE, MMM d, yyyy"
    return f.string(from: initialDate)
  }

  private var newNoteSheetContent: some View {
    VStack(alignment: .leading, spacing: 16) {
      Text(toolbarDateText)
        .font(.system(size: 13, weight: .medium))
        .foregroundStyle(.secondary)
        .padding(.top, 8)
        .padding(.bottom, 4)

      TextField("Title", text: $title)
        .textFieldStyle(.plain)
        .font(.system(size: 34, weight: .bold))
        .textInputAutocapitalization(.sentences)
        .disableAutocorrection(false)
        .focused($focusedField, equals: .title)
        .submitLabel(.next)
        .onSubmit {
          focusedBlockId =
            blocks.first(where: {
              if case .text = $0.content { return true }
              return false
            })?.id
        }
        .disabled(isVoiceRecording)

      DiaryBlockListView(
        blocks: $blocks,
        insertImageAfterBlockId: focusedBlockId,
        imagesToInsert: imagesToInsert,
        isVoiceRecording: isVoiceRecording,
        displayedRecordingText: displayedTextForRecording,
        onImagesInserted: { imagesToInsert = [] },
        onBodyChanged: nil,
        onFocusBlock: { focusedBlockId = $0 },
        imageForViewer: $imageForViewer,
        blockIdForViewer: $blockIdForViewer
      )
      .frame(maxWidth: .infinity, alignment: .topLeading)
      .padding(.top, 2)
      .fullScreenCover(
        isPresented: Binding(
          get: { imageForViewer != nil },
          set: {
            if !$0 {
              imageForViewer = nil
              blockIdForViewer = nil
            }
          })
      ) {
        if let img = imageForViewer {
          DiaryImageViewer(
            image: img,
            onDismiss: {
              imageForViewer = nil
              blockIdForViewer = nil
            },
            onDelete: {
              if let id = blockIdForViewer {
                blocks.removeAll { $0.id == id }
              }
              blockIdForViewer = nil
              imageForViewer = nil
            }
          )
        }
      }

      if !attachedFileURLs.isEmpty {
        newNoteFileList
      }

      Spacer(minLength: 120)
    }
    .padding(.horizontal, 18)
    .padding(.bottom, 20)
  }

  private var newNoteFileList: some View {
    VStack(alignment: .leading, spacing: 6) {
      ForEach(Array(attachedFileURLs.enumerated()), id: \.offset) { idx, url in
        HStack(spacing: 8) {
          Image(systemName: "doc.fill")
            .font(.system(size: 14))
            .foregroundStyle(.secondary)
          Text(url.lastPathComponent)
            .font(.system(size: 15))
            .lineLimit(1)
            .truncationMode(.middle)
          Spacer(minLength: 8)
          Button {
            Haptics.impact(.light)
            attachedFileURLs.remove(at: idx)
          } label: {
            Image(systemName: "xmark.circle.fill")
              .font(.system(size: 20))
              .symbolRenderingMode(.hierarchical)
              .foregroundStyle(.secondary)
          }
        }
        .padding(.vertical, 6)
        .padding(.horizontal, 10)
        .background(Color(.secondarySystemBackground))
        .clipShape(RoundedRectangle(cornerRadius: 8, style: .continuous))
      }
    }
    .frame(maxWidth: .infinity, alignment: .leading)
  }

  var body: some View {
    NavigationStack {
      ScrollView {
        newNoteSheetContent
      }
      .scrollDismissesKeyboard(.interactively)
      .background(AppTheme.background)
      .toolbarBackground(.hidden, for: .navigationBar)
      .toolbar {
        ToolbarItem(placement: .topBarLeading) {
          Button {
            Haptics.impact(.light)
            onSaveDraft(title, blocks.textContent)
            dismiss()
          } label: {
            Image(systemName: "chevron.left")
              .font(.system(size: 17, weight: .semibold))
          }
          .accessibilityLabel("Save draft and close")
          .disabled(isVoiceRecording || isSavingEntry)
        }

        ToolbarItemGroup(placement: .topBarTrailing) {
          Menu {
            Button {
              let text = [title, blocks.textContent].joined(separator: "\n\n").trimmingCharacters(
                in: .whitespacesAndNewlines)
              if !text.isEmpty { UIPasteboard.general.string = text }
            } label: {
              Label("Copy", systemImage: "doc.on.doc")
            }
            Button {
              title = ""
            } label: {
              Label("Remove Title", systemImage: "character.cursor.ibeam")
            }
            Divider()
            Button(role: .destructive) {
              onSaveDraft("", "")
              dismiss()
            } label: {
              Label("Discard draft", systemImage: "trash")
            }
          } label: {
            Image(systemName: "ellipsis.circle")
              .font(.system(size: 18, weight: .semibold))
          }
          .accessibilityLabel("More")
          .disabled(isVoiceRecording || isSavingEntry)

          Button("Add") {
            guard canAddNote else {
              Haptics.notification(.warning)
              return
            }
            Haptics.impact(.light)
            Task {
              await MainActor.run { isSavingEntry = true }
              do {
                let draft = try await saveNewEntryWithBlocks()
                await MainActor.run {
                  onAdd(draft)
                  dismiss()
                }
              } catch {
                await MainActor.run {
                  saveErrorMessage = DiaryService.userFacingMessage(from: error)
                }
              }
              await MainActor.run { isSavingEntry = false }
            }
          }
          .fontWeight(.semibold)
          .disabled(isVoiceRecording || isSavingEntry || !canAddNote)
        }
      }
      .safeAreaInset(edge: .bottom, spacing: 0) {
        NoteAccessoryTray(
          accentColor: accentColor,
          isVoiceRecording: isVoiceRecording,
          onAudioTap: { startVoiceRecording() },
          onEndRecording: { endVoiceRecording() },
          onPhotoLibrary: { showPhotoLibrarySheet = true },
          onCamera: {
            if UIImagePickerController.isSourceTypeAvailable(.camera) {
              showCamera = true
            }
          },
          onFiles: { showFileImporter = true }
        )
      }
    }
    .fileImporter(
      isPresented: $showFileImporter, allowedContentTypes: [.item, .pdf, .plainText, .image],
      allowsMultipleSelection: true
    ) { result in
      switch result {
      case .success(let urls):
        attachedFileURLs.append(contentsOf: urls)
      case .failure:
        break
      }
    }
    .photosPicker(
      isPresented: $showPhotoLibrarySheet, selection: $photoLibrarySelection, maxSelectionCount: 5,
      matching: .images
    )
    .onChange(of: photoLibrarySelection) { _, new in
      guard !new.isEmpty else { return }
      Task {
        let images = await loadImagesFromPickerItems(new)
        await MainActor.run {
          imagesToInsert.append(contentsOf: images)
          photoLibrarySelection = []
        }
      }
    }
    .fullScreenCover(isPresented: $showCamera) {
      CameraPickerView(
        onImage: { image in
          imagesToInsert.append(image)
          showCamera = false
        },
        onCancel: { showCamera = false }
      )
      .ignoresSafeArea()
    }
    .alert(
      "Voice input",
      isPresented: Binding(
        get: { voiceErrorMessage != nil },
        set: { if !$0 { voiceErrorMessage = nil } }
      )
    ) {
      Button("OK", role: .cancel) { voiceErrorMessage = nil }
    } message: {
      if let msg = voiceErrorMessage { Text(msg) }
    }
    .alert(
      "Couldn't save note",
      isPresented: Binding(
        get: { saveErrorMessage != nil },
        set: { if !$0 { saveErrorMessage = nil } }
      )
    ) {
      Button("OK", role: .cancel) { saveErrorMessage = nil }
    } message: {
      if let msg = saveErrorMessage { Text(msg) }
    }
  }

  private func loadImagesFromPickerItems(_ items: [PhotosPickerItem]) async -> [UIImage] {
    var result: [UIImage] = []
    for item in items {
      do {
        if let data = try await item.loadTransferable(type: Data.self),
          let image = UIImage(data: data)
        {
          result.append(image)
        }
      } catch {}
    }
    return result
  }

  /// Saves the new note (including image blocks) to Supabase and returns a draft with `entryId` set
  /// so `addEntry` only inserts locally.
  private func saveNewEntryWithBlocks() async throws -> NewJournalEntryDraft {
    let selectedDay = Calendar.current.startOfDay(for: initialDate)
    let today = Calendar.current.startOfDay(for: Date())
    guard selectedDay <= today else {
      throw NSError(
        domain: "Diary",
        code: 422,
        userInfo: [
          NSLocalizedDescriptionKey:
            "You can't add a note for this date yet because that day hasn't happened."
        ]
      )
    }

    // Load any photos still pending in selection (user may tap Add before onChange completes).
    let pendingSelection = await MainActor.run { photoLibrarySelection }
    if !pendingSelection.isEmpty {
      let images = await loadImagesFromPickerItems(pendingSelection)
      await MainActor.run {
        imagesToInsert.append(contentsOf: images)
        photoLibrarySelection = []
      }
    }
    await MainActor.run { flushPendingImagesIntoBlocksNewNote() }

    let hasTitle = !title.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
    let hasText = !blocks.textContent.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
    let hasPhotos = blocks.photoCount > 0
    guard hasTitle || hasText || hasPhotos else {
      throw NSError(
        domain: "Diary",
        code: 422,
        userInfo: [NSLocalizedDescriptionKey: "Add a title, text, or photo before saving."]
      )
    }

    let payload: [DiaryBlockPayload] = await MainActor.run {
      blocks.compactMap { block in
        switch block.content {
        case .text(let s):
          return .text(id: block.id, content: s)
        case .image(_, let path?) where path.isEmpty == false:
          return .imageRemote(id: block.id, remotePath: path)
        case .image(let img?, _):
          return .imageLocal(id: block.id, image: img)
        case .image(nil, _):
          return nil
        }
      }
    }

    let tz = TimeZone.current.abbreviation() ?? "UTC"
    let titleText = title.trimmingCharacters(in: .whitespacesAndNewlines)
    let finalTitle = titleText.isEmpty ? "Untitled" : titleText

    guard let userId = AuthService.shared.currentUserId, let uid = UUID(uuidString: userId) else {
      throw NSError(
        domain: "Diary",
        code: 401,
        userInfo: [NSLocalizedDescriptionKey: "Sign in to save this note."]
      )
    }

    let entryId = try await DiaryService.shared.saveEntry(
      userId: uid,
      entryId: nil,
      date: initialDate,
      title: finalTitle,
      bodyBlocks: payload,
      timezoneAbbreviation: tz
    )
    return NewJournalEntryDraft(
      date: initialDate,
      title: titleText,
      body: blocks.textContent,
      photoCount: blocks.photoCount,
      entryId: entryId
    )
  }

  private func flushPendingImagesIntoBlocksNewNote() {
    guard !imagesToInsert.isEmpty else { return }
    var insertIndex = blocks.count
    if let id = focusedBlockId, let idx = blocks.firstIndex(where: { $0.id == id }) {
      insertIndex = idx + 1
    }
    for image in imagesToInsert.reversed() {
      blocks.insert(.text(""), at: min(insertIndex + 1, blocks.count))
      blocks.insert(.image(image, storagePath: nil), at: min(insertIndex, blocks.count))
    }
    imagesToInsert = []
  }

  private func startVoiceRecording() {
    guard !diarySTTService.isRecording else { return }
    if !UserDefaults.standard.bool(forKey: PreferenceKeys.dictationEnabled) {
      Haptics.notification(.warning)
      voiceErrorMessage = "Turn on Dictation in Settings to use voice input."
      return
    }
    if !NetworkMonitor.shared.isOnline {
      voiceErrorMessage = "Voice input requires internet."
      return
    }
    Task { @MainActor in
      guard await AuthService.shared.getAccessToken() != nil else {
        voiceErrorMessage = "Sign in to use the microphone."
        return
      }
      if !diarySTTService.isConnected {
        await diarySTTService.connect()
      }
      guard diarySTTService.isConnected else {
        voiceErrorMessage = diarySTTService.lastError ?? "Could not connect. Try again."
        return
      }
      bodyBeforeRecording = blocks.textContent
      diarySTTService.startRecording()
      if diarySTTService.lastError != nil {
        voiceErrorMessage = diarySTTService.lastError
        return
      }
      withAnimation(.spring(response: 0.4, dampingFraction: 0.82)) {
        isVoiceRecording = true
      }
    }
  }

  private func endVoiceRecording() {
    diarySTTService.stopRecording()
    Task { @MainActor in
      try? await Task.sleep(nanoseconds: 500_000_000)
      let finalText = diarySTTService.userTranscript.trimmingCharacters(in: .whitespacesAndNewlines)
      let newText =
        finalText.isEmpty
        ? bodyBeforeRecording
        : bodyBeforeRecording + (bodyBeforeRecording.isEmpty ? "" : " ") + finalText
      if let lastIdx = blocks.indices.reversed().first(where: {
        if case .text = blocks[$0].content { return true }
        return false
      }) {
        blocks[lastIdx].content = .text(newText)
      } else {
        blocks.append(.text(newText))
      }
      withAnimation(.spring(response: 0.4, dampingFraction: 0.82)) {
        isVoiceRecording = false
        bodyBeforeRecording = ""
      }
      diarySTTService.disconnect()
    }
  }
}

// MARK: - Edit existing note (same sheet format as new note)

private struct EditDiaryNoteSheet: View {
  @Environment(\.dismiss) private var dismiss
  let entry: JournalEntry
  let accentColor: Color
  let onSave: (JournalEntry) -> Void
  let onDelete: () -> Void

  @State private var title: String
  @State private var blocks: [DiaryBlock] = []
  @State private var imagesToInsert: [UIImage] = []
  @State private var attachedFileURLs: [URL] = []
  @State private var imageForViewer: UIImage?
  @State private var blockIdForViewer: UUID?
  @State private var isVoiceRecording: Bool = false
  @State private var bodyBeforeRecording: String = ""
  @State private var voiceErrorMessage: String?
  @State private var showPhotoLibrarySheet: Bool = false
  @State private var photoLibrarySelection: [PhotosPickerItem] = []
  @State private var showCamera: Bool = false
  @State private var showFileImporter: Bool = false
  @State private var isSavingEntry: Bool = false
  @State private var saveErrorMessage: String?
  @State private var didLoadBlocksFromApi: Bool = false
  @State private var isLoadingBlocksFromApi: Bool = false
  @StateObject private var diarySTTService = DeepgramStreamingSTTService()
  @FocusState private var focusedField: EditSheetField?
  @FocusState private var focusedBlockId: UUID?

  private enum EditSheetField: Hashable {
    case title
    case body
  }

  private var displayedTextForRecording: String {
    if isVoiceRecording {
      let transcript = diarySTTService.userTranscript.trimmingCharacters(
        in: .whitespacesAndNewlines)
      return transcript.isEmpty
        ? bodyBeforeRecording
        : bodyBeforeRecording + (bodyBeforeRecording.isEmpty ? "" : " ") + transcript
    }
    return blocks.textContent
  }

  init(
    entry: JournalEntry, accentColor: Color, onSave: @escaping (JournalEntry) -> Void,
    onDelete: @escaping () -> Void
  ) {
    self.entry = entry
    self.accentColor = accentColor
    self.onSave = onSave
    self.onDelete = onDelete
    _title = State(initialValue: entry.title)
  }

  private var toolbarDateText: String {
    let f = DateFormatter()
    f.dateFormat = "EEE, MMM d, yyyy"
    return f.string(from: entry.date)
  }

  private var editSheetContent: some View {
    VStack(alignment: .leading, spacing: 16) {
      Text(toolbarDateText)
        .font(.system(size: 13, weight: .medium))
        .foregroundStyle(.secondary)
        .padding(.top, 8)
        .padding(.bottom, 4)

      TextField("Title", text: $title)
        .textFieldStyle(.plain)
        .font(.system(size: 34, weight: .bold))
        .textInputAutocapitalization(.sentences)
        .disableAutocorrection(false)
        .focused($focusedField, equals: .title)
        .submitLabel(.next)
        .onSubmit {
          focusedBlockId =
            blocks.first(where: {
              if case .text = $0.content { return true }
              return false
            })?.id
        }
        .disabled(isVoiceRecording)

      DiaryBlockListView(
        blocks: $blocks,
        insertImageAfterBlockId: focusedBlockId,
        imagesToInsert: imagesToInsert,
        isVoiceRecording: isVoiceRecording,
        displayedRecordingText: displayedTextForRecording,
        onImagesInserted: { imagesToInsert = [] },
        onBodyChanged: nil,
        onFocusBlock: { focusedBlockId = $0 },
        imageForViewer: $imageForViewer,
        blockIdForViewer: $blockIdForViewer
      )
      .frame(maxWidth: .infinity, alignment: .topLeading)
      .padding(.top, 2)
      .fullScreenCover(
        isPresented: Binding(
          get: { imageForViewer != nil },
          set: {
            if !$0 {
              imageForViewer = nil
              blockIdForViewer = nil
            }
          })
      ) {
        if let img = imageForViewer {
          DiaryImageViewer(
            image: img,
            onDismiss: {
              imageForViewer = nil
              blockIdForViewer = nil
            },
            onDelete: {
              if let id = blockIdForViewer {
                blocks.removeAll { $0.id == id }
              }
              blockIdForViewer = nil
              imageForViewer = nil
            }
          )
        }
      }

      if !attachedFileURLs.isEmpty {
        editSheetFileList
      }

      Spacer(minLength: 120)
    }
    .padding(.horizontal, 18)
    .padding(.bottom, 20)
  }

  private var editSheetFileList: some View {
    VStack(alignment: .leading, spacing: 6) {
      ForEach(Array(attachedFileURLs.enumerated()), id: \.offset) { idx, url in
        HStack(spacing: 8) {
          Image(systemName: "doc.fill")
            .font(.system(size: 14))
            .foregroundStyle(.secondary)
          Text(url.lastPathComponent)
            .font(.system(size: 15))
            .lineLimit(1)
            .truncationMode(.middle)
          Spacer(minLength: 8)
          Button {
            Haptics.impact(.light)
            attachedFileURLs.remove(at: idx)
          } label: {
            Image(systemName: "xmark.circle.fill")
              .font(.system(size: 20))
              .symbolRenderingMode(.hierarchical)
              .foregroundStyle(.secondary)
          }
        }
        .padding(.vertical, 6)
        .padding(.horizontal, 10)
        .background(Color(.secondarySystemBackground))
        .clipShape(RoundedRectangle(cornerRadius: 8, style: .continuous))
      }
    }
    .frame(maxWidth: .infinity, alignment: .leading)
  }

  private var editNoteBase: some View {
    NavigationStack {
      ScrollView {
        if isLoadingBlocksFromApi && blocks.isEmpty {
          VStack(spacing: 12) {
            ProgressView()
              .scaleEffect(1.1)
              .padding(.top, 40)
            Text("Loading note…")
              .font(.system(size: 15, weight: .medium))
              .foregroundStyle(.secondary)
          }
          .frame(maxWidth: .infinity, maxHeight: .infinity)
        } else {
          editSheetContent
        }
      }
      .scrollDismissesKeyboard(.interactively)
      .background(AppTheme.background)
      .toolbarBackground(.hidden, for: .navigationBar)
      .toolbar {
        ToolbarItem(placement: .topBarLeading) {
          Button {
            Haptics.impact(.light)
            dismiss()
          } label: {
            Image(systemName: "chevron.left")
              .font(.system(size: 17, weight: .semibold))
          }
          .accessibilityLabel("Close")
          .disabled(isVoiceRecording || isSavingEntry)
        }

        ToolbarItemGroup(placement: .topBarTrailing) {
          Menu {
            Button {
              let text = [title, blocks.textContent].joined(separator: "\n\n").trimmingCharacters(
                in: .whitespacesAndNewlines)
              if !text.isEmpty { UIPasteboard.general.string = text }
            } label: {
              Label("Copy", systemImage: "doc.on.doc")
            }
            Button {
              title = ""
            } label: {
              Label("Remove Title", systemImage: "character.cursor.ibeam")
            }
            Divider()
            Button(role: .destructive) {
              Haptics.impact(.light)
              Task {
                if let userId = AuthService.shared.currentUserId, let uid = UUID(uuidString: userId)
                {
                  try? await DiaryService.shared.deleteEntry(userId: uid, entryId: entry.id)
                }
                await MainActor.run {
                  onDelete()
                  dismiss()
                }
              }
            } label: {
              Label("Move to Trash", systemImage: "trash")
            }
          } label: {
            Image(systemName: "ellipsis.circle")
              .font(.system(size: 18, weight: .semibold))
          }
          .accessibilityLabel("More")
          .disabled(isVoiceRecording || isSavingEntry)

          Button("Save") {
            Haptics.impact(.light)
            Task {
              await MainActor.run { isSavingEntry = true }
              let didSave = await saveEntryToSupabase()
              await MainActor.run {
                isSavingEntry = false
                if didSave {
                  let updated = JournalEntry(
                    id: entry.id,
                    date: entry.date,
                    title: title.trimmingCharacters(in: .whitespacesAndNewlines),
                    body: blocks.textContent,
                    createdAt: entry.createdAt,
                    timezoneAbbreviation: entry.timezoneAbbreviation,
                    photoCount: blocks.photoCount,
                    firstPhotoURL: entry.firstPhotoURL
                  )
                  onSave(updated)
                  dismiss()
                }
              }
            }
          }
          .fontWeight(.semibold)
          .disabled(isVoiceRecording || isSavingEntry)
        }
      }
      .safeAreaInset(edge: .bottom, spacing: 0) {
        NoteAccessoryTray(
          accentColor: accentColor,
          isVoiceRecording: isVoiceRecording,
          onAudioTap: { startVoiceRecording() },
          onEndRecording: { endVoiceRecording() },
          onPhotoLibrary: { showPhotoLibrarySheet = true },
          onCamera: {
            if UIImagePickerController.isSourceTypeAvailable(.camera) {
              showCamera = true
            }
          },
          onFiles: { showFileImporter = true }
        )
      }
    }
  }

  var body: some View {
    editNoteBase
    .fileImporter(
      isPresented: $showFileImporter, allowedContentTypes: [.item, .pdf, .plainText, .image],
      allowsMultipleSelection: true
    ) { result in
      switch result {
      case .success(let urls):
        attachedFileURLs.append(contentsOf: urls)
      case .failure:
        break
      }
    }
    .photosPicker(
      isPresented: $showPhotoLibrarySheet, selection: $photoLibrarySelection, maxSelectionCount: 5,
      matching: .images
    )
    .onChange(of: photoLibrarySelection) { _, new in
      guard !new.isEmpty else { return }
      Task {
        let images = await loadImagesFromPickerItems(new)
        await MainActor.run {
          imagesToInsert.append(contentsOf: images)
          photoLibrarySelection = []
        }
      }
    }
    .fullScreenCover(isPresented: $showCamera) {
      CameraPickerView(
        onImage: { image in
          imagesToInsert.append(image)
          showCamera = false
        },
        onCancel: { showCamera = false }
      )
      .ignoresSafeArea()
    }
    .alert(
      "Voice input",
      isPresented: Binding(
        get: { voiceErrorMessage != nil },
        set: { if !$0 { voiceErrorMessage = nil } }
      )
    ) {
      Button("OK", role: .cancel) { voiceErrorMessage = nil }
    } message: {
      if let msg = voiceErrorMessage { Text(msg) }
    }
    .alert(
      "Couldn't save note",
      isPresented: Binding(
        get: { saveErrorMessage != nil },
        set: { if !$0 { saveErrorMessage = nil } }
      )
    ) {
      Button("OK", role: .cancel) { saveErrorMessage = nil }
    } message: {
      if let msg = saveErrorMessage { Text(msg) }
    }
    .onAppear {
      if blocks.isEmpty, !didLoadBlocksFromApi {
        didLoadBlocksFromApi = true
        isLoadingBlocksFromApi = true
        guard let userId = AuthService.shared.currentUserId, let uid = UUID(uuidString: userId)
        else {
          blocks = entry.body.isEmpty ? [.text("")] : [.text(entry.body)]
          isLoadingBlocksFromApi = false
          return
        }
        Task {
          defer { Task { @MainActor in isLoadingBlocksFromApi = false } }
          do {
            guard
              let row = try await DiaryService.shared.fetchEntry(userId: uid, entryId: entry.id),
              !row.body_blocks.isEmpty
            else {
              await MainActor.run {
                blocks = entry.body.isEmpty ? [.text("")] : [.text(entry.body)]
                DispatchQueue.main.asyncAfter(deadline: .now() + 0.25) {
                  focusedBlockId =
                    blocks.first(where: {
                      if case .text = $0.content { return true }
                      return false
                    })?.id
                }
              }
              return
            }
            let decoded = DiaryService.decodeBodyBlocks(row.body_blocks)
            var loaded: [DiaryBlock] = []
            for d in decoded {
              switch d.content {
              case .text(let s):
                loaded.append(DiaryBlock(id: d.id, content: .text(s)))
              case .imageURL(let url, let path):
                let img = await DiaryService.shared.loadImageFromURL(url)
                loaded.append(DiaryBlock(id: d.id, content: .image(img, storagePath: path)))
              }
            }
            await MainActor.run {
              blocks =
                loaded.isEmpty ? (entry.body.isEmpty ? [.text("")] : [.text(entry.body)]) : loaded
              DispatchQueue.main.asyncAfter(deadline: .now() + 0.25) {
                focusedBlockId =
                  blocks.first(where: {
                    if case .text = $0.content { return true }
                    return false
                  })?.id
              }
            }
          } catch {
            await MainActor.run {
              blocks = entry.body.isEmpty ? [.text("")] : [.text(entry.body)]
              DispatchQueue.main.asyncAfter(deadline: .now() + 0.25) {
                focusedBlockId =
                  blocks.first(where: {
                    if case .text = $0.content { return true }
                    return false
                  })?.id
              }
            }
          }
        }
      }
    }
  }

  private func loadImagesFromPickerItems(_ items: [PhotosPickerItem]) async -> [UIImage] {
    var result: [UIImage] = []
    for item in items {
      do {
        if let data = try await item.loadTransferable(type: Data.self),
          let image = UIImage(data: data)
        {
          result.append(image)
        }
      } catch {}
    }
    return result
  }

  private func saveEntryToSupabase() async -> Bool {
    let pendingSelection = await MainActor.run { photoLibrarySelection }
    if !pendingSelection.isEmpty {
      let images = await loadImagesFromPickerItems(pendingSelection)
      await MainActor.run {
        imagesToInsert.append(contentsOf: images)
        photoLibrarySelection = []
      }
    }
    await MainActor.run { flushPendingImagesIntoBlocks() }

    guard let userId = AuthService.shared.currentUserId, let uid = UUID(uuidString: userId) else {
      await MainActor.run { saveErrorMessage = "Sign in to save this note." }
      return false
    }
    let payload: [DiaryBlockPayload] = await MainActor.run {
      blocks.compactMap { block in
        switch block.content {
        case .text(let s):
          return .text(id: block.id, content: s)
        case .image(_, let path?) where path.isEmpty == false:
          return .imageRemote(id: block.id, remotePath: path)
        case .image(let img?, _):
          return .imageLocal(id: block.id, image: img)
        case .image(nil, _):
          return nil
        }
      }
    }
    let tz = TimeZone.current.abbreviation() ?? "UTC"
    let titleText = title.trimmingCharacters(in: .whitespacesAndNewlines)
    let finalTitle = titleText.isEmpty ? "Untitled" : titleText
    do {
      _ = try await DiaryService.shared.saveEntry(
        userId: uid,
        entryId: entry.id,
        date: entry.date,
        title: finalTitle,
        bodyBlocks: payload,
        timezoneAbbreviation: tz
      )
      await MainActor.run { saveErrorMessage = nil }
      return true
    } catch {
      await MainActor.run { saveErrorMessage = DiaryService.userFacingMessage(from: error) }
      return false
    }
  }

  private func flushPendingImagesIntoBlocks() {
    guard !imagesToInsert.isEmpty else { return }
    var insertIndex = blocks.count
    if let id = focusedBlockId, let idx = blocks.firstIndex(where: { $0.id == id }) {
      insertIndex = idx + 1
    }
    for image in imagesToInsert.reversed() {
      blocks.insert(.text(""), at: min(insertIndex + 1, blocks.count))
      blocks.insert(.image(image, storagePath: nil), at: min(insertIndex, blocks.count))
    }
    imagesToInsert = []
  }

  private func startVoiceRecording() {
    guard !diarySTTService.isRecording else { return }
    if !UserDefaults.standard.bool(forKey: PreferenceKeys.dictationEnabled) {
      Haptics.notification(.warning)
      voiceErrorMessage = "Turn on Dictation in Settings to use voice input."
      return
    }
    if !NetworkMonitor.shared.isOnline {
      voiceErrorMessage = "Voice input requires internet."
      return
    }
    Task { @MainActor in
      guard await AuthService.shared.getAccessToken() != nil else {
        voiceErrorMessage = "Sign in to use the microphone."
        return
      }
      if !diarySTTService.isConnected {
        await diarySTTService.connect()
      }
      guard diarySTTService.isConnected else {
        voiceErrorMessage = diarySTTService.lastError ?? "Could not connect. Try again."
        return
      }
      bodyBeforeRecording = blocks.textContent
      diarySTTService.startRecording()
      if diarySTTService.lastError != nil {
        voiceErrorMessage = diarySTTService.lastError
        return
      }
      withAnimation(.spring(response: 0.4, dampingFraction: 0.82)) {
        isVoiceRecording = true
      }
    }
  }

  private func endVoiceRecording() {
    diarySTTService.stopRecording()
    Task { @MainActor in
      try? await Task.sleep(nanoseconds: 500_000_000)
      let finalText = diarySTTService.userTranscript.trimmingCharacters(in: .whitespacesAndNewlines)
      let newText =
        finalText.isEmpty
        ? bodyBeforeRecording
        : bodyBeforeRecording + (bodyBeforeRecording.isEmpty ? "" : " ") + finalText
      if let lastIdx = blocks.indices.reversed().first(where: {
        if case .text = blocks[$0].content { return true }
        return false
      }) {
        blocks[lastIdx].content = .text(newText)
      } else {
        blocks.append(.text(newText))
      }
      withAnimation(.spring(response: 0.4, dampingFraction: 0.82)) {
        isVoiceRecording = false
        bodyBeforeRecording = ""
      }
      diarySTTService.disconnect()
    }
  }
}

// MARK: - Diary photo library sheet

private struct DiaryPhotoLibrarySheet: View {
  @Environment(\.dismiss) private var dismiss
  @State private var selectedItems: [PhotosPickerItem] = []
  var onImagesPicked: (([UIImage]) -> Void)?

  var body: some View {
    NavigationStack {
      VStack(spacing: 24) {
        PhotosPicker(selection: $selectedItems, maxSelectionCount: 5, matching: .images) {
          Label("Choose from Photo Library", systemImage: "photo.on.rectangle.angled")
            .font(.system(size: 17, weight: .medium))
            .frame(maxWidth: .infinity)
            .padding(.vertical, 16)
        }
        .buttonStyle(.borderedProminent)
        .padding(.horizontal, 24)
        .padding(.top, 32)

        Spacer()
      }
      .navigationTitle("Add Photo")
      .navigationBarTitleDisplayMode(.inline)
      .toolbar {
        ToolbarItem(placement: .cancellationAction) {
          Button("Cancel") { dismiss() }
        }
      }
      .onChange(of: selectedItems) { _, new in
        guard !new.isEmpty else { return }
        Task {
          let images = await loadImages(from: new)
          await MainActor.run {
            if !images.isEmpty { onImagesPicked?(images) }
            selectedItems = []
            dismiss()
          }
        }
      }
    }
  }

  private func loadImages(from items: [PhotosPickerItem]) async -> [UIImage] {
    var result: [UIImage] = []
    for item in items {
      do {
        if let data = try await item.loadTransferable(type: Data.self),
          let image = UIImage(data: data)
        {
          result.append(image)
        }
      } catch {}
    }
    return result
  }
}

// MARK: - Bottom accessory tray – single Liquid Glass group (iOS 26)

private struct NoteAccessoryTray: View {
  @AppStorage(PreferenceKeys.elevenLabsVoiceName) private var selectedVoiceName: String = ""
  let accentColor: Color
  var isVoiceRecording: Bool = false
  var onAudioTap: (() -> Void)? = nil
  var onEndRecording: (() -> Void)? = nil
  var onPhotoLibrary: (() -> Void)? = nil
  var onCamera: (() -> Void)? = nil
  var onFiles: (() -> Void)? = nil

  private var effectiveAudioAction: () -> Void {
    { if isVoiceRecording { onEndRecording?() } else { onAudioTap?() } }
  }

  var body: some View {
    VStack(spacing: 10) {
      Divider()
        .opacity(0.12)

      HStack(alignment: .bottom, spacing: 8) {
        if #available(iOS 26.0, *) {
          NoteAccessoryGlassBar(
            accentColor: accentColor,
            isVoiceRecording: isVoiceRecording,
            onAudioTap: effectiveAudioAction,
            onPhotoLibrary: onPhotoLibrary,
            onCamera: onCamera,
            onFiles: onFiles
          )
          .transition(.move(edge: .bottom).combined(with: .opacity))
        } else {
          NoteAccessoryFallbackBar(
            accentColor: accentColor,
            isVoiceRecording: isVoiceRecording,
            onAudioTap: effectiveAudioAction,
            onPhotoLibrary: onPhotoLibrary,
            onCamera: onCamera,
            onFiles: onFiles
          )
          .transition(.move(edge: .bottom).combined(with: .opacity))
        }

        if isVoiceRecording {
          Button(action: {
            Haptics.impact(.medium)
            onEndRecording?()
          }) {
            NoteAccessoryRecordingGhostView(selectedVoiceName: selectedVoiceName)
              .frame(width: 56, height: 56)
          }
          .buttonStyle(.plain)
          .accessibilityLabel("Stop recording")
          .transition(.move(edge: .trailing).combined(with: .opacity))
        }
      }
    }
    .padding(.horizontal, isVoiceRecording ? 16 : 20)
    .padding(.bottom, 10)
    .shadow(color: Color.black.opacity(0.06), radius: 8, x: 0, y: -2)
    .animation(.spring(response: 0.4, dampingFraction: 0.82), value: isVoiceRecording)
  }
}

private struct NoteAccessoryRecordingGhostView: View {
  let selectedVoiceName: String

  private var ghostVideoName: String {
    ElevenLabsVoiceSuggestionsView.ghostVideoName(for: selectedVoiceName)
      ?? selectedVoiceName.lowercased().trimmingCharacters(in: .whitespacesAndNewlines)
  }

  private var hasGhostVideo: Bool {
    guard !ghostVideoName.isEmpty else { return false }
    return Bundle.main.url(forResource: ghostVideoName, withExtension: "mp4") != nil
  }

  var body: some View {
    Group {
      if hasGhostVideo {
        TransparentVideoPlayerView(
          videoName: ghostVideoName,
          videoExtension: "mp4",
          startTime: ElevenLabsVoiceSuggestionsView.ghostStartTimes[ghostVideoName] ?? 0
        )
      } else if let uiImage = ElevenLabsVoiceSuggestionsView.ghostUIImage(for: selectedVoiceName) {
        Image(uiImage: uiImage)
          .resizable()
          .scaledToFit()
      } else {
        Image(systemName: "person.crop.circle.fill")
          .resizable()
          .scaledToFit()
          .foregroundStyle(.secondary)
          .padding(6)
      }
    }
  }
}

@available(iOS 26.0, *)
private struct NoteAccessoryGlassBar: View {
  let accentColor: Color
  var isVoiceRecording: Bool = false
  var onAudioTap: (() -> Void)? = nil
  var onPhotoLibrary: (() -> Void)? = nil
  var onCamera: (() -> Void)? = nil
  var onFiles: (() -> Void)? = nil

  private let glassShape = RoundedRectangle(cornerRadius: 24, style: .continuous)

  var body: some View {
    GlassEffectContainer(spacing: 0) {
      HStack(spacing: 0) {
        Menu {
          Button {
            Haptics.impact(.light)
            onPhotoLibrary?()
          } label: {
            Label("Photo Library", systemImage: "photo.on.rectangle.angled")
          }
          Button {
            Haptics.impact(.light)
            onCamera?()
          } label: {
            Label("Camera", systemImage: "camera")
          }
          Button {
            Haptics.impact(.light)
            onFiles?()
          } label: {
            Label("Files", systemImage: "doc")
          }
        } label: {
          accessoryIconLabel(systemName: "paperclip")
        }
        .buttonStyle(.plain)
        .frame(maxWidth: .infinity)
        .contentShape(Rectangle())
        .accessibilityLabel("Attach")

        noteAccessoryDivider()

        Button {
          Haptics.impact(.light)
          onPhotoLibrary?()
        } label: {
          accessoryIconLabel(systemName: "photo.on.rectangle.angled")
        }
        .buttonStyle(.plain)
        .frame(maxWidth: .infinity)
        .contentShape(Rectangle())
        .accessibilityLabel("Photo library")

        noteAccessoryDivider()

        Button {
          Haptics.impact(.light)
          onCamera?()
        } label: {
          accessoryIconLabel(systemName: "camera")
        }
        .buttonStyle(.plain)
        .frame(maxWidth: .infinity)
        .contentShape(Rectangle())
        .accessibilityLabel("Camera")

        noteAccessoryDivider()

        Button {
          Haptics.impact(.light)
          onAudioTap?()
        } label: {
          accessoryIconLabel(
            systemName: isVoiceRecording ? "stop.fill" : "waveform",
            tint: isVoiceRecording ? .red : .primary
          )
        }
        .buttonStyle(.plain)
        .frame(maxWidth: .infinity)
        .contentShape(Rectangle())
        .accessibilityLabel(isVoiceRecording ? "Stop recording" : "Audio")
      }
      .frame(maxWidth: .infinity)
      .contentShape(Rectangle())
      .padding(.horizontal, 8)
      .padding(.vertical, 8)
      .frame(height: 52)
    }
    .glassEffect(.regular.interactive(), in: glassShape)
  }

  private func noteAccessoryDivider() -> some View {
    Rectangle()
      .fill(Color.primary.opacity(0.35))
      .frame(width: 1, height: 28)
      .allowsHitTesting(false)
      .accessibilityHidden(true)
  }

  private func accessoryIconLabel(systemName: String, tint: Color = .primary) -> some View {
    Image(systemName: systemName)
      .font(.system(size: 18, weight: .medium))
      .symbolRenderingMode(.hierarchical)
      .foregroundStyle(tint)
      .frame(maxWidth: .infinity)
      .padding(.vertical, 10)
      .contentShape(Rectangle())
  }
}

private struct NoteAccessoryFallbackBar: View {
  let accentColor: Color
  var isVoiceRecording: Bool = false
  var onAudioTap: (() -> Void)? = nil
  var onPhotoLibrary: (() -> Void)? = nil
  var onCamera: (() -> Void)? = nil
  var onFiles: (() -> Void)? = nil

  private let barShape = RoundedRectangle(cornerRadius: 24, style: .continuous)

  var body: some View {
    HStack(spacing: 0) {
      Menu {
        Button {
          Haptics.impact(.light)
          onPhotoLibrary?()
        } label: {
          Label("Photo Library", systemImage: "photo.on.rectangle.angled")
        }
        Button {
          Haptics.impact(.light)
          onCamera?()
        } label: {
          Label("Camera", systemImage: "camera")
        }
        Button {
          Haptics.impact(.light)
          onFiles?()
        } label: {
          Label("Files", systemImage: "doc")
        }
      } label: {
        accessoryIconLabel(systemName: "paperclip")
      }
      .buttonStyle(.plain)
      .frame(maxWidth: .infinity)
      .contentShape(Rectangle())
      .accessibilityLabel("Attach")
      fallbackDivider()
      Button {
        Haptics.impact(.light)
        onPhotoLibrary?()
      } label: {
        accessoryIconLabel(systemName: "photo.on.rectangle.angled")
      }
      .buttonStyle(.plain)
      .frame(maxWidth: .infinity)
      .contentShape(Rectangle())
      .accessibilityLabel("Photo library")
      fallbackDivider()
      Button {
        Haptics.impact(.light)
        onCamera?()
      } label: {
        accessoryIconLabel(systemName: "camera")
      }
      .buttonStyle(.plain)
      .frame(maxWidth: .infinity)
      .contentShape(Rectangle())
      .accessibilityLabel("Camera")
      fallbackDivider()
      Button {
        Haptics.impact(.light)
        onAudioTap?()
      } label: {
        accessoryIconLabel(
          systemName: isVoiceRecording ? "stop.fill" : "waveform",
          tint: isVoiceRecording ? .red : .primary
        )
      }
      .buttonStyle(.plain)
      .frame(maxWidth: .infinity)
      .contentShape(Rectangle())
      .accessibilityLabel(isVoiceRecording ? "Stop recording" : "Audio")
    }
    .frame(maxWidth: .infinity)
    .contentShape(Rectangle())
    .padding(.horizontal, 8)
    .padding(.vertical, 8)
    .frame(height: 52)
    .background(barShape.fill(.ultraThinMaterial))
  }

  private func fallbackDivider() -> some View {
    Rectangle()
      .fill(Color.primary.opacity(0.35))
      .frame(width: 1, height: 28)
      .allowsHitTesting(false)
      .accessibilityHidden(true)
  }

  private func accessoryIconLabel(systemName: String, tint: Color = .primary) -> some View {
    Image(systemName: systemName)
      .font(.system(size: 18, weight: .medium))
      .symbolRenderingMode(.hierarchical)
      .foregroundStyle(tint)
      .frame(maxWidth: .infinity)
      .padding(.vertical, 10)
      .contentShape(Rectangle())
  }
}

// MARK: - Color hex (for diary header color persistence)
// init?(hex:) is provided in Shared/AppTheme.swift

extension Color {
  fileprivate var hexString: String {
    let uic = UIColor(self)
    var r: CGFloat = 0
    var g: CGFloat = 0
    var b: CGFloat = 0
    var a: CGFloat = 0
    uic.getRed(&r, green: &g, blue: &b, alpha: &a)
    return String(format: "#%02X%02X%02X", Int(r * 255), Int(g * 255), Int(b * 255))
  }
}
