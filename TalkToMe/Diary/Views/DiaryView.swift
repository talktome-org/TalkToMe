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
  @State private var showPastCalendar: Bool = false
  @AppStorage("diary_friend_card_dismissed") private var friendCardDismissed: Bool = false
  @State private var showFriendsSection: Bool = false
  @State private var showDescriptionEditor: Bool = false

  var body: some View {
    NavigationStack {
      ZStack(alignment: .bottomTrailing) {
        AppTheme.background.ignoresSafeArea()

        VStack(spacing: 0) {
          DiaryHeroCardView(name: viewModel.diaryName, gradientColors: heroGradientColors) {
            showDescriptionEditor = true
          }

          if !friendCardDismissed {
            addFriendCard
              .padding(.horizontal, 16)
              .padding(.bottom, 4)
              .zIndex(1)
          }

          JournalSheetView(
            tab: $tab,
            stats: viewModel.stats,
            entries: $viewModel.entries,
            todoItems: $viewModel.todoItems,
            accentColor: viewModel.diaryColor,
            diaryDescription: viewModel.diaryDescription,
            onAddEntryForDate: { date in
              newEntrySession = NewEntrySession(initialDate: date)
            },
            onSelectEntry: { entry in
              editEntrySession = EditEntrySession(entry: entry)
            },
            onDeleteEntry: { entry in
              deleteEntry(entry)
            },
            onTodoChange: {
              viewModel.saveTodos()
            }
          )
          .frame(maxWidth: .infinity, maxHeight: .infinity)
          .padding(.top, -70)
        }
        .padding(.top, 0)
        .padding(.bottom, 0)
        .ignoresSafeArea(edges: [.top, .bottom])

        GlassFloatingActionButton(systemName: "plus") {
          Haptics.impact(.light)
          newEntrySession = NewEntrySession(initialDate: Date())
        }
        .padding(.trailing, 20)
        .padding(.bottom, 24)
      }
      .navigationTitle("")
      .navigationBarTitleDisplayMode(.inline)
      .toolbarVisibility(.hidden, for: .navigationBar)
      .navigationDestination(isPresented: $showFriendsSection) {
        FriendsAndContactsSectionView()
      }
      .navigationDestination(isPresented: $showDescriptionEditor) {
        DiaryDescriptionEditorView(
          name: viewModel.diaryName,
          description: viewModel.diaryDescription,
          color: viewModel.diaryColor,
          onChange: { newName, newDescription, newColor in
            viewModel.saveSettings(
              name: newName,
              description: newDescription,
              color: newColor
            )
          }
        )
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
    .sheet(isPresented: $showPastCalendar) {
      PastCalendarSheet(
        entries: $viewModel.entries,
        accentColor: viewModel.diaryColor,
        selectedDay: Binding(
          get: { Calendar.current.startOfDay(for: Date()) },
          set: { _ in }
        ),
        onAddEntryForDate: { date in
          newEntrySession = NewEntrySession(initialDate: date)
        },
        onSelectEntry: { entry in
          editEntrySession = EditEntrySession(entry: entry)
        }
      )
    }
    .onAppear { viewModel.loadIfNeeded() }
    .onChange(of: AuthService.shared.currentUserId) { _, _ in viewModel.loadIfNeeded() }
    .onReceive(NotificationCenter.default.publisher(for: .openNewDiaryEntry)) { _ in
      newEntrySession = NewEntrySession(initialDate: Date())
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

  private var addFriendCard: some View {
    let shape = RoundedRectangle(cornerRadius: 14, style: .continuous)
    let gradient = LinearGradient(
      colors: [Color(red: 0.63, green: 0.32, blue: 0.98), Color(red: 0.90, green: 0.40, blue: 0.65)],
      startPoint: .leading,
      endPoint: .trailing
    )

    return Button {
      Haptics.impact(.light)
      withAnimation(.spring(response: 0.3, dampingFraction: 0.8)) {
        friendCardDismissed = true
      }
      showFriendsSection = true
    } label: {
      HStack(spacing: 12) {
        Image(systemName: "person.2.fill")
          .font(.system(size: 16, weight: .semibold))
          .foregroundStyle(gradient)

        Text("Add a friend")
          .font(.system(size: 14, weight: .semibold))
          .foregroundStyle(.primary)

        Spacer(minLength: 0)

        Image(systemName: "chevron.right")
          .font(.system(size: 12, weight: .semibold))
          .foregroundStyle(.tertiary)
      }
      .padding(.horizontal, 14)
      .padding(.vertical, 11)
      .background { GlassCardBackground(shape: shape) }
      .clipShape(shape)
      .overlay(
        shape
          .stroke(gradient.opacity(0.2), lineWidth: 0.6)
      )
    }
    .buttonStyle(.plain)
    .transition(.asymmetric(
      insertion: .scale(scale: 0.95).combined(with: .opacity),
      removal: .scale(scale: 0.95).combined(with: .opacity)
    ))
  }

  private var heroGradientColors: [Color] {
    let base = viewModel.diaryColor
    let peach = base.blended(with: Color(red: 1.00, green: 0.84, blue: 0.76), amount: 0.36)
    let mint = base.blended(with: Color(red: 0.74, green: 0.96, blue: 0.86), amount: 0.50)
    let sky = base.blended(with: Color(red: 0.67, green: 0.85, blue: 1.00), amount: 0.62)
    let lilac = base.blended(with: Color(red: 0.86, green: 0.80, blue: 1.00), amount: 0.34)
    return [
      peach,
      mint,
      sky,
      lilac,
      mint.blended(with: lilac, amount: 0.42),
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
        LocalDatabase.shared.deleteDiaryEntry(id: entry.id)
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
  let name: String
  let gradientColors: [Color]
  var onEdit: (() -> Void)?

  private var displayHeaderDateString: String {
    let now = Date()
    let cal = Calendar.current
    let day = cal.component(.day, from: now)
    let year = cal.component(.year, from: now)

    let weekdayFormatter = DateFormatter()
    weekdayFormatter.dateFormat = "EEEE"
    let weekday = weekdayFormatter.string(from: now)

    let monthFormatter = DateFormatter()
    monthFormatter.dateFormat = "MMM"
    let month = monthFormatter.string(from: now)

    return "\(weekday), \(month) \(day) • \(year)"
  }

  private var baseGradientColors: [Color] {
    [
      AppTheme.brand.opacity(0.98),
      AppTheme.brand.opacity(0.92),
      AppTheme.accent.opacity(0.90),
      AppTheme.accent.opacity(0.84),
      AppTheme.accent.opacity(0.76),
    ]
  }

  private var styledGradientColors: [Color] {
    let source = gradientColors.isEmpty ? baseGradientColors : gradientColors
    if colorScheme == .dark {
      let deepBase = AppTheme.background.blended(with: .black, amount: 0.25)
      return [
        source[0].blended(with: deepBase, amount: 0.40),
        source[1].blended(with: .black, amount: 0.34),
        source[2].blended(with: deepBase, amount: 0.46),
        source[3].blended(with: .black, amount: 0.38),
        source[4].blended(with: deepBase, amount: 0.42),
      ]
    }

    let lightLift = Color(red: 0.98, green: 0.99, blue: 1.00)
    return [
      source[0].blended(with: .white, amount: 0.34),
      source[1].blended(with: lightLift, amount: 0.32),
      source[2].blended(with: .white, amount: 0.24),
      source[3].blended(with: lightLift, amount: 0.28),
      source[4].blended(with: .white, amount: 0.22),
    ]
  }

  var body: some View {
    let source = styledGradientColors
    let c0 = source[0]
    let c1 = source[min(1, source.count - 1)]
    let c2 = source[min(2, source.count - 1)]
    let c3 = source[min(3, source.count - 1)]
    let c4 = source[min(4, source.count - 1)]

    ZStack {
      LinearGradient(
        colors: [c0, c1, c2, c3, c4],
        startPoint: .topLeading,
        endPoint: .bottomTrailing
      )

      RadialGradient(
        colors: [
          c1.opacity(colorScheme == .dark ? 0.54 : 0.72),
          .clear,
        ],
        center: .topLeading,
        startRadius: 24,
        endRadius: 290
      )

      RadialGradient(
        colors: [
          c3.opacity(colorScheme == .dark ? 0.48 : 0.62),
          .clear,
        ],
        center: .bottomTrailing,
        startRadius: 20,
        endRadius: 260
      )

      RadialGradient(
        colors: [
          .white.opacity(colorScheme == .dark ? 0.06 : 0.22),
          .clear,
        ],
        center: .top,
        startRadius: 10,
        endRadius: 230
      )

      LinearGradient(
        colors: [
          .white.opacity(colorScheme == .dark ? 0.04 : 0.22),
          .clear,
          .black.opacity(colorScheme == .dark ? 0.26 : 0.09),
        ],
        startPoint: .top,
        endPoint: .bottom
      )

      FloatingOrb(
        size: 210,
        color: .white.opacity(colorScheme == .dark ? 0.18 : 0.42),
        startOffset: CGSize(width: -118, height: -36),
        drift: CGSize(width: 20, height: 18),
        blurRadius: 1.5,
        duration: 8.0
      )

      FloatingOrb(
        size: 120,
        color: c2.blended(with: .white, amount: colorScheme == .dark ? 0.08 : 0.30)
          .opacity(colorScheme == .dark ? 0.32 : 0.36),
        startOffset: CGSize(width: -18, height: 68),
        drift: CGSize(width: -14, height: -18),
        blurRadius: 0.8,
        duration: 6.7
      )

      FloatingOrb(
        size: 108,
        color: c4.blended(with: .white, amount: colorScheme == .dark ? 0.06 : 0.34)
          .opacity(colorScheme == .dark ? 0.24 : 0.35),
        startOffset: CGSize(width: 126, height: 54),
        drift: CGSize(width: -18, height: -20),
        blurRadius: 1.0,
        duration: 7.4
      )

      VStack(alignment: .leading, spacing: 6) {
        HStack(spacing: 10) {
          Image(systemName: "doc.text")
            .font(.system(size: 30, weight: .medium))
            .foregroundStyle(.primary)

          Text(name.isEmpty ? "My Diary" : name)
            .font(.system(size: 38, weight: .bold))
            .foregroundStyle(.primary)

        }

        HStack(spacing: 10) {
          Text(displayHeaderDateString)
            .font(.system(size: 16, weight: .semibold))
            .foregroundStyle(.secondary)

          if let onEdit {
            Button {
              Haptics.impact(.light)
              onEdit()
            } label: {
              Text("Edit")
                .font(.system(size: 14, weight: .semibold))
                .foregroundStyle(.primary.opacity(0.8))
                .padding(.horizontal, 14)
                .padding(.vertical, 7)
                .background {
                  Capsule()
                    .fill(.ultraThinMaterial)
                }
            }
            .buttonStyle(.plain)
          }
        }
      }
      .padding(.leading, 24)
      .padding(.top, 100)
      .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }
    .frame(height: 280)
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
  case todo
}

private struct JournalEntry: Identifiable, Hashable {
  let id: UUID
  var date: Date
  var title: String
  var body: String
  var createdAt: Date
  var timezoneAbbreviation: String
  /// Number of attached media blocks in this entry (non-text blocks from body_blocks).
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
  var maxStreakDays: Int
  var entriesCount: Int
  var mediaCount: Int
  var uniqueDaysCount: Int
  var todayEntriesCount: Int
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
  private static let colorCacheKey = "diary_accent_color_hex"
  private static let nameCacheKey = "diary_name_cache"

  @Published var diaryName: String
  @Published var diaryDescription: String = ""
  @Published var diaryColor: Color
  @Published var entries: [JournalEntry] = []
  @Published var todoItems: [DiaryTodoItem] = []
  @Published var isLoading: Bool = false
  @Published var loadError: String?

  var todoPendingCount: Int {
    max(0, todoItems.count - todoItems.filter(\.isCompleted).count)
  }

  var todoCompletedCount: Int {
    todoItems.filter(\.isCompleted).count
  }

  init() {
    let cachedHex = UserDefaults.standard.string(forKey: Self.colorCacheKey)
    diaryColor = cachedHex.flatMap { Color(hex: $0) } ?? AppTheme.brand
    diaryName = UserDefaults.standard.string(forKey: Self.nameCacheKey) ?? "My Diary"
    // Load entries from GRDB instantly
    if let userId = AuthService.shared.currentUserId, let uid = UUID(uuidString: userId) {
      let localRows = DiaryService.shared.localEntries(userId: uid)
      if !localRows.isEmpty {
        entries = mapRowsToEntries(localRows)
      }
    }
    loadTodos()
  }

  func loadIfNeeded() {
    guard let userId = AuthService.shared.currentUserId, let uid = UUID(uuidString: userId) else {
      entries = []
      return
    }
    loadTodos()
    Task {
      await loadCachedData(userId: uid)
      await loadSettings(userId: uid)
      await loadEntries(userId: uid)
    }
  }

  func loadTodos() {
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

  func saveTodos() {
    guard let data = try? JSONEncoder().encode(todoItems) else { return }
    UserDefaults.standard.set(data, forKey: diaryTodoStorageKey)
  }

  private func loadCachedData(userId: UUID) async {
    if let cachedSettings = await DiaryService.shared.cachedSettings(userId: userId) {
      await MainActor.run {
        diaryName = cachedSettings.name
        diaryDescription = cachedSettings.description
        diaryColor = Color(hex: cachedSettings.headerColorHex) ?? AppTheme.brand
      }
    }
    if let cachedRows = await DiaryService.shared.cachedEntries(userId: userId) {
      let mapped = mapRowsToEntries(cachedRows)
      await MainActor.run {
        entries = mapped
      }
    }
  }

  private func loadSettings(userId: UUID) async {
    do {
      let (name, desc, hex) = try await DiaryService.shared.fetchSettings(userId: userId)
      await MainActor.run {
        diaryName = name
        diaryDescription = desc
        diaryColor = Color(hex: hex) ?? AppTheme.brand
        UserDefaults.standard.set(hex, forKey: Self.colorCacheKey)
        UserDefaults.standard.set(name, forKey: Self.nameCacheKey)
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
      let list = mapRowsToEntries(rows)
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

  private func mapRowsToEntries(_ rows: [DiaryEntryRow]) -> [JournalEntry] {
    rows.compactMap { row -> JournalEntry? in
      guard let date = DiaryService.date(from: row.date) else { return nil }
      let body = DiaryService.textContentFromBodyBlocks(row.body_blocks)
      let createdAt = DiaryService.parseISO8601(row.created_at) ?? date
      let mediaBlockCount = row.body_blocks.reduce(0) { partialResult, block in
        let type = (block["type"] ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
        guard !type.isEmpty, type != "text" else { return partialResult }
        return partialResult + 1
      }
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
        photoCount: mediaBlockCount,
        firstPhotoURL: firstPhotoURL
      )
    }
  }

  func saveSettings(name: String, description: String, color: Color) {
    guard let userId = AuthService.shared.currentUserId, let uid = UUID(uuidString: userId) else {
      return
    }
    // Optimistic update + local cache
    diaryName = name
    diaryDescription = description
    diaryColor = color
    let hex = color.hexString
    UserDefaults.standard.set(hex, forKey: Self.colorCacheKey)
    UserDefaults.standard.set(name, forKey: Self.nameCacheKey)
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

  var displayHeroDateString: String {
    let now = Date()
    let cal = Calendar.current
    let day = cal.component(.day, from: now)
    let year = cal.component(.year, from: now)
    let f = DateFormatter()
    f.dateFormat = "EEEE"
    let weekday = f.string(from: now)
    let suffix: String
    switch day {
    case 1, 21, 31: suffix = "st"
    case 2, 22: suffix = "nd"
    case 3, 23: suffix = "rd"
    default: suffix = "th"
    }
    return "\(weekday) \(day)\(suffix), \(year)"
  }

  var stats: JournalStats {
    let cal = Calendar.current
    let entriesCount = entries.count
    let uniqueDays = Set(entries.map { cal.startOfDay(for: $0.date) })

    let today = cal.startOfDay(for: Date())
    let todayEntriesCount = entries.filter { entry in
      cal.isDate(entry.date, inSameDayAs: today)
    }.count

    // Current streak: consecutive days with >=1 note, ending today.
    var streak = 0
    var cursor = today
    while uniqueDays.contains(cursor) {
      streak += 1
      guard let prev = cal.date(byAdding: .day, value: -1, to: cursor) else { break }
      cursor = prev
    }

    // Max streak: longest consecutive run across all note days.
    let sortedDays = uniqueDays.sorted()
    var maxStreak = 0
    var currentRun = 0
    var previousDay: Date?
    for day in sortedDays {
      if let previousDay,
        let expectedNext = cal.date(byAdding: .day, value: 1, to: previousDay),
        cal.isDate(day, inSameDayAs: expectedNext)
      {
        currentRun += 1
      } else {
        currentRun = 1
      }
      maxStreak = max(maxStreak, currentRun)
      previousDay = day
    }

    let mediaCount = entries.reduce(0) { $0 + $1.photoCount }

    return JournalStats(
      streakDays: streak,
      maxStreakDays: maxStreak,
      entriesCount: entriesCount,
      mediaCount: mediaCount,
      uniqueDaysCount: uniqueDays.count,
      todayEntriesCount: todayEntriesCount
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
  let stats: JournalStats
  @Binding var entries: [JournalEntry]
  @Binding var todoItems: [DiaryTodoItem]
  let accentColor: Color
  let diaryDescription: String
  let onAddEntryForDate: (Date) -> Void
  let onSelectEntry: (JournalEntry) -> Void
  let onDeleteEntry: (JournalEntry) -> Void
  let onTodoChange: () -> Void

  @State private var selectedCalendarDay: Date? = Calendar.current.startOfDay(for: Date())

  var body: some View {
    VStack(spacing: 0) {
      JournalTabBar(tab: $tab, accentColor: accentColor)

      Rectangle()
        .fill(Color(.separator).opacity(0.4))
        .frame(height: 1.2)

      Group {
        switch tab {
        case .overview:
          JournalOverviewTab(
            entries: $entries,
            stats: stats,
            accentColor: accentColor,
            diaryDescription: diaryDescription,
            selectedDay: $selectedCalendarDay,
            onAddEntryForDate: onAddEntryForDate,
            onSelectEntry: onSelectEntry
          )
        case .list:
          JournalListTab(
            entries: entries,
            onSelectEntry: onSelectEntry,
            onDeleteEntry: onDeleteEntry
          )
        case .todo:
          JournalTodoTab(todoItems: $todoItems, onTodoChange: onTodoChange)
        }
      }
      .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .top)
    }
    .background(AppTheme.background)
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
  let accentColor: Color
  @State private var isKeyboardVisible: Bool = false

  var body: some View {
    HStack(spacing: 8) {
      tabButton(.overview, title: "Overview")
      tabButton(.list, title: "Entries")
      tabButton(.todo, title: "To-Do")
    }
    .padding(.horizontal, 30)
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
      .foregroundStyle(isActive ? .white : .secondary)
      .padding(.horizontal, 8)
      .padding(.vertical, 9)
      .frame(maxWidth: .infinity)
      .background {
        if isActive {
          Capsule(style: .continuous)
            .fill(
              LinearGradient(
                colors: [
                  accentColor,
                  accentColor.blended(with: Color(red: 0.86, green: 0.80, blue: 1.00), amount: 0.35),
                ],
                startPoint: .leading,
                endPoint: .trailing
              )
            )
        } else {
          GlassCardBackground(shape: Capsule(style: .continuous))
        }
      }
      .clipShape(Capsule(style: .continuous))
      .overlay(
        Capsule(style: .continuous)
          .stroke(Color(.separator).opacity(isActive ? 0.0 : 0.25), lineWidth: isActive ? 0 : 0.5)
      )
    }
    .buttonStyle(.plain)
  }
}

// MARK: - Overview tab

private struct JournalOverviewTab: View {
  @Binding var entries: [JournalEntry]
  let stats: JournalStats
  let accentColor: Color
  let diaryDescription: String
  @Binding var selectedDay: Date?
  let onAddEntryForDate: (Date) -> Void
  let onSelectEntry: (JournalEntry) -> Void

  @State private var currentPage: Int?
  @State private var showDaySheet: Bool = false
  @State private var sheetDay: Date?
  private let cal = Calendar.current

  private var currentMonthIndex: Int {
    cal.component(.month, from: Date()) - 1
  }

  private var monthDates: [Date] {
    let year = cal.component(.year, from: Date())
    return (1...12).compactMap { month in
      cal.date(from: DateComponents(year: year, month: month, day: 1))
    }
  }

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

  var body: some View {
    let months = monthDates

    ScrollView {
      VStack(alignment: .leading, spacing: 24) {
        VStack(alignment: .leading, spacing: 8) {
          Text("Statistics")
            .font(.system(size: 18, weight: .bold))
            .foregroundStyle(.primary)
            .padding(.horizontal, 4)

          JournalStatisticsCards(stats: stats, accentColor: accentColor)
        }
        .padding(.horizontal, 16)

        VStack(spacing: 8) {
          if months.count > 1 {
            HStack(spacing: 4) {
              ForEach(months.indices, id: \.self) { index in
                Capsule()
                  .fill(index == (currentPage ?? currentMonthIndex) ? Color.primary.opacity(0.7) : Color.primary.opacity(0.15))
                  .frame(width: index == (currentPage ?? currentMonthIndex) ? 14 : 5, height: 3)
                  .animation(.spring(response: 0.3, dampingFraction: 0.8), value: currentPage)
              }
            }
          }

          ScrollView(.horizontal, showsIndicators: false) {
            HStack(alignment: .top, spacing: 6) {
              ForEach(months.indices, id: \.self) { index in
                MonthGridView(
                  monthStart: months[index],
                  dayVisuals: dayVisuals,
                  accentColor: accentColor,
                  selectedDay: $selectedDay,
                  onDayTapped: { date in
                    let normalizedDay = cal.startOfDay(for: date)
                    sheetDay = normalizedDay
                    showDaySheet = true
                  }
                )
                .containerRelativeFrame(.horizontal)
                .id(index)
                .visualEffect { content, proxy in
                  let scrollBounds = proxy.bounds(of: .scrollView(axis: .horizontal)) ?? .zero
                  let scrollCenter = scrollBounds.width / 2
                  let cardMid = proxy.frame(in: .scrollView(axis: .horizontal)).midX
                  let distance = cardMid - scrollCenter
                  let normalized = scrollCenter > 0 ? distance / scrollCenter : 0
                  let clamped = max(-1.0, min(1.0, normalized))
                  let absVal = abs(clamped)

                  return content
                    .scaleEffect(1.0 - absVal * 0.06)
                    .opacity(1.0 - absVal * 0.4)
                    .offset(y: absVal * absVal * 8)
                }
              }
            }
            .scrollTargetLayout()
          }
          .contentMargins(.horizontal, 0, for: .scrollContent)
          .scrollPosition(id: $currentPage)
          .scrollTargetBehavior(.viewAligned)
          .scrollClipDisabled()
        }
        .padding(.horizontal, 16)

        Spacer().frame(height: 120)
      }
      .padding(.top, 14)
    }
    .scrollIndicators(.hidden)
    .scrollDismissesKeyboard(.immediately)
    .onAppear {
      currentPage = currentMonthIndex
    }
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
          maxStreakLabel: "Max \(stats.maxStreakDays)d",
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
              title: "ON THIS DAY", value: "\(stats.todayEntriesCount)", accentColor: accentColor)
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
  let maxStreakLabel: String?
  let accentColor: Color

  var body: some View {
    let shape = RoundedRectangle(cornerRadius: 18, style: .continuous)
    Button {
    } label: {
      VStack(spacing: 0) {
        HStack(alignment: .top, spacing: 6) {
          HStack(spacing: 6) {
            Image(systemName: "flame.fill")
              .font(.system(size: 11, weight: .bold))
              .foregroundStyle(accentColor)

            Text(title)
              .font(.system(size: 11, weight: .semibold))
              .foregroundStyle(.secondary)
          }
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
      .overlay(alignment: .bottomTrailing) {
        if let maxStreakLabel {
          Text(maxStreakLabel)
            .font(.system(size: 10, weight: .semibold))
            .foregroundStyle(.secondary)
            .padding(.horizontal, 7)
            .padding(.vertical, 3)
            .background(.ultraThinMaterial, in: Capsule())
            .padding(10)
        }
      }
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

// MARK: - Past calendar sheet

private struct PastCalendarSheet: View {
  @Environment(\.dismiss) private var dismiss
  @Binding var entries: [JournalEntry]
  let accentColor: Color
  @Binding var selectedDay: Date?
  let onAddEntryForDate: (Date) -> Void
  let onSelectEntry: (JournalEntry) -> Void

  @State private var showDaySheet: Bool = false
  @State private var sheetDay: Date?

  private let cal = Calendar.current

  private var pastMonths: [Date] {
    let currentYear = cal.component(.year, from: Date())
    var months: [Date] = []
    for year in stride(from: currentYear - 1, through: max(currentYear - 3, 2020), by: -1) {
      for month in stride(from: 12, through: 1, by: -1) {
        if let date = cal.date(from: DateComponents(year: year, month: month, day: 1)) {
          months.append(date)
        }
      }
    }
    return months
  }

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

  var body: some View {
    NavigationStack {
      ScrollView {
        LazyVStack(spacing: 14) {
          ForEach(pastMonths, id: \.self) { monthStart in
            MonthGridView(
              monthStart: monthStart,
              dayVisuals: dayVisuals,
              accentColor: accentColor,
              selectedDay: $selectedDay,
              onDayTapped: { date in
                let normalizedDay = cal.startOfDay(for: date)
                sheetDay = normalizedDay
                showDaySheet = true
              }
            )
          }
        }
        .padding(.horizontal, 16)
        .padding(.vertical, 16)
      }
      .scrollIndicators(.hidden)
      .background(AppTheme.background)
      .navigationTitle("Past Months")
      .navigationBarTitleDisplayMode(.inline)
      .toolbar {
        ToolbarItem(placement: .topBarTrailing) {
          Button("Done") { dismiss() }
            .font(.system(size: 17, weight: .semibold))
        }
      }
    }
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
}

private struct GlassCardBackground<S: Shape>: View {
  let shape: S

  var body: some View {
    if #available(iOS 26.0, *) {
      Color.clear
        .glassEffect(.regular.interactive(), in: shape)
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
  @State private var searchText: String = ""
  @FocusState private var isSearchFocused: Bool

  private var filteredEntries: [JournalEntry] {
    guard !searchText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else { return entries }
    let query = searchText.lowercased()
    return entries.filter { entry in
      entry.title.lowercased().contains(query)
        || entry.body.lowercased().contains(query)
    }
  }

  private var grouped: [(key: String, value: [JournalEntry])] {
    let source = filteredEntries
    let formatter = DateFormatter()
    formatter.dateFormat = "LLLL yyyy"
    let dict = Dictionary(grouping: source) { entry in
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
      VStack(spacing: 0) {
        // Search bar
        if !entries.isEmpty {
          HStack(spacing: 8) {
            Image(systemName: "magnifyingglass")
              .font(.system(size: 15, weight: .medium))
              .foregroundStyle(.secondary)

            TextField("Search entries...", text: $searchText)
              .font(.system(size: 16))
              .focused($isSearchFocused)

            if !searchText.isEmpty {
              Button {
                searchText = ""
              } label: {
                Image(systemName: "xmark.circle.fill")
                  .font(.system(size: 15))
                  .foregroundStyle(.tertiary)
              }
              .buttonStyle(.plain)
            }
          }
          .padding(.horizontal, 12)
          .padding(.vertical, 10)
          .background(Color(.tertiarySystemFill))
          .clipShape(RoundedRectangle(cornerRadius: 12, style: .continuous))
          .padding(.horizontal, 18)
          .padding(.top, 12)
          .padding(.bottom, 4)
        }

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
        } else if filteredEntries.isEmpty {
          VStack(spacing: 10) {
            Spacer().frame(height: 36)
            Image(systemName: "magnifyingglass")
              .font(.system(size: 28, weight: .semibold))
              .foregroundStyle(.secondary)
            Text("No results")
              .font(.system(size: 16, weight: .semibold))
            Text("Try a different search term.")
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
    }
    .scrollIndicators(.hidden)
    .scrollDismissesKeyboard(.interactively)
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
  @Binding var todoItems: [DiaryTodoItem]
  let onTodoChange: () -> Void
  @State private var newTodoTitle: String = ""
  @FocusState private var isAddFieldFocused: Bool
  @State private var showReorderHint: Bool = false
  @State private var activeReorderItemId: UUID?
  @State private var isReorderDragging: Bool = false
  @State private var activeReorderTranslationY: CGFloat = 0
  @State private var reorderReferenceTranslationY: CGFloat = 0

  // Require a deliberate press-and-hold before drag can start.
  private let todoReorderLongPressDuration: Double = 0.7
  // Swap only after the dragged row has passed roughly one full row+gap distance.
  private let todoReorderSwapDistance: CGFloat = 72

  private var pendingCount: Int {
    max(0, todoItems.count - completedCount)
  }

  private var completedCount: Int {
    todoItems.filter(\.isCompleted).count
  }

  private var completionRatio: CGFloat {
    let total = pendingCount + completedCount
    guard total > 0 else { return 0 }
    return CGFloat(completedCount) / CGFloat(total)
  }

  var body: some View {
    ScrollView {
      VStack(alignment: .leading, spacing: 12) {
        todoSummaryCard
        addTaskCard

        if showReorderHint {
          reorderHintCard
        }

        if !todoItems.isEmpty {
          LazyVStack(spacing: 10) {
            ForEach($todoItems) { $item in
              let isActiveDrag = activeReorderItemId == item.id && isReorderDragging

              TodoRowView(
                item: $item,
                isBeingReordered: isActiveDrag,
                onShowReorderHint: todoItems.count > 1
                  ? {
                    Haptics.impact(.light)
                    showReorderHint = true
                  }
                  : nil,
                onDelete: {
                  removeTodo(id: item.id)
                }
              )
              .frame(maxWidth: .infinity, alignment: .leading)
              .contentShape(Rectangle())
              .offset(y: isActiveDrag ? activeReorderTranslationY : 0)
              .zIndex(isActiveDrag ? 1 : 0)
              .simultaneousGesture(
                reorderGesture(for: item.id),
                including: todoItems.count > 1 ? .all : .none
              )
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
    .onTapGesture {
      isAddFieldFocused = false
    }
    .onChange(of: todoItems) { _, _ in onTodoChange() }
  }

  private var todoSummaryCard: some View {
    let shape = RoundedRectangle(cornerRadius: 16, style: .continuous)

    return VStack(alignment: .leading, spacing: 12) {
      HStack(alignment: .center, spacing: 14) {
        VStack(alignment: .leading, spacing: 4) {
          Text("To-Do")
            .font(.system(size: 24, weight: .bold))
            .foregroundStyle(.primary)

          Text(
            pendingCount == 0
              ? "All tasks are done."
              : "\(pendingCount) \(pendingCount == 1 ? "task" : "tasks") pending"
          )
          .font(.system(size: 13, weight: .medium))
          .foregroundStyle(.secondary)
        }

        Spacer(minLength: 8)

        ZStack {
          Circle()
            .stroke(Color.primary.opacity(0.15), lineWidth: 3.5)
          Circle()
            .trim(from: 0, to: completionRatio)
            .stroke(
              Color.primary.opacity(0.85),
              style: StrokeStyle(lineWidth: 3.5, lineCap: .round)
            )
            .rotationEffect(.degrees(-90))
        }
        .frame(width: 38, height: 38)
      }

      HStack(spacing: 16) {
        todoSummaryPill(icon: "circle", title: "Pending", value: "\(pendingCount)", tint: AppTheme.accent)
        todoSummaryPill(icon: "checkmark.circle.fill", title: "Done", value: "\(completedCount)", tint: .green)
      }
    }
    .padding(.horizontal, 16)
    .padding(.vertical, 16)
    .background { GlassCardBackground(shape: shape) }
    .clipShape(shape)
    .overlay(
      shape
        .stroke(Color(.separator).opacity(0.22), lineWidth: 0.6)
    )
  }

  private func todoSummaryPill(icon: String, title: String, value: String, tint: Color) -> some View {
    HStack(spacing: 4) {
      Image(systemName: icon)
        .font(.system(size: 12, weight: .semibold))
        .foregroundStyle(tint)

      Text(title)
        .font(.system(size: 13, weight: .semibold))
        .foregroundStyle(.secondary)

      Text(value)
        .font(.system(size: 13, weight: .bold, design: .rounded))
        .foregroundStyle(.primary)
        .monospacedDigit()
    }
  }

  private var addTaskCard: some View {
    let trimmed = newTodoTitle.trimmingCharacters(in: .whitespacesAndNewlines)
    let canAdd = !trimmed.isEmpty
    let shape = RoundedRectangle(cornerRadius: 16, style: .continuous)

    return HStack(spacing: 10) {
      TextField("Tap to add a task...", text: $newTodoTitle)
        .font(.system(size: 16, weight: .regular))
        .focused($isAddFieldFocused)
        .submitLabel(.done)
        .onSubmit { addTodo() }

      Button {
        addTodo()
      } label: {
        Text("Add")
          .font(.system(size: 14, weight: .semibold))
          .foregroundStyle(canAdd ? .primary : .tertiary)
          .padding(.horizontal, 14)
          .padding(.vertical, 7)
          .background(Color(.tertiarySystemFill))
          .clipShape(Capsule())
      }
      .buttonStyle(.plain)
      .disabled(!canAdd)
    }
    .padding(.horizontal, 16)
    .padding(.vertical, 14)
    .background { GlassCardBackground(shape: shape) }
    .clipShape(shape)
    .overlay(
      shape
        .stroke(Color(.separator).opacity(0.22), lineWidth: 0.6)
    )
    .contentShape(shape)
    .onTapGesture {
      isAddFieldFocused = true
    }
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

  private var reorderHintCard: some View {
    HStack(spacing: 10) {
      Label("Hold and drag tasks to reorder", systemImage: "hand.draw")
        .font(.system(size: 13, weight: .semibold))
        .foregroundStyle(AppTheme.accent)

      Spacer(minLength: 8)

      Button("Got it") {
        showReorderHint = false
      }
      .font(.system(size: 13, weight: .bold))
      .padding(.horizontal, 12)
      .padding(.vertical, 6)
      .background(AppTheme.accent.opacity(0.14))
      .foregroundStyle(AppTheme.accent)
      .clipShape(Capsule())
      .buttonStyle(.plain)
    }
    .padding(.horizontal, 12)
    .padding(.vertical, 10)
    .background(Color(.secondarySystemBackground).opacity(0.75))
    .clipShape(RoundedRectangle(cornerRadius: 14, style: .continuous))
    .overlay(
      RoundedRectangle(cornerRadius: 14, style: .continuous)
        .stroke(Color(.separator).opacity(0.2), lineWidth: 0.6)
    )
  }

  private var emptyStateCard: some View {
    VStack(spacing: 10) {
      Spacer().frame(height: 22)
      Image(systemName: "checklist.checked")
        .font(.system(size: 28, weight: .semibold))
        .foregroundStyle(.secondary)
      Text("No tasks yet")
        .font(.system(size: 16, weight: .semibold))
      Text("Add your first task above.")
        .font(.system(size: 14))
        .foregroundStyle(.secondary)
    }
    .frame(maxWidth: .infinity)
  }

  private func removeTodo(id: UUID) {
    Haptics.impact(.light)
    withAnimation(.easeInOut(duration: 0.2)) {
      todoItems.removeAll { $0.id == id }
      if activeReorderItemId == id || todoItems.count < 2 {
        activeReorderItemId = nil
        isReorderDragging = false
        reorderReferenceTranslationY = 0
        activeReorderTranslationY = 0
      }
    }
  }

  private func reorderGesture(for itemId: UUID) -> some Gesture {
    LongPressGesture(minimumDuration: todoReorderLongPressDuration)
      .sequenced(before: DragGesture(minimumDistance: 0, coordinateSpace: .global))
      .onChanged { value in
        switch value {
        case .first(false):
          endReorder()
        case .second(true, nil):
          beginReorderIfNeeded(for: itemId)
        case .second(true, let drag?):
          beginReorderIfNeeded(for: itemId)
          updateReorder(translationY: drag.translation.height)
        case .second(false, _):
          endReorder()
        default:
          break
        }
      }
      .onEnded { _ in
        endReorder()
      }
  }

  private func beginReorderIfNeeded(for itemId: UUID) {
    guard activeReorderItemId == nil else {
      isReorderDragging = true
      return
    }
    guard todoItems.firstIndex(where: { $0.id == itemId }) != nil else { return }

    Haptics.impact(.light)
    activeReorderItemId = itemId
    isReorderDragging = true
    reorderReferenceTranslationY = 0
    activeReorderTranslationY = 0
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
    withAnimation(.spring(response: 0.22, dampingFraction: 0.9)) {
      activeReorderItemId = nil
      isReorderDragging = false
      reorderReferenceTranslationY = 0
      activeReorderTranslationY = 0
    }
  }
}

private struct TodoRowView: View {
  @Binding var item: DiaryTodoItem
  var isBeingReordered: Bool = false
  var onShowReorderHint: (() -> Void)?
  var onDelete: (() -> Void)?

  var body: some View {
    let shape = RoundedRectangle(cornerRadius: 14, style: .continuous)
    let isElevated = isBeingReordered
    let borderColor: Color = isBeingReordered
      ? Color.primary.opacity(0.35)
      : Color(.separator).opacity(item.isCompleted ? 0.12 : 0.22)
    let borderWidth: CGFloat = isBeingReordered ? 1.2 : 0.5

    HStack(alignment: .center, spacing: 12) {
      Button {
        Haptics.impact(.light)
        withAnimation(.spring(response: 0.25, dampingFraction: 0.82)) {
          item.isCompleted.toggle()
        }
      } label: {
        Image(systemName: item.isCompleted ? "checkmark.circle.fill" : "circle")
          .font(.system(size: 20, weight: .medium))
          .foregroundStyle(item.isCompleted ? Color.primary.opacity(0.5) : Color.primary.opacity(0.2))
          .contentTransition(.symbolEffect(.replace))
      }
      .buttonStyle(.plain)

      Text(item.title)
        .font(.system(size: 16, weight: .medium))
        .strikethrough(item.isCompleted)
        .foregroundStyle(item.isCompleted ? .tertiary : .primary)
        .frame(maxWidth: .infinity, alignment: .leading)
        .multilineTextAlignment(.leading)
        .transaction { $0.animation = nil }

      if let onDelete = onDelete {
        Menu {
          if let onShowReorderHint = onShowReorderHint {
            Button {
              onShowReorderHint()
            } label: {
              Label("Reorder Tasks", systemImage: "arrow.up.arrow.down")
            }
          }

          Button(role: .destructive) {
            onDelete()
          } label: {
            Label("Delete Task", systemImage: "trash")
          }
        } label: {
          Image(systemName: "ellipsis")
            .font(.system(size: 15, weight: .semibold))
            .foregroundStyle(.tertiary)
            .frame(width: 28, height: 28)
        }
        .disabled(isBeingReordered)
      }
    }
    .padding(.horizontal, 14)
    .padding(.vertical, 12)
    .background { GlassCardBackground(shape: shape) }
    .clipShape(shape)
    .overlay(
      shape
        .stroke(borderColor, lineWidth: borderWidth)
    )
    .shadow(
      color: isElevated ? .black.opacity(0.12) : .black.opacity(0.03),
      radius: isElevated ? 10 : 4,
      x: 0,
      y: isElevated ? 4 : 1
    )
    .scaleEffect(isElevated ? 1.01 : 1)
    .frame(maxWidth: .infinity, alignment: .leading)
    .contentShape(Rectangle())
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

private struct CalendarDayVisual {
  let notesCount: Int
  let hasPhoto: Bool
  let firstPhotoURL: String?
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

  private var allDays: [Date?] {
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
    // Pad to complete the last row (multiple of 7), but don't add extra full empty rows
    let remainder = result.count % 7
    if remainder != 0 {
      result.append(contentsOf: Array(repeating: nil as Date?, count: 7 - remainder))
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
    let gridColumns = Array(repeating: GridItem(.flexible(), spacing: 3), count: 7)

    VStack(alignment: .leading, spacing: 8) {
      // Month header — above the glass card
      HStack(alignment: .center, spacing: 8) {
        Text(title)
          .font(.system(size: 18, weight: .bold))
          .foregroundStyle(.primary)

        Spacer(minLength: 8)

        HStack(spacing: 5) {
          Image(systemName: "record.circle")
            .font(.system(size: 9, weight: .semibold))
          Text("\(monthEntryDaysCount) logged")
            .font(.system(size: 10, weight: .semibold))
            .monospacedDigit()
        }
        .foregroundStyle(.secondary)
        .padding(.horizontal, 9)
        .padding(.vertical, 5)
        .background(Color(.tertiarySystemFill))
        .clipShape(Capsule())
      }
      .padding(.horizontal, 4)

      // Glass card with weekdays + day grid + chevron
      VStack(spacing: 6) {
        LazyVGrid(columns: gridColumns, spacing: 4) {
          ForEach(Array(weekdaySymbols.enumerated()), id: \.offset) { _, sym in
            Text(sym)
              .font(.system(size: 9, weight: .semibold))
              .foregroundStyle(.secondary.opacity(0.88))
              .frame(maxWidth: .infinity)
              .padding(.vertical, 1)
          }
        }
        .padding(.horizontal, 9)

        LazyVGrid(columns: gridColumns, spacing: 4) {
          ForEach(Array(allDays.enumerated()), id: \.offset) { _, date in
            DayCell(
              date: date,
              dayVisual: date.flatMap { dayVisuals[cal.startOfDay(for: $0)] },
              accentColor: accentColor,
              selectedDay: $selectedDay,
              onDayTapped: onDayTapped
            )
          }
        }
        .padding(.horizontal, 9)

      }
      .padding(.vertical, 9)
      .background { GlassCardBackground(shape: shape) }
      .clipShape(shape)
      .overlay(
        shape
          .stroke(Color(.separator).opacity(0.2), lineWidth: 0.6)
      )
    }
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
          let shape = RoundedRectangle(cornerRadius: 9, style: .continuous)

          ZStack {
            if isSelected && !hasPhoto {
              LinearGradient(
                colors: [
                  accentColor.opacity(0.55),
                  accentColor.blended(with: Color(red: 0.86, green: 0.80, blue: 1.00), amount: 0.35).opacity(0.45),
                ],
                startPoint: .topLeading,
                endPoint: .bottomTrailing
              )
            } else {
              Color(.secondarySystemBackground)
                .opacity(0.32)
            }

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
            } else if hasPhoto {
              LinearGradient(
                colors: [accentColor.opacity(0.34), AppTheme.accent.opacity(0.24)],
                startPoint: .topLeading,
                endPoint: .bottomTrailing
              )
            } else if hasEntry {
              accentColor.opacity(0.15)
            }

            if hasPhoto {
              LinearGradient(
                colors: [.black.opacity(0.24), .clear, .black.opacity(0.14)],
                startPoint: .top,
                endPoint: .bottom
              )
            }

            Text("\(cal.component(.day, from: date))")
              .font(
                .system(
                  size: 13, weight: isToday ? .bold : .semibold, design: .rounded)
                )
              .foregroundStyle(textColor)
              .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .center)

            if hasEntry {
              HStack(spacing: 2) {
                ForEach(0..<indicatorCount, id: \.self) { idx in
                  Capsule()
                    .fill(indicatorColor.opacity(idx == 0 ? 1.0 : 0.78))
                    .frame(width: idx == 0 ? 7 : 3.5, height: 3)
                }
              }
              .padding(.bottom, 3)
              .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .bottom)
            }
          }
          .frame(height: 36)
          .frame(maxWidth: .infinity)
          .clipShape(shape)
          .overlay(
            shape
              .stroke(borderColor, lineWidth: isSelected || isToday ? 1.1 : 0.55)
          )
        }
        .buttonStyle(.plain)
      } else {
        Color.clear
          .frame(height: 36)
          .frame(maxWidth: .infinity)
      }
    }
  }

  private var textColor: Color {
    if hasPhoto { return .white }
    if isSelected { return .primary }
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
    VStack(spacing: 0) {
      // Header
      VStack(alignment: .leading, spacing: 4) {
        Text(formattedDate)
          .font(.system(size: 22, weight: .bold))
          .foregroundStyle(.primary)

        Text("\(dayEntries.count) \(dayEntries.count == 1 ? "entry" : "entries")")
          .font(.system(size: 14, weight: .medium))
          .foregroundStyle(.secondary)
      }
      .frame(maxWidth: .infinity, alignment: .leading)
      .padding(.horizontal, 20)
      .padding(.top, 24)
      .padding(.bottom, 20)

      Divider()
        .padding(.horizontal, 16)

      // Content
      ScrollView {
        VStack(spacing: 10) {
          if dayEntries.isEmpty {
            if isFutureDay {
              Text("This date hasn\u{2019}t arrived yet.")
                .font(.system(size: 15))
                .foregroundStyle(.secondary)
                .frame(maxWidth: .infinity)
                .padding(.vertical, 32)
            }
          } else {
            ForEach(dayEntries) { entry in
              Button {
                Haptics.impact(.light)
                onSelectEntry(entry)
              } label: {
                JournalListRowContent(entry: entry)
              }
              .buttonStyle(.plain)
            }
          }
        }
        .padding(.horizontal, 16)
        .padding(.top, 16)
        .padding(.bottom, 12)
      }

      // Add entry button
      if !isFutureDay {
        Button {
          Haptics.impact(.light)
          onAddEntry()
        } label: {
          HStack(spacing: 8) {
            Image(systemName: "plus")
              .font(.system(size: 15, weight: .bold))

            Text("New entry")
              .font(.system(size: 16, weight: .semibold))
          }
          .foregroundStyle(.white)
          .frame(maxWidth: .infinity)
          .padding(.vertical, 14)
          .background(
            LinearGradient(
              colors: [
                accentColor,
                accentColor.blended(with: Color(red: 0.86, green: 0.80, blue: 1.00), amount: 0.35),
              ],
              startPoint: .leading,
              endPoint: .trailing
            )
          )
          .clipShape(RoundedRectangle(cornerRadius: 14, style: .continuous))
        }
        .padding(.horizontal, 16)
        .padding(.bottom, 16)
      }
    }
    .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .top)
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
          .glassEffect(.regular.interactive(), in: Circle())
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

private struct DiaryDescriptionEditorView: View {
  @State private var name: String
  @State private var description: String
  @State private var selectedColor: Color
  private let descriptionMaxLength: Int = 10_000
  let onChange: (String, String, Color) -> Void

  private let fieldShape = RoundedRectangle(cornerRadius: 14, style: .continuous)

  private let accentPresets: [Color] = [
    AppTheme.brand,
    Color(red: 0.95, green: 0.45, blue: 0.25),
    Color(red: 0.25, green: 0.72, blue: 0.68),
    Color(red: 0.63, green: 0.32, blue: 0.98),
    Color(red: 0.90, green: 0.40, blue: 0.65),
    Color(red: 0.26, green: 0.58, blue: 1.00),
    Color(red: 0.98, green: 0.65, blue: 0.30),
    Color(red: 0.40, green: 0.85, blue: 0.55),
  ]

  init(name: String, description: String, color: Color, onChange: @escaping (String, String, Color) -> Void) {
    _name = State(initialValue: name)
    _description = State(initialValue: description)
    _selectedColor = State(initialValue: color)
    self.onChange = onChange
  }

  private func notifyChange() {
    onChange(
      name.trimmingCharacters(in: .whitespacesAndNewlines),
      description.trimmingCharacters(in: .whitespacesAndNewlines),
      selectedColor
    )
  }

  var body: some View {
    ScrollView {
      VStack(alignment: .leading, spacing: 24) {
        // Name
        VStack(alignment: .leading, spacing: 8) {
          Text("Name")
            .font(.system(size: 13, weight: .semibold))
            .foregroundStyle(.secondary)

          TextField("My Diary", text: $name)
            .textContentType(.none)
            .textInputAutocapitalization(.words)
            .autocorrectionDisabled()
            .font(.system(size: 17))
            .padding(.horizontal, 14)
            .padding(.vertical, 12)
            .background { GlassCardBackground(shape: fieldShape) }
            .clipShape(fieldShape)
            .overlay(
              fieldShape
                .stroke(Color(.separator).opacity(0.25), lineWidth: 0.5)
            )
            .onChange(of: name) { _, _ in notifyChange() }
        }

        // Description
        VStack(alignment: .leading, spacing: 8) {
          Text("Description")
            .font(.system(size: 13, weight: .semibold))
            .foregroundStyle(.secondary)

          ZStack(alignment: .topLeading) {
            if description.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
              Text("What\u{2019}s this diary about?")
                .font(.system(size: 16))
                .foregroundStyle(.tertiary)
                .padding(.horizontal, 16)
                .padding(.vertical, 14)
                .allowsHitTesting(false)
            }

            TextEditor(text: $description)
              .font(.system(size: 16))
              .frame(minHeight: 160)
              .padding(.horizontal, 10)
              .padding(.vertical, 6)
              .scrollContentBackground(.hidden)
              .onChange(of: description) { _, newValue in
                if newValue.count > descriptionMaxLength {
                  description = String(newValue.prefix(descriptionMaxLength))
                }
                notifyChange()
              }
          }
          .background { GlassCardBackground(shape: fieldShape) }
          .clipShape(fieldShape)
          .overlay(
            fieldShape
              .stroke(Color(.separator).opacity(0.25), lineWidth: 0.5)
          )

          Text("\(description.count)/\(descriptionMaxLength)")
            .font(.system(size: 11, weight: .medium))
            .foregroundStyle(.tertiary)
            .frame(maxWidth: .infinity, alignment: .trailing)
        }

        // Accent Color
        VStack(alignment: .leading, spacing: 8) {
          Text("Accent Color")
            .font(.system(size: 13, weight: .semibold))
            .foregroundStyle(.secondary)

          HStack(spacing: 12) {
            ForEach(Array(accentPresets.enumerated()), id: \.offset) { _, preset in
              Button {
                Haptics.impact(.light)
                selectedColor = preset
                notifyChange()
              } label: {
                Circle()
                  .fill(preset)
                  .frame(width: 34, height: 34)
                  .overlay {
                    if preset.hexString == selectedColor.hexString {
                      Circle()
                        .stroke(.white, lineWidth: 2.5)
                        .frame(width: 28, height: 28)
                    }
                  }
              }
              .buttonStyle(.plain)
            }
          }
          .padding(.horizontal, 14)
          .padding(.vertical, 12)
          .background { GlassCardBackground(shape: fieldShape) }
          .clipShape(fieldShape)
          .overlay(
            fieldShape
              .stroke(Color(.separator).opacity(0.25), lineWidth: 0.5)
          )
        }
      }
      .padding(.horizontal, 20)
      .padding(.top, 8)
    }
    .background(AppTheme.background)
    .navigationTitle("Edit Diary")
    .navigationBarTitleDisplayMode(.inline)
  }
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

  private let fieldShape = RoundedRectangle(cornerRadius: 14, style: .continuous)

  var body: some View {
    NavigationStack {
      ScrollView {
          VStack(alignment: .leading, spacing: 24) {
            // Name
            VStack(alignment: .leading, spacing: 8) {
              Text("Name")
                .font(.system(size: 13, weight: .semibold))
                .foregroundStyle(.secondary)

              TextField("My Diary", text: $name)
                .textContentType(.none)
                .textInputAutocapitalization(.words)
                .autocorrectionDisabled()
                .focused($focusedField, equals: .name)
                .font(.system(size: 17))
                .padding(.horizontal, 14)
                .padding(.vertical, 12)
                .background { GlassCardBackground(shape: fieldShape) }
                .clipShape(fieldShape)
                .overlay(
                  fieldShape
                    .stroke(Color(.separator).opacity(0.25), lineWidth: 0.5)
                )
            }

            // Description
            VStack(alignment: .leading, spacing: 8) {
              Text("Description")
                .font(.system(size: 13, weight: .semibold))
                .foregroundStyle(.secondary)

              ZStack(alignment: .topLeading) {
                if description.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                  Text("What\u{2019}s this diary about?")
                    .font(.system(size: 16))
                    .foregroundStyle(.tertiary)
                    .padding(.horizontal, 16)
                    .padding(.vertical, 14)
                    .allowsHitTesting(false)
                }

                TextEditor(text: $description)
                  .focused($focusedField, equals: .description)
                  .font(.system(size: 16))
                  .frame(minHeight: 100)
                  .padding(.horizontal, 10)
                  .padding(.vertical, 6)
                  .scrollContentBackground(.hidden)
                  .onChange(of: description) { _, newValue in
                    if newValue.count > descriptionMaxLength {
                      description = String(newValue.prefix(descriptionMaxLength))
                    }
                  }
              }
              .background { GlassCardBackground(shape: fieldShape) }
              .clipShape(fieldShape)
              .overlay(
                fieldShape
                  .stroke(Color(.separator).opacity(0.25), lineWidth: 0.5)
              )

              Text("\(description.count)/\(descriptionMaxLength)")
                .font(.system(size: 11, weight: .medium))
                .foregroundStyle(.tertiary)
                .frame(maxWidth: .infinity, alignment: .trailing)
            }

            // Color
            VStack(alignment: .leading, spacing: 8) {
              Text("Header color")
                .font(.system(size: 13, weight: .semibold))
                .foregroundStyle(.secondary)

              DiaryColorPicker(selected: $color)
            }

            Spacer(minLength: 10)
          }
          .padding(.horizontal, 20)
          .padding(.top, 8)
        }
      .background(AppTheme.background)
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
}

private struct DiaryEditorBackdrop: View {
  @Environment(\.colorScheme) private var colorScheme
  let accentColor: Color

  var body: some View {
    let top = accentColor.blended(
      with: colorScheme == .dark ? AppTheme.background : Color(red: 0.97, green: 0.99, blue: 1.0),
      amount: colorScheme == .dark ? 0.58 : 0.30
    )
    let mid =
      colorScheme == .dark
      ? AppTheme.background.blended(with: .black, amount: 0.28)
      : AppTheme.background.blended(with: .white, amount: 0.44)
    let bottom =
      colorScheme == .dark
      ? AppTheme.surface.blended(with: .black, amount: 0.20)
      : AppTheme.surface.blended(with: .white, amount: 0.18)
    let glow = accentColor.blended(with: .white, amount: colorScheme == .dark ? 0.08 : 0.26)

    ZStack {
      LinearGradient(
        colors: [top, mid, bottom],
        startPoint: .topLeading,
        endPoint: .bottomTrailing
      )

      RadialGradient(
        colors: [
          glow.opacity(colorScheme == .dark ? 0.30 : 0.44),
          .clear,
        ],
        center: .topLeading,
        startRadius: 10,
        endRadius: 300
      )

      LinearGradient(
        colors: [
          .white.opacity(colorScheme == .dark ? 0.03 : 0.18),
          .clear,
          .black.opacity(colorScheme == .dark ? 0.20 : 0.08),
        ],
        startPoint: .top,
        endPoint: .bottom
      )

      FloatingOrb(
        size: 260,
        color: accentColor.opacity(colorScheme == .dark ? 0.28 : 0.34),
        startOffset: CGSize(width: -180, height: -210),
        drift: CGSize(width: 34, height: 30),
        blurRadius: 2.4,
        duration: 9.0
      )

      FloatingOrb(
        size: 180,
        color: .white.opacity(colorScheme == .dark ? 0.12 : 0.30),
        startOffset: CGSize(width: 130, height: -120),
        drift: CGSize(width: -22, height: 26),
        blurRadius: 2.0,
        duration: 7.6
      )

      FloatingOrb(
        size: 150,
        color: AppTheme.brand.opacity(colorScheme == .dark ? 0.20 : 0.28),
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
  @Environment(\.colorScheme) private var colorScheme
  @Binding var selected: Color

  private struct Option: Hashable {
    let name: String
    let color: Color

    func swatch(for scheme: ColorScheme) -> Color {
      if scheme == .dark {
        return color.blended(with: AppTheme.background, amount: 0.34)
      }
      return color
    }
  }

  private struct GradientOption: Hashable {
    let name: String
    let lightColors: [Color]
    let darkColors: [Color]
    let anchor: Color

    func colors(for scheme: ColorScheme) -> [Color] {
      scheme == .dark ? darkColors : lightColors
    }
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
  private let gradientOptions: [GradientOption] = [
    .init(
      name: "Coral Bloom",
      lightColors: [
        Color(red: 1.00, green: 0.77, blue: 0.72),
        Color(red: 1.00, green: 0.90, blue: 0.68),
      ],
      darkColors: [
        Color(red: 0.53, green: 0.30, blue: 0.34),
        Color(red: 0.58, green: 0.42, blue: 0.28),
      ],
      anchor: Color(red: 0.95, green: 0.72, blue: 0.64)
    ),
    .init(
      name: "Lavender Peach",
      lightColors: [
        Color(red: 0.79, green: 0.76, blue: 1.00),
        Color(red: 1.00, green: 0.84, blue: 0.77),
      ],
      darkColors: [
        Color(red: 0.43, green: 0.34, blue: 0.62),
        Color(red: 0.57, green: 0.39, blue: 0.33),
      ],
      anchor: Color(red: 0.83, green: 0.73, blue: 0.86)
    ),
    .init(
      name: "Azure Mint",
      lightColors: [
        Color(red: 0.60, green: 0.84, blue: 1.00),
        Color(red: 0.69, green: 0.96, blue: 0.87),
      ],
      darkColors: [
        Color(red: 0.27, green: 0.40, blue: 0.64),
        Color(red: 0.26, green: 0.54, blue: 0.43),
      ],
      anchor: Color(red: 0.61, green: 0.84, blue: 0.90)
    ),
    .init(
      name: "Berry Sky",
      lightColors: [
        Color(red: 0.98, green: 0.73, blue: 0.86),
        Color(red: 0.70, green: 0.79, blue: 1.00),
      ],
      darkColors: [
        Color(red: 0.56, green: 0.31, blue: 0.47),
        Color(red: 0.30, green: 0.38, blue: 0.62),
      ],
      anchor: Color(red: 0.80, green: 0.70, blue: 0.91)
    ),
    .init(
      name: "Sunset Rose",
      lightColors: [
        Color(red: 1.00, green: 0.71, blue: 0.60),
        Color(red: 0.99, green: 0.64, blue: 0.76),
      ],
      darkColors: [
        Color(red: 0.55, green: 0.31, blue: 0.29),
        Color(red: 0.57, green: 0.29, blue: 0.43),
      ],
      anchor: Color(red: 0.93, green: 0.63, blue: 0.66)
    ),
    .init(
      name: "Forest Lake",
      lightColors: [
        Color(red: 0.60, green: 0.88, blue: 0.68),
        Color(red: 0.49, green: 0.79, blue: 0.90),
      ],
      darkColors: [
        Color(red: 0.26, green: 0.46, blue: 0.30),
        Color(red: 0.24, green: 0.39, blue: 0.52),
      ],
      anchor: Color(red: 0.54, green: 0.79, blue: 0.72)
    ),
  ]

  var body: some View {
    VStack(alignment: .leading, spacing: 12) {
      VStack(alignment: .leading, spacing: 12) {
        HStack(spacing: 10) {
          ForEach(gradientOptions, id: \.self) { option in
            Button {
              Haptics.impact(.light)
              selected = option.anchor
            } label: {
              ZStack {
                Circle()
                  .fill(
                    LinearGradient(
                      colors: option.colors(for: colorScheme),
                      startPoint: .topLeading,
                      endPoint: .bottomTrailing
                    )
                  )
                  .frame(width: 34, height: 34)
                  .overlay(
                    Circle()
                      .stroke(Color.white.opacity(0.9), lineWidth: 1)
                  )

                if isSelected(option.anchor) {
                  Image(systemName: "checkmark")
                    .font(.system(size: 14, weight: .bold))
                    .foregroundStyle(
                      colorScheme == .dark
                        ? Color.white.opacity(0.9)
                        : Color.black.opacity(0.75)
                    )
                }
              }
              .frame(maxWidth: .infinity)
            }
            .buttonStyle(.plain)
            .accessibilityLabel("\(option.name) gradient")
          }
        }

        Divider()
          .overlay(Color(.separator).opacity(0.20))

        LazyVGrid(columns: columns, spacing: 12) {
          ForEach(options, id: \.self) { option in
            Button {
              Haptics.impact(.light)
              selected = option.color
            } label: {
              ZStack {
                Circle()
                  .fill(option.swatch(for: colorScheme))
                  .frame(width: 34, height: 34)
                  .overlay(
                    Circle()
                      .stroke(Color.white.opacity(0.92), lineWidth: 1)
                  )

                if isSelected(option.color) {
                  Image(systemName: "checkmark")
                    .font(.system(size: 14, weight: .bold))
                    .foregroundStyle(
                      colorScheme == .dark
                        ? Color.white.opacity(0.9)
                        : Color.black.opacity(0.75)
                    )
                }
              }
              .frame(maxWidth: .infinity)
            }
            .buttonStyle(.plain)
            .accessibilityLabel(option.name)
          }
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
    guard let a = rgba(of: candidate), let b = rgba(of: selected) else {
      return String(describing: candidate) == String(describing: selected)
    }

    let difference =
      abs(a.r - b.r) + abs(a.g - b.g) + abs(a.b - b.b) + abs(a.a - b.a)
    return difference < 0.045
  }

  private func rgba(of color: Color) -> (r: CGFloat, g: CGFloat, b: CGFloat, a: CGFloat)? {
    var r: CGFloat = 0
    var g: CGFloat = 0
    var b: CGFloat = 0
    var a: CGFloat = 0

    guard UIColor(color).getRed(&r, green: &g, blue: &b, alpha: &a) else {
      return nil
    }
    return (r, g, b, a)
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

        // Try loading from local GRDB cache first (instant)
        if let cachedRow = LocalDatabase.shared.loadDiaryEntry(id: entry.id),
           !cachedRow.body_blocks.isEmpty {
          let decoded = DiaryService.decodeBodyBlocks(cachedRow.body_blocks)
          var loaded: [DiaryBlock] = []
          for d in decoded {
            switch d.content {
            case .text(let s):
              loaded.append(DiaryBlock(id: d.id, content: .text(s)))
            case .imageURL(_, let path):
              // Load image async later, show text blocks instantly
              loaded.append(DiaryBlock(id: d.id, content: .image(nil, storagePath: path)))
            }
          }
          if !loaded.isEmpty {
            blocks = loaded
            entry.body = blocks.textContent
            // Load images in background
            Task {
              for (i, d) in decoded.enumerated() {
                if case .imageURL(let url, _) = d.content {
                  let img = await DiaryService.shared.loadImageFromURL(url)
                  await MainActor.run {
                    if i < blocks.count {
                      if case .image(_, let path) = blocks[i].content {
                        blocks[i] = DiaryBlock(id: d.id, content: .image(img, storagePath: path))
                      }
                    }
                  }
                }
              }
            }
            return
          }
        }

        // Fallback: fetch from API
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

        // Try loading from local GRDB cache first (instant)
        if let cachedRow = LocalDatabase.shared.loadDiaryEntry(id: entry.id),
           !cachedRow.body_blocks.isEmpty {
          let decoded = DiaryService.decodeBodyBlocks(cachedRow.body_blocks)
          var loaded: [DiaryBlock] = []
          for d in decoded {
            switch d.content {
            case .text(let s):
              loaded.append(DiaryBlock(id: d.id, content: .text(s)))
            case .imageURL(_, let path):
              loaded.append(DiaryBlock(id: d.id, content: .image(nil, storagePath: path)))
            }
          }
          if !loaded.isEmpty {
            blocks = loaded
            Task {
              for (i, d) in decoded.enumerated() {
                if case .imageURL(let url, _) = d.content {
                  let img = await DiaryService.shared.loadImageFromURL(url)
                  await MainActor.run {
                    if i < blocks.count {
                      if case .image(_, let path) = blocks[i].content {
                        blocks[i] = DiaryBlock(id: d.id, content: .image(img, storagePath: path))
                      }
                    }
                  }
                }
              }
            }
            return
          }
        }

        // Fallback: fetch from API
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
