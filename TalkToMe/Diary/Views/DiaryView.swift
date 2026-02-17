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
  @State private var showNewEntry: Bool = false
  @State private var showDiaryEditor: Bool = false
  @State private var entryToEdit: JournalEntry? = nil
  @State private var entryDateForNewNote: Date = Date()
  @State private var draftTitle: String = ""
  @State private var draftBody: String = ""

  var body: some View {
    NavigationStack {
      ZStack(alignment: .bottomTrailing) {
        AppTheme.background.ignoresSafeArea()

        VStack(spacing: 0) {
          // Top section: "My Diary" + date
          header
            .padding(.horizontal, 16)
            .padding(.top, 12)
            .padding(.bottom, 36)
            .frame(maxWidth: .infinity, alignment: .leading)
            .background(topTintBackground)

          // White sheet overlaps header by 24pt so rounded top corners show
          JournalSheetView(
            tab: $tab,
            description: viewModel.diaryDescription,
            stats: viewModel.stats,
            entries: $viewModel.entries,
            accentColor: viewModel.diaryColor,
            onEditDiary: { showDiaryEditor = true },
            onAddEntryForDate: { date in
              entryDateForNewNote = date
              showNewEntry = true
            },
            onSelectEntry: { entry in
              entryToEdit = entry
            }
          )
          .frame(maxWidth: .infinity, maxHeight: .infinity)
          .padding(.top, -24)
        }
        .ignoresSafeArea(edges: .bottom)

        GlassFloatingActionButton(systemName: "plus") {
          Haptics.impact(.light)
          entryDateForNewNote = Date()
          showNewEntry = true
        }
        .padding(.trailing, 18)
        .padding(.bottom, 50)
      }
      .navigationTitle("Diary")
      .navigationBarTitleDisplayMode(.inline)
      .toolbarBackground(.hidden, for: .navigationBar)
      .toolbar {
        ToolbarItem(placement: .topBarLeading) {
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
    .sheet(isPresented: $showNewEntry) {
      NewDiaryNoteSheet(
        accentColor: viewModel.diaryColor,
        initialDate: entryDateForNewNote,
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
    .sheet(item: $entryToEdit) { entry in
      EditDiaryNoteSheet(
        entry: entry,
        accentColor: viewModel.diaryColor,
        onSave: { updated in
          if let idx = viewModel.entries.firstIndex(where: { $0.id == updated.id }) {
            viewModel.entries[idx] = updated
          }
          entryToEdit = nil
        },
        onDelete: {
          viewModel.entries.removeAll { $0.id == entry.id }
          entryToEdit = nil
        }
      )
      .presentationDetents([.large])
      .presentationDragIndicator(.visible)
    }
    .onAppear { viewModel.loadIfNeeded() }
    .onChange(of: AuthService.shared.currentUserId) { _, _ in viewModel.loadIfNeeded() }
  }

  private var topTintBackground: some View {
    // User-selectable color: only the top section with "My Diary" + date.
    // Editable via DiaryEditorSheet → Color picker.
    viewModel.diaryColor
      .ignoresSafeArea(edges: .top)
      .allowsHitTesting(false)
  }

  private var header: some View {
    VStack(alignment: .leading, spacing: 8) {
      HStack(spacing: 10) {
        Text(
          viewModel.diaryName.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
            ? "My Diary" : viewModel.diaryName
        )
        .font(.system(size: 34, weight: .bold))
        .foregroundStyle(.primary)
      }

      // Date + year in one organic line.
      HStack(spacing: 8) {
        Text(viewModel.displayTodayString)
        Text("•")
          .foregroundStyle(.secondary.opacity(0.7))
        Text(viewModel.displayYearString)
      }
      .font(.system(size: 14, weight: .semibold))
      .foregroundStyle(.secondary)
    }
  }
}

#Preview {
  DiaryView()
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
        return JournalEntry(
          id: row.id,
          date: date,
          title: row.title,
          body: body,
          createdAt: createdAt,
          timezoneAbbreviation: row.timezone_abbreviation,
          photoCount: photoCount
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
        photoCount: 0
      )
      entries.insert(entry, at: 0)
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
          photoCount: 0
        )
        await MainActor.run { entries.insert(entry, at: 0) }
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

  @State private var selectedCalendarDay: Date? = Calendar.current.startOfDay(for: Date())

  var body: some View {
    VStack(spacing: 0) {
      JournalTabBar(tab: $tab)
        .padding(.top, 10)
        .padding(.bottom, 8)

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
          JournalListTab(entries: entries, onSelectEntry: onSelectEntry)
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
    .background(
      UnevenRoundedRectangle(
        topLeadingRadius: 32,
        bottomLeadingRadius: 0,
        bottomTrailingRadius: 0,
        topTrailingRadius: 32,
        style: .continuous
      )
      .fill(AppTheme.background)
      .shadow(color: Color.black.opacity(0.08), radius: 18, x: 0, y: -2)
    )
    .clipShape(
      UnevenRoundedRectangle(
        topLeadingRadius: 32,
        bottomLeadingRadius: 0,
        bottomTrailingRadius: 0,
        topTrailingRadius: 32,
        style: .continuous
      )
    )
  }
}

private struct JournalTabBar: View {
  @Binding var tab: JournalTab

  var body: some View {
    HStack(spacing: 18) {
      tabButton(.overview, label: AnyView(Image(systemName: "book")))
      tabButton(.list, label: AnyView(Text("List")))
      tabButton(.calendar, label: AnyView(Text("Calendar")))
      tabButton(.todo, label: AnyView(Text("ToDo")))

      Spacer(minLength: 0)
    }
    .padding(.horizontal, 18)
    .font(.system(size: 15, weight: .semibold))
    .foregroundStyle(.primary)
  }

  @ViewBuilder
  private func tabButton(_ value: JournalTab, label: AnyView) -> some View {
    Button {
      Haptics.impact(.light)
      withAnimation(.spring(response: 0.26, dampingFraction: 0.78)) {
        tab = value
      }
    } label: {
      VStack(spacing: 8) {
        label
          .opacity(tab == value ? 1.0 : 0.6)
        Rectangle()
          .fill(tab == value ? Color.primary : Color.clear)
          .frame(height: 2)
          .clipShape(Capsule())
      }
      .padding(.horizontal, value == .overview ? 2 : 6)
      .padding(.vertical, 6)
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
      VStack(alignment: .leading, spacing: 18) {
        VStack(alignment: .leading, spacing: 8) {
          Text("Description")
            .font(.system(size: 22, weight: .bold))
            .foregroundStyle(.primary)

          Text(
            description.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
              ? "Add a description…" : description
          )
          .font(.system(size: 15, weight: .regular))
          .foregroundStyle(
            description.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
              ? .secondary : .primary
          )
          .opacity(description.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty ? 1.0 : 0.9)
          .frame(maxWidth: .infinity, minHeight: 64, alignment: .topLeading)
          .contentShape(Rectangle())
          .onTapGesture {
            Haptics.impact(.light)
            onEditDiary()
          }
        }
        .padding(.horizontal, 18)
        .padding(.top, 18)

        VStack(alignment: .leading, spacing: 12) {
          Text("Statistics")
            .font(.system(size: 22, weight: .bold))
            .foregroundStyle(.primary)

          // First big card: Streak (number centered). Then Entries, Media, Days, On this day.
          HStack(alignment: .top, spacing: 12) {
            GlassStatBigCard(
              title: "STREAK",
              value: "\(stats.streakDays)",
              subtitle: "Days",
              accentColor: accentColor
            )

            VStack(spacing: 12) {
              HStack(spacing: 12) {
                GlassStatSmallCard(
                  title: "ENTRIES", value: "\(stats.entriesCount)", accentColor: accentColor)
                GlassStatSmallCard(
                  title: "MEDIA", value: "\(stats.mediaCount)", accentColor: accentColor)
              }
              HStack(spacing: 12) {
                GlassStatSmallCard(
                  title: "DAYS", value: "\(stats.uniqueDaysCount)", accentColor: accentColor)
                GlassStatSmallCard(
                  title: "ON THIS DAY", value: "\(stats.onThisDayCount)", accentColor: accentColor)
              }
            }
          }
        }
        .padding(.horizontal, 18)

        Spacer().frame(height: 120)
      }
    }
    .scrollIndicators(.hidden)
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
        Text(title)
          .font(.system(size: 11, weight: .semibold))
          .foregroundStyle(.secondary)

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
      .frame(maxWidth: .infinity, maxHeight: .infinity)
      .padding(16)
      .frame(width: 150, height: 160)
      .background { GlassCardBackground(shape: shape, interactive: true) }
      .overlay { shape.stroke(accentColor.opacity(0.16), lineWidth: 1) }
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
          .font(.system(size: 11, weight: .semibold))
          .foregroundStyle(.secondary)

        Text(value)
          .font(.system(size: 22, weight: .bold, design: .rounded))
          .monospacedDigit()
          .foregroundStyle(.primary)

        Spacer(minLength: 0)
      }
      .padding(14)
      .frame(maxWidth: .infinity)
      .frame(height: 74)
      .background { GlassCardBackground(shape: shape, interactive: true) }
      .overlay { shape.stroke(accentColor.opacity(0.14), lineWidth: 1) }
    }
    .buttonStyle(StatCardButtonStyle())
  }
}

private struct GlassCardBackground<S: Shape>: View {
  let shape: S
  var interactive: Bool = false

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
              JournalEntryRowLink(entry: entry, onSelect: onSelectEntry)
                .padding(.horizontal, 18)
            }
          }
          Spacer().frame(height: 120)
        }
      }
    }
    .scrollIndicators(.hidden)
  }
}

// MARK: - ToDo list tab

private let diaryTodoStorageKey = "diary_todo_items"

private struct JournalTodoTab: View {
  @State private var todoItems: [DiaryTodoItem] = []
  @State private var newTodoTitle: String = ""
  @FocusState private var isAddFieldFocused: Bool

  var body: some View {
    VStack(spacing: 0) {
      // Add row: text field + plus button on the right
      HStack(spacing: 12) {
        TextField("New task", text: $newTodoTitle)
          .font(.system(size: 16, weight: .regular))
          .focused($isAddFieldFocused)
          .submitLabel(.done)
          .onSubmit { addTodo() }
          .padding(.vertical, 12)
          .padding(.leading, 16)
          .padding(.trailing, 8)
          .background(AppTheme.surface)
          .clipShape(RoundedRectangle(cornerRadius: 12, style: .continuous))

        Button {
          addTodo()
        } label: {
          Image(systemName: "plus.circle.fill")
            .font(.system(size: 32))
            .symbolRenderingMode(.hierarchical)
            .foregroundStyle(
              newTodoTitle.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
                ? Color.secondary.opacity(0.6) : AppTheme.accent)
        }
        .buttonStyle(.plain)
        .disabled(newTodoTitle.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
      }
      .padding(.horizontal, 18)
      .padding(.vertical, 14)
      .background(AppTheme.background)
      .overlay(alignment: .bottom) {
        Rectangle().fill(Color(.separator)).opacity(0.5).frame(height: 0.5)
      }

      if todoItems.isEmpty {
        VStack(spacing: 8) {
          Spacer().frame(height: 48)
          Image(systemName: "checklist")
            .font(.system(size: 24, weight: .light))
            .foregroundStyle(.tertiary)
          Text("Nothing yet")
            .font(.system(size: 15, weight: .regular))
            .foregroundStyle(.tertiary)
        }
        .frame(maxWidth: .infinity)
        Spacer(minLength: 0)
      } else {
        List {
          ForEach($todoItems) { $item in
            TodoRowView(
              item: $item,
              onDelete: {
                todoItems.removeAll { $0.id == item.id }
                saveTodos()
              }
            )
            .listRowInsets(EdgeInsets(top: 10, leading: 18, bottom: 10, trailing: 18))
            .listRowSeparator(.visible)
            .listRowBackground(Color.clear)
            .swipeActions(edge: .trailing, allowsFullSwipe: true) {
              Button(role: .destructive) {
                Haptics.impact(.light)
                todoItems.removeAll { $0.id == item.id }
                saveTodos()
              } label: {
                Label("Delete", systemImage: "trash")
              }
            }
          }
        }
        .listStyle(.plain)
        .scrollContentBackground(.hidden)
      }
    }
    .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .top)
    .onAppear { loadTodos() }
    .onChange(of: todoItems) { _, _ in saveTodos() }
  }

  private func addTodo() {
    let trimmed = newTodoTitle.trimmingCharacters(in: .whitespacesAndNewlines)
    guard !trimmed.isEmpty else { return }
    Haptics.impact(.light)
    todoItems.insert(DiaryTodoItem(title: trimmed), at: 0)
    newTodoTitle = ""
  }

  private func loadTodos() {
    guard let data = UserDefaults.standard.data(forKey: diaryTodoStorageKey),
      let decoded = try? JSONDecoder().decode([DiaryTodoItem].self, from: data)
    else { return }
    todoItems = decoded
  }

  private func saveTodos() {
    guard let data = try? JSONEncoder().encode(todoItems) else { return }
    UserDefaults.standard.set(data, forKey: diaryTodoStorageKey)
  }
}

private struct TodoRowView: View {
  @Binding var item: DiaryTodoItem
  var onDelete: (() -> Void)?

  @State private var isSlidingOut: Bool = false
  private let slideOutDuration: Double = 0.28

  var body: some View {
    HStack(alignment: .center, spacing: 12) {
      Button {
        Haptics.impact(.light)
        item.isCompleted.toggle()
      } label: {
        Image(systemName: item.isCompleted ? "checkmark.circle.fill" : "circle")
          .font(.system(size: 20, weight: .light))
          .foregroundStyle(
            item.isCompleted ? Color.primary.opacity(0.6) : Color.primary.opacity(0.25))
      }
      .buttonStyle(.plain)

      Text(item.title)
        .font(.system(size: 16, weight: .regular))
        .strikethrough(item.isCompleted, color: item.isCompleted ? Color.secondary : nil)
        .foregroundStyle(item.isCompleted ? .secondary : .primary)
        .frame(maxWidth: .infinity, alignment: .leading)
        .multilineTextAlignment(.leading)

      if item.isCompleted, let onDelete = onDelete {
        Button {
          Haptics.impact(.light)
          withAnimation(.easeIn(duration: slideOutDuration)) {
            isSlidingOut = true
          }
          DispatchQueue.main.asyncAfter(deadline: .now() + slideOutDuration) {
            onDelete()
          }
        } label: {
          Image(systemName: "trash")
            .font(.system(size: 16, weight: .medium))
            .foregroundStyle(.secondary)
            .symbolRenderingMode(.hierarchical)
        }
        .buttonStyle(.plain)
      }
    }
    .contentShape(Rectangle())
    .offset(x: isSlidingOut ? -UIScreen.main.bounds.width : 0)
    .opacity(isSlidingOut ? 0 : 1)
    .animation(.easeIn(duration: slideOutDuration), value: isSlidingOut)
  }
}

private struct JournalEntryRowLink: View {
  let entry: JournalEntry
  let onSelect: (JournalEntry) -> Void

  var body: some View {
    Button {
      Haptics.impact(.light)
      onSelect(entry)
    } label: {
      JournalListRowContent(entry: entry)
    }
    .buttonStyle(.plain)
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
    .padding(.vertical, 12)
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

  private var daysWithEntries: Set<Date> {
    Set(entries.map { cal.startOfDay(for: $0.date) })
  }

  private func entries(on day: Date) -> [JournalEntry] {
    let s = cal.startOfDay(for: day)
    return
      entries
      .filter { cal.startOfDay(for: $0.date) == s }
      .sorted { $0.createdAt > $1.createdAt }
  }

  var body: some View {
    ScrollView {
      VStack(spacing: 16) {
        MonthCalendarScrollView(
          daysWithEntries: daysWithEntries,
          accentColor: accentColor,
          selectedDay: $selectedDay,
          onDayTapped: { date in
            sheetDay = cal.startOfDay(for: date)
            showDaySheet = true
          }
        )
        .padding(.top, 12)

        if let selectedDay {
          let dayEntries = entries(on: selectedDay)
          if dayEntries.isEmpty {
            Text("No entries on this day")
              .font(.system(size: 14))
              .foregroundStyle(.secondary)
              .frame(maxWidth: .infinity, alignment: .center)
              .padding(.top, 6)
          } else {
            VStack(alignment: .leading, spacing: 10) {
              Text("On this day")
                .font(.system(size: 14, weight: .semibold))
                .foregroundStyle(.secondary)
                .padding(.horizontal, 18)

              ForEach(dayEntries) { entry in
                JournalEntryRowLink(entry: entry, onSelect: onSelectEntry)
                  .padding(.horizontal, 18)
              }
            }
          }
        }

        Spacer().frame(height: 120)
      }
    }
    .scrollIndicators(.hidden)
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
          onSelectEntry: onSelectEntry
        )
        .presentationDetents([.medium, .large])
        .presentationDragIndicator(.visible)
      }
    }
  }
}

private struct MonthCalendarScrollView: View {
  let daysWithEntries: Set<Date>
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
    LazyVStack(spacing: 18) {
      ForEach(monthsToShow, id: \.self) { monthStart in
        MonthGridView(
          monthStart: monthStart,
          daysWithEntries: daysWithEntries,
          accentColor: accentColor,
          selectedDay: $selectedDay,
          onDayTapped: onDayTapped
        )
        .padding(.horizontal, 14)
      }
    }
  }
}

private struct MonthGridView: View {
  let monthStart: Date
  let daysWithEntries: Set<Date>
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

  var body: some View {
    VStack(alignment: .leading, spacing: 10) {
      Text(title)
        .font(.system(size: 14, weight: .semibold))
        .foregroundStyle(.secondary)
        .padding(.horizontal, 4)

      LazyVGrid(columns: Array(repeating: GridItem(.flexible(), spacing: 6), count: 7), spacing: 10)
      {
        ForEach(weekdaySymbols, id: \.self) { sym in
          Text(sym)
            .font(.system(size: 11, weight: .semibold))
            .foregroundStyle(.secondary.opacity(0.9))
            .frame(maxWidth: .infinity)
        }

        ForEach(Array(days.enumerated()), id: \.offset) { _, date in
          DayCell(
            date: date,
            monthStart: monthStart,
            daysWithEntries: daysWithEntries,
            accentColor: accentColor,
            selectedDay: $selectedDay,
            onDayTapped: onDayTapped
          )
        }
      }
      .padding(.horizontal, 4)
    }
  }
}

private struct DayCell: View {
  let date: Date?
  let monthStart: Date
  let daysWithEntries: Set<Date>
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
    guard let date else { return false }
    return daysWithEntries.contains(cal.startOfDay(for: date))
  }

  var body: some View {
    Group {
      if let date {
        Button {
          Haptics.impact(.light)
          withAnimation(.easeInOut(duration: 0.15)) {
            selectedDay = cal.startOfDay(for: date)
          }
          onDayTapped?(date)
        } label: {
          ZStack {
            RoundedRectangle(cornerRadius: 10, style: .continuous)
              .fill(backgroundFill)

            Text("\(cal.component(.day, from: date))")
              .font(
                .system(
                  size: 15, weight: isSelected || isToday ? .bold : .regular, design: .rounded)
              )
              .foregroundStyle(.primary)
              .frame(maxWidth: .infinity, maxHeight: .infinity)

            if hasEntry {
              Circle()
                .fill(accentColor)
                .frame(width: 6, height: 6)
                .offset(y: 16)
            }
          }
          .frame(height: 36)
        }
        .buttonStyle(.plain)
      } else {
        Color.clear
          .frame(height: 36)
      }
    }
  }

  private var backgroundFill: Color {
    if isSelected { return Color.primary.opacity(0.16) }
    if isToday { return Color.primary.opacity(0.12) }
    if hasEntry { return accentColor.opacity(0.10) }
    return Color.clear
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
              Image(systemName: "book.closed")
                .font(.system(size: 36))
                .foregroundStyle(.secondary)
              Text("No entries on this day")
                .font(.system(size: 16, weight: .medium))
                .foregroundStyle(.secondary)
              Text("Tap the button below to add your first entry.")
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
            Haptics.impact(.light)
            onAddEntry()
          } label: {
            HStack(spacing: 10) {
              Image(systemName: "plus.circle.fill")
                .font(.system(size: 20))
              Text("Add entry")
                .font(.system(size: 17, weight: .semibold))
            }
            .foregroundStyle(.white)
            .frame(maxWidth: .infinity)
            .padding(.vertical, 16)
            .background(accentColor)
            .clipShape(RoundedRectangle(cornerRadius: 14, style: .continuous))
          }
          .padding(.horizontal, 20)
          .padding(.top, 8)
          .padding(.bottom, 24)
        }
      }
      .background(Color(.systemBackground))
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
  /// If set, entry is already persisted remotely and should not be re-saved as text-only.
  var entryId: UUID? = nil
}

// Replaced with a full-page editor style sheet (`NewDiaryNoteSheet`).

private struct DiaryEditorSheet: View {
  @Environment(\.dismiss) private var dismiss

  @State private var name: String
  @State private var description: String
  @State private var color: Color

  let onSave: (String, String, Color) -> Void

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
      ScrollView {
        VStack(alignment: .leading, spacing: 16) {
          VStack(alignment: .leading, spacing: 10) {
            Text("Diary name")
              .font(.system(size: 13, weight: .semibold))
              .foregroundStyle(.secondary)

            TextField("My Diary", text: $name)
              .textContentType(.none)
              .textInputAutocapitalization(.words)
              .autocorrectionDisabled()
              .padding(.horizontal, 12)
              .padding(.vertical, 10)
              .background(Color(.secondarySystemBackground))
              .clipShape(RoundedRectangle(cornerRadius: 12, style: .continuous))
          }

          VStack(alignment: .leading, spacing: 10) {
            Text("Description")
              .font(.system(size: 13, weight: .semibold))
              .foregroundStyle(.secondary)

            TextEditor(text: $description)
              .font(.system(size: 15))
              .frame(minHeight: 160)
              .padding(10)
              .background(Color(.secondarySystemBackground))
              .clipShape(RoundedRectangle(cornerRadius: 12, style: .continuous))
          }

          VStack(alignment: .leading, spacing: 10) {
            Text("Header color")
              .font(.system(size: 13, weight: .semibold))
              .foregroundStyle(.secondary)

            Text("Changes the top section tint above the tabs.")
              .font(.system(size: 12))
              .foregroundStyle(.tertiary)

            DiaryColorPicker(selected: $color)
          }

          Divider()
            .padding(.top, 6)

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

          Spacer(minLength: 10)
        }
        .padding(18)
      }
      .navigationTitle("Diary editor")
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
                    .stroke(Color.white.opacity(0.8), lineWidth: 1)
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
      .background(Color(.secondarySystemBackground))
      .clipShape(RoundedRectangle(cornerRadius: 12, style: .continuous))

      // Still allow custom colors, but keep it secondary.
      ColorPicker("Custom", selection: $selected, supportsOpacity: false)
        .font(.system(size: 14, weight: .semibold))
        .padding(12)
        .background(Color(.secondarySystemBackground))
        .clipShape(RoundedRectangle(cornerRadius: 12, style: .continuous))
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

  var body: some View {
    VStack(alignment: .leading, spacing: 10) {
      editorContent
    }
    .background(Color(.systemBackground))
    .navigationBarTitleDisplayMode(.inline)
    .toolbarBackground(.hidden, for: .navigationBar)
    .ignoresSafeArea(.keyboard, edges: .bottom)
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
              case .imageStoragePath(let path):
                let img = await DiaryService.shared.loadImage(storagePath: path)
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
          return .imageRemote(id: block.id, storagePath: path)
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
      .background(Color(.systemBackground))
      .toolbarBackground(.hidden, for: .navigationBar)
      .ignoresSafeArea(.keyboard, edges: .bottom)
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

    let payload: [DiaryBlockPayload] = await MainActor.run {
      blocks.compactMap { block in
        switch block.content {
        case .text(let s):
          return .text(id: block.id, content: s)
        case .image(_, let path?) where path.isEmpty == false:
          return .imageRemote(id: block.id, storagePath: path)
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

  var body: some View {
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
      .background(Color(.systemBackground))
      .toolbarBackground(.hidden, for: .navigationBar)
      .ignoresSafeArea(.keyboard, edges: .bottom)
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
                    photoCount: blocks.photoCount
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
              case .imageStoragePath(let path):
                let img = await DiaryService.shared.loadImage(storagePath: path)
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
          return .imageRemote(id: block.id, storagePath: path)
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
  let accentColor: Color
  var isVoiceRecording: Bool = false
  var onAudioTap: (() -> Void)? = nil
  var onEndRecording: (() -> Void)? = nil
  var onPhotoLibrary: (() -> Void)? = nil
  var onCamera: (() -> Void)? = nil
  var onFiles: (() -> Void)? = nil

  private var recordingTransition: AnyTransition {
    .asymmetric(
      insertion: .move(edge: .bottom).combined(with: .opacity),
      removal: .move(edge: .bottom).combined(with: .opacity)
    )
  }

  var body: some View {
    VStack(spacing: 10) {
      Divider()
        .opacity(0.12)

      if isVoiceRecording {
        NoteAccessoryRecordingBar(onEnd: { onEndRecording?() })
          .transition(recordingTransition)
          .zIndex(1)
      } else if #available(iOS 26.0, *) {
        NoteAccessoryGlassBar(
          accentColor: accentColor, onAudioTap: onAudioTap, onPhotoLibrary: onPhotoLibrary,
          onCamera: onCamera, onFiles: onFiles
        )
        .transition(recordingTransition)
      } else {
        NoteAccessoryFallbackBar(
          accentColor: accentColor, onAudioTap: onAudioTap, onPhotoLibrary: onPhotoLibrary,
          onCamera: onCamera, onFiles: onFiles
        )
        .transition(recordingTransition)
      }
    }
    .padding(.horizontal, isVoiceRecording ? 14 : 20)
    .padding(.bottom, 10)
    .shadow(color: Color.black.opacity(0.06), radius: 8, x: 0, y: -2)
    .animation(.spring(response: 0.4, dampingFraction: 0.82), value: isVoiceRecording)
  }
}

private struct NoteAccessoryRecordingBar: View {
  let onEnd: () -> Void

  private let barShape = RoundedRectangle(cornerRadius: 28, style: .continuous)
  private let backgroundMinHeight: CGFloat = 68
  private let contentPadding: CGFloat = 14

  var body: some View {
    Group {
      if #available(iOS 26.0, *) {
        recordingBarContent
          .padding(.horizontal, contentPadding)
          .padding(.vertical, contentPadding)
          .frame(maxWidth: .infinity, minHeight: backgroundMinHeight)
          .glassEffect(.regular.interactive(), in: barShape)
      } else {
        recordingBarContent
          .padding(.horizontal, contentPadding)
          .padding(.vertical, contentPadding)
          .frame(maxWidth: .infinity, minHeight: backgroundMinHeight)
          .background(barShape.fill(.ultraThinMaterial))
      }
    }
  }

  private var recordingBarContent: some View {
    HStack(spacing: 16) {
      HStack(spacing: 12) {
        Circle()
          .fill(Color.red)
          .frame(width: 12, height: 12)
        Text("Recording…")
          .font(.system(size: 19, weight: .semibold))
          .foregroundStyle(.primary)
      }

      Spacer(minLength: 16)

      Button(action: {
        Haptics.impact(.medium)
        onEnd()
      }) {
        Text("End")
          .font(.system(size: 17, weight: .semibold))
          .foregroundColor(.red)
          .padding(.horizontal, 20)
          .padding(.vertical, 12)
          .background(.ultraThinMaterial, in: Capsule())
      }
      .buttonStyle(.plain)
    }
  }
}

@available(iOS 26.0, *)
private struct NoteAccessoryGlassBar: View {
  let accentColor: Color
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
          Image(systemName: "paperclip")
            .font(.system(size: 18, weight: .medium))
            .symbolRenderingMode(.hierarchical)
            .foregroundStyle(.primary)
            .frame(maxWidth: .infinity)
            .padding(.vertical, 10)
        }
        .buttonStyle(.plain)
        .accessibilityLabel("Attach")

        noteAccessoryDivider()

        Button {
          Haptics.impact(.light)
          onPhotoLibrary?()
        } label: {
          Image(systemName: "photo.on.rectangle.angled")
            .font(.system(size: 18, weight: .medium))
            .symbolRenderingMode(.hierarchical)
            .foregroundStyle(.primary)
            .frame(maxWidth: .infinity)
            .padding(.vertical, 10)
        }
        .buttonStyle(.plain)
        .accessibilityLabel("Photo library")

        noteAccessoryDivider()

        Button {
          Haptics.impact(.light)
          onCamera?()
        } label: {
          Image(systemName: "camera")
            .font(.system(size: 18, weight: .medium))
            .symbolRenderingMode(.hierarchical)
            .foregroundStyle(.primary)
            .frame(maxWidth: .infinity)
            .padding(.vertical, 10)
        }
        .buttonStyle(.plain)
        .accessibilityLabel("Camera")

        noteAccessoryDivider()

        Button {
          Haptics.impact(.light)
          onAudioTap?()
        } label: {
          Image(systemName: "waveform")
            .font(.system(size: 18, weight: .medium))
            .symbolRenderingMode(.hierarchical)
            .foregroundStyle(.primary)
            .frame(maxWidth: .infinity)
            .padding(.vertical, 10)
            .contentShape(Rectangle())
        }
        .buttonStyle(.plain)
        .accessibilityLabel("Audio")
      }
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
  }
}

private struct NoteAccessoryFallbackBar: View {
  let accentColor: Color
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
        Image(systemName: "paperclip")
          .font(.system(size: 18, weight: .medium))
          .symbolRenderingMode(.hierarchical)
          .foregroundStyle(.primary)
          .frame(maxWidth: .infinity)
          .padding(.vertical, 10)
      }
      .buttonStyle(.plain)
      .accessibilityLabel("Attach")
      fallbackDivider()
      Button {
        Haptics.impact(.light)
        onPhotoLibrary?()
      } label: {
        Image(systemName: "photo.on.rectangle.angled")
          .font(.system(size: 18, weight: .medium))
          .symbolRenderingMode(.hierarchical)
          .foregroundStyle(.primary)
          .frame(maxWidth: .infinity)
          .padding(.vertical, 10)
      }
      .buttonStyle(.plain)
      .accessibilityLabel("Photo library")
      fallbackDivider()
      Button {
        Haptics.impact(.light)
        onCamera?()
      } label: {
        Image(systemName: "camera")
          .font(.system(size: 18, weight: .medium))
          .symbolRenderingMode(.hierarchical)
          .foregroundStyle(.primary)
          .frame(maxWidth: .infinity)
          .padding(.vertical, 10)
      }
      .buttonStyle(.plain)
      .accessibilityLabel("Camera")
      fallbackDivider()
      Button {
        Haptics.impact(.light)
        onAudioTap?()
      } label: {
        Image(systemName: "waveform")
          .font(.system(size: 18, weight: .medium))
          .symbolRenderingMode(.hierarchical)
          .foregroundStyle(.primary)
          .frame(maxWidth: .infinity)
          .padding(.vertical, 10)
          .contentShape(Rectangle())
      }
      .buttonStyle(.plain)
      .accessibilityLabel("Audio")
    }
    .padding(.horizontal, 8)
    .padding(.vertical, 8)
    .frame(height: 52)
    .background(barShape.fill(.ultraThinMaterial))
  }

  private func fallbackDivider() -> some View {
    Rectangle()
      .fill(Color.primary.opacity(0.35))
      .frame(width: 1, height: 28)
  }

  private func accessoryIconButton(_ systemImage: String, label: String) -> some View {
    Button {
      Haptics.impact(.light)
    } label: {
      Image(systemName: systemImage)
        .font(.system(size: 18, weight: .medium))
        .symbolRenderingMode(.hierarchical)
        .foregroundStyle(.primary)
        .frame(maxWidth: .infinity)
        .padding(.vertical, 10)
    }
    .buttonStyle(.plain)
    .accessibilityLabel(label)
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
