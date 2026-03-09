import SwiftUI

/// ViewModifier that adds rename, delete, and report dialogs for chat sessions.
/// Extracts session management UI from ChatView for cleaner separation of concerns.
struct ChatSessionActionsModifier: ViewModifier {
    let sessionId: UUID?
    let currentTitle: String
    let onRename: (UUID, String) async -> Void
    let onDelete: (UUID) async -> Void
    let onDeleteNavigateAway: () -> Void

    @State private var showRenameChatPrompt: Bool = false
    @State private var renameChatTitle: String = ""
    @State private var showDeleteChatConfirm: Bool = false
    @State private var showReportChatConfirm: Bool = false
    @State private var showReportChatThanks: Bool = false

    func body(content: Content) -> some View {
        content
            .alert("Rename chat", isPresented: $showRenameChatPrompt) {
                TextField("Title", text: $renameChatTitle)
                Button("Cancel", role: .cancel) {}
                Button("Save") {
                    let title = renameChatTitle.trimmingCharacters(in: .whitespacesAndNewlines)
                    guard !title.isEmpty, let sid = sessionId else { return }
                    Haptics.impact(.light)
                    Task { @MainActor in
                        await onRename(sid, title)
                    }
                }
            } message: {
                Text("Give this chat a new title.")
            }
            .confirmationDialog("Delete chat?", isPresented: $showDeleteChatConfirm, titleVisibility: .visible) {
                Button("Delete", role: .destructive) {
                    guard let sid = sessionId else { return }
                    Haptics.impact(.light)
                    Task { @MainActor in
                        await onDelete(sid)
                        onDeleteNavigateAway()
                    }
                }
                Button("Cancel", role: .cancel) {}
            } message: {
                Text("This will delete the chat and all its messages. If this chat is linked, it will be deleted on their end too.")
            }
            .confirmationDialog("Report chat?", isPresented: $showReportChatConfirm, titleVisibility: .visible) {
                Button("Report", role: .destructive) {
                    showReportChatThanks = true
                }
                Button("Cancel", role: .cancel) {}
            } message: {
                Text("This will flag the conversation for review. (Not wired yet)")
            }
            .alert("Thanks", isPresented: $showReportChatThanks) {
                Button("OK", role: .cancel) {}
            } message: {
                Text("Report received. (Not wired yet)")
            }
    }

    // MARK: - Trigger Methods

    func showRename() {
        renameChatTitle = currentTitle
        showRenameChatPrompt = true
    }

    func showDelete() {
        showDeleteChatConfirm = true
    }

    func showReport() {
        showReportChatConfirm = true
    }
}

// MARK: - Toolbar Menu Content

/// A view that provides the session actions menu content for toolbars.
/// Use this in conjunction with ChatSessionActionsModifier.
struct ChatSessionActionsMenu: View {
    let sessionId: UUID?
    let currentTitle: String
    let onRenameRequest: () -> Void
    let onArchiveRequest: () -> Void
    let onDeleteRequest: () -> Void
    let onReportRequest: () -> Void

    @State private var showActions: Bool = false

    private var formattedTitle: String {
        let words = currentTitle.split(separator: " ")
        guard words.count > 2 else { return currentTitle }
        let firstLine = words.prefix(2).joined(separator: " ")
        let rest = words.dropFirst(2).joined(separator: " ")
        return firstLine + "\n" + rest
    }

    var body: some View {
        Button {
            // Dismiss keyboard first so it doesn't interfere with the action sheet
            UIApplication.shared.sendAction(
                #selector(UIResponder.resignFirstResponder),
                to: nil, from: nil, for: nil
            )
            showActions = true
        } label: {
            Label("More", systemImage: "ellipsis")
        }
        .confirmationDialog(formattedTitle, isPresented: $showActions, titleVisibility: .visible) {
            Button("Rename") {
                guard sessionId != nil else { return }
                onRenameRequest()
            }
            .disabled(sessionId == nil)

            Button("Report") {
                onReportRequest()
            }
            .disabled(sessionId == nil)

            Button("Archive") {
                onArchiveRequest()
            }
            .disabled(sessionId == nil)

            Button("Delete", role: .destructive) {
                onDeleteRequest()
            }
            .disabled(sessionId == nil)
        }
    }
}

// MARK: - Combined Coordinator

/// Coordinates both the menu and the dialogs for session actions.
/// Provides a cleaner API for ChatView to use.
@MainActor
final class ChatSessionActionsCoordinator: ObservableObject {
    @Published var showRenameChatPrompt: Bool = false
    @Published var renameChatTitle: String = ""
    @Published var showDeleteChatConfirm: Bool = false
    @Published var showReportFlow: Bool = false
    @Published var showArchiveChatConfirm: Bool = false
    @Published var showArchiveChatThanks: Bool = false

    func requestRename(currentTitle: String) {
        renameChatTitle = currentTitle
        showRenameChatPrompt = true
    }

    func requestDelete() {
        showDeleteChatConfirm = true
    }

    func requestReport() {
        showReportFlow = true
    }

    func requestArchive() {
        showArchiveChatConfirm = true
    }
}

// MARK: - Session Actions Dialogs Modifier

/// A ViewModifier that uses the coordinator to present session action dialogs.
struct ChatSessionActionsDialogsModifier: ViewModifier {
    @ObservedObject var coordinator: ChatSessionActionsCoordinator
    let sessionId: UUID?
    let onRename: (UUID, String) async -> Void
    let onDelete: (UUID) async -> Void
    let onDeleteNavigateAway: () -> Void

    func body(content: Content) -> some View {
        content
            .alert("Rename chat", isPresented: $coordinator.showRenameChatPrompt) {
                TextField("Title", text: $coordinator.renameChatTitle)
                Button("Cancel", role: .cancel) {}
                Button("Save") {
                    let title = coordinator.renameChatTitle.trimmingCharacters(in: .whitespacesAndNewlines)
                    guard !title.isEmpty, let sid = sessionId else { return }
                    Haptics.impact(.light)
                    Task { @MainActor in
                        await onRename(sid, title)
                    }
                }
            } message: {
                Text("Give this chat a new title.")
            }
            .confirmationDialog("Delete chat?", isPresented: $coordinator.showDeleteChatConfirm, titleVisibility: .visible) {
                Button("Delete", role: .destructive) {
                    guard let sid = sessionId else { return }
                    Haptics.impact(.light)
                    Task { @MainActor in
                        await onDelete(sid)
                        onDeleteNavigateAway()
                    }
                }
                Button("Cancel", role: .cancel) {}
            } message: {
                Text("This will delete the chat and all its messages. If this chat is linked, it will be deleted on their end too.")
            }
            .sheet(isPresented: $coordinator.showReportFlow) {
                ReportConversationFlowView(isPresented: $coordinator.showReportFlow)
            }
            .confirmationDialog("Archive chat?", isPresented: $coordinator.showArchiveChatConfirm, titleVisibility: .visible) {
                Button("Archive") {
                    coordinator.showArchiveChatThanks = true
                }
                Button("Cancel", role: .cancel) {}
            } message: {
                Text("This will archive the conversation. (Not wired yet)")
            }
            .alert("Thanks", isPresented: $coordinator.showArchiveChatThanks) {
                Button("OK", role: .cancel) {}
            } message: {
                Text("Archive received. (Not wired yet)")
            }
    }
}

private struct ReportReason: Identifiable, Hashable {
    let id: String
    let title: String
}

private struct ReportCategory: Identifiable, Hashable {
    let id: String
    let title: String
    let reasons: [ReportReason]
}

private struct ReportConversationFlowView: View {
    @Binding var isPresented: Bool

    @State private var selectedCategory: ReportCategory? = nil
    @State private var selectedReason: ReportReason? = nil
    @State private var detailsText: String = ""
    @State private var showSubmittedState: Bool = false

    // Overlay states
    @State private var showReasonOverlay: Bool = false
    @State private var reasonDragOffset: CGFloat = 0
    @State private var showDetailsOverlay: Bool = false
    @State private var detailsDragOffset: CGFloat = 0

    private let dismissThreshold: CGFloat = 100

    private let categories: [ReportCategory] = [
        ReportCategory(
            id: "violence-self-harm",
            title: "Violent or self-harm content",
            reasons: [
                ReportReason(id: "threats", title: "Threats or encouragement of violence"),
                ReportReason(id: "gender-violence", title: "Violence targeting gender identity"),
                ReportReason(id: "sexual-violence", title: "Sexually violent behavior"),
                ReportReason(id: "weapons", title: "Weapon-related harm"),
                ReportReason(id: "suicide-self-injury", title: "Suicide or self-injury"),
                ReportReason(id: "eating-disorders", title: "Eating-disorder promotion"),
                ReportReason(id: "human-trafficking", title: "Human trafficking or exploitation"),
                ReportReason(id: "terrorism", title: "Extremist violence")
            ]
        ),
        ReportCategory(
            id: "sexual-abuse",
            title: "Sexual abuse or exploitation",
            reasons: [
                ReportReason(id: "coercion", title: "Coercion or non-consensual behavior"),
                ReportReason(id: "explicit-abuse", title: "Graphic sexual abuse"),
                ReportReason(id: "solicitation", title: "Sexual solicitation"),
                ReportReason(id: "other", title: "Other sexual abuse concern")
            ]
        ),
        ReportCategory(
            id: "minor-exploitation",
            title: "Exploitation of minors",
            reasons: [
                ReportReason(id: "grooming", title: "Grooming behavior"),
                ReportReason(id: "minor-sexual-content", title: "Sexual content involving a minor"),
                ReportReason(id: "child-endangerment", title: "Child endangerment"),
                ReportReason(id: "other", title: "Other minor safety concern")
            ]
        ),
        ReportCategory(
            id: "bullying-harassment",
            title: "Harassment or bullying",
            reasons: [
                ReportReason(id: "targeted-harassment", title: "Targeted harassment"),
                ReportReason(id: "hate-speech", title: "Hateful or degrading language"),
                ReportReason(id: "threatening-language", title: "Threatening language"),
                ReportReason(id: "other", title: "Other harassment concern")
            ]
        ),
        ReportCategory(
            id: "fraud",
            title: "Scams, spam, or deception",
            reasons: [
                ReportReason(id: "impersonation", title: "Impersonation"),
                ReportReason(id: "financial-scam", title: "Financial scam"),
                ReportReason(id: "phishing", title: "Phishing or credential theft"),
                ReportReason(id: "spam", title: "Spam or mass unsolicited messages")
            ]
        ),
        ReportCategory(
            id: "privacy",
            title: "Privacy or personal-data misuse",
            reasons: [
                ReportReason(id: "doxxing", title: "Sharing private personal information"),
                ReportReason(id: "tracking", title: "Stalking or unauthorized tracking"),
                ReportReason(id: "images", title: "Non-consensual personal images"),
                ReportReason(id: "other", title: "Other privacy concern")
            ]
        ),
        ReportCategory(
            id: "ip",
            title: "Copyright or IP misuse",
            reasons: [
                ReportReason(id: "copyright", title: "Copyright infringement"),
                ReportReason(id: "trademark", title: "Trademark misuse"),
                ReportReason(id: "piracy", title: "Pirated material"),
                ReportReason(id: "other", title: "Other IP concern")
            ]
        ),
        ReportCategory(
            id: "age-inappropriate",
            title: "Inappropriate for younger users",
            reasons: [
                ReportReason(id: "adult-content", title: "Adult content"),
                ReportReason(id: "graphic-content", title: "Graphic disturbing content"),
                ReportReason(id: "substance-use", title: "Substance abuse promotion"),
                ReportReason(id: "other", title: "Other age-related concern")
            ]
        ),
        ReportCategory(
            id: "other",
            title: "Other concern",
            reasons: [
                ReportReason(id: "policy", title: "Policy violation"),
                ReportReason(id: "safety", title: "General safety concern"),
                ReportReason(id: "quality", title: "Abusive or harmful quality issue"),
                ReportReason(id: "other", title: "Something else")
            ]
        )
    ]

    var body: some View {
        NavigationStack {
            ZStack {
                AppTheme.background.ignoresSafeArea()

                // Level 0: Category selection
                categoryListView

                // Level 1: Reason selection overlay
                if showReasonOverlay, let category = selectedCategory {
                    reasonListView(for: category)
                        .frame(maxWidth: .infinity, maxHeight: .infinity)
                        .background(AppTheme.background)
                        .offset(x: reasonDragOffset)
                        .overlay(alignment: .leading) {
                            Color.clear
                                .frame(width: 24)
                                .contentShape(Rectangle())
                                .gesture(reasonDragGesture)
                        }
                        .zIndex(1)
                }

                // Level 2: Details + submit overlay
                if showDetailsOverlay, let reason = selectedReason {
                    detailsView(for: reason)
                        .frame(maxWidth: .infinity, maxHeight: .infinity)
                        .background(AppTheme.background)
                        .offset(x: detailsDragOffset)
                        .overlay(alignment: .leading) {
                            Color.clear
                                .frame(width: 24)
                                .contentShape(Rectangle())
                                .gesture(detailsDragGesture)
                        }
                        .zIndex(2)
                }
            }
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .topBarLeading) {
                    if showDetailsOverlay {
                        Button { dismissDetailsOverlay() } label: {
                            Image(systemName: "chevron.left")
                                .font(.body.weight(.semibold))
                        }
                    } else if showReasonOverlay {
                        Button { dismissReasonOverlay() } label: {
                            Image(systemName: "chevron.left")
                                .font(.body.weight(.semibold))
                        }
                    } else {
                        Button("Cancel") {
                            isPresented = false
                        }
                    }
                }

                ToolbarItem(placement: .principal) {
                    Text("Report conversation")
                        .font(.headline)
                }

                ToolbarItem(placement: .topBarTrailing) {
                    if showDetailsOverlay {
                        Button("Submit") {
                            showSubmittedState = true
                        }
                    }
                }
            }
        }
        .alert("Report submitted", isPresented: $showSubmittedState) {
            Button("Done") {
                showDetailsOverlay = false
                showReasonOverlay = false
                selectedReason = nil
                selectedCategory = nil
                detailsText = ""
                detailsDragOffset = 0
                reasonDragOffset = 0
                isPresented = false
            }
        } message: {
            Text("Thanks. We saved your report and will review it.")
        }
    }

    // MARK: - Category List

    private var categoryListView: some View {
        ScrollView {
            VStack(spacing: 18) {
                Text("Why are you reporting this conversation?")
                    .font(.headline)
                    .foregroundStyle(.primary)
                    .multilineTextAlignment(.center)

                VStack(spacing: 0) {
                    ForEach(categories.indices, id: \.self) { index in
                        Button { selectCategory(categories[index]) } label: {
                            optionRow(title: categories[index].title)
                        }
                        .buttonStyle(.plain)

                        if index < categories.count - 1 {
                            Divider().padding(.leading, 16)
                        }
                    }
                }
                .background(
                    RoundedRectangle(cornerRadius: 20, style: .continuous)
                        .fill(Color(uiColor: .secondarySystemBackground))
                )

                Spacer(minLength: 0)
            }
            .padding(.horizontal, 16)
            .padding(.top, 18)
        }
    }

    // MARK: - Reason List

    private func reasonListView(for category: ReportCategory) -> some View {
        ScrollView {
            VStack(spacing: 18) {
                Text(category.title)
                    .font(.title3.weight(.semibold))
                    .foregroundStyle(.primary)
                    .multilineTextAlignment(.center)

                Text("Choose the option that best matches what happened.")
                    .font(.subheadline)
                    .foregroundStyle(.secondary)

                VStack(spacing: 0) {
                    ForEach(category.reasons.indices, id: \.self) { index in
                        Button { selectReason(category.reasons[index]) } label: {
                            optionRow(title: category.reasons[index].title)
                        }
                        .buttonStyle(.plain)

                        if index < category.reasons.count - 1 {
                            Divider().padding(.leading, 16)
                        }
                    }
                }
                .background(
                    RoundedRectangle(cornerRadius: 20, style: .continuous)
                        .fill(Color(uiColor: .secondarySystemBackground))
                )

                Spacer(minLength: 0)
            }
            .padding(.horizontal, 16)
            .padding(.top, 18)
        }
    }

    // MARK: - Details View

    private func detailsView(for reason: ReportReason) -> some View {
        ScrollView {
            VStack(spacing: 18) {
                Text(reason.title)
                    .font(.headline)
                    .foregroundStyle(.primary)
                    .multilineTextAlignment(.center)

                ZStack(alignment: .topLeading) {
                    RoundedRectangle(cornerRadius: 16, style: .continuous)
                        .fill(Color(uiColor: .secondarySystemBackground))
                        .frame(minHeight: 130)

                    TextEditor(text: $detailsText)
                        .frame(minHeight: 130)
                        .padding(10)
                        .scrollContentBackground(.hidden)
                        .background(.clear)

                    if detailsText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                        Text("Share additional context")
                            .foregroundStyle(.secondary)
                            .padding(.top, 18)
                            .padding(.leading, 16)
                            .allowsHitTesting(false)
                    }
                }

                Spacer(minLength: 0)
            }
            .padding(.horizontal, 16)
            .padding(.top, 18)
        }
    }

    // MARK: - Shared Option Row

    private func optionRow(title: String) -> some View {
        HStack(spacing: 12) {
            Text(title)
                .foregroundStyle(.primary)
                .multilineTextAlignment(.leading)
            Spacer()
            Image(systemName: "chevron.right")
                .font(.footnote.weight(.semibold))
                .foregroundStyle(.secondary)
        }
        .padding(.horizontal, 16)
        .padding(.vertical, 14)
    }

    // MARK: - Navigation

    private func selectCategory(_ category: ReportCategory) {
        selectedCategory = category
        reasonDragOffset = UIScreen.main.bounds.width
        showReasonOverlay = true
        withAnimation(.easeOut(duration: 0.20)) {
            reasonDragOffset = 0
        }
    }

    private func selectReason(_ reason: ReportReason) {
        selectedReason = reason
        detailsDragOffset = UIScreen.main.bounds.width
        showDetailsOverlay = true
        withAnimation(.easeOut(duration: 0.20)) {
            detailsDragOffset = 0
        }
    }

    private func dismissReasonOverlay() {
        withAnimation(.easeOut(duration: 0.25)) {
            reasonDragOffset = UIScreen.main.bounds.width
        }
        DispatchQueue.main.asyncAfter(deadline: .now() + 0.25) {
            showReasonOverlay = false
            selectedCategory = nil
            reasonDragOffset = 0
        }
    }

    private func dismissDetailsOverlay() {
        UIApplication.shared.sendAction(#selector(UIResponder.resignFirstResponder), to: nil, from: nil, for: nil)
        withAnimation(.easeOut(duration: 0.25)) {
            detailsDragOffset = UIScreen.main.bounds.width
        }
        DispatchQueue.main.asyncAfter(deadline: .now() + 0.25) {
            showDetailsOverlay = false
            selectedReason = nil
            detailsText = ""
            detailsDragOffset = 0
        }
    }

    // MARK: - Drag Gestures

    private var reasonDragGesture: some Gesture {
        DragGesture()
            .onChanged { value in
                if value.translation.width > 0 {
                    reasonDragOffset = value.translation.width
                }
            }
            .onEnded { value in
                if value.translation.width > dismissThreshold || value.predictedEndTranslation.width > dismissThreshold * 2 {
                    withAnimation(.easeOut(duration: 0.2)) {
                        reasonDragOffset = UIScreen.main.bounds.width
                    }
                    DispatchQueue.main.asyncAfter(deadline: .now() + 0.2) {
                        showReasonOverlay = false
                        selectedCategory = nil
                        reasonDragOffset = 0
                    }
                } else {
                    withAnimation(.spring(response: 0.3, dampingFraction: 0.7)) {
                        reasonDragOffset = 0
                    }
                }
            }
    }

    private var detailsDragGesture: some Gesture {
        DragGesture()
            .onChanged { value in
                if value.translation.width > 0 {
                    detailsDragOffset = value.translation.width
                }
            }
            .onEnded { value in
                if value.translation.width > dismissThreshold || value.predictedEndTranslation.width > dismissThreshold * 2 {
                    UIApplication.shared.sendAction(#selector(UIResponder.resignFirstResponder), to: nil, from: nil, for: nil)
                    withAnimation(.easeOut(duration: 0.2)) {
                        detailsDragOffset = UIScreen.main.bounds.width
                    }
                    DispatchQueue.main.asyncAfter(deadline: .now() + 0.2) {
                        showDetailsOverlay = false
                        selectedReason = nil
                        detailsText = ""
                        detailsDragOffset = 0
                    }
                } else {
                    withAnimation(.spring(response: 0.3, dampingFraction: 0.7)) {
                        detailsDragOffset = 0
                    }
                }
            }
    }
}

extension View {
    /// Adds session action dialogs (rename, delete, report) to the view.
    func chatSessionActions(
        coordinator: ChatSessionActionsCoordinator,
        sessionId: UUID?,
        onRename: @escaping (UUID, String) async -> Void,
        onDelete: @escaping (UUID) async -> Void,
        onDeleteNavigateAway: @escaping () -> Void
    ) -> some View {
        modifier(ChatSessionActionsDialogsModifier(
            coordinator: coordinator,
            sessionId: sessionId,
            onRename: onRename,
            onDelete: onDelete,
            onDeleteNavigateAway: onDeleteNavigateAway
        ))
    }
}
