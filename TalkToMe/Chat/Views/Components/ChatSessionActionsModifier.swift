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
    let onDeleteRequest: () -> Void
    let onReportRequest: () -> Void

    var body: some View {
        Menu {
            Button("Rename chat", systemImage: "pencil") {
                guard sessionId != nil else { return }
                onRenameRequest()
            }
            .disabled(sessionId == nil)

            Button("Report chat", systemImage: "exclamationmark.bubble") {
                onReportRequest()
            }
            .disabled(sessionId == nil)

            Button("Delete chat", systemImage: "trash", role: .destructive) {
                onDeleteRequest()
            }
            .disabled(sessionId == nil)
        } label: {
            Label("More", systemImage: "ellipsis")
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
    @Published var showReportChatConfirm: Bool = false
    @Published var showReportChatThanks: Bool = false

    func requestRename(currentTitle: String) {
        renameChatTitle = currentTitle
        showRenameChatPrompt = true
    }

    func requestDelete() {
        showDeleteChatConfirm = true
    }

    func requestReport() {
        showReportChatConfirm = true
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
            .confirmationDialog("Report chat?", isPresented: $coordinator.showReportChatConfirm, titleVisibility: .visible) {
                Button("Report", role: .destructive) {
                    coordinator.showReportChatThanks = true
                }
                Button("Cancel", role: .cancel) {}
            } message: {
                Text("This will flag the conversation for review. (Not wired yet)")
            }
            .alert("Thanks", isPresented: $coordinator.showReportChatThanks) {
                Button("OK", role: .cancel) {}
            } message: {
                Text("Report received. (Not wired yet)")
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
