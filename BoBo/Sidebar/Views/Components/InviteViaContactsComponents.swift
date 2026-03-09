import SwiftUI
import Contacts
import MessageUI

struct InviteContactsInlineListView: View {
    let onInvite: (String) -> Void

    @StateObject private var viewModel = InviteContactsListViewModel()

    var body: some View {
        Group {
            if let message = viewModel.errorMessage, !message.isEmpty {
                VStack(spacing: 10) {
                    Text(message)
                        .font(.system(size: 13))
                        .foregroundStyle(.secondary)
                        .multilineTextAlignment(.center)
                        .padding(.horizontal, 12)

                    Button("Try Again") {
                        Task { await viewModel.load() }
                    }
                    .buttonStyle(.bordered)
                }
                .frame(maxWidth: .infinity)
                .padding(.vertical, 8)
            } else if viewModel.isLoading {
                HStack(spacing: 10) {
                    ProgressView()
                    Text("Loading contacts…")
                        .font(.system(size: 13))
                        .foregroundStyle(.secondary)
                    Spacer()
                }
                .padding(.vertical, 6)
            } else {
                LazyVStack(spacing: 0) {
                    ForEach(viewModel.rows) { row in
                        HStack(spacing: 12) {
                            VStack(alignment: .leading, spacing: 2) {
                                Text(row.name)
                                    .font(.system(size: 16, weight: .semibold))
                                    .foregroundStyle(.primary)
                                    .lineLimit(1)
                                Text(row.phone)
                                    .font(.system(size: 13))
                                    .foregroundStyle(.secondary)
                                    .lineLimit(1)
                            }
                            Spacer()
                            Button("Invite") {
                                onInvite(row.phone)
                            }
                            .buttonStyle(.borderedProminent)
                        }
                        .padding(.vertical, 10)

                        Divider()
                            .opacity(0.35)
                    }
                }
                .clipShape(RoundedRectangle(cornerRadius: 12, style: .continuous))
            }
        }
        .task {
            await viewModel.load()
        }
    }
}

@MainActor
final class InviteContactsListViewModel: ObservableObject {
    struct Row: Identifiable {
        let id: String
        let name: String
        let phone: String
    }

    @Published var rows: [Row] = []
    @Published var isLoading: Bool = false
    @Published var errorMessage: String? = nil

    private let store = CNContactStore()

    func load() async {
        isLoading = true
        errorMessage = nil
        rows = []

        let granted: Bool
        do {
            granted = try await store.requestAccess(for: .contacts)
        } catch {
            isLoading = false
            errorMessage = "Couldn't access contacts."
            return
        }

        guard granted else {
            isLoading = false
            errorMessage = "Contacts permission is off. Enable it in Settings to invite people."
            return
        }

        do {
            // CNContactStore enumeration can be expensive; do it off the main actor so presenting
            // the sheet doesn't freeze the UI.
            let out = try await Task.detached(priority: .userInitiated) { () throws -> [Row] in
                let store = CNContactStore()
                let keys: [CNKeyDescriptor] = [
                    CNContactGivenNameKey as CNKeyDescriptor,
                    CNContactFamilyNameKey as CNKeyDescriptor,
                    CNContactPhoneNumbersKey as CNKeyDescriptor
                ]

                let request = CNContactFetchRequest(keysToFetch: keys)
                request.unifyResults = true
                request.sortOrder = .userDefault

                var rows: [Row] = []
                try store.enumerateContacts(with: request) { contact, _ in
                    let name = [contact.givenName, contact.familyName]
                        .joined(separator: " ")
                        .trimmingCharacters(in: .whitespacesAndNewlines)
                    let displayName = name.isEmpty ? "Unknown" : name

                    for phoneValue in contact.phoneNumbers {
                        let phone = phoneValue.value.stringValue.trimmingCharacters(in: .whitespacesAndNewlines)
                        guard !phone.isEmpty else { continue }
                        rows.append(Row(id: "\(contact.identifier)|\(phone)", name: displayName, phone: phone))
                    }
                }
                return rows
            }.value

            rows = out
            isLoading = false
            if rows.isEmpty {
                errorMessage = "No contacts with phone numbers found."
            }
        } catch {
            isLoading = false
            errorMessage = "Failed to load contacts."
        }
    }
}

struct InviteMessageComposerView: UIViewControllerRepresentable {
    static var canSendText: Bool { MFMessageComposeViewController.canSendText() }

    let recipients: [String]
    let body: String
    @Binding var isPresented: Bool

    func makeCoordinator() -> Coordinator {
        Coordinator(isPresented: $isPresented)
    }

    func makeUIViewController(context: Context) -> MFMessageComposeViewController {
        let vc = MFMessageComposeViewController()
        vc.messageComposeDelegate = context.coordinator
        vc.recipients = recipients
        vc.body = body
        return vc
    }

    func updateUIViewController(_ uiViewController: MFMessageComposeViewController, context: Context) {}

    final class Coordinator: NSObject, MFMessageComposeViewControllerDelegate {
        @Binding private var isPresented: Bool

        init(isPresented: Binding<Bool>) {
            self._isPresented = isPresented
        }

        func messageComposeViewController(
            _ controller: MFMessageComposeViewController,
            didFinishWith result: MessageComposeResult
        ) {
            isPresented = false
        }
    }
}

