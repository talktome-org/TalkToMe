import SwiftUI

struct SidebarRenameSheetView: View {
    @Binding var isPresented: Bool
    @Binding var renameText: String
    let onSave: (String) -> Void

    var body: some View {
        VStack(spacing: 16) {
            Text("Rename Conversation")
                .font(.system(size: 20, weight: .semibold))

            TextField("Title", text: $renameText)
                .textInputAutocapitalization(.sentences)
                .disableAutocorrection(true)
                .padding(12)
                .background(Color(.secondarySystemBackground))
                .clipShape(RoundedRectangle(cornerRadius: 10, style: .continuous))

            HStack {
                Button("Cancel") { isPresented = false }
                Spacer()
                Button("Save") {
                    let text = renameText
                    isPresented = false
                    onSave(text)
                }
                .disabled(renameText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
            }
            .padding(.top, 6)
        }
        .padding(20)
        .presentationDetents([.medium])
    }
}


