import SwiftUI

struct ContactSupportView: View {
    @State private var isCopied: Bool = false

    private let supportEmail = "team.talktome@gmail.com"

    var body: some View {
        ScrollView {
            VStack(spacing: 24) {
                // Header
                VStack(spacing: 8) {
                    Image(systemName: "envelope.circle.fill")
                        .font(.system(size: 48))
                        .foregroundStyle(.secondary)

                    Text("We're here to help")
                        .font(.system(size: 15))
                        .foregroundStyle(.secondary)
                }
                .padding(.top, 12)

                // Actions card
                VStack(spacing: 0) {
                    // Send email row
                    Button {
                        Haptics.impact(.medium)
                        if let url = URL(string: "mailto:\(supportEmail)") {
                            UIApplication.shared.open(url)
                        }
                    } label: {
                        HStack(spacing: 14) {
                            Image(systemName: "paperplane")
                                .font(.system(size: 18))
                                .foregroundStyle(.secondary)
                                .frame(width: 30, height: 30)

                            Text("Send Email")
                                .font(.system(size: 17))
                                .foregroundStyle(Color(.label))

                            Spacer()

                            Image(systemName: "arrow.up.right")
                                .font(.system(size: 14, weight: .semibold))
                                .foregroundStyle(Color(.tertiaryLabel))
                        }
                        .padding(.horizontal, 16)
                        .frame(minHeight: 44)
                    }
                    .buttonStyle(.plain)

                    Divider()
                        .padding(.leading, 60)

                    // Copy email row
                    Button {
                        UIPasteboard.general.string = supportEmail
                        Haptics.notification(.success)
                        withAnimation(.spring(response: 0.25, dampingFraction: 0.9)) { isCopied = true }
                        DispatchQueue.main.asyncAfter(deadline: .now() + 1.5) {
                            withAnimation(.spring(response: 0.25, dampingFraction: 0.9)) { isCopied = false }
                        }
                    } label: {
                        HStack(spacing: 14) {
                            Image(systemName: isCopied ? "checkmark" : "doc.on.doc")
                                .font(.system(size: 18))
                                .foregroundStyle(.secondary)
                                .frame(width: 30, height: 30)
                                .contentTransition(.symbolEffect(.replace))

                            VStack(alignment: .leading, spacing: 2) {
                                Text(isCopied ? "Copied!" : "Copy Email Address")
                                    .font(.system(size: 17))
                                    .foregroundStyle(Color(.label))
                                    .contentTransition(.numericText())

                                Text(supportEmail)
                                    .font(.system(size: 13))
                                    .foregroundStyle(.secondary)
                            }

                            Spacer()
                        }
                        .padding(.horizontal, 16)
                        .frame(minHeight: 44)
                    }
                    .buttonStyle(.plain)
                }
                .padding(.vertical, 6)
                .background(Color(.secondarySystemGroupedBackground))
                .clipShape(RoundedRectangle(cornerRadius: 16, style: .continuous))

                // Footer
                Text("We typically respond within 24 hours.")
                    .font(.system(size: 13))
                    .foregroundStyle(.secondary)
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .padding(.horizontal, 20)
            }
            .padding(.horizontal, 16)
            .padding(.bottom, 24)
        }
        .navigationTitle("Contact Support")
        .navigationBarTitleDisplayMode(.inline)
        .background(Color(.systemGroupedBackground))
    }
}

#Preview {
    NavigationStack {
        ContactSupportView()
    }
}
