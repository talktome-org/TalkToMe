import SwiftUI

struct ContactSupportView: View {
    @State private var isCopied: Bool = false

    private let supportEmail = "team.talktome@gmail.com"

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 0) {
                Text("Have a question, found a bug, or just want to say hi? We'd love to hear from you.")
                    .font(.system(size: 15))
                    .foregroundStyle(.secondary)
                    .padding(.horizontal, 20)
                    .padding(.top, 8)
                    .padding(.bottom, 24)

                // Get in Touch
                sectionHeader("Get in Touch")

                VStack(spacing: 0) {
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

                            VStack(alignment: .leading, spacing: 2) {
                                Text("Send Email")
                                    .font(.system(size: 17))
                                    .foregroundStyle(Color(.label))

                                Text("Opens your default mail app")
                                    .font(.system(size: 13))
                                    .foregroundStyle(.secondary)
                            }

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
                .settingsCard()
                .padding(.bottom, 20)

                // Helpful Tips
                sectionHeader("Helpful Tips")

                VStack(spacing: 0) {
                    tipRow("Include your device model and iOS version so we can reproduce issues faster.")

                    Divider()
                        .padding(.leading, 60)

                    tipRow("If something looks wrong, a screenshot goes a long way.")

                    Divider()
                        .padding(.leading, 60)

                    tipRow("For account or data deletion requests, use the email address linked to your account.")
                }
                .settingsCard()

                Text("We typically respond within 24 hours.")
                    .font(.system(size: 13))
                    .foregroundStyle(.secondary)
                    .padding(.horizontal, 20)
                    .padding(.top, 6)
            }
            .padding(.horizontal, 16)
            .padding(.bottom, 24)
        }
        .scrollIndicators(.hidden)
        .navigationTitle("Contact Support")
        .navigationBarTitleDisplayMode(.inline)
        .background(Color(.systemGroupedBackground))
    }

    private func sectionHeader(_ title: String) -> some View {
        Text(title)
            .font(.system(size: 13))
            .foregroundStyle(.secondary)
            .textCase(.uppercase)
            .padding(.horizontal, 20)
            .padding(.bottom, 8)
    }

    private func tipRow(_ text: String) -> some View {
        HStack(spacing: 14) {
            Image(systemName: "circle.fill")
                .font(.system(size: 5))
                .foregroundStyle(Color(.tertiaryLabel))
                .frame(width: 30, height: 30)

            Text(text)
                .font(.system(size: 17))
                .foregroundStyle(Color(.label))
                .fixedSize(horizontal: false, vertical: true)
        }
        .padding(.horizontal, 16)
        .frame(minHeight: 44)
    }
}

private extension View {
    func settingsCard() -> some View {
        self
            .padding(.vertical, 6)
            .background(Color(.secondarySystemGroupedBackground))
            .clipShape(RoundedRectangle(cornerRadius: 26, style: .continuous))
    }
}

#Preview {
    NavigationStack {
        ContactSupportView()
    }
}
