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

                // Get in Touch section
                VStack(alignment: .leading, spacing: 6) {
                    Text("Get in Touch")
                        .font(.system(size: 13))
                        .foregroundStyle(.secondary)
                        .textCase(.uppercase)
                        .padding(.horizontal, 20)
                        .padding(.bottom, 2)

                    VStack(spacing: 0) {
                        // Send email row
                        Button {
                            Haptics.impact(.medium)
                            if let url = URL(string: "mailto:\(supportEmail)") {
                                UIApplication.shared.open(url)
                            }
                        } label: {
                            supportRow(
                                icon: "paperplane",
                                title: "Send Email",
                                subtitle: "Opens your default mail app",
                                trailing: AnyView(
                                    Image(systemName: "arrow.up.right")
                                        .font(.system(size: 14, weight: .semibold))
                                        .foregroundStyle(Color(.tertiaryLabel))
                                )
                            )
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
                            supportRow(
                                icon: isCopied ? "checkmark" : "doc.on.doc",
                                title: isCopied ? "Copied!" : "Copy Email Address",
                                subtitle: supportEmail,
                                trailing: nil
                            )
                        }
                        .buttonStyle(.plain)
                    }
                    .padding(.vertical, 8)
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .background(Color(.secondarySystemGroupedBackground))
                    .clipShape(RoundedRectangle(cornerRadius: 16, style: .continuous))
                }
                .padding(.bottom, 20)

                // Tips section
                VStack(alignment: .leading, spacing: 6) {
                    Text("Helpful Tips")
                        .font(.system(size: 13))
                        .foregroundStyle(.secondary)
                        .textCase(.uppercase)
                        .padding(.horizontal, 20)
                        .padding(.bottom, 2)

                    VStack(spacing: 0) {
                        tipRow("Include your device model and iOS version so we can reproduce issues faster.")

                        Divider()
                            .padding(.leading, 60)

                        tipRow("If something looks wrong, a screenshot goes a long way.")

                        Divider()
                            .padding(.leading, 60)

                        tipRow("For account or data deletion requests, use the email address linked to your account.")
                    }
                    .padding(.vertical, 8)
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .background(Color(.secondarySystemGroupedBackground))
                    .clipShape(RoundedRectangle(cornerRadius: 16, style: .continuous))

                    Text("We typically respond within 24 hours.")
                        .font(.system(size: 13))
                        .foregroundStyle(.secondary)
                        .padding(.horizontal, 20)
                        .padding(.top, 6)
                }
            }
            .padding(.horizontal, 16)
            .padding(.bottom, 24)
        }
        .scrollIndicators(.hidden)
        .navigationTitle("Contact Support")
        .navigationBarTitleDisplayMode(.inline)
        .background(AppTheme.background)
    }

    private func supportRow(icon: String, title: String, subtitle: String, trailing: AnyView?) -> some View {
        HStack(alignment: .top, spacing: 14) {
            Image(systemName: icon)
                .font(.system(size: 18))
                .foregroundStyle(.secondary)
                .frame(width: 30)
                .padding(.top, 8)
                .contentTransition(.symbolEffect(.replace))

            VStack(alignment: .leading, spacing: 4) {
                Text(title)
                    .font(.system(size: 15, weight: .semibold))
                    .foregroundStyle(Color(.label))
                    .contentTransition(.numericText())

                Text(subtitle)
                    .font(.system(size: 15))
                    .foregroundStyle(Color(.secondaryLabel))
                    .lineSpacing(4)
            }
            .frame(maxWidth: .infinity, alignment: .leading)

            if let trailing {
                trailing
                    .padding(.top, 10)
            }
        }
        .padding(.horizontal, 16)
        .padding(.vertical, 10)
    }

    private func tipRow(_ text: String) -> some View {
        HStack(alignment: .top, spacing: 14) {
            Image(systemName: "circle.fill")
                .font(.system(size: 5))
                .foregroundStyle(Color(.tertiaryLabel))
                .frame(width: 30)
                .padding(.top, 8)

            Text(text)
                .font(.system(size: 15))
                .foregroundStyle(Color(.label))
                .lineSpacing(4)
                .fixedSize(horizontal: false, vertical: true)
                .frame(maxWidth: .infinity, alignment: .leading)
        }
        .padding(.horizontal, 16)
        .padding(.vertical, 10)
    }
}

#Preview {
    NavigationStack {
        ContactSupportView()
    }
}
