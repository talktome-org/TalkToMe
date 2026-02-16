import SwiftUI

private struct PolicySection: Identifiable {
    let id = UUID()
    let title: String
    let items: [String]
}

private let policySections: [PolicySection] = [
    PolicySection(
        title: "Information We Collect",
        items: [
            "Account and Authentication: We use Supabase for authentication via Apple Sign-in, Google Sign-in, or email and password. We process your user ID, basic profile metadata from your identity provider (e.g., name, email), and session tokens stored securely in Keychain.",
            "Profile: You can set a display name and a short bio. If you upload an avatar, the image is stored in Supabase Storage and accessed via signed URLs.",
            "Chat: Your messages and AI-generated responses are stored so you can view history and continue conversations. This includes session titles, message previews, and any images or files you attach.",
            "Diary: If you use the diary, your entries (including text, titles, and photos) are stored. Diary photos are uploaded to Supabase Storage.",
            "Friends and Partner Linking: If you link with a partner using friend codes, we store relationship mappings, partner message drafts, and delivery statuses. You can assign display names to partners for a personalized experience.",
            "Notifications: If you enable push notifications, we store your device token, platform, and bundle identifier to deliver notifications for partner messages and chat updates. You can disable this at any time in Settings.",
            "Voice: If you use voice input, your audio is streamed to Deepgram for real-time transcription. If you enable voice playback, assistant text is sent to ElevenLabs for text-to-speech. Audio is used only to provide these features and is not stored permanently.",
            "Local Cache: We cache profile data, chat sessions, and messages locally on your device using a local database to provide fast, offline-ready access."
        ]
    ),
    PolicySection(
        title: "How We Use Information",
        items: [
            "To provide and improve core features including AI chat, diary, partner messaging, voice, and notifications.",
            "To maintain security, prevent abuse, and ensure reliable delivery of messages and notifications.",
            "To personalize your experience, such as displaying your and your partner's avatars and names."
        ]
    ),
    PolicySection(
        title: "Third-Party Services",
        items: [
            "Supabase: Authentication, database, and file storage.",
            "Deepgram: Real-time speech-to-text transcription for voice input.",
            "ElevenLabs: Text-to-speech audio generation for voice playback.",
            "Apple Push Notification service (APNs): Delivering push notifications to your device.",
            "Each provider only receives data necessary to perform its service. We do not sell your personal information."
        ]
    ),
    PolicySection(
        title: "Data Retention",
        items: [
            "We retain your profile, chat messages, diary entries, and linking data for as long as your account is active.",
            "You can delete individual chat sessions and diary entries from within the app.",
            "You can request full account and data deletion by contacting support."
        ]
    ),
    PolicySection(
        title: "Your Choices",
        items: [
            "Notifications: Enable or disable push notifications in Settings at any time.",
            "Partner Linking: Link or unlink a partner. Unlinking stops future partner message deliveries.",
            "Voice: Enable or disable voice input and voice playback independently in Settings.",
            "Profile: Update your display name, bio, and avatar at any time.",
            "Appearance: Choose between light, dark, or system theme."
        ]
    ),
    PolicySection(
        title: "Security",
        items: [
            "We use HTTPS/TLS for all data in transit, signed URLs for secure file access, and Keychain for credential storage. We continually work to protect your information, though no method is completely infallible."
        ]
    ),
    PolicySection(
        title: "Children's Privacy",
        items: [
            "TalkToMe is not intended for children under 13. If you believe a child has provided us personal information, please contact us so we can take appropriate action."
        ]
    ),
    PolicySection(
        title: "Changes to This Policy",
        items: [
            "We may update this Policy to reflect changes to our practices. Continued use of the app after updates means you accept the revised Policy."
        ]
    ),
    PolicySection(
        title: "Contact Us",
        items: [
            "For privacy questions or requests (including data deletion), contact: sgzrov@gmail.com, muhammad84044@gmail.com"
        ]
    )
]

struct PrivacyPolicyView: View {
    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 0) {
                Text("Effective: February 15, 2026")
                    .font(.system(size: 13))
                    .foregroundStyle(.secondary)
                    .padding(.horizontal, 20)
                    .padding(.top, 8)
                    .padding(.bottom, 12)

                Text("This Privacy Policy explains how TalkToMe collects, uses, and shares information when you use the TalkToMe iOS application. By using the app, you agree to this Policy.")
                    .font(.system(size: 15))
                    .foregroundStyle(.secondary)
                    .padding(.horizontal, 20)
                    .padding(.bottom, 24)

                ForEach(Array(policySections.enumerated()), id: \.element.id) { index, section in
                    PolicySectionCard(index: index + 1, section: section)
                        .padding(.bottom, 20)
                }
            }
            .padding(.horizontal, 16)
            .padding(.bottom, 24)
        }
        .scrollIndicators(.hidden)
        .navigationTitle("Privacy Policy")
        .navigationBarTitleDisplayMode(.inline)
        .background(Color(.systemGroupedBackground))
    }
}

private struct PolicySectionCard: View {
    let index: Int
    let section: PolicySection

    var body: some View {
        VStack(alignment: .leading, spacing: 6) {
            Text("\(index). \(section.title)")
                .font(.system(size: 13))
                .foregroundStyle(.secondary)
                .textCase(.uppercase)
                .padding(.horizontal, 20)
                .padding(.bottom, 2)

            VStack(spacing: 0) {
                ForEach(Array(section.items.enumerated()), id: \.offset) { itemIndex, item in
                    policyRow(item)

                    if itemIndex < section.items.count - 1 {
                        Divider()
                            .padding(.leading, 60)
                    }
                }
            }
            .padding(.vertical, 8)
            .frame(maxWidth: .infinity, alignment: .leading)
            .background(Color(.secondarySystemGroupedBackground))
            .clipShape(RoundedRectangle(cornerRadius: 16, style: .continuous))
        }
    }

    private func policyRow(_ text: String) -> some View {
        HStack(alignment: .top, spacing: 14) {
            Image(systemName: "circle.fill")
                .font(.system(size: 5))
                .foregroundStyle(Color(.tertiaryLabel))
                .frame(width: 30)
                .padding(.top, 8)

            styledText(text)
                .font(.system(size: 15))
                .lineSpacing(4)
                .fixedSize(horizontal: false, vertical: true)
                .frame(maxWidth: .infinity, alignment: .leading)
        }
        .padding(.horizontal, 16)
        .padding(.vertical, 10)
    }

    private func styledText(_ text: String) -> Text {
        guard let colonRange = text.range(of: ": ") else {
            return Text(text)
                .foregroundColor(Color(.label))
        }
        let label = String(text[text.startIndex..<colonRange.lowerBound])
        let rest = String(text[colonRange.upperBound...])
        return Text(label + ": ")
            .fontWeight(.semibold)
            .foregroundColor(Color(.label))
        + Text(rest)
            .foregroundColor(Color(.secondaryLabel))
    }
}

#Preview {
    NavigationStack {
        PrivacyPolicyView()
    }
}
