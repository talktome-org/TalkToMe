import SwiftUI

struct PrivacyPolicyView: View {
    private let policyText: String = """
    Privacy Policy
    Effective: October 14, 2025

    This Privacy Policy explains how TalkToMe ("we", "us", or "our") collects, uses, and shares information when you use the TalkToMe iOS application (the "App"). By using the App, you agree to this Policy.

    1) Information We Collect
    - Account and Authentication: We use Supabase for authentication (Apple/Google sign-in). We process your Supabase user ID, basic profile metadata provided by your identity provider (e.g., name, picture), and session tokens.
    - Profile: If you choose, you can set a display name (full name) and a short bio. We store these in our database.
    - Avatars: If you upload an avatar, the image is stored in Supabase Storage and referenced by a signed URL. We may also use provider profile images if available.
    - Chats and Sessions: When you chat, your messages and session information (including optional generated titles and the most recent message preview) are stored so you can view history and continue conversations.
    - Partner Linking: If you link with a partner, we store relationship mappings and, when you send partner drafts/requests, their content and status (pending/delivered/accepted) to deliver messages and maintain history for both sides.
    - Notifications: If you enable push notifications, we store your device push token, platform (iOS), and app bundle identifier to send notifications (e.g., partner messages/requests). You can disable push at any time in Settings.
    - Diagnostics and App Events: We may log limited technical information (e.g., request status codes, streaming state, minimal debug logs) to operate the service and troubleshoot issues. These logs are not used for advertising.
    - Voice and Speech: If you use voice features, microphone and speech recognition permissions are requested. Audio is recorded locally to enable live transcription; we do not upload audio for transcription. Transcribed text only becomes part of your chat if you send it.

    2) How We Use Information
    - To provide and improve core features (chat, session history, partner linking, notifications, profile/avatars).
    - To maintain security, prevent abuse, and ensure reliable delivery of messages and notifications.
    - To personalize your experience (e.g., showing your and your partner’s avatars and names).

    3) Sharing and Disclosure
    - Service Providers: We use trusted service providers to operate the App, including Supabase (authentication, database, storage) and Apple Push Notification service (APNs). Providers only receive data necessary to perform their services.
    - Legal and Safety: We may disclose information if required by law or to protect rights, safety, and the integrity of the service.
    - We do not sell your personal information.

    4) Data Retention
    - We retain profile information, chat messages, sessions, and linking data for as long as your account is active or as needed to provide the service.
    - You can delete individual chat sessions from within the App. You can also request account/data deletion by contacting support.

    5) Your Choices
    - Notifications: Enable or disable push notifications in Settings at any time.
    - Partner Linking: You may link/unlink a partner. Unlinking stops new partner deliveries and removes the link mapping going forward.
    - Profile: You can update your display name/bio and avatar.

    6) Security
    - We use industry-standard protections such as HTTPS/TLS in transit and signed URLs for avatar access. No security method is perfect, but we continually work to protect your information.

    7) Children’s Privacy
    - The App is not intended for children under 13. If you believe a child has provided us personal information, please contact us so we can take appropriate action.

    8) Changes to This Policy
    - We may update this Policy to reflect changes to our practices. Continued use of the App after updates means you accept the revised Policy.

    9) Contact Us
    - For privacy questions or requests (including deletion), contact: sgzrov@gmail.com, muhammad84044@gmail.com
    """

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 16) {
                VStack(alignment: .leading, spacing: 12) {
                    Text("Privacy Policy")
                        .font(.system(size: 20, weight: .semibold))
                        .foregroundColor(.primary)

                    Text(policyText)
                        .font(.system(size: 16, weight: .regular))
                        .foregroundColor(.primary)
                        .multilineTextAlignment(.leading)
                }
                .padding(16)
                .background(
                    RoundedRectangle(cornerRadius: 12)
                        .fill(Color(.systemBackground))
                        .overlay(
                            RoundedRectangle(cornerRadius: 12)
                                .stroke(Color.black.opacity(0.1), lineWidth: 1)
                        )
                )
            }
            .padding(20)
        }
        .scrollIndicators(.hidden)
        .background(Color(.systemGroupedBackground).ignoresSafeArea())
    }
}

#Preview {
    PrivacyPolicyView()
}


