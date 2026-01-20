import SwiftUI

struct SettingsCardView: View {

    @EnvironmentObject private var friendsVM: FriendsViewModel
    @AppStorage(PreferenceKeys.appearancePreference) private var appearance: String = "System"

    let section: SettingsSection
    let onToggle: (Int) -> Void
    let onAction: (Int) -> Void
    let onPickerSelect: ((Int, String) -> Void)?
    let headerAccessory: AnyView?

    init(
        section: SettingsSection,
        onToggle: @escaping (Int) -> Void,
        onAction: @escaping (Int) -> Void,
        onPickerSelect: ((Int, String) -> Void)? = nil,
        headerAccessory: AnyView? = nil
    ) {
        self.section = section
        self.onToggle = onToggle
        self.onAction = onAction
        self.onPickerSelect = onPickerSelect
        self.headerAccessory = headerAccessory
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 6) {
            HStack {
                Text(section.title)
                    .font(.system(size: 13, weight: .regular))
                    .foregroundColor(.secondary)
                    .textCase(.uppercase)

                Spacer()

                if let headerAccessory {
                    headerAccessory
                }
            }
            .padding(.horizontal, 20)
            .padding(.bottom, 2)

            VStack(spacing: 0) {
                ForEach(Array(section.settings.enumerated()), id: \.offset) { index, setting in
                    Group {
                        switch setting.type {
                        case .friendsCode:
                            FriendCodeInlineRow()

                        case .picker(let options):
                            HStack(spacing: 12) {
                                Image(systemName: setting.icon)
                                    .font(.system(size: 16, weight: .medium))
                                    .foregroundColor(.primary)
                                    .frame(width: 24, height: 24)

                                VStack(alignment: .leading, spacing: 2) {
                                    Text(setting.title)
                                        .font(.system(size: 16, weight: .regular))
                                        .foregroundColor(.primary)
                                        .multilineTextAlignment(.leading)
                                }

                                Spacer()

                                // Segmented control for appearance selection
                                Picker("", selection: Binding(
                                    get: { appearance },
                                    set: { newValue in
                                        appearance = newValue
                                        onPickerSelect?(index, newValue)
                                    }
                                )) {
                                    ForEach(options, id: \.self) { option in
                                        Text(option).tag(option)
                                    }
                                }
                                .pickerStyle(.segmented)
                                .frame(maxWidth: 220)
                                .labelsHidden()
                            }
                            .padding(.horizontal, 16)
                            .padding(.vertical, 10)
                            .background(Color(.systemBackground))

                        case .toggle:
                            HStack(spacing: 12) {
                                Image(systemName: setting.icon)
                                    .font(.system(size: 16, weight: .medium))
                                    .foregroundColor(.primary)
                                    .frame(width: 24, height: 24)

                                VStack(alignment: .leading, spacing: 2) {
                                    Text(setting.title)
                                        .font(.system(size: 16, weight: .regular))
                                        .foregroundColor(.primary)
                                        .multilineTextAlignment(.leading)
                                    if let subtitle = setting.subtitle {
                                        Text(subtitle)
                                            .font(.system(size: 13, weight: .regular))
                                            .foregroundColor(.secondary)
                                            .multilineTextAlignment(.leading)
                                    }
                                }

                                Spacer()
                                Toggle("", isOn: Binding(
                                    get: {
                                        if case .toggle(let isOn) = setting.type { return isOn }
                                        return false
                                    },
                                    set: { _ in onToggle(index) }
                                ))
                                .labelsHidden()
                                .tint(.green)
                                .allowsHitTesting(true)
                            }
                            .padding(.horizontal, 16)
                            .padding(.vertical, 10)
                            .background(Color(.systemBackground))

                        case .navigation:
                            NavigationLink(destination: viewForTitle(setting.title)) {
                                HStack(spacing: 12) {
                                    Image(systemName: setting.icon)
                                        .font(.system(size: 16, weight: .medium))
                                        .foregroundColor(.primary)
                                        .frame(width: 24, height: 24)

                                    VStack(alignment: .leading, spacing: 2) {
                                        Text(setting.title)
                                            .font(.system(size: 16, weight: .regular))
                                            .foregroundColor(.primary)
                                            .multilineTextAlignment(.leading)
                                    }

                                    Spacer()
                                    Image(systemName: "chevron.right")
                                        .font(.system(size: 13, weight: .semibold))
                                        .foregroundColor(.secondary)
                                }
                                .padding(.horizontal, 16)
                                .padding(.vertical, 10)
                                .background(Color(.systemBackground))
                            }

                        case .action:
                            Button(action: { onAction(index) }) {
                                HStack(spacing: 12) {
                                    Image(systemName: setting.icon)
                                        .font(.system(size: 16, weight: .medium))
                                        .foregroundColor(.primary)
                                        .frame(width: 24, height: 24)

                                    VStack(alignment: .leading, spacing: 2) {
                                        if setting.title == "Sign Out" {
                                            Text(setting.title)
                                                .font(.system(size: 16, weight: .regular))
                                                .foregroundColor(.red)
                                                .multilineTextAlignment(.leading)
                                        } else {
                                            Text(setting.title)
                                                .font(.system(size: 16, weight: .regular))
                                                .foregroundColor(.primary)
                                                .multilineTextAlignment(.leading)
                                            if let subtitle = setting.subtitle {
                                                Text(subtitle)
                                                    .font(.system(size: 13, weight: .regular))
                                                    .foregroundColor(.secondary)
                                                    .multilineTextAlignment(.leading)
                                            }
                                        }
                                    }

                                    Spacer()
                                }
                                .padding(.horizontal, 16)
                                .padding(.vertical, 10)
                                .background(Color(.systemBackground))
                            }
                            .buttonStyle(PlainButtonStyle())
                        }
                    }
                }
            }
            .background(Color(.systemBackground))
            .clipShape(RoundedRectangle(cornerRadius: 12))

            // Section Footer (if needed)
            if shouldShowFooter() {
                HStack {
                    Text(getFooterText())
                        .font(.system(size: 13, weight: .regular))
                        .foregroundColor(.secondary)
                        .multilineTextAlignment(.leading)

                    Spacer()
                }
                .padding(.horizontal, 20)
                .padding(.top, 6)
            }
        }
    }

    private func shouldShowFooter() -> Bool {
        switch section.title {
        case "Privacy & Data":
            return true
        case "About":
            return true
        default:
            return false
        }
    }

    private func getFooterText() -> String {
        switch section.title {
        case "Privacy & Data":
            return "Clearing chat history removes your local conversation history."
        case "About":
            return "TalkToMe helps you reflect and communicate more clearly using AI."
        default:
            return ""
        }
    }
}

private struct FriendCodeInlineRow: View {
    @EnvironmentObject private var friendsVM: FriendsViewModel
    @State private var codeToAdd: String = ""

    var body: some View {
        VStack(spacing: 10) {
            HStack(spacing: 12) {
                Image(systemName: "number")
                    .font(.system(size: 16, weight: .medium))
                    .foregroundColor(.primary)
                    .frame(width: 24, height: 24)

                VStack(alignment: .leading, spacing: 2) {
                    Text("Your code")
                        .font(.system(size: 16, weight: .regular))
                        .foregroundColor(.primary)
                    Text(friendsVM.myCode ?? "— — — —")
                        .font(.system(size: 14, weight: .semibold, design: .rounded))
                        .foregroundStyle(.secondary)
                        .monospacedDigit()
                }

                Spacer()

                Button("Refresh") { Task { await friendsVM.refreshMyCode(force: true) } }
                    .font(.system(size: 13, weight: .semibold))
                    .buttonStyle(.bordered)
            }

            HStack(spacing: 10) {
                TextField("Enter 4-digit code", text: $codeToAdd)
                    .keyboardType(.numberPad)
                    .textInputAutocapitalization(.never)
                    .disableAutocorrection(true)
                    .padding(.horizontal, 12)
                    .padding(.vertical, 10)
                    .background(Color(.secondarySystemBackground))
                    .clipShape(RoundedRectangle(cornerRadius: 12, style: .continuous))

                Button("Add") {
                    let c = codeToAdd.trimmingCharacters(in: .whitespacesAndNewlines)
                    Task { await friendsVM.addFriendByCode(c) }
                }
                .buttonStyle(.borderedProminent)
                .disabled(codeToAdd.trimmingCharacters(in: .whitespacesAndNewlines).count != 4 || friendsVM.isAddingFriend)
            }

            if let msg = friendsVM.lastActionMessage, !msg.isEmpty {
                Text(msg)
                    .font(.system(size: 13))
                    .foregroundStyle(.secondary)
            }
        }
        .padding(.horizontal, 16)
        .padding(.vertical, 12)
        .background(Color(.systemBackground))
        .task { await friendsVM.refreshMyCode() }
    }
}

@ViewBuilder
private func viewForTitle(_ title: String) -> some View {
    switch title {
    case "Contact Support":
        ContactSupportView()
    case "Privacy Policy":
        PrivacyPolicyView()
    default:
        EmptyView()
    }
}

#Preview {
    SettingsCardView(
        section: SettingsSection(
            title: "App Settings",
            icon: "gear",
            gradient: [Color.blue, Color.purple],
            settings: [
                SettingItem(title: "Notifications", subtitle: "Push notifications", type: .toggle(true), icon: "bell"),
                SettingItem(title: "Dark Mode", subtitle: "Use dark appearance", type: .toggle(false), icon: "moon")
            ]
        ),
        onToggle: { _ in },
        onAction: { _ in },
        onPickerSelect: { _, _ in }
    )
    .padding(20)
    .environmentObject(FriendsViewModel(accessTokenProvider: { "" }))
}
