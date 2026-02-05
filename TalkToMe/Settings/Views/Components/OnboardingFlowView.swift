import SwiftUI

struct OnboardingFlowView: View {
    @ObservedObject var viewModel: OnboardingViewModel

    private enum Screen: Int, CaseIterable {
        case name = 0
        case gender = 1
        case dob = 2
        case topics = 3

        var index: Int { rawValue }
        static var count: Int { allCases.count }
    }

    @State private var screen: Screen = .name
    @State private var localName: String = ""
    @State private var localGender: String = ""
    @State private var localDob: Date = Calendar.current.date(byAdding: .year, value: -25, to: Date()) ?? Date()
    @State private var dobProvided: Bool = false
    @State private var localTopics: Set<String> = []

    @State private var isSaving: Bool = false
    @State private var gentleHint: String? = nil

    @FocusState private var nameFieldFocused: Bool

    var body: some View {
        ZStack {
            background
            VStack(spacing: 0) {
                topBar
                content
                bottomBar
            }
        }
        .onAppear {
            localName = viewModel.fullName
            localGender = viewModel.gender
            localTopics = viewModel.relationshipTopics
            if let dob = viewModel.dateOfBirth {
                localDob = dob
                dobProvided = true
            } else {
                dobProvided = false
            }
            Task { await viewModel.markStartedIfNeeded() }
            DispatchQueue.main.asyncAfter(deadline: .now() + 0.15) { nameFieldFocused = true }
        }
    }

    private var background: some View {
        LinearGradient(
            colors: [
                Color(UIColor.systemBackground),
                Color(UIColor.secondarySystemBackground).opacity(0.45),
                Color(UIColor.systemBackground)
            ],
            startPoint: .topLeading,
            endPoint: .bottomTrailing
        )
        .ignoresSafeArea()
    }

    private var topBar: some View {
        VStack(spacing: 10) {
            HStack {
                if screen != .name {
                    Button(action: goBack) {
                        Image(systemName: "chevron.left")
                            .font(.system(size: 16, weight: .semibold))
                            .frame(width: 40, height: 40)
                    }
                    .buttonStyle(.plain)
                } else {
                    Color.clear.frame(width: 40, height: 40)
                }

                Spacer()

                Text("Step \(screen.index + 1) of \(Screen.count)")
                    .font(.footnote.weight(.semibold))
                    .foregroundColor(.secondary)

                Spacer()

                Button(action: { Task { await skipOneStep() } }) {
                    Text("Skip")
                        .font(.footnote.weight(.semibold))
                        .frame(height: 40)
                }
                .buttonStyle(.plain)
                .foregroundColor(.secondary)
            }
            .padding(.horizontal, 20)
            .padding(.top, 14)

            if screen != .name {
                progressBar
                    .padding(.horizontal, 20)
                    .transition(.move(edge: .top).combined(with: .opacity))
            }
        }
    }

    private var progressBar: some View {
        GeometryReader { geo in
            let total = max(1, Screen.count)
            let t = CGFloat(screen.index + 1) / CGFloat(total)
            ZStack(alignment: .leading) {
                Capsule().fill(Color.primary.opacity(0.08)).frame(height: 6)
                Capsule().fill(Color.purple.opacity(0.70)).frame(width: geo.size.width * t, height: 6)
            }
        }
        .frame(height: 6)
    }

    private var content: some View {
        VStack(alignment: .leading, spacing: 18) {
            Spacer(minLength: 10)
            Group {
                switch screen {
                case .name:
                    stepHeader(
                        title: "What should we call you?",
                        subtitle: "A name makes your space feel more personal. You can always change it later."
                    )
                    nameInput
                case .gender:
                    stepHeader(
                        title: "How do you describe your gender?",
                        subtitle: "Optional — share only what feels right."
                    )
                    genderOptions
                case .dob:
                    stepHeader(
                        title: "What’s your date of birth?",
                        subtitle: "We use this to tailor tone and context. You can skip if you prefer."
                    )
                    dobPicker
                case .topics:
                    stepHeader(
                        title: "What kinds of relationships do you want to talk about here?",
                        subtitle: "Pick as many as you’d like. This helps shape suggestions."
                    )
                    topicsMultiSelect
                }
            }

            if let hint = gentleHint, !hint.isEmpty {
                Text(hint)
                    .font(.footnote)
                    .foregroundColor(Color.secondary)
                    .transition(.opacity)
                    .padding(.top, 2)
            }

            Spacer(minLength: 0)
        }
        .padding(.horizontal, 20)
        .padding(.top, 18)
        .animation(.easeInOut(duration: 0.25), value: screen)
    }

    private func stepHeader(title: String, subtitle: String) -> some View {
        VStack(alignment: .leading, spacing: 8) {
            Text(title)
                .font(.system(size: 28, weight: .semibold))
                .foregroundColor(.primary)
                .kerning(-0.2)
            Text(subtitle)
                .font(.system(size: 15, weight: .regular))
                .foregroundColor(.secondary)
                .fixedSize(horizontal: false, vertical: true)
        }
    }

    private var nameInput: some View {
        TextField("Your name", text: $localName)
            .textInputAutocapitalization(.words)
            .autocorrectionDisabled(true)
            .textFieldStyle(.plain)
            .padding(.horizontal, 14)
            .padding(.vertical, 14)
            .background(
                RoundedRectangle(cornerRadius: 16, style: .continuous)
                    .fill(Color(UIColor.secondarySystemBackground))
                    .overlay(
                        RoundedRectangle(cornerRadius: 16, style: .continuous)
                            .stroke(Color.primary.opacity(0.08), lineWidth: 1)
                    )
            )
            .submitLabel(.continue)
            .focused($nameFieldFocused)
            .onSubmit { Task { await goNext() } }
    }

    private var genderOptions: some View {
        VStack(spacing: 10) {
            ForEach(genderChoices, id: \.self) { option in
                selectableRow(
                    title: option,
                    isSelected: normalizedGender(localGender) == normalizedGender(option)
                ) {
                    if option == "Prefer to self-describe" {
                        localGender = ""
                    } else if option == "Prefer not to say" {
                        localGender = "Prefer not to say"
                    } else {
                        localGender = option
                    }
                }
            }

            if normalizedGender(localGender) == "" {
                TextField("Type what feels right", text: $localGender)
                    .textFieldStyle(.plain)
                    .padding(.horizontal, 14)
                    .padding(.vertical, 14)
                    .background(
                        RoundedRectangle(cornerRadius: 16, style: .continuous)
                            .fill(Color(UIColor.secondarySystemBackground))
                            .overlay(
                                RoundedRectangle(cornerRadius: 16, style: .continuous)
                                    .stroke(Color.primary.opacity(0.08), lineWidth: 1)
                            )
                    )
            }
        }
    }

    private var dobPicker: some View {
        VStack(alignment: .leading, spacing: 10) {
            DatePicker(
                "Date of birth",
                selection: $localDob,
                in: ...Date(),
                displayedComponents: [.date]
            )
            .datePickerStyle(.graphical)
            .labelsHidden()
            .onChange(of: localDob) { _, _ in
                dobProvided = true
            }
            .padding(12)
            .background(
                RoundedRectangle(cornerRadius: 16, style: .continuous)
                    .fill(Color(UIColor.secondarySystemBackground))
                    .overlay(
                        RoundedRectangle(cornerRadius: 16, style: .continuous)
                            .stroke(Color.primary.opacity(0.08), lineWidth: 1)
                    )
            )

            Text("You can adjust this later in settings.")
                .font(.footnote)
                .foregroundColor(.secondary)
        }
    }

    private var topicsMultiSelect: some View {
        VStack(spacing: 10) {
            ForEach(topicChoices, id: \.self) { option in
                selectableRow(title: option, isSelected: localTopics.contains(option)) {
                    if localTopics.contains(option) {
                        localTopics.remove(option)
                    } else {
                        localTopics.insert(option)
                    }
                }
            }
        }
    }

    private func selectableRow(title: String, isSelected: Bool, action: @escaping () -> Void) -> some View {
        Button(action: action) {
            HStack(spacing: 12) {
                Image(systemName: isSelected ? "checkmark.circle.fill" : "circle")
                    .font(.system(size: 18, weight: .semibold))
                    .foregroundColor(isSelected ? .purple : Color.primary.opacity(0.25))
                Text(title)
                    .font(.system(size: 16, weight: .semibold))
                    .foregroundColor(.primary)
                Spacer()
            }
            .padding(.horizontal, 14)
            .padding(.vertical, 14)
            .background(
                RoundedRectangle(cornerRadius: 16, style: .continuous)
                    .fill(Color(UIColor.secondarySystemBackground))
                    .overlay(
                        RoundedRectangle(cornerRadius: 16, style: .continuous)
                            .stroke(isSelected ? Color.purple.opacity(0.35) : Color.primary.opacity(0.08), lineWidth: 1)
                    )
            )
        }
        .buttonStyle(.plain)
    }

    private var bottomBar: some View {
        VStack(spacing: 12) {
            Button(action: { Task { await goNext() } }) {
                HStack(spacing: 10) {
                    Text(primaryCTA)
                        .font(.system(size: 16, weight: .semibold))
                    if isSaving {
                        ProgressView().tint(.white)
                    } else {
                        Image(systemName: "arrow.right")
                            .font(.system(size: 14, weight: .semibold))
                    }
                }
                .frame(maxWidth: .infinity)
                .frame(height: 54)
                .background(
                    RoundedRectangle(cornerRadius: 18, style: .continuous)
                        .fill(Color.purple)
                )
                .foregroundColor(.white)
            }
            .disabled(isSaving)

            Text("You’re in control — you can edit these anytime.")
                .font(.footnote)
                .foregroundColor(.secondary)
        }
        .padding(.horizontal, 20)
        .padding(.bottom, 22)
    }

    private var primaryCTA: String {
        switch screen {
        case .name: return "Continue"
        case .gender: return "Sounds good"
        case .dob: return "Next"
        case .topics: return "Finish setup"
        }
    }

    private func goBack() {
        guard let prev = Screen(rawValue: max(0, screen.rawValue - 1)) else { return }
        gentleHint = nil
        withAnimation(.easeInOut(duration: 0.25)) { screen = prev }
    }

    private func goNext() async {
        await goNext(skipValidation: false)
    }

    private func goNext(skipValidation: Bool) async {
        gentleHint = nil

        if !skipValidation, screen == .name {
            let trimmed = localName.trimmingCharacters(in: .whitespacesAndNewlines)
            if trimmed.isEmpty {
                withAnimation(.easeInOut(duration: 0.2)) {
                    gentleHint = "If you’d rather not share, you can tap Skip."
                }
                return
            }
        }

        // Best-effort partial save (do not block progression).
        await saveCurrentScreenIfNeeded()

        if screen == .topics {
            await finish(skipAllowed: false)
            return
        }

        guard let next = Screen(rawValue: min(Screen.count - 1, screen.rawValue + 1)) else { return }
        withAnimation(.easeInOut(duration: 0.25)) { screen = next }
        if next == .name {
            DispatchQueue.main.asyncAfter(deadline: .now() + 0.1) { nameFieldFocused = true }
        } else {
            nameFieldFocused = false
        }
    }

    private func skipOneStep() async {
        gentleHint = nil
        // For DOB, skipping should not store the default date.
        if screen == .dob {
            dobProvided = false
        }
        // On the last step, skipping "this step" means finishing without requiring selections.
        if screen == .topics {
            await finish(skipAllowed: true)
            return
        }
        await goNext(skipValidation: true)
    }

    private func saveCurrentScreenIfNeeded() async {
        await MainActor.run {
            viewModel.fullName = localName.trimmingCharacters(in: .whitespacesAndNewlines)
            viewModel.gender = localGender.trimmingCharacters(in: .whitespacesAndNewlines)
            viewModel.dateOfBirth = dobProvided ? localDob : nil
            viewModel.relationshipTopics = localTopics
        }

        isSaving = true
        defer { isSaving = false }
        do { try await viewModel.persistPartial() } catch { /* gentle: ignore */ }
    }

    private func finish(skipAllowed: Bool) async {
        if !skipAllowed, screen == .name {
            let trimmed = localName.trimmingCharacters(in: .whitespacesAndNewlines)
            if trimmed.isEmpty {
                withAnimation(.easeInOut(duration: 0.2)) {
                    gentleHint = "If you’d rather not share, you can tap Skip."
                }
                return
            }
        }

        await saveCurrentScreenIfNeeded()

        isSaving = true
        defer { isSaving = false }
        do {
            try await viewModel.complete()
        } catch {
            // Strict completion: keep onboarding open until Supabase reflects "completed".
            withAnimation(.easeInOut(duration: 0.2)) {
                gentleHint = "We couldn’t save your setup just yet. Please try again."
            }
        }
    }

    private func normalizedGender(_ value: String) -> String {
        value.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    }

    private var genderChoices: [String] {
        ["Woman", "Man", "Non-binary", "Trans", "Prefer to self-describe", "Prefer not to say"]
    }

    private var topicChoices: [String] {
        ["Personal / Romantic", "Work", "Family", "Friendships", "Other"]
    }
}

