import SwiftUI
import UIKit

struct OnboardingFlowView: View {
  @ObservedObject var viewModel: OnboardingViewModel
  @AppStorage(PreferenceKeys.elevenLabsVoiceName) private var selectedVoiceName: String = ""

  private enum Screen: Int, CaseIterable, Identifiable {
    case name = 0
    case gender = 1
    case dob = 2
    case topics = 3

    var id: Int { rawValue }
    var index: Int { rawValue }
    static var count: Int { allCases.count }

    var shortTitle: String {
      switch self {
      case .name: return "Name"
      case .gender: return "Identity"
      case .dob: return "Birthday"
      case .topics: return "Topics"
      }
    }

    var iconName: String {
      switch self {
      case .name: return "person.fill"
      case .gender: return "person.2.fill"
      case .dob: return "calendar"
      case .topics: return "bubble.left.and.text.bubble.right.fill"
      }
    }

    var title: String {
      switch self {
      case .name: return "What should we call you?"
      case .gender: return "How do you describe your gender?"
      case .dob: return "What is your date of birth?"
      case .topics: return "What kinds of relationships do you want to talk about here?"
      }
    }

    var subtitle: String {
      switch self {
      case .name:
        return "A name makes your space feel personal. You can always change it later."
      case .gender:
        return "Optional. Share only what feels right to you."
      case .dob:
        return "We use this to tune tone and context. You can skip this step."
      case .topics:
        return "Pick one or more to shape suggestions from your buddy."
      }
    }
  }

  @State private var screen: Screen = .name
  @State private var localName: String = ""
  @State private var localGender: String = ""
  @State private var localDob: Date =
    Calendar.current.date(byAdding: .year, value: -25, to: Date()) ?? Date()
  @State private var dobProvided: Bool = false
  @State private var localTopics: Set<String> = []

  @State private var isSaving: Bool = false
  @State private var gentleHint: String? = nil
  @State private var isMovingBack: Bool = false
  @State private var showSplash: Bool = false
  @State private var splashOpacity: Double = 0
  @State private var isKeyboardVisible: Bool = false

  @FocusState private var nameFieldFocused: Bool

  private let topicColumns: [GridItem] = [
    GridItem(.flexible(), spacing: 10),
    GridItem(.flexible(), spacing: 10),
  ]
  private let dobPickerHeight: CGFloat = 352

  var body: some View {
    ZStack {
      background

      VStack(spacing: 0) {
        topBar

        ScrollView(showsIndicators: false) {
          VStack(spacing: 14) {
            mascotHero
            contentCard
          }
          .padding(.horizontal, 16)
          .padding(.top, 10)
          .padding(.bottom, shouldCompactForKeyboard ? 16 : 120)
        }
        .scrollDismissesKeyboard(.interactively)
      }
      .blur(radius: showSplash ? 10 : 0)
      .animation(.easeInOut(duration: 0.5), value: showSplash)

      if showSplash {
        completionSplash
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
    .safeAreaInset(edge: .bottom, spacing: 0) {
      bottomBar
    }
    .onReceive(NotificationCenter.default.publisher(for: UIResponder.keyboardWillChangeFrameNotification)) { note in
      guard
        let frame = note.userInfo?[UIResponder.keyboardFrameEndUserInfoKey] as? CGRect
      else { return }
      let keyboardHeight = max(0, UIScreen.main.bounds.height - frame.origin.y)
      withAnimation(.easeOut(duration: 0.22)) {
        isKeyboardVisible = keyboardHeight > 0
      }
    }
    .onReceive(NotificationCenter.default.publisher(for: UIResponder.keyboardWillHideNotification)) { _ in
      withAnimation(.easeOut(duration: 0.22)) {
        isKeyboardVisible = false
      }
    }
  }

  private var background: some View {
    ZStack {
      AppTheme.background.ignoresSafeArea()

      LinearGradient(
        colors: [
          AppTheme.brand.opacity(0.18),
          AppTheme.accent.opacity(0.12),
          .clear,
        ],
        startPoint: .topLeading,
        endPoint: .bottomTrailing
      )
      .ignoresSafeArea()

      Circle()
        .fill(AppTheme.brand.opacity(0.12))
        .frame(width: 280, height: 280)
        .blur(radius: 40)
        .offset(x: -140, y: -320)

      Circle()
        .fill(AppTheme.accent.opacity(0.14))
        .frame(width: 260, height: 260)
        .blur(radius: 42)
        .offset(x: 150, y: -260)
    }
  }

  private var topBar: some View {
    VStack(spacing: 12) {
      HStack(spacing: 10) {
        if screen != .name {
          topActionButton(icon: "chevron.left", label: "Back", action: goBack)
        } else {
          Color.clear.frame(width: 76, height: 36)
        }

        Spacer()

        Text("Step \(screen.index + 1) of \(Screen.count)")
          .font(.system(size: 13, weight: .semibold, design: .rounded))
          .foregroundColor(AppTheme.textSecondary)

        Spacer()

        topActionButton(icon: "forward.fill", label: "Skip") {
          Task { await skipOneStep() }
        }
        .disabled(isSaving)
      }
      .padding(.horizontal, 16)
      .padding(.top, 12)

      progressBar
        .padding(.horizontal, 16)
    }
  }

  private func topActionButton(icon: String, label: String, action: @escaping () -> Void) -> some View {
    Button(action: action) {
      HStack(spacing: 6) {
        Image(systemName: icon)
          .font(.system(size: 11, weight: .semibold))
        Text(label)
          .font(.system(size: 13, weight: .semibold))
      }
      .foregroundColor(AppTheme.textPrimary)
      .padding(.horizontal, 10)
      .frame(height: 36)
      .background(
        Capsule(style: .continuous)
          .fill(AppTheme.surface.opacity(0.82))
      )
      .overlay(
        Capsule(style: .continuous)
          .stroke(Color.primary.opacity(0.08), lineWidth: 1)
      )
      .contentShape(Capsule())
    }
    .buttonStyle(.plain)
  }

  private var progressBar: some View {
    GeometryReader { geo in
      let total = max(1, Screen.count)
      let dotInset: CGFloat = 14
      let badgeSize: CGFloat = 30
      let intervalCount = max(1, total - 1)
      let trackWidth = max(0, geo.size.width - (dotInset * 2))
      let progress = CGFloat(screen.index) / CGFloat(intervalCount)
      let indicatorCenterX = dotInset + (trackWidth * progress)
      let fillStartX: CGFloat = 0
      let fillWidth = screen == .name ? 0 : max(0, indicatorCenterX - fillStartX)
      let badgeOffsetX = min(
        max(0, indicatorCenterX - (badgeSize / 2)),
        max(0, geo.size.width - badgeSize)
      )

      ZStack(alignment: .leading) {
        RoundedRectangle(cornerRadius: 17, style: .continuous)
          .fill(AppTheme.surface.opacity(0.78))

        RoundedRectangle(cornerRadius: 17, style: .continuous)
          .fill(
            LinearGradient(
              colors: [AppTheme.brand.opacity(0.95), AppTheme.accent.opacity(0.92)],
              startPoint: .leading,
              endPoint: .trailing
            )
          )
          .frame(width: fillWidth)
          .offset(x: fillStartX)

        HStack(spacing: 0) {
          ForEach(Screen.allCases) { item in
            HStack(spacing: 0) {
              Circle()
                .fill(item.index <= screen.index ? Color.white : Color.white.opacity(0.38))
                .frame(width: 8, height: 8)
              if item.index < Screen.count - 1 {
                Spacer(minLength: 0)
              }
            }
          }
        }
        .padding(.horizontal, 14)
      }
      .frame(height: 34)
      .overlay(
        RoundedRectangle(cornerRadius: 17, style: .continuous)
          .stroke(Color.white.opacity(0.16), lineWidth: 0.8)
      )
      .overlay(alignment: .leading) {
        progressMascotBadge
          .offset(x: badgeOffsetX)
      }
    }
    .frame(height: 34)
    .animation(.spring(response: 0.5, dampingFraction: 0.82), value: screen)
  }

  private var progressMascotBadge: some View {
    ZStack {
      Circle()
        .fill(.white)
      mascotImage
        .padding(4)
    }
    .frame(width: 30, height: 30)
    .shadow(color: AppTheme.accent.opacity(0.28), radius: 7, x: 0, y: 3)
  }

  private var mascotHero: some View {
    HStack(spacing: 14) {
      mascotAvatar(size: 92)
        .frame(width: 92, height: 92)
        .background(
          RoundedRectangle(cornerRadius: 22, style: .continuous)
            .fill(AppTheme.surface.opacity(0.9))
        )
        .overlay(
          RoundedRectangle(cornerRadius: 22, style: .continuous)
            .stroke(Color.primary.opacity(0.08), lineWidth: 1)
        )

      VStack(alignment: .leading, spacing: 6) {
        Text("Meet \(buddyDisplayName)")
          .font(.system(size: 22, weight: .bold))
          .foregroundColor(AppTheme.textPrimary)
          .lineLimit(1)

        Text(heroSubtitle)
          .font(.system(size: 13, weight: .medium))
          .foregroundColor(AppTheme.textSecondary)
          .lineLimit(2)
          .frame(maxWidth: .infinity, minHeight: 34, alignment: .topLeading)

        HStack(spacing: 7) {
          Label(screen.shortTitle, systemImage: screen.iconName)
            .font(.system(size: 11, weight: .semibold))
            .padding(.horizontal, 10)
            .padding(.vertical, 6)
            .background(AppTheme.surface.opacity(0.9), in: Capsule())

          if let description = buddyDescription, !description.isEmpty {
            Text(description)
              .font(.system(size: 11, weight: .medium))
              .foregroundColor(AppTheme.textSecondary)
              .lineLimit(1)
          }
        }
      }

      Spacer(minLength: 0)
    }
    .padding(14)
    .background(
      RoundedRectangle(cornerRadius: 28, style: .continuous)
        .fill(AppTheme.surface.opacity(0.78))
        .overlay(
          LinearGradient(
            colors: [
              AppTheme.brand.opacity(0.18),
              AppTheme.accent.opacity(0.12),
              .clear,
            ],
            startPoint: .topLeading,
            endPoint: .bottomTrailing
          )
          .clipShape(RoundedRectangle(cornerRadius: 28, style: .continuous))
        )
    )
    .overlay(
      RoundedRectangle(cornerRadius: 28, style: .continuous)
        .stroke(Color.primary.opacity(0.08), lineWidth: 1)
    )
  }

  @ViewBuilder
  private func mascotAvatar(size: CGFloat) -> some View {
    if let ghostVideoName, hasGhostVideo {
      TransparentVideoPlayerView(
        videoName: ghostVideoName,
        videoExtension: "mp4",
        startTime: ElevenLabsVoiceSuggestionsView.ghostStartTimes[ghostVideoName] ?? 0
      )
      .frame(width: size, height: size)
      .clipShape(RoundedRectangle(cornerRadius: size * 0.24, style: .continuous))
    } else {
      mascotImage
        .padding(10)
        .frame(width: size, height: size)
        .background(
          RoundedRectangle(cornerRadius: size * 0.24, style: .continuous)
            .fill(Color(.tertiarySystemFill))
        )
    }
  }

  @ViewBuilder
  private var mascotImage: some View {
    if let uiImage = ElevenLabsVoiceSuggestionsView.ghostUIImage(for: selectedVoiceName) {
      Image(uiImage: uiImage)
        .resizable()
        .scaledToFit()
    } else {
      Image(systemName: "sparkles")
        .font(.system(size: 20, weight: .semibold))
        .foregroundColor(.secondary)
    }
  }

  private var contentCard: some View {
    VStack(alignment: .leading, spacing: 18) {
      stepHeader

      Group {
        switch screen {
        case .name:
          nameInput
        case .gender:
          genderOptions
        case .dob:
          dobPicker
        case .topics:
          topicsMultiSelect
        }
      }

      if let hint = gentleHint, !hint.isEmpty {
        Text(hint)
          .font(.system(size: 13, weight: .medium))
          .foregroundColor(AppTheme.textSecondary)
          .padding(.horizontal, 12)
          .padding(.vertical, 10)
          .background(
            RoundedRectangle(cornerRadius: 12, style: .continuous)
              .fill(AppTheme.surface.opacity(0.65))
          )
          .transition(.opacity)
      }
    }
    .padding(18)
    .background(
      RoundedRectangle(cornerRadius: 28, style: .continuous)
        .fill(AppTheme.surface.opacity(0.88))
    )
    .overlay(
      RoundedRectangle(cornerRadius: 28, style: .continuous)
        .stroke(Color.primary.opacity(0.08), lineWidth: 1)
    )
    .id(screen)
    .transition(
      .asymmetric(
        insertion: .move(edge: isMovingBack ? .leading : .trailing).combined(with: .opacity),
        removal: .move(edge: isMovingBack ? .trailing : .leading).combined(with: .opacity)
      )
    )
    .animation(.spring(response: 0.45, dampingFraction: 0.82), value: screen)
  }

  private var stepHeader: some View {
    VStack(alignment: .leading, spacing: 8) {
      Text(screen.title)
        .font(.system(size: 29, weight: .bold))
        .foregroundColor(AppTheme.textPrimary)
        .kerning(-0.35)
        .fixedSize(horizontal: false, vertical: true)

      Text(screen.subtitle)
        .font(.system(size: 15, weight: .regular))
        .foregroundColor(AppTheme.textSecondary)
        .lineLimit(2)
        .frame(maxWidth: .infinity, minHeight: 40, alignment: .topLeading)
    }
  }

  private var nameInput: some View {
    HStack(spacing: 10) {
      Image(systemName: "person.fill")
        .font(.system(size: 16, weight: .semibold))
        .foregroundColor(AppTheme.textSecondary)
        .frame(width: 22)

      TextField("Your name", text: $localName)
        .textInputAutocapitalization(.words)
        .autocorrectionDisabled(true)
        .textFieldStyle(.plain)
        .submitLabel(.continue)
        .focused($nameFieldFocused)
        .onSubmit { Task { await goNext() } }
    }
    .padding(.horizontal, 14)
    .padding(.vertical, 14)
    .background(
      RoundedRectangle(cornerRadius: 18, style: .continuous)
        .fill(AppTheme.surface)
        .overlay(
          RoundedRectangle(cornerRadius: 18, style: .continuous)
            .stroke(nameFieldFocused ? AppTheme.accent.opacity(0.55) : Color.primary.opacity(0.08), lineWidth: 1)
        )
    )
  }

  private var genderOptions: some View {
    VStack(spacing: 10) {
      ForEach(genderChoices, id: \.self) { option in
        selectableRow(
          title: option,
          systemImage: genderIcon(for: option),
          isSelected: normalizedGender(localGender) == normalizedGender(option)
        ) {
          withAnimation(.spring(response: 0.3, dampingFraction: 0.8)) {
            localGender = option
          }
        }
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
      .frame(maxWidth: .infinity, minHeight: dobPickerHeight, maxHeight: dobPickerHeight, alignment: .top)
      .labelsHidden()
      .transaction { transaction in
        transaction.animation = nil
      }
      .onChange(of: localDob) { _, _ in
        dobProvided = true
      }
      .padding(12)
      .background(
        RoundedRectangle(cornerRadius: 18, style: .continuous)
          .fill(AppTheme.surface)
          .overlay(
            RoundedRectangle(cornerRadius: 18, style: .continuous)
              .stroke(Color.primary.opacity(0.08), lineWidth: 1)
          )
      )

      Text("You can adjust this later in Settings.")
        .font(.system(size: 13, weight: .medium))
        .foregroundColor(AppTheme.textSecondary)
    }
  }

  private var topicsMultiSelect: some View {
    LazyVGrid(columns: topicColumns, spacing: 10) {
      ForEach(topicChoices, id: \.self) { option in
        topicCard(title: option, isSelected: localTopics.contains(option)) {
          withAnimation(.spring(response: 0.3, dampingFraction: 0.8)) {
            if localTopics.contains(option) {
              localTopics.remove(option)
            } else {
              localTopics.insert(option)
            }
          }
        }
      }
    }
  }

  private func selectableRow(title: String, systemImage: String, isSelected: Bool, action: @escaping () -> Void)
    -> some View
  {
    Button(action: action) {
      HStack(spacing: 12) {
        Image(systemName: systemImage)
          .font(.system(size: 16, weight: .semibold))
          .foregroundColor(isSelected ? .white : AppTheme.textSecondary)
          .frame(width: 24)

        Text(title)
          .font(.system(size: 16, weight: .semibold))
          .foregroundColor(isSelected ? .white : AppTheme.textPrimary)

        Spacer()

        Image(systemName: isSelected ? "checkmark.circle.fill" : "circle")
          .font(.system(size: 18, weight: .semibold))
          .foregroundColor(isSelected ? .white : Color.primary.opacity(0.22))
      }
      .padding(.horizontal, 14)
      .padding(.vertical, 14)
      .background(
        RoundedRectangle(cornerRadius: 18, style: .continuous)
          .fill(
            isSelected
              ? AnyShapeStyle(
                LinearGradient(
                  colors: [AppTheme.brand.opacity(0.95), AppTheme.accent.opacity(0.92)],
                  startPoint: .leading,
                  endPoint: .trailing
                ))
              : AnyShapeStyle(AppTheme.surface)
          )
          .overlay(
            RoundedRectangle(cornerRadius: 18, style: .continuous)
              .stroke(isSelected ? Color.white.opacity(0.22) : Color.primary.opacity(0.08), lineWidth: 1)
          )
      )
      .contentShape(RoundedRectangle(cornerRadius: 18, style: .continuous))
    }
    .buttonStyle(.plain)
  }

  private func topicCard(title: String, isSelected: Bool, action: @escaping () -> Void) -> some View {
    Button(action: action) {
      HStack(spacing: 8) {
        Image(systemName: topicIcon(for: title))
          .font(.system(size: 14, weight: .semibold))
          .foregroundColor(isSelected ? .white : AppTheme.textSecondary)

        Text(title)
          .font(.system(size: 13, weight: .semibold))
          .foregroundColor(isSelected ? .white : AppTheme.textPrimary)
          .lineLimit(2)
          .frame(maxWidth: .infinity, alignment: .leading)
      }
      .padding(.horizontal, 12)
      .padding(.vertical, 12)
      .frame(minHeight: 62)
      .background(
        RoundedRectangle(cornerRadius: 16, style: .continuous)
          .fill(
            isSelected
              ? AnyShapeStyle(
                LinearGradient(
                  colors: [AppTheme.brand.opacity(0.95), AppTheme.accent.opacity(0.92)],
                  startPoint: .topLeading,
                  endPoint: .bottomTrailing
                ))
              : AnyShapeStyle(AppTheme.surface)
          )
          .overlay(
            RoundedRectangle(cornerRadius: 16, style: .continuous)
              .stroke(isSelected ? Color.white.opacity(0.2) : Color.primary.opacity(0.08), lineWidth: 1)
          )
      )
      .contentShape(RoundedRectangle(cornerRadius: 16, style: .continuous))
    }
    .buttonStyle(.plain)
  }

  private var bottomBar: some View {
    VStack(spacing: 10) {
      Button(action: { Task { await goNext() } }) {
        HStack(spacing: 10) {
          Text(primaryCTA)
            .font(.system(size: 17, weight: .semibold))

          Spacer(minLength: 0)

          if isSaving {
            ProgressView()
              .tint(.white)
          } else {
            Image(systemName: screen == .topics ? "checkmark" : "arrow.right")
              .font(.system(size: 14, weight: .bold))
          }
        }
        .padding(.horizontal, 18)
        .frame(height: 56)
        .background(
          RoundedRectangle(cornerRadius: 20, style: .continuous)
            .fill(
              LinearGradient(
                colors: [AppTheme.brand.opacity(0.95), AppTheme.accent.opacity(0.95)],
                startPoint: .leading,
                endPoint: .trailing
              )
            )
        )
        .foregroundColor(.white)
        .contentShape(RoundedRectangle(cornerRadius: 20, style: .continuous))
      }
      .buttonStyle(.plain)
      .disabled(isSaving)

      if !shouldCompactForKeyboard {
        Text("You stay in control. Everything here can be changed later in Settings.")
          .font(.system(size: 12, weight: .medium))
          .foregroundColor(AppTheme.textSecondary)
          .multilineTextAlignment(.center)
      }
    }
    .padding(.horizontal, 16)
    .padding(.top, 10)
    .padding(.bottom, shouldCompactForKeyboard ? 8 : 14)
  }

  private var completionSplash: some View {
    ZStack {
      AppTheme.background
        .ignoresSafeArea()
        .opacity(splashOpacity)

      VStack(spacing: 16) {
        ZStack {
          Circle()
            .fill(
              LinearGradient(
                colors: [AppTheme.brand.opacity(0.9), AppTheme.accent.opacity(0.86)],
                startPoint: .topLeading,
                endPoint: .bottomTrailing
              )
            )
            .frame(width: 148, height: 148)

          mascotAvatar(size: 112)
            .clipShape(RoundedRectangle(cornerRadius: 26, style: .continuous))
        }

        Text("You are all set")
          .font(.system(size: 30, weight: .bold))
          .foregroundColor(AppTheme.textPrimary)

        Text("\(buddyDisplayName) is ready whenever you are.")
          .font(.system(size: 15, weight: .medium))
          .foregroundColor(AppTheme.textSecondary)
      }
      .opacity(splashOpacity)
      .scaleEffect(splashOpacity > 0.5 ? 1.0 : 0.9)
    }
  }

  private var primaryCTA: String {
    switch screen {
    case .name: return "Continue"
    case .gender: return "Looks good"
    case .dob: return "Next"
    case .topics: return "Finish setup"
    }
  }

  private var heroSubtitle: String {
    switch screen {
    case .name:
      return "Your little ghost guide helps you personalize this space in under a minute."
    case .gender:
      return "Optional details help your buddy respond in a way that feels right."
    case .dob:
      return "Birthday context helps tune responses, but this step is always optional."
    case .topics:
      return "Choose your conversation themes and jump straight into your first chat."
    }
  }

  private var buddyDisplayName: String {
    let trimmed = selectedVoiceName.trimmingCharacters(in: .whitespacesAndNewlines)
    guard !trimmed.isEmpty else { return "Buddy" }
    return trimmed.prefix(1).uppercased() + trimmed.dropFirst().lowercased()
  }

  private var buddyDescription: String? {
    ElevenLabsVoiceSuggestionsView.ghostDescription(for: selectedVoiceName)
  }

  private var ghostVideoName: String? {
    let mapped = ElevenLabsVoiceSuggestionsView.ghostVideoName(for: selectedVoiceName)
    let fallback = selectedVoiceName.lowercased().trimmingCharacters(in: .whitespacesAndNewlines)
    let resolved = (mapped ?? fallback).trimmingCharacters(in: .whitespacesAndNewlines)
    return resolved.isEmpty ? nil : resolved
  }

  private var hasGhostVideo: Bool {
    guard let ghostVideoName else { return false }
    return Bundle.main.url(forResource: ghostVideoName, withExtension: "mp4") != nil
  }

  private func goBack() {
    guard let prev = Screen(rawValue: max(0, screen.rawValue - 1)) else { return }
    gentleHint = nil
    isMovingBack = true
    withAnimation(.spring(response: 0.45, dampingFraction: 0.82)) {
      screen = prev
    }
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
          gentleHint = "If you would rather not share, you can tap Skip."
        }
        return
      }
    }

    await saveCurrentScreenIfNeeded()

    if screen == .topics {
      await finish(skipAllowed: false)
      return
    }

    if screen == .name {
      dismissKeyboard()
      isKeyboardVisible = false
      nameFieldFocused = false
    }

    guard let next = Screen(rawValue: min(Screen.count - 1, screen.rawValue + 1)) else { return }
    isMovingBack = false
    withAnimation(.spring(response: 0.45, dampingFraction: 0.82)) {
      screen = next
    }

    if next == .name {
      DispatchQueue.main.asyncAfter(deadline: .now() + 0.1) { nameFieldFocused = true }
    } else {
      nameFieldFocused = false
    }
  }

  private var shouldCompactForKeyboard: Bool {
    screen == .name && isKeyboardVisible
  }

  private func dismissKeyboard() {
    UIApplication.shared.sendAction(#selector(UIResponder.resignFirstResponder), to: nil, from: nil, for: nil)
  }

  private func skipOneStep() async {
    gentleHint = nil
    if screen == .dob {
      dobProvided = false
    }
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
    do {
      try await viewModel.persistPartial()
    } catch {
      // Keep progression smooth and non-blocking for partial saves.
    }
  }

  private func finish(skipAllowed: Bool) async {
    if !skipAllowed, screen == .name {
      let trimmed = localName.trimmingCharacters(in: .whitespacesAndNewlines)
      if trimmed.isEmpty {
        withAnimation(.easeInOut(duration: 0.2)) {
          gentleHint = "If you would rather not share, you can tap Skip."
        }
        return
      }
    }

    await saveCurrentScreenIfNeeded()

    isSaving = true
    defer { isSaving = false }

    withAnimation(.easeIn(duration: 0.5)) {
      showSplash = true
      splashOpacity = 1.0
    }

    try? await Task.sleep(nanoseconds: 1_300_000_000)

    withAnimation(.easeOut(duration: 0.45)) {
      splashOpacity = 0.0
    }

    try? await Task.sleep(nanoseconds: 350_000_000)

    do {
      try await viewModel.complete()
    } catch {
      withAnimation { showSplash = false }
      withAnimation(.easeInOut(duration: 0.2)) {
        gentleHint = "We could not save your setup yet. Please try again."
      }
    }
  }

  private func normalizedGender(_ value: String) -> String {
    value.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
  }

  private func genderIcon(for option: String) -> String {
    switch option {
    case "Woman": return "person.crop.circle.fill"
    case "Man": return "person.crop.circle"
    case "Non-binary": return "equal.circle"
    case "Trans": return "arrow.left.and.right.circle"
    default: return "eye.slash.circle"
    }
  }

  private func topicIcon(for topic: String) -> String {
    switch topic {
    case "Personal / Romantic": return "heart.fill"
    case "Work": return "briefcase.fill"
    case "Family": return "house.fill"
    case "Friendships": return "person.2.fill"
    default: return "sparkles"
    }
  }

  private var genderChoices: [String] {
    ["Woman", "Man", "Non-binary", "Trans", "Prefer not to say"]
  }

  private var topicChoices: [String] {
    ["Personal / Romantic", "Work", "Family", "Friendships", "Other"]
  }
}
