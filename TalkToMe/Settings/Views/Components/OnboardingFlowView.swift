import SwiftUI
import UIKit

struct OnboardingFlowView: View {
    @ObservedObject var viewModel: OnboardingViewModel
    @EnvironmentObject private var linkVM: LinkViewModel

    @State private var tempName: String = ""
    @State private var tempPartner: String = ""
    @State private var lastStepIndex: Int = 0
    @FocusState private var nameFieldFocused: Bool
    @FocusState private var partnerFieldFocused: Bool
    @State private var isDismissing: Bool = false

    // Visual index for the progress dots and transitions.
    // We intentionally ignore the hidden `.none` step so skipping from first screen
    // highlights the second dot (not the third).
    private func stepIndex(_ s: OnboardingViewModel.Step) -> Int {
        switch s {
        case .none, .asked_name: return 0
        case .asked_partner: return 1
        case .suggested_link: return 2
        case .completed: return 2
        }
    }

    var body: some View {
        ZStack {
            // Dim the background and block interactions
            LinearGradient(colors: [Color.black.opacity(0.55), Color.black.opacity(0.35)], startPoint: .topLeading, endPoint: .bottomTrailing)
                .ignoresSafeArea()
                .allowsHitTesting(true)

            // Card container with animated step transitions
            ZStack {
                cardView
                    .id(viewModel.step)
                    .transition(currentTransition)
            }
            .animation(.spring(response: 0.36, dampingFraction: 0.9), value: viewModel.step)
            .animation(.spring(response: 0.36, dampingFraction: 0.9), value: isDismissing)
            .padding(20)
            .frame(maxWidth: 460)
            .background(
                LinearGradient(colors: [Color(UIColor.systemBackground), Color(UIColor.systemBackground).opacity(0.98)], startPoint: .top, endPoint: .bottom)
            )
            .overlay(
                RoundedRectangle(cornerRadius: 18, style: .continuous)
                    .stroke(Color.primary.opacity(0.08), lineWidth: 1)
            )
            .clipShape(RoundedRectangle(cornerRadius: 18, style: .continuous))
            .shadow(color: Color.black.opacity(0.18), radius: 24, x: 0, y: 12)
            .padding(.horizontal, 24)
            .scaleEffect(isDismissing ? 0.94 : 1)
            .opacity(isDismissing ? 0 : 1)
            .offset(y: isDismissing ? 24 : 0)
            .blur(radius: isDismissing ? 6 : 0)
        }
        .onChange(of: viewModel.step, initial: false) { _, new in
            lastStepIndex = stepIndex(new)
            switch new {
            case .none, .asked_name:
                DispatchQueue.main.asyncAfter(deadline: .now() + 0.05) { nameFieldFocused = true; partnerFieldFocused = false }
            case .asked_partner:
                DispatchQueue.main.asyncAfter(deadline: .now() + 0.05) { partnerFieldFocused = true; nameFieldFocused = false }
            default:
                nameFieldFocused = false; partnerFieldFocused = false
            }
        }
    }

    private var currentTransition: AnyTransition {
        let current = stepIndex(viewModel.step)
        let forward = current >= lastStepIndex
        let insertion = AnyTransition.move(edge: forward ? .trailing : .leading).combined(with: .opacity)
        let removal = AnyTransition.move(edge: forward ? .leading : .trailing).combined(with: .opacity)
        return .asymmetric(insertion: insertion, removal: removal)
    }

    @ViewBuilder
    private var cardView: some View {
        VStack(spacing: 16) {
            header
            stepDots
            switch viewModel.step {
            case .none, .asked_name:
                VStack(spacing: 14) {
                    TextField("Your name", text: $tempName)
                        .textFieldStyle(.plain)
                        .padding(.horizontal, 14)
                        .padding(.vertical, 12)
                        .background(
                            RoundedRectangle(cornerRadius: 14, style: .continuous)
                                .fill(Color(UIColor.secondarySystemBackground))
                                .overlay(
                                    RoundedRectangle(cornerRadius: 14, style: .continuous)
                                        .stroke(Color.primary.opacity(0.08), lineWidth: 1)
                                )
                        )
                        .submitLabel(.continue)
                        .focused($nameFieldFocused)
                        .onSubmit { continueFromName() }
                        .onAppear {
                            tempName = viewModel.fullName
                            DispatchQueue.main.asyncAfter(deadline: .now() + 0.05) { nameFieldFocused = true }
                        }
                    if let err = viewModel.errorMessage, !err.isEmpty {
                        Text(err).font(.footnote).foregroundColor(.red)
                    }
                    HStack {
                        Button("Skip") { Task { await viewModel.skipCurrent() } }
                        Spacer()
                        Button("Continue") {
                            continueFromName()
                        }
                        .buttonStyle(.borderedProminent)
                        .tint(.purple)
                    }
                }
            case .asked_partner:
                VStack(spacing: 14) {
                    TextField("Partner name", text: $tempPartner)
                        .textFieldStyle(.plain)
                        .padding(.horizontal, 14)
                        .padding(.vertical, 12)
                        .background(
                            RoundedRectangle(cornerRadius: 14, style: .continuous)
                                .fill(Color(UIColor.secondarySystemBackground))
                                .overlay(
                                    RoundedRectangle(cornerRadius: 14, style: .continuous)
                                        .stroke(Color.primary.opacity(0.08), lineWidth: 1)
                                )
                        )
                        .submitLabel(.continue)
                        .focused($partnerFieldFocused)
                        .onSubmit { continueFromPartner() }
                        .onAppear {
                            tempPartner = viewModel.partnerName
                            DispatchQueue.main.asyncAfter(deadline: .now() + 0.05) { partnerFieldFocused = true }
                        }
                    if let err = viewModel.errorMessage, !err.isEmpty {
                        Text(err).font(.footnote).foregroundColor(.red)
                    }
                    HStack {
                        Button("Skip") { Task { await viewModel.skipCurrent() } }
                        Spacer()
                        Button("Continue") {
                            continueFromPartner()
                        }
                        .buttonStyle(.borderedProminent)
                        .tint(.purple)
                    }
                }
            case .suggested_link:
                VStack(spacing: 16) {
                    // Title moved to header

                    Group {
                        switch linkVM.state {
                        case .creating:
                            HStack { Spacer(); ProgressView("Preparing link…"); Spacer() }
                                .padding(12)
                        case .shareReady(let url):
                            HStack(spacing: 10) {
                                Image(systemName: "link")
                                    .foregroundColor(.purple)
                                Text(truncatedDisplay(for: url))
                                    .font(.footnote)
                                    .foregroundColor(.secondary)
                                    .lineLimit(1)
                                    .truncationMode(.middle)
                                Spacer()
                                ShareLink(item: url) {
                                    Image(systemName: "square.and.arrow.up")
                                        .font(.system(size: 16, weight: .semibold))
                                        .foregroundColor(.purple)
                                        .frame(width: 36, height: 36)
                                        .background(
                                            Circle()
                                    .fill(Color(UIColor.systemBackground))
                                    .overlay(Circle().stroke(Color.primary.opacity(0.08), lineWidth: 1))
                                        )
                                }
                            }
                            .padding(12)
                        case .linked:
                            HStack {
                                Image(systemName: "checkmark.circle.fill").foregroundColor(.green)
                                Text("You're linked")
                                Spacer()
                            }.padding(12)
                        case .accepting, .unlinking:
                            HStack { Spacer(); ProgressView("Working…"); Spacer() }.padding(12)
                        case .idle, .unlinked, .error:
                            HStack { Spacer(); Button("Generate link") { Task { await linkVM.ensureInviteReady() } }.buttonStyle(.borderedProminent).tint(.purple); Spacer() }.padding(4)
                        }
                    }
                    .background(
                        RoundedRectangle(cornerRadius: 12, style: .continuous)
                            .fill(Color(UIColor.secondarySystemBackground))
                            .overlay(
                                RoundedRectangle(cornerRadius: 12, style: .continuous)
                                    .stroke(Color.primary.opacity(0.08), lineWidth: 1)
                            )
                    )

                    HStack {
                        Spacer()
                        Button("Complete") {
                            withAnimation(.easeInOut(duration: 0.16)) { isDismissing = true }
                            DispatchQueue.main.asyncAfter(deadline: .now() + 0.16) {
                                Task { try? await viewModel.complete(skippedLinkSuggestion: false) }
                            }
                        }
                            .buttonStyle(.borderedProminent)
                            .tint(.purple)
                            .disabled(isDismissing)
                    }
                }
                .onAppear { Task { await linkVM.ensureInviteReady() } }
            case .completed:
                VStack { Text("All set!") }
            }
        }
    }

    private func truncatedDisplay(for url: URL) -> String {
        let host = url.host ?? ""
        let path = url.path
        if host.isEmpty && path.isEmpty { return "Invite link" }
        let shortPath = path.isEmpty ? "…" : "/…"
        return host.isEmpty ? "link://\(shortPath)" : "\(host)\(shortPath)"
    }

    private func continueFromName() {
        let value = tempName.trimmingCharacters(in: .whitespacesAndNewlines)
        Task { await viewModel.setFullName(value.isEmpty ? nil : value) }
    }

    private func continueFromPartner() {
        let value = tempPartner.trimmingCharacters(in: .whitespacesAndNewlines)
        Task { await viewModel.setPartnerName(value.isEmpty ? nil : value) }
    }

    // MARK: - Header and Progress Dots

    private var header: some View {
        VStack(spacing: 10) {
            ZStack {
                Circle()
                    .fill(Color(UIColor.secondarySystemBackground))
                    .frame(width: 56, height: 56)
                    .overlay(Circle().stroke(Color.primary.opacity(0.08), lineWidth: 1))
                Image(systemName: headerSymbol)
                    .font(.system(size: 22, weight: .semibold))
                    .foregroundColor(.purple)
            }
            Text(headerTitle)
                .font(.title2).bold()
                .foregroundColor(.secondary)
                .multilineTextAlignment(.center)
        }
        .frame(maxWidth: .infinity)
    }

    private var stepDots: some View {
        let total = 3
        let current = max(0, min(total - 1, stepIndex(viewModel.step)))
        return HStack(spacing: 8) {
            ForEach(0..<total, id: \.self) { i in
                Circle()
                    .fill(i == current ? Color.purple : Color.primary.opacity(0.2))
                    .frame(width: i == current ? 16 : 8, height: 8)
                    .overlay(
                        Capsule(style: .continuous)
                            .fill(Color.purple)
                            .frame(width: i == current ? 24 : 0, height: 8)
                            .opacity(i == current ? 1 : 0)
                    )
                    .animation(.spring(response: 0.35, dampingFraction: 0.9), value: current)
            }
        }
        .frame(maxWidth: .infinity)
    }

    private var headerTitle: String {
        switch viewModel.step {
        case .none, .asked_name: return "What's your name?"
        case .asked_partner: return "What's your partner's name?"
        case .suggested_link: return "Share this link to connect with your partner"
        case .completed: return "All set"
        }
    }

    private var headerSymbol: String {
        switch viewModel.step {
        case .none, .asked_name: return "person.crop.circle"
        case .asked_partner: return "heart"
        case .suggested_link: return "link"
        case .completed: return "checkmark.circle"
        }
    }
}


