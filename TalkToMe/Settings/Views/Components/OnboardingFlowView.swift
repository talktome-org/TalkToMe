import SwiftUI

struct OnboardingFlowView: View {
    @ObservedObject var viewModel: OnboardingViewModel

    @State private var tempName: String = ""
    @FocusState private var nameFieldFocused: Bool
    @State private var isDismissing: Bool = false

    var body: some View {
        ZStack {
            LinearGradient(
                colors: [Color.black.opacity(0.55), Color.black.opacity(0.35)],
                startPoint: .topLeading,
                endPoint: .bottomTrailing
            )
            .ignoresSafeArea()

            VStack(spacing: 16) {
                header

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
                        Button("Skip") { Task { try? await viewModel.complete() } }
                        Spacer()
                        Button("Continue") { continueFromName() }
                            .buttonStyle(.borderedProminent)
                            .tint(.purple)
                    }
                }

                Spacer(minLength: 0)
            }
            .padding(20)
            .frame(maxWidth: 460)
            .background(
                LinearGradient(
                    colors: [Color(UIColor.systemBackground), Color(UIColor.systemBackground).opacity(0.98)],
                    startPoint: .top,
                    endPoint: .bottom
                )
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
    }

    private var header: some View {
        VStack(spacing: 10) {
            ZStack {
                Circle()
                    .fill(Color(UIColor.secondarySystemBackground))
                    .frame(width: 56, height: 56)
                    .overlay(Circle().stroke(Color.primary.opacity(0.08), lineWidth: 1))
                Image(systemName: "person.crop.circle")
                    .font(.system(size: 22, weight: .semibold))
                    .foregroundColor(.purple)
            }
            Text("What's your name?")
                .font(.title2).bold()
                .foregroundColor(.secondary)
                .multilineTextAlignment(.center)
        }
        .frame(maxWidth: .infinity)
    }

    private func continueFromName() {
        let value = tempName.trimmingCharacters(in: .whitespacesAndNewlines)
        Task { await viewModel.setFullName(value.isEmpty ? nil : value) }
    }
}

