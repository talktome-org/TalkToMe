import SwiftUI
import UIKit

struct InputAreaView: View {
    @StateObject private var voiceService = VoiceRecordingService()

    @Binding var inputText: String
    @Binding var isLoading: Bool
    @Binding var focusSnippet: String?
    @Binding var pendingAttachments: [PendingAttachment]
    @Binding var isMediaPanelVisible: Bool

    let isInputFocused: FocusState<Bool>.Binding
    let send: () -> Void
    let stop: () -> Void
    let onVoiceRecordingStart: () -> Void

    private let sendSize: CGFloat = 34

    @Environment(\.colorScheme) private var colorScheme


    private var trimmedInput: String {
        inputText.trimmingCharacters(in: .whitespacesAndNewlines)
    }

    private var canSend: Bool {
        !(trimmedInput.isEmpty && pendingAttachments.isEmpty)
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            focusSnippetView
            contentRow
        }
        .padding(.leading, 16)
        .padding(.trailing, 8)
        .padding(.vertical, 5)
        .background(inputBarBackground)
        .clipShape(RoundedRectangle(cornerRadius: 25, style: .continuous))
        .overlay(inputBarOverlay)
        .shadow(color: Color.black.opacity(0.08), radius: 12, x: 0, y: 4)
        .padding(.horizontal, 16)
        .padding(.vertical, 12)
        .animation(.easeInOut(duration: 0.2), value: isInputFocused.wrappedValue)
    }

    @ViewBuilder
    private var focusSnippetView: some View {
        if let snippet = focusSnippet, !snippet.isEmpty {
            HStack(alignment: .top, spacing: 8) {
                Image(systemName: "quote.opening")
                    .foregroundColor(.secondary)
                Text(snippet)
                    .font(.caption)
                    .foregroundColor(.secondary)
                    .lineLimit(3)
                    .multilineTextAlignment(.leading)
                Spacer(minLength: 8)
                Button(action: { withAnimation { focusSnippet = nil } }) {
                    Image(systemName: "xmark.circle.fill")
                        .foregroundColor(.secondary)
                }
                .buttonStyle(.plain)
            }
            .padding(.horizontal, 14)
            .padding(.vertical, 10)
            .background(.regularMaterial, in: RoundedRectangle(cornerRadius: 14, style: .continuous))
            .overlay(
                RoundedRectangle(cornerRadius: 14, style: .continuous)
                    .strokeBorder(
                        colorScheme == .dark ? Color.white.opacity(0.12) : Color.black.opacity(0.06),
                        lineWidth: 1
                    )
            )
        }
    }

    private var contentRow: some View {
        HStack(alignment: .bottom, spacing: 8) {
            inputStack
            trailingControls
        }
    }

    private var trailingControls: some View {
        HStack(spacing: 10) {
            plusButton
            sendButton
        }
    }

    private var plusButton: some View {
        Button(action: {
            if isMediaPanelVisible {
                // Switch back to keyboard
                withAnimation(.easeInOut(duration: 0.22)) {
                    isMediaPanelVisible = false
                }
                isInputFocused.wrappedValue = true
            } else {
                // Switch to media panel
                isInputFocused.wrappedValue = false
                withAnimation(.easeInOut(duration: 0.22)) {
                    isMediaPanelVisible = true
                }
            }
        }) {
            ZStack {
                Image(systemName: "plus")
                    .opacity(isMediaPanelVisible ? 0 : 1)
                Image(systemName: "keyboard")
                    .opacity(isMediaPanelVisible ? 1 : 0)
            }
            .font(.system(size: 14, weight: .semibold))
            .foregroundColor(Color(red: 0.74, green: 0.34, blue: 0.92))
            .frame(width: 32, height: 32)
            .contentShape(Rectangle())
            .animation(.easeInOut(duration: 0.18), value: isMediaPanelVisible)
        }
        .buttonStyle(.plain)
    }

    private var inputStack: some View {
        VStack {
            attachmentsStrip
            textFieldOrWaveform
        }
        .frame(minHeight: 36)
    }

    @ViewBuilder
    private var attachmentsStrip: some View {
        if !pendingAttachments.isEmpty {
            ScrollView(.horizontal, showsIndicators: false) {
                HStack(spacing: 8) {
                    ForEach(pendingAttachments) { att in
                        attachmentThumb(att)
                    }
                }
                .padding(.horizontal, 4)
                .padding(.top, 2)
                .padding(.bottom, 6)
            }
        }
    }

    private func attachmentThumb(_ att: PendingAttachment) -> some View {
        ZStack(alignment: .topTrailing) {
            RoundedRectangle(cornerRadius: 12, style: .continuous)
                .fill(.thinMaterial)
                .frame(width: 54, height: 54)
                .overlay(attachmentThumbContent(att))

            Button(action: {
                Haptics.impact(.light)
                withAnimation(.spring(response: 0.35, dampingFraction: 0.9)) {
                    pendingAttachments.removeAll { $0.id == att.id }
                }
            }) {
                Image(systemName: "xmark.circle.fill")
                    .foregroundColor(.secondary)
                    .background(Color.clear)
            }
            .buttonStyle(.plain)
            .padding(4)
        }
    }

    @ViewBuilder
    private func attachmentThumbContent(_ att: PendingAttachment) -> some View {
        if case .image(let data, _) = att.kind, let uiImage = UIImage(data: data) {
            Image(uiImage: uiImage)
                .resizable()
                .scaledToFill()
                .frame(width: 54, height: 54)
                .clipShape(RoundedRectangle(cornerRadius: 12, style: .continuous))
        } else {
            Image(systemName: "doc")
                .font(.system(size: 16, weight: .semibold))
                .foregroundColor(.secondary)
        }
    }

    @ViewBuilder
    private var textFieldOrWaveform: some View {
        if voiceService.isShowingRecordingPlaceholder {
            WaveformPlaceholderView(
                currentLevel: voiceService.spawnLevel,
                barWidth: 3,
                barSpacing: 2,
                minHeight: 3,
                maxHeight: 16,
                color: Color(red: 0.54, green: 0.32, blue: 0.78)
            )
            .padding(.horizontal, 4)
            .padding(.vertical, 10)
        } else {
            TextField("Share what's on your mind", text: $inputText, axis: .vertical)
                .lineLimit(1...5)
                .lineSpacing(2)
                .font(.system(size: 17, weight: .regular))
                .foregroundColor(.primary)
                .textFieldStyle(.plain)
                .fixedSize(horizontal: false, vertical: true)
                .padding(.horizontal, 4)
                .padding(.vertical, 6)
                .focused(isInputFocused)
        }
    }

    private var sendButton: some View {
        Button(action: sendButtonTapped) {
            ZStack {
                Circle()
                    .fill(
                        LinearGradient(
                            colors: [Color(red: 0.5, green: 0.3, blue: 0.7), Color(red: 0.4, green: 0.2, blue: 0.6)],
                            startPoint: .topLeading,
                            endPoint: .bottomTrailing
                        )
                    )
                    .frame(width: sendSize, height: sendSize)
                    .shadow(color: Color(red: 0.4, green: 0.2, blue: 0.6).opacity(0.3), radius: 4, x: 0, y: 2)

                ZStack {
                    if !canSend {
                        Image(systemName: voiceService.isRecording ? "stop.fill" : "mic.fill")
                            .font(.system(size: 14, weight: .semibold))
                            .foregroundColor(.white)
                            .transition(.asymmetric(
                                insertion: .move(edge: .leading).combined(with: .opacity),
                                removal: .move(edge: .leading).combined(with: .opacity)
                            ))
                    } else {
                        Image(systemName: isLoading ? "stop.fill" : "arrow.up")
                            .font(.system(size: 14, weight: .semibold))
                            .foregroundColor(.white)
                            .transition(.asymmetric(
                                insertion: .move(edge: .trailing).combined(with: .opacity),
                                removal: .move(edge: .trailing).combined(with: .opacity)
                            ))
                    }
                }
            }
        }
        .scaleEffect(voiceService.isRecording && !canSend ? 1.1 : 1.0)
        .animation(.spring(response: 0.3, dampingFraction: 0.7), value: voiceService.isRecording)
        .animation(.spring(response: 0.35, dampingFraction: 0.85, blendDuration: 0.2), value: inputText)
    }

    private func sendButtonTapped() {
        if !canSend {
            if voiceService.isRecording {
                Haptics.impact(.medium)
                voiceService.stopRecording()
                inputText = voiceService.transcribedText
            } else {
                Haptics.impact(.medium)
                onVoiceRecordingStart()
                Task { await voiceService.startRecording() }
            }
            return
        }

        Haptics.impact(.light)
        if isLoading {
            stop()
        } else {
            isInputFocused.wrappedValue = false
            withAnimation(.easeInOut(duration: 0.22)) {
                isMediaPanelVisible = false
            }
            send()
        }
    }

    @ViewBuilder
    private var inputBarBackground: some View {
        if #available(iOS 26.0, *) {
            Color.clear
                .glassEffect(.regular, in: RoundedRectangle(cornerRadius: 25, style: .continuous))
        } else {
            RoundedRectangle(cornerRadius: 25, style: .continuous)
                .fill(.ultraThinMaterial)
        }
    }

    @ViewBuilder
    private var inputBarOverlay: some View {
        if #available(iOS 26.0, *) {
            EmptyView()
        } else {
            RoundedRectangle(cornerRadius: 25, style: .continuous)
                .strokeBorder(
                    colorScheme == .dark ? Color.white.opacity(0.15) : Color.black.opacity(0.08),
                    lineWidth: 1
                )
        }
    }
}

#Preview {
    @FocusState var isFocused: Bool
    InputAreaView(
        inputText: .constant(""),
        isLoading: .constant(false),
        focusSnippet: .constant(nil),
        pendingAttachments: .constant([]),
        isMediaPanelVisible: .constant(false),
        isInputFocused: $isFocused,
        send: {},
        stop: {},
        onVoiceRecordingStart: {}
    )
}



