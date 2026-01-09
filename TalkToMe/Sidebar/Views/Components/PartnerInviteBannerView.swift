import SwiftUI
import UIKit

struct PartnerInviteBannerView: View {
    @EnvironmentObject private var linkVM: LinkViewModel
    @Environment(\.colorScheme) private var colorScheme

    @State private var copied: Bool = false

    private var accentGradient: LinearGradient {
        LinearGradient(
            colors: [
                Color(red: 0.84, green: 0.66, blue: 1.00),
                Color(red: 0.60, green: 0.42, blue: 1.00)
            ],
            startPoint: .topLeading,
            endPoint: .bottomTrailing
        )
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack(alignment: .center, spacing: 12) {
                ZStack {
                    Group {
                        if #available(iOS 26.0, *) {
                            ZStack {
                                Color.clear
                                    .glassEffect(.regular, in: Circle())
                                    .opacity(0.98)
                                Circle()
                                    .fill(
                                        LinearGradient(
                                            colors: [
                                                Color(red: 0.84, green: 0.66, blue: 1.00),
                                                Color(red: 0.60, green: 0.42, blue: 1.00)
                                            ],
                                            startPoint: .topLeading,
                                            endPoint: .bottomTrailing
                                        )
                                    )
                                    .opacity(0.35)
                                    .allowsHitTesting(false)
                            }
                        } else {
                            Circle()
                                .fill(accentGradient)
                        }
                    }
                    .frame(width: 28, height: 28)
                    .shadow(color: Color.black.opacity(0.08), radius: 6, x: 0, y: 3)
                    Image(systemName: "person.2.fill")
                        .font(.system(size: 14, weight: .bold))
                        .foregroundColor(.primary)
                }

                VStack(alignment: .leading, spacing: 2) {
                    Text("Connect with your partner")
                        .font(.system(size: 16, weight: .semibold))
                        .foregroundColor(.primary)
                    Text("Share your invite to unlock shared sessions and partner messages.")
                        .font(.system(size: 13))
                        .foregroundColor(.secondary)
                        .fixedSize(horizontal: false, vertical: true)
                }
            }

            linkRowOrButton()
        }
        .padding(16)
        .background(
            Group {
                if #available(iOS 26.0, *) {
                    ZStack {
                        Color.clear
                            .glassEffect(.regular, in: RoundedRectangle(cornerRadius: 20, style: .continuous))
                            .opacity(0.95)
                        RoundedRectangle(cornerRadius: 20, style: .continuous)
                            .fill(
                                LinearGradient(
                                    colors: [
                                        Color(red: 0.84, green: 0.66, blue: 1.00),
                                        Color(red: 0.60, green: 0.42, blue: 1.00)
                                    ],
                                    startPoint: .topLeading,
                                    endPoint: .bottomTrailing
                                )
                            )
                            .opacity(0.25)
                            .allowsHitTesting(false)
                        RoundedRectangle(cornerRadius: 20, style: .continuous)
                            .stroke(
                                LinearGradient(
                                    colors: [
                                        Color.white.opacity(colorScheme == .dark ? 0.18 : 0.14),
                                        Color.white.opacity(0.04)
                                    ],
                                    startPoint: .topLeading,
                                    endPoint: .bottomTrailing
                                ),
                                lineWidth: 1
                            )
                    }
                } else {
                    ZStack {
                        RoundedRectangle(cornerRadius: 20, style: .continuous)
                            .fill(.ultraThinMaterial)
                        RoundedRectangle(cornerRadius: 20, style: .continuous)
                            .stroke(
                                (colorScheme == .dark ? Color.white.opacity(0.10) : Color.black.opacity(0.06)),
                                lineWidth: 1
                            )
                        RoundedRectangle(cornerRadius: 20, style: .continuous)
                            .fill(
                                LinearGradient(
                                    colors: [
                                        Color(red: 0.84, green: 0.66, blue: 1.00).opacity(0.16),
                                        Color(red: 0.60, green: 0.42, blue: 1.00).opacity(0.16)
                                    ],
                                    startPoint: .topLeading,
                                    endPoint: .bottomTrailing
                                )
                            )
                            .allowsHitTesting(false)
                    }
                }
            }
        )
        .shadow(color: Color.black.opacity(colorScheme == .dark ? 0.18 : 0.08), radius: 14, x: 0, y: 8)
    }

    @ViewBuilder
    private func linkRowOrButton() -> some View {
        switch linkVM.state {
        case .creating:
            HStack {
                Spacer()
                ProgressView("Preparing link…")
                Spacer()
            }
        case .shareReady(let url):
            HStack(spacing: 10) {
                Image(systemName: "link")
                    .font(.system(size: 16, weight: .medium))
                    .foregroundColor(.primary)

                Text(truncatedDisplay(for: url))
                    .font(.footnote)
                    .foregroundColor(.secondary)
                    .lineLimit(1)
                    .truncationMode(.middle)

                Spacer()

                Button(action: {
                    UIPasteboard.general.string = url.absoluteString
                    withAnimation(.easeInOut(duration: 0.15)) { copied = true }
                    DispatchQueue.main.asyncAfter(deadline: .now() + 3.0) {
                        withAnimation(.easeInOut(duration: 0.15)) { copied = false }
                    }
                }) {
                    Image(systemName: copied ? "checkmark" : "doc.on.doc")
                        .font(.system(size: 16, weight: .semibold))
                        .foregroundColor(.primary)
                        .frame(width: 34, height: 34)
                        .background {
                            Circle()
                                .fill(.ultraThinMaterial)
                                .overlay(
                                    Circle().stroke(Color.primary.opacity(0.12), lineWidth: 1)
                                )
                        }
                        .shadow(color: Color.black.opacity(0.06), radius: 6, x: 0, y: 3)
                }
                .buttonStyle(PlainButtonStyle())

                ShareLink(item: url) {
                    Image(systemName: "square.and.arrow.up")
                        .font(.system(size: 16, weight: .semibold))
                        .foregroundColor(.primary)
                        .frame(width: 34, height: 34)
                        .background {
                            Circle()
                                .fill(.ultraThinMaterial)
                                .overlay(
                                    Circle().stroke(Color.primary.opacity(0.12), lineWidth: 1)
                                )
                        }
                        .shadow(color: Color.black.opacity(0.06), radius: 6, x: 0, y: 3)
                }
            }
            .padding(12)
            .background(
                Group {
                    if #available(iOS 26.0, *) {
                        ZStack {
                            Color.clear
                                .glassEffect(.regular, in: RoundedRectangle(cornerRadius: 14, style: .continuous))
                                .opacity(0.93)
                            RoundedRectangle(cornerRadius: 14, style: .continuous)
                                .fill(
                                    LinearGradient(
                                        colors: [
                                            Color(red: 0.84, green: 0.66, blue: 1.00),
                                            Color(red: 0.60, green: 0.42, blue: 1.00)
                                        ],
                                        startPoint: .topLeading,
                                        endPoint: .bottomTrailing
                                    )
                                )
                                .opacity(0.22)
                                .allowsHitTesting(false)
                            RoundedRectangle(cornerRadius: 14, style: .continuous)
                                .stroke(
                                    LinearGradient(
                                        colors: [
                                            Color.white.opacity(colorScheme == .dark ? 0.16 : 0.12),
                                            Color.white.opacity(0.04)
                                        ],
                                        startPoint: .topLeading,
                                        endPoint: .bottomTrailing
                                    ),
                                    lineWidth: 1
                                )
                        }
                    } else {
                        RoundedRectangle(cornerRadius: 14, style: .continuous)
                            .fill(.ultraThinMaterial)
                            .overlay(
                                RoundedRectangle(cornerRadius: 14, style: .continuous)
                                    .stroke(
                                        (colorScheme == .dark ? Color.white.opacity(0.08) : Color.black.opacity(0.07)),
                                        lineWidth: 1
                                    )
                            )
                            .overlay(
                                RoundedRectangle(cornerRadius: 14, style: .continuous)
                                    .fill(Color(red: 0.80, green: 0.52, blue: 1.00).opacity(0.20))
                            )
                    }
                }
            )
            .shadow(color: Color.black.opacity(colorScheme == .dark ? 0.20 : 0.05), radius: 8, x: 0, y: 4)
        case .linked:
            EmptyView()
        case .accepting, .unlinking:
            HStack {
                Spacer()
                ProgressView("Working…")
                Spacer()
            }
        case .idle, .unlinked, .error:
            Button(action: {
                Haptics.impact(.light)
                Task { await linkVM.ensureInviteReady() }
            }) {
                HStack(spacing: 8) {
                    Image(systemName: "link")
                        .font(.system(size: 14, weight: .semibold))
                    Text("Get invite link")
                        .font(.system(size: 14, weight: .semibold))
                }
                .frame(maxWidth: .infinity)
            }
            .buttonStyle(.borderedProminent)
        }
    }

    private func truncatedDisplay(for url: URL) -> String {
        let host = url.host ?? ""
        let path = url.path
        if host.isEmpty && path.isEmpty { return "Invite link" }
        let shortPath = path.isEmpty ? "…" : "/…"
        return host.isEmpty ? "link://\(shortPath)" : "\(host)\(shortPath)"
    }
}


