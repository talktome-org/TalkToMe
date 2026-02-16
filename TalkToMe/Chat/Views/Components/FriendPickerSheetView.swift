import SwiftUI

struct FriendPickerSheetView: View {
    @Binding var isPresented: Bool
    let friends: [FriendSummary]
    let isLoading: Bool
    let errorMessage: String?
    let onRetry: () -> Void
    let onPick: (UUID) -> Void

    var body: some View {
        NavigationStack {
            VStack(spacing: 12) {
                Text("Pick a friend")
                    .font(.system(size: 14, weight: .semibold))
                    .foregroundStyle(.secondary)
                    .padding(.top, 6)

                if friends.isEmpty {
                    if let errorMessage, !errorMessage.isEmpty {
                        VStack(spacing: 10) {
                            Text(errorMessage)
                                .font(.system(size: 14))
                                .foregroundStyle(.secondary)
                                .multilineTextAlignment(.center)

                            Button("Retry") {
                                Haptics.impact(.light)
                                onRetry()
                            }
                            .buttonStyle(.bordered)
                        }
                        .padding(.top, 6)
                    } else if isLoading {
                        VStack(spacing: 10) {
                            ProgressView()
                            Text("Loading friends…")
                                .font(.system(size: 14))
                                .foregroundStyle(.secondary)
                        }
                        .padding(.top, 6)
                    } else {
                        Text("No friends yet. Add one with a 4-digit code first.")
                            .font(.system(size: 14))
                            .foregroundStyle(.secondary)
                            .multilineTextAlignment(.center)
                            .padding(.top, 6)
                    }
                } else {
                    // If we already have cached friends, show the list immediately.
                    // We can still refresh in the background without blocking selection.
                    if isLoading {
                        HStack(spacing: 8) {
                            ProgressView().controlSize(.small)
                            Text("Refreshing…")
                                .font(.system(size: 13, weight: .medium))
                                .foregroundStyle(.secondary)
                            Spacer()
                        }
                        .padding(.top, 2)
                    } else if let errorMessage, !errorMessage.isEmpty {
                        HStack(spacing: 10) {
                            Text(errorMessage)
                                .font(.system(size: 13))
                                .foregroundStyle(.secondary)
                                .lineLimit(2)
                            Spacer()
                            Button("Retry") {
                                Haptics.impact(.light)
                                onRetry()
                            }
                            .buttonStyle(.bordered)
                            .controlSize(.small)
                        }
                        .padding(.top, 2)
                    }

                    List {
                        ForEach(friends) { friend in
                            Button {
                                Haptics.impact(.light)
                                onPick(friend.id)
                            } label: {
                                HStack {
                                    ZStack(alignment: .bottomTrailing) {
                                        SidebarAvatarView(avatarURL: friend.avatarURL)
                                            .frame(width: 34, height: 34)
                                            .clipShape(Circle())

                                        if friend.isOnline {
                                            Circle()
                                                .fill(Color.green)
                                                .frame(width: 10, height: 10)
                                                .overlay(
                                                    Circle()
                                                        .strokeBorder(Color(.systemBackground), lineWidth: 1.5)
                                                )
                                                .offset(x: 1, y: 1)
                                        }
                                    }

                                    VStack(alignment: .leading, spacing: 2) {
                                        Text(friend.fullName.isEmpty ? "Friend" : friend.fullName)
                                            .font(.system(size: 16, weight: .semibold))
                                            .foregroundStyle(.primary)
                                        if let presence = friend.presenceText {
                                            Text(presence)
                                                .font(.system(size: 12))
                                                .foregroundStyle(friend.isOnline ? .green : .secondary)
                                                .lineLimit(1)
                                        }
                                    }
                                    Spacer()
                                    Image(systemName: "chevron.right")
                                        .font(.system(size: 12, weight: .semibold))
                                        .foregroundStyle(.secondary)
                                }
                                .padding(.vertical, 6)
                            }
                            .buttonStyle(.plain)
                        }
                    }
                    .listStyle(.plain)
                }

                Spacer(minLength: 0)
            }
            .padding(.horizontal, 16)
            .navigationTitle("Send to…")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .topBarLeading) {
                    Button("Cancel") {
                        isPresented = false
                    }
                }
            }
        }
    }
}
