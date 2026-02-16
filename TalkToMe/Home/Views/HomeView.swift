//
//  HomeView.swift
//  TalkToMe
//
//  Created by Stephan on 02.02.2026.
//

import SwiftUI

struct HomeCardView: View {
    let title: String
    let overview: String
    let gradientColors: [Color]

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            // Image placeholder
            ZStack {
                LinearGradient(
                    colors: gradientColors,
                    startPoint: .topLeading,
                    endPoint: .bottomTrailing
                )

                Image(systemName: "sparkles")
                    .font(.system(size: 32, weight: .light))
                    .foregroundStyle(.white.opacity(0.5))
            }
            .frame(height: 160)
            .clipped()

            // Text content
            VStack(alignment: .leading, spacing: 6) {
                Text(title)
                    .font(.system(size: 18, weight: .semibold))
                    .foregroundStyle(Color(.label))

                Text(overview)
                    .font(.system(size: 14))
                    .foregroundStyle(Color(.secondaryLabel))
                    .lineLimit(2)
            }
            .padding(.horizontal, 14)
            .padding(.vertical, 12)
        }
        .background(Color(.secondarySystemBackground))
        .clipShape(RoundedRectangle(cornerRadius: 16, style: .continuous))
        .overlay(
            RoundedRectangle(cornerRadius: 16, style: .continuous)
                .stroke(Color(.separator).opacity(0.3), lineWidth: 0.5)
        )
    }
}

struct HomeView: View {
    var body: some View {
        NavigationStack {
            ScrollView {
                VStack(spacing: 16) {
                    HomeCardView(
                        title: "Weekly Ritual",
                        overview: "Time for your weekly relationship ritual with David, guided by Em.",
                        gradientColors: [Color(red: 0.55, green: 0.75, blue: 0.95), Color(red: 0.85, green: 0.65, blue: 0.45)]
                    )

                    HomeCardView(
                        title: "Daily Check-In",
                        overview: "A quick moment to share how you're feeling today and stay connected.",
                        gradientColors: [Color(red: 0.70, green: 0.55, blue: 0.85), Color(red: 0.45, green: 0.65, blue: 0.90)]
                    )

                    HomeCardView(
                        title: "Gratitude Practice",
                        overview: "Reflect on three things you appreciate about each other this week.",
                        gradientColors: [Color(red: 0.45, green: 0.80, blue: 0.65), Color(red: 0.35, green: 0.60, blue: 0.75)]
                    )

                    HomeCardView(
                        title: "Conflict Resolution",
                        overview: "Work through a recent disagreement with guided questions from Em.",
                        gradientColors: [Color(red: 0.90, green: 0.55, blue: 0.50), Color(red: 0.75, green: 0.40, blue: 0.65)]
                    )

                    HomeCardView(
                        title: "Dream Together",
                        overview: "Share your hopes and plans for the future as a couple.",
                        gradientColors: [Color(red: 0.50, green: 0.60, blue: 0.85), Color(red: 0.70, green: 0.50, blue: 0.80)]
                    )
                }
                .padding(.horizontal, 16)
                .padding(.top, 8)
                .padding(.bottom, 24)
            }
            .background(Color(.systemBackground))
            .navigationTitle("Home")
            .navigationBarTitleDisplayMode(.large)
        }
    }
}

#Preview {
    HomeView()
}
