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
          .foregroundStyle(AppTheme.textPrimary)

        Text(overview)
          .font(.system(size: 14))
          .foregroundStyle(AppTheme.textSecondary)
          .lineLimit(2)
      }
      .padding(.horizontal, 14)
      .padding(.vertical, 12)
    }
    .background(AppTheme.surface)
    .clipShape(RoundedRectangle(cornerRadius: 16, style: .continuous))
    //.environment(\.colorScheme, .light)
    .overlay(
      RoundedRectangle(cornerRadius: 16, style: .continuous)
        .stroke(Color.primary.opacity(0.1), lineWidth: 0.5)
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
            gradientColors: [AppTheme.brand, AppTheme.accent]
          )

          HomeCardView(
            title: "Daily Check-In",
            overview: "A quick moment to share how you're feeling today and stay connected.",
            gradientColors: [AppTheme.surface, AppTheme.brand]
          )

          HomeCardView(
            title: "Gratitude Practice",
            overview: "Reflect on three things you appreciate about each other this week.",
            gradientColors: [AppTheme.brand.opacity(0.9), AppTheme.accent.opacity(0.7)]
          )

          HomeCardView(
            title: "Conflict Resolution",
            overview: "Work through a recent disagreement with guided questions from Em.",
            gradientColors: [AppTheme.partner, AppTheme.brand]
          )

          HomeCardView(
            title: "Dream Together",
            overview: "Share your hopes and plans for the future as a couple.",
            gradientColors: [AppTheme.background.opacity(0.8), AppTheme.accent]
          )
        }
        .padding(.horizontal, 16)
        .padding(.top, 8)
        .padding(.bottom, 24)
      }
      .background(AppTheme.background)
      .navigationTitle("Home")
      .navigationBarTitleDisplayMode(.large)
    }
  }
}

#Preview {
  HomeView()
}
