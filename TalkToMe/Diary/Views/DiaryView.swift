//
//  DiaryView.swift
//  TalkToMe
//
//  Created by Stephan on 02.02.2026.
//

import SwiftUI

struct DiaryView: View {
    var body: some View {
        NavigationStack {
            VStack(spacing: 20) {
                Image(systemName: "book.fill")
                    .font(.system(size: 60))
                    .foregroundStyle(.secondary)
                
                Text("Diary")
                    .font(.title)
                    .fontWeight(.semibold)
                
                Text("Coming soon")
                    .font(.subheadline)
                    .foregroundStyle(.secondary)
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity)
            .background(Color(.systemBackground))
            .navigationTitle("Diary")
            .navigationBarTitleDisplayMode(.large)
        }
    }
}

#Preview {
    DiaryView()
}
