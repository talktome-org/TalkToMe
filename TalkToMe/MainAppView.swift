//
//  MainAppView.swift
//  TalkToMe
//
//  Created by Stephan  on 29.08.2025.
//

import SwiftUI

struct MainAppView: View {
    @EnvironmentObject private var navigationViewModel: SidebarNavigationViewModel
    @EnvironmentObject private var sessionsViewModel: ChatSessionsViewModel
    @StateObject private var onboardingVM = OnboardingViewModel()

    var body: some View {
        ZStack {
            Group {
                let viewId: String = {
                    if let sid = sessionsViewModel.activeSessionId { return "session_\(sid.uuidString)" }
                    return "new_\(sessionsViewModel.chatViewKey.uuidString)"
                }()
                ChatView(sessionId: sessionsViewModel.activeSessionId)
                    .id(viewId)
            }

            if onboardingVM.step != .completed && !onboardingVM.isLoading {
                OnboardingFlowView(viewModel: onboardingVM)
                    .transition(.opacity)
                    .zIndex(10)
            }
        }
        .task { await onboardingVM.load() }
    }
}

#Preview {
    MainAppView()
        .environmentObject(SidebarNavigationViewModel())
        .environmentObject(ChatSessionsViewModel())
}
