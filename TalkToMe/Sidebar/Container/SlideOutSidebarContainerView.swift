import SwiftUI

struct SlideOutSidebarContainerView<Content: View>: View {

    @EnvironmentObject private var navigationViewModel: SidebarNavigationViewModel
    @EnvironmentObject private var sessionsViewModel: ChatSessionsViewModel
    @EnvironmentObject private var linkVM: LinkViewModel

    @StateObject private var containerViewModel = SidebarContainerViewModel()

    @Namespace private var profileNamespace

    @Environment(\.scenePhase) private var scenePhase

    let content: Content

    init(@ViewBuilder content: () -> Content) {
        self.content = content()
    }

    var body: some View {
        GeometryReader { proxy in
            let width: CGFloat = proxy.size.width
            let blurIntensity: CGFloat = containerViewModel.blurIntensity(
                width: width,
                isOpen: navigationViewModel.isOpen,
                dragOffset: navigationViewModel.dragOffset
            )

            ZStack {
                content
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
                    .offset(x: navigationViewModel.isOpen ? width + navigationViewModel.dragOffset : max(0, navigationViewModel.dragOffset))
                    .blur(radius: blurIntensity)
                    .animation(.spring(response: 0.34, dampingFraction: 0.72, blendDuration: 0), value: navigationViewModel.isOpen)
                    .animation(.interactiveSpring(response: 0.26, dampingFraction: 0.78, blendDuration: 0), value: navigationViewModel.dragOffset)

                let sidebarOffsetX: CGFloat = containerViewModel.sidebarOffsetX(
                    width: width,
                    isOpen: navigationViewModel.isOpen,
                    dragOffset: navigationViewModel.dragOffset
                )

                SidebarView(
                    isOpen: $navigationViewModel.isOpen,
                    profileNamespace: profileNamespace
                )
                .offset(x: sidebarOffsetX)
                .animation(.spring(response: 0.34, dampingFraction: 0.72, blendDuration: 0), value: navigationViewModel.isOpen)
                .animation(.interactiveSpring(response: 0.26, dampingFraction: 0.78, blendDuration: 0), value: navigationViewModel.dragOffset)
            }
            .sheet(isPresented: $navigationViewModel.showSettingsSheet) {
                SettingsView(
                    profileNamespace: profileNamespace,
                    isPresented: $navigationViewModel.showSettingsSheet
                )
                .environmentObject(sessionsViewModel)
                .environmentObject(linkVM)
                .presentationDetents([.large])
                .presentationDragIndicator(.visible)
            }
            .contentShape(Rectangle())
            .simultaneousGesture(
                DragGesture(minimumDistance: 5)
                    .onChanged { value in
                        containerViewModel.handleDragChanged(
                            dx: value.translation.width,
                            dy: value.translation.height,
                            startX: value.startLocation.x,
                            width: width,
                            navigationViewModel: navigationViewModel
                        )
                    }
                    .onEnded { value in
                        containerViewModel.handleDragEnded(
                            dx: value.translation.width,
                            dy: value.translation.height,
                            velocityX: value.velocity.width,
                            startX: value.startLocation.x,
                            width: width,
                            navigationViewModel: navigationViewModel
                        )
                    }
            )
        }
        .onAppear {
            containerViewModel.onAppear(
                navigationViewModel: navigationViewModel,
                sessionsViewModel: sessionsViewModel,
                linkViewModel: linkVM
            )
        }
        .onChange(of: scenePhase, initial: false) { _, newPhase in
            containerViewModel.onScenePhaseChange(
                newPhase,
                navigationViewModel: navigationViewModel,
                sessionsViewModel: sessionsViewModel
            )
        }
    }
}

#Preview {
    SlideOutSidebarContainerView {
        Text("Main Content")
            .frame(maxWidth: .infinity, maxHeight: .infinity)
            .background(Color.blue.opacity(0.1))
    }
    .environmentObject(LinkViewModel(accessTokenProvider: {
        return "mock-access-token"
    }))
}