import SwiftUI

struct SidebarPendingRequestsView: View {
    let requests: [BackendService.PartnerPendingRequest]
    let onTap: (BackendService.PartnerPendingRequest) -> Void

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            ForEach(requests, id: \.id) { request in
                Button(action: { onTap(request) }) {
                    PendingRequestRowView(request: request)
                }
                .buttonStyle(.plain)
            }
        }
        .padding(12)
        .background(
            RoundedRectangle(cornerRadius: 16, style: .continuous)
                .fill(Color(.systemGray6))
        )
        .padding(.horizontal, 16)
    }
}


