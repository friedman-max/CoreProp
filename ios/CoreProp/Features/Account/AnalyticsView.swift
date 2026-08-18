import SwiftUI
import CorePropKit

/// A concise performance summary computed from the user's own logged slips —
/// the only performance numbers the app can honestly show. Deeper analytics
/// (calibration curves, CLV, the observatory) live on the web app.
struct AnalyticsView: View {
    @EnvironmentObject private var model: AppModel
    @State private var summary: BacktestSummary?
    @State private var state: LoadState = .idle

    var body: some View {
        ScrollView {
            VStack(spacing: 14) {
                switch state {
                case .idle, .loading:
                    ProgressView().tint(Theme.primary2).padding(.top, 60)
                case .failed(let m):
                    ErrorStateView(message: m) { Task { await load() } }
                case .empty:
                    EmptyStateView(systemImage: "chart.bar", title: "No settled slips yet",
                                   message: "Performance appears once your logged slips resolve.")
                case .loaded:
                    if let s = summary { cards(s) }
                }
                Text("These are your logged-slip results. Calibration, closing-line value, and the market observatory are available in the web app.")
                    .font(Theme.ui(11)).foregroundColor(Theme.text3)
                    .multilineTextAlignment(.center).padding(.horizontal, 8)
            }
            .padding(16)
        }
        .background(Theme.bg.ignoresSafeArea())
        .navigationTitle("Performance")
        .navigationBarTitleDisplayMode(.inline)
        .task { await load() }
    }

    private func cards(_ s: BacktestSummary) -> some View {
        VStack(spacing: 12) {
            HStack(spacing: 10) {
                StatTile(label: "Slip hit rate", value: s.slipHitRate.map { Fmt.percent($0, decimals: 0) } ?? "—")
                StatTile(label: "Leg hit rate", value: s.legHitRate.map { Fmt.percent($0, decimals: 0) } ?? "—")
            }
            HStack(spacing: 10) {
                StatTile(label: "Actual ROI", value: s.roi.map { Fmt.signedPercent($0) } ?? "—",
                         tone: (s.roi ?? 0) >= 0 ? .good : .bad)
                StatTile(label: "Settled slips", value: "\(s.completedSlips)")
            }
        }
    }

    private func load() async {
        if summary == nil { state = .loading }
        do {
            let slips = try await model.client.backtestSlips()
            let s = BacktestSummary.compute(slips)
            summary = s
            state = s.completedSlips == 0 ? .empty : .loaded
        } catch let e as APIError {
            if e.isPaymentRequired { await model.handlePaymentRequired() }
            state = .failed(e.display)
        } catch {
            state = .failed(error.localizedDescription)
        }
    }
}
