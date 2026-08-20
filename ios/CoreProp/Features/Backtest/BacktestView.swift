import SwiftUI
import CorePropKit

/// Logged slips + a performance summary strip. Outcomes are recomputed
/// client-side from the legs (`BacktestScoring`), matching `page-backtest.jsx`.
struct BacktestView: View {
    @EnvironmentObject private var model: AppModel
    @StateObject private var vm = BacktestViewModel()
    @State private var pendingDelete: BacktestSlip?

    var body: some View {
        NavigationStack {
            content
                .background(Theme.bg.ignoresSafeArea())
                .navigationTitle("Backtest")
                .navigationBarTitleDisplayMode(.inline)
        }
        .task { await vm.load(client: model.client, model: model) }
        .confirmationDialog("Delete this slip?",
                            isPresented: Binding(get: { pendingDelete != nil },
                                                 set: { if !$0 { pendingDelete = nil } }),
                            titleVisibility: .visible) {
            Button("Delete", role: .destructive) {
                if let slip = pendingDelete {
                    Task { await vm.delete(slip, client: model.client, model: model) }
                }
                pendingDelete = nil
            }
            Button("Cancel", role: .cancel) { pendingDelete = nil }
        }
    }

    @ViewBuilder
    private var content: some View {
        switch vm.state {
        case .idle, .loading:
            ScrollView {
                VStack(spacing: Theme.s3) {
                    summaryStrip(loading: true)
                    ForEach(0..<3, id: \.self) { _ in
                        // `rMd`, not a bare 12, so the skeleton is on the radius
                        // scale. Worth knowing: the SlipCard it stands in for is
                        // `rLg`, so the placeholder is one step tighter than the
                        // real card — 12 -> rMd is a token migration at the same
                        // value, not a claim that the two now match.
                        RoundedRectangle(cornerRadius: Theme.rMd).fill(Theme.card).frame(height: 150)
                    }
                }
                .padding(Theme.s4)
            }
        case .failed(let m):
            ScrollView { ErrorStateView(message: m) { Task { await vm.load(client: model.client, model: model) } } }
        case .empty:
            ScrollView {
                VStack(spacing: Theme.s4) {
                    summaryStrip(loading: false)
                    EmptyStateView(systemImage: "tray",
                                   title: "No logged slips yet",
                                   message: "Build a slip on the Slip tab and log it, or enable Auto-Backtest in Account.")
                }
                .padding(Theme.s4)
            }
            .refreshable { await vm.load(client: model.client, model: model) }
        case .loaded:
            ScrollView {
                // s3 (12), not web's 16. `.bt-slips-grid{gap:16px}` is a *grid*
                // gutter doing double duty horizontally between three columns;
                // this is a single column of full-width cards, where 12 is enough
                // separation now that each card carries a 2pt outcome border.
                VStack(spacing: Theme.s3) {
                    summaryStrip(loading: false)
                    ForEach(vm.slips) { slip in
                        SlipCard(slip: slip, deleting: vm.deleting.contains(slip.id)) {
                            pendingDelete = slip
                        }
                    }
                }
                .padding(Theme.s4)
            }
            .refreshable { await vm.load(client: model.client, model: model) }
        }
    }

    private func summaryStrip(loading: Bool) -> some View {
        let s = vm.summary
        // 10 -> s3 (12): 10 sits exactly between s2 and s3, and the scale rule is
        // ties round up. Web's `.bt-summary{gap:10px}` is off its own scale here.
        return HStack(spacing: Theme.s3) {
            StatTile(label: "Slip hit rate",
                     value: s.slipHitRate.map { Fmt.percent($0, decimals: 0) } ?? "—",
                     tone: .neutral, loading: loading)
            StatTile(label: "Leg hit rate",
                     value: s.legHitRate.map { Fmt.percent($0, decimals: 0) } ?? "—",
                     tone: .neutral, loading: loading)
            StatTile(label: "Actual ROI",
                     value: s.roi.map { Fmt.signedPercent($0) } ?? "—",
                     tone: (s.roi ?? 0) >= 0 ? .good : .bad, loading: loading)
        }
    }
}
