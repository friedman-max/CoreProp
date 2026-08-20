import SwiftUI
import Charts
import CorePropKit

/// Performance analytics from `/api/analytics`: a cumulative P&L timeline, a
/// calibration reliability curve, closing-line-value, and accuracy metrics —
/// the native counterpart of the web Analytics tab, rendered with Swift Charts.
struct AnalyticsView: View {
    @EnvironmentObject private var model: AppModel
    @State private var data: AnalyticsData?
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
                    EmptyStateView(systemImage: "chart.xyaxis.line", title: "No settled slips yet",
                                   message: "Performance appears once your logged slips resolve.")
                case .loaded:
                    if let d = data {
                        statGrid(d)
                        if let points = pnlPoints(d), points.count >= 1 { pnlCard(points) }
                        if d.populatedBuckets.count >= 2 { calibrationCard(d) }
                        clvCard(d)
                    }
                }
                Text("Metrics are computed from your resolved logged slips. The web app adds the full observatory and per-prop breakdowns.")
                    .font(Theme.ui(11)).foregroundColor(Theme.text3)
                    .multilineTextAlignment(.center).padding(.horizontal, 8)
            }
            .padding(16)
        }
        .background(Theme.bg.ignoresSafeArea())
        .navigationTitle("Performance")
        .navigationBarTitleDisplayMode(.inline)
        .task { await load() }
        .refreshable { await load() }
    }

    // MARK: Stat grid

    private func statGrid(_ d: AnalyticsData) -> some View {
        VStack(spacing: 10) {
            HStack(spacing: 10) {
                StatTile(label: "Hit rate", value: Fmt.percent(d.hitRate, decimals: 1))
                StatTile(label: "ROI / slip",
                         value: d.roiPerSlip.map { Fmt.signedPercent($0) } ?? "—",
                         tone: (d.roiPerSlip ?? 0) >= 0 ? .good : .bad)
            }
            HStack(spacing: 10) {
                StatTile(label: "Brier score", value: fmtNum(d.brierScore, 3))
                StatTile(label: "Log loss", value: fmtNum(d.logLoss, 3))
            }
            HStack(spacing: 10) {
                StatTile(label: "Resolved legs", value: d.nResolved.map { "\($0)" } ?? "—")
                StatTile(label: "Avg predicted", value: Fmt.percent(d.avgPredictedProb, decimals: 1))
            }
        }
    }

    // MARK: P&L timeline

    private func pnlPoints(_ d: AnalyticsData) -> [DatedPnl]? {
        let raw = (d.pnlTimeline ?? []).compactMap { p -> (Date, Double)? in
            guard let date = p.date, let cum = p.cumPnl else { return nil }
            return (date, cum)
        }.sorted { $0.0 < $1.0 }
        guard !raw.isEmpty else { return nil }
        // Index-based id so two slips settling in the same second can't collide.
        return raw.enumerated().map { DatedPnl(id: $0.offset, date: $0.element.0, cum: $0.element.1) }
    }

    private func pnlCard(_ points: [DatedPnl]) -> some View {
        let last = points.last?.cum ?? 0
        return VStack(alignment: .leading, spacing: 10) {
            HStack {
                Text("CUMULATIVE P&L").font(Theme.ui(10.5, .semibold)).kerning(0.6).foregroundColor(Theme.text3)
                Spacer()
                Text("\(last >= 0 ? "+" : "")\(String(format: "%.2f", last))u")
                    .font(Theme.mono(18, .bold))
                    .foregroundColor(last >= 0 ? Theme.green : Theme.red2)
            }
            Chart {
                ForEach(points) { p in
                    LineMark(x: .value("Date", p.date), y: .value("P&L", p.cum))
                        .foregroundStyle(Theme.primary2)
                        .interpolationMethod(.monotone)
                    AreaMark(x: .value("Date", p.date), y: .value("P&L", p.cum))
                        .foregroundStyle(LinearGradient(colors: [Theme.primary.opacity(0.25), .clear],
                                                        startPoint: .top, endPoint: .bottom))
                        .interpolationMethod(.monotone)
                }
                RuleMark(y: .value("Break-even", 0))
                    .foregroundStyle(Theme.text4)
                    .lineStyle(StrokeStyle(lineWidth: 1, dash: [4, 3]))
            }
            .chartYAxis { AxisMarks(position: .leading) }
            .frame(height: 180)
            Text("\(points.count) settled")
                .font(Theme.ui(11)).foregroundColor(Theme.text3)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .cpCard()
    }

    // MARK: Calibration reliability

    private func calibrationCard(_ d: AnalyticsData) -> some View {
        let buckets = d.populatedBuckets
        let points = buckets.compactMap { b -> CalPoint? in
            guard let p = b.predictedAvg, let a = b.actualAvg else { return nil }
            return CalPoint(predicted: p, actual: a)
        }
        return VStack(alignment: .leading, spacing: 10) {
            Text("CALIBRATION RELIABILITY").font(Theme.ui(10.5, .semibold)).kerning(0.6).foregroundColor(Theme.text3)
            Text("Predicted vs. actual hit rate per probability bucket. On the dashed line = perfectly calibrated.")
                .font(Theme.ui(11)).foregroundColor(Theme.text3)
            Chart {
                // Perfect-calibration reference (y = x).
                ForEach([CalPoint(predicted: 0.3, actual: 0.3), CalPoint(predicted: 1.0, actual: 1.0)]) { r in
                    LineMark(x: .value("Predicted", r.predicted), y: .value("Perfect", r.actual),
                             series: .value("s", "perfect"))
                        .foregroundStyle(Theme.text4)
                        .lineStyle(StrokeStyle(lineWidth: 1, dash: [4, 3]))
                }
                ForEach(points) { p in
                    LineMark(x: .value("Predicted", p.predicted), y: .value("Actual", p.actual),
                             series: .value("s", "actual"))
                        .foregroundStyle(Theme.primary2)
                        .interpolationMethod(.monotone)
                    PointMark(x: .value("Predicted", p.predicted), y: .value("Actual", p.actual))
                        .foregroundStyle(Theme.primary2)
                }
            }
            .chartXScale(domain: 0.3...1.0)
            .chartYScale(domain: 0.3...1.0)
            .frame(height: 200)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .cpCard()
    }

    // MARK: CLV

    private func clvCard(_ d: AnalyticsData) -> some View {
        VStack(alignment: .leading, spacing: 10) {
            Text("CLOSING LINE VALUE").font(Theme.ui(10.5, .semibold)).kerning(0.6).foregroundColor(Theme.text3)
            HStack(spacing: 12) {
                clvStat("CLV+ rate", Fmt.percent(d.clvPlusRate, decimals: 0))
                clvStat("Avg CLV", Fmt.signedPercent(d.avgClvPct))
                clvStat("Tracked", d.nClvTracked.map { "\($0)" } ?? "—")
            }
            if let cov = d.clvCoveragePct {
                Text("Closing line captured on \(Fmt.percent(cov, decimals: 0)) of your legs.")
                    .font(Theme.ui(11)).foregroundColor(Theme.text3)
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .cpCard()
    }

    private func clvStat(_ label: String, _ value: String) -> some View {
        VStack(spacing: 4) {
            Text(label).font(Theme.ui(9, .semibold)).kerning(0.4).foregroundColor(Theme.text3)
            Text(value).font(Theme.mono(16, .bold)).foregroundColor(Theme.text)
        }
        .frame(maxWidth: .infinity)
    }

    private func fmtNum(_ v: Double?, _ decimals: Int) -> String {
        guard let v else { return "—" }
        return String(format: "%.\(decimals)f", v)
    }

    private func load() async {
        if data == nil { state = .loading }
        do {
            let d = try await model.client.analytics()
            data = d
            state = d.hasResolvedData ? .loaded : .empty
        } catch let e as APIError {
            if e.isPaymentRequired { await model.handlePaymentRequired() }
            state = .failed(e.display)
        } catch {
            state = .failed(error.localizedDescription)
        }
    }

    private struct DatedPnl: Identifiable {
        let id: Int
        let date: Date
        let cum: Double
    }
    private struct CalPoint: Identifiable {
        let predicted: Double
        let actual: Double
        var id: Double { predicted }
    }
}
