import SwiftUI
import CorePropKit

/// A logged slip, tinted by its recomputed outcome (win green / loss red /
/// push amber / pending blue). Payout/hits are computed client-side from the
/// legs (`BacktestScoring`) so the payout table is the single ruler.
struct SlipCard: View {
    let slip: BacktestSlip
    let deleting: Bool
    let onDelete: () -> Void

    private var outcome: SlipOutcome { BacktestScoring.outcome(for: slip) }

    private var accent: Color {
        switch outcome.status {
        case .win:     return Theme.green
        case .loss:    return Theme.red
        case .push:    return Theme.amber
        case .pending: return Theme.pending
        }
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            header
            Divider().overlay(Theme.hair)
            ForEach(slip.legs) { leg in legRow(leg) }
            Divider().overlay(Theme.hair)
            footer
        }
        .padding(14)
        .background(
            LinearGradient(colors: [Theme.cardGradTop, Theme.cardGradBot], startPoint: .top, endPoint: .bottom)
        )
        .clipShape(RoundedRectangle(cornerRadius: 12, style: .continuous))
        .overlay(
            RoundedRectangle(cornerRadius: 12, style: .continuous).stroke(accent.opacity(0.55), lineWidth: 1.5)
        )
        .overlay(alignment: .leading) {
            Rectangle().fill(accent).frame(width: 3)
                .clipShape(RoundedRectangle(cornerRadius: 2))
        }
    }

    private var header: some View {
        HStack(spacing: 8) {
            Text(slip.slipTypeLabel.uppercased())
                .font(Theme.ui(11, .bold)).kerning(0.5).foregroundColor(Theme.text2)
            Text(Fmt.shortDate(slip.timestampDate))
                .font(Theme.mono(11)).foregroundColor(Theme.text3)
            Spacer()
            statusBadge
            Button(action: onDelete) {
                if deleting { ProgressView().controlSize(.mini) }
                else { Image(systemName: "trash").font(.system(size: 12)).foregroundColor(Theme.text3) }
            }
            .buttonStyle(.plain)
            .disabled(deleting)
            .accessibilityLabel("Delete slip")
        }
    }

    private var statusBadge: some View {
        let (text, fg, bg): (String, Color, Color) = {
            switch outcome.status {
            case .win:     return ("WIN", Color(hex: 0x86EFAC), Theme.greenHi)
            case .loss:    return ("LOSS", Color(hex: 0xFCA5A5), Theme.redHi)
            case .push:    return ("PUSH", Color(hex: 0xFDE68A), Theme.amber.opacity(0.14))
            case .pending: return ("PENDING", Color(hex: 0x93C5FD), Theme.pending.opacity(0.14))
            }
        }()
        return Text(text)
            .font(Theme.ui(9, .bold)).foregroundColor(fg)
            .padding(.horizontal, 7).padding(.vertical, 3)
            .background(bg).clipShape(Capsule())
    }

    private func legRow(_ leg: SlipLeg) -> some View {
        HStack(spacing: 8) {
            Text("\(leg.legNum ?? 0)")
                .font(Theme.mono(11, .bold)).foregroundColor(Theme.text3)
                .frame(width: 20)
            VStack(alignment: .leading, spacing: 1) {
                Text(leg.player ?? "—").font(Theme.ui(13, .semibold)).foregroundColor(Theme.text).lineLimit(1)
                HStack(spacing: 5) {
                    if !leg.sideLabel.isEmpty { SideBadge(side: leg.sideLabel) }
                    Text("\(leg.prop ?? "") \(Fmt.line(leg.line))")
                        .font(Theme.ui(11)).foregroundColor(Theme.text3).lineLimit(1)
                }
            }
            Spacer()
            if let pct = leg.truePct {
                Text(Fmt.percentValue(pct)).font(Theme.mono(12, .medium)).foregroundColor(Theme.text3)
            }
            legResultChip(leg.result)
        }
    }

    private func legResultChip(_ result: LegResult) -> some View {
        let (text, color): (String, Color) = {
            switch result {
            case .hit:     return ("HIT", Theme.green)
            case .miss:    return ("MISS", Color(hex: 0xFCA5A5))
            case .push:    return ("PUSH", Color(hex: 0xFDE68A))
            case .dnp:     return ("DNP", Theme.text3)
            case .pending: return ("•", Color(hex: 0x93C5FD))
            }
        }()
        return Text(text)
            .font(Theme.ui(9, .bold)).foregroundColor(color)
            .frame(width: 42)
    }

    private var footer: some View {
        HStack {
            footerMetric("PAYOUT", outcome.payout.map { String(format: "%.2f×", $0) } ?? "—")
            Spacer()
            footerMetric("HITS", outcome.hits.map { "\($0)/\(outcome.effectiveLegs)" } ?? "—")
            Spacer()
            footerMetric("PROJ EV", Fmt.signedPercent(slip.projSlipEvPct))
        }
    }

    private func footerMetric(_ label: String, _ value: String) -> some View {
        VStack(alignment: .leading, spacing: 2) {
            Text(label).font(Theme.ui(9, .semibold)).foregroundColor(Theme.text4)
            Text(value).font(Theme.mono(13, .semibold)).foregroundColor(Theme.text2)
        }
    }
}
