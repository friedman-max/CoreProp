import SwiftUI
import CorePropKit

/// A logged slip, tinted by its recomputed outcome (win green / loss red /
/// push #FBBF24 / pending blue). Payout/hits are computed client-side from the
/// legs (`BacktestScoring`) so the payout table is the single ruler.
///
/// Three things here are already right and must stay that way — web only reached
/// them in Phase 2b, iOS was there first:
/// * **ONE** left bar. Web carried a 4px `inset` bar underneath the 3px
///   `::before` bar, two stacked bars of different widths and colours. This is a
///   single overlay; do not add a second.
/// * A **flat** accent — solid bar, solid border, no gradient and no
///   outcome-coloured blurred glow.
/// * **Blue** pending. Pending means "not settled yet"; amber implies caution,
///   and push is the outcome that legitimately reads amber.
struct SlipCard: View {
    let slip: BacktestSlip
    let deleting: Bool
    let onDelete: () -> Void

    private var outcome: SlipOutcome { BacktestScoring.outcome(for: slip) }

    private var accent: Color {
        switch outcome.status {
        case .win:     return Theme.green
        case .loss:    return Theme.red
        // `Theme.push` (#FBBF24), not `Theme.amber` (#F59E0B): amber is the
        // warning colour, and a pushed slip is an outcome, not a warning. Web
        // has always used two different hues here; iOS conflated them.
        case .push:    return Theme.push
        case .pending: return Theme.pending
        }
    }

    /// Web's per-outcome card fill and border (`.bt-slip-compact.is-*`), which
    /// iOS lacked entirely: every card was neutral grey behind one faint
    /// `accent.opacity(0.55)` hairline, so a losing slip announced itself only
    /// through a 3pt bar and a badge. Web tints the whole card so that a loss
    /// reads as "RED" from across the grid, and does it flat.
    ///
    /// These four pairs are web's verbatim and are deliberately NOT one ramp:
    /// loss is pushed hardest (.22 fill / .75 border) because a miss is the thing
    /// you must not skim past, and push is the softest (.08 / .40) because a push
    /// is a non-event. Don't regularise them.
    private var surface: (fill: Color, border: Color) {
        switch outcome.status {
        case .win:     return (Theme.green.opacity(0.16),   Theme.green.opacity(0.65))
        case .loss:    return (Theme.red.opacity(0.22),     Theme.red.opacity(0.75))
        case .push:    return (Theme.push.opacity(0.08),    Theme.push.opacity(0.40))
        case .pending: return (Theme.pending.opacity(0.14), Theme.pending.opacity(0.60))
        }
    }

    /// One shape, used by both the clip and the border. The radius used to be
    /// typed as a bare `12` at two call sites, which is exactly how a card ends
    /// up clipped at one radius and stroked at another.
    private var shape: RoundedRectangle {
        RoundedRectangle(cornerRadius: Theme.rLg, style: .continuous)
    }

    var body: some View {
        VStack(alignment: .leading, spacing: Theme.s3) {
            header
            Divider().overlay(Theme.hair)
            ForEach(slip.legs) { leg in legRow(leg) }
            Divider().overlay(Theme.hair)
            footer
        }
        // Web is `padding:var(--s-4) var(--s-5)` — wider at the sides than top
        // and bottom, because the left bar eats into the leading gutter.
        .padding(.vertical, Theme.s4)
        .padding(.horizontal, Theme.s5)
        .background {
            // Flat `card` under a flat outcome tint. The two-stop
            // `cardGradTop`/`cardGradBot` gradient that used to be the whole
            // background is gone; those two constants stay in `Theme` but are
            // documented there as unused, so don't route back through them.
            ZStack {
                Theme.card
                surface.fill
            }
        }
        // The single left bar, painted *inside* the clip so its ends follow the
        // corner curve — that is what `overflow:hidden` on web's .bt-slip does.
        // It used to be overlaid after the clip with its own `cornerRadius: 2`,
        // which rounded a 3pt sliver by 2pt (invisible) while still letting the
        // bar shoot straight past both rounded corners; the overshoot only got
        // worse when the radius went 12 -> 16. The 2pt clip has no web analogue
        // and is dropped.
        .overlay(alignment: .leading) { Rectangle().fill(accent).frame(width: 3) }
        .clipShape(shape)
        // 2pt, matching web's `border-width:2px`. `strokeBorder`, not `stroke`,
        // so the whole 2pt lands *inside* the shape the way a CSS border does — a
        // centred 2pt `stroke` puts half of it outside the fill and reads as 1pt,
        // which is how the old 1.5pt stroke was really rendering at ~0.75pt.
        .overlay(shape.strokeBorder(surface.border, lineWidth: 2))
    }

    private var header: some View {
        HStack(spacing: Theme.s2) {
            Text(slip.slipTypeLabel.uppercased())
                // tracking, not kerning: tracking is the letter-spacing analogue,
                // kerning adjusts specific glyph pairs. .04em at 11pt is 0.44 —
                // the same arithmetic LeaguePill uses at the same size.
                .font(Theme.ui(11, .bold)).tracking(0.44).foregroundColor(Theme.text2)
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
            case .win:     return ("WIN", Theme.green2, Theme.greenHi)
            case .loss:    return ("LOSS", Theme.red2, Theme.redHi)
            case .push:    return ("PUSH", Theme.amber2, Theme.push.opacity(0.14))
            case .pending: return ("PENDING", Theme.blue2, Theme.pending.opacity(0.14))
            }
        }()
        return Text(text)
            .font(Theme.ui(9, .bold)).foregroundColor(fg)
            // 7 -> s2 and 3 -> s1. Web pads this badge `var(--s-1) 10px`; the 10px
            // is off web's own scale, and 8 is the nearest step to iOS's 7, so the
            // badge lands on the scale without ballooning.
            .padding(.horizontal, Theme.s2).padding(.vertical, Theme.s1)
            .background(bg).clipShape(Capsule())
    }

    private func legRow(_ leg: SlipLeg) -> some View {
        HStack(spacing: Theme.s2) {
            Text("\(leg.legNum ?? 0)")
                .font(Theme.mono(11, .bold)).foregroundColor(Theme.text3)
                .frame(width: 20)
            // 1 -> s1 (4). Web separates the same two lines by
            // `.bt-leg-prop{margin-top:3px}`, so 4 is both the nearest scale step
            // and closer to web than the 1 it replaces.
            VStack(alignment: .leading, spacing: Theme.s1) {
                Text(leg.player ?? "—").font(Theme.ui(13, .semibold)).foregroundColor(Theme.text).lineLimit(1)
                // 5 -> s2 (8), which is web's own `.bt-leg-prop{gap:8px}` for
                // this row rather than the 4 that bare nearest-step would give.
                HStack(spacing: Theme.s2) {
                    if !leg.sideLabel.isEmpty { SideBadge(side: leg.sideLabel) }
                    Text("\(leg.prop ?? "") \(Fmt.line(leg.line))")
                        .font(Theme.ui(11)).foregroundColor(Theme.text3).lineLimit(1)
                }
            }
            Spacer()
            if let pct = leg.truePct { legTruePct(pct) }
            legResultChip(leg.result)
        }
    }

    /// Web's `.bt-leg-pct`: `--primary-2` text on a `--primary-lo` fill inside a
    /// `--primary-hi` border. iOS had this as bare `text3`, already the quietest
    /// thing in the row — and it got quieter still once the card behind it gained
    /// an outcome tint, because muted grey on a red `.22` fill reads worse than
    /// the same grey on neutral card. This is the one number on a logged slip that
    /// carries the model's judgement, so it gets promoted the way web promoted it.
    ///
    /// The explicit `foregroundColor(Theme.primary2)` is load-bearing rather than
    /// decorative: `primaryHi` (.22) behind *inherited* muted text measures 4.45:1
    /// and fails AA, which is why `primaryLo` exists at all and why web's twin
    /// states its colour instead of inheriting one.
    private func legTruePct(_ pct: Double) -> some View {
        Text(Fmt.percentValue(pct))
            .font(Theme.mono(12, .bold))
            .foregroundColor(Theme.primary2)
            // 2pt vertical is web's own sub-step value for this chip
            // (`padding:2px 7px`) and is what keeps it visibly smaller than the
            // result pill beside it, which web pads at 6px. Rounding it to s1
            // would erase that hierarchy.
            .padding(.vertical, 2)
            .padding(.horizontal, Theme.s2)
            .frame(minWidth: 50)
            .background(Theme.primaryLo)
            .clipShape(RoundedRectangle(cornerRadius: Theme.rSm, style: .continuous))
            .overlay(
                RoundedRectangle(cornerRadius: Theme.rSm, style: .continuous)
                    .strokeBorder(Theme.primaryHi, lineWidth: 1)
            )
    }

    private func legResultChip(_ result: LegResult) -> some View {
        let (text, color): (String, Color) = {
            switch result {
            case .hit:     return ("HIT", Theme.green)
            case .miss:    return ("MISS", Theme.red2)
            case .push:    return ("PUSH", Theme.amber2)
            case .dnp:     return ("DNP", Theme.text3)
            case .pending: return ("•", Theme.blue2)
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
        // 2pt is web's own `.bt-slip-foot-c{gap:2px}` — a label bound to the number
        // directly under it, not a gap between two things. s1 would unbind them.
        VStack(alignment: .leading, spacing: 2) {
            // `text3`, not `text4`. `text4` is 2.3:1 and is for decorative or
            // disabled glyphs; PAYOUT / HITS / PROJ EV are the labels a user reads
            // to know which of the three numbers is which.
            Text(label).font(Theme.ui(9, .semibold)).foregroundColor(Theme.text3)
            Text(value).font(Theme.mono(13, .semibold)).foregroundColor(Theme.text2)
        }
    }
}
