import SwiftUI
import CorePropKit

/// The Slip Builder / EV calculator. Reads the shared selection, computes
/// Power/Flex EV client-side (independence model, mirroring `ev-page.jsx`), and
/// offers a server "optimize" (correlation-aware) and "log to backtest".
struct SlipView: View {
    @EnvironmentObject private var model: AppModel
    @EnvironmentObject private var slip: SlipStore
    @StateObject private var vm = SlipViewModel()

    private var probs: [Double] { slip.trueProbs }
    private var n: Int { slip.count }

    var body: some View {
        NavigationStack {
            Group {
                if slip.bets.isEmpty { emptyState }
                else { builder }
            }
            .background(Theme.bg.ignoresSafeArea())
            .navigationTitle("Slip Builder")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                if !slip.bets.isEmpty {
                    ToolbarItem(placement: .navigationBarTrailing) {
                        Button("Clear") { slip.clear(); vm.clearServerResult() }
                            .foregroundColor(Theme.red2)
                    }
                }
            }
        }
    }

    private var emptyState: some View {
        EmptyStateView(systemImage: "rectangle.stack.badge.plus",
                       title: "Your slip is empty",
                       message: "Add legs from the Bets tab (tap the + on a bet), then build a Power or Flex slip here.")
    }

    private var builder: some View {
        ScrollView {
            // 14 -> s4 (16). 14 is equidistant from s3 and s4 and the scale rule is
            // ties round up, which is the same call the phase made for the 14pt
            // card padding.
            VStack(spacing: Theme.s4) {
                summaryCard
                legsCard
                if let sr = vm.serverResult { optimizeCard(sr) }
                actions
                Text("EV is an independence estimate mirroring the web calculator. \"Optimize\" runs the server's correlation-aware model.")
                    .font(Theme.ui(11)).foregroundColor(Theme.text3)
                    .multilineTextAlignment(.center).padding(.horizontal, Theme.s2)
            }
            .padding(Theme.s4)
        }
        .safeAreaInset(edge: .bottom) { bannerView }
    }

    // MARK: Summary

    private var summaryCard: some View {
        let power = SlipEV.powerEV(probs)
        let flex = SlipEV.flexEV(probs)
        let combined = probs.reduce(1.0, *)
        let be = SlipEV.breakEven(n: n, type: vm.slipType)
        return VStack(spacing: Theme.s4) {
            Picker("Slip type", selection: $vm.slipType) {
                Text("Power").tag(SlipType.power)
                Text("Flex").tag(SlipType.flex)
            }
            .pickerStyle(.segmented)

            HStack(spacing: Theme.s3) {
                statBlock("LEGS", "\(n)", Theme.text)
                statBlock("ALL-HIT", Fmt.percent(combined), Theme.text)
                statBlock("BREAK-EVEN/LEG", Fmt.percent(be), Theme.primary2)
            }

            HStack(spacing: Theme.s3) {
                evBlock("POWER EV", power, highlight: vm.slipType == .power)
                evBlock("FLEX EV", flex, highlight: vm.slipType == .flex)
            }
        }
        .cpCard()
    }

    // `statBlock`'s and `evBlock`'s labels stay UPPERCASE. Web de-capsed its *tile*
    // labels (.bt-card-label)
    // because an all-caps label competes with the 22px number it introduces, but
    // LEGS / ALL-HIT / POWER EV are section labels over a numeric block, which is
    // web's kept-caps case. What changes is the API: tracking, not kerning —
    // tracking is letter-spacing, kerning adjusts specific glyph pairs. Web's
    // micro-label rate is .04em, which at 9pt is 0.36, not the flat 0.5 that was
    // here for both sizes.
    private func statBlock(_ label: String, _ value: String, _ color: Color) -> some View {
        VStack(spacing: Theme.s1) {
            Text(label).font(Theme.ui(9, .semibold)).tracking(0.36).foregroundColor(Theme.text3)
                .multilineTextAlignment(.center)
            Text(value).font(Theme.mono(16, .bold)).foregroundColor(color)
        }
        .frame(maxWidth: .infinity)
    }

    private func evBlock(_ label: String, _ ev: Double?, highlight: Bool) -> some View {
        // `text4` is legitimate here and only here: this is the *disabled* site —
        // there is no EV to show — not muted text a user is meant to read.
        let color: Color = ev == nil ? Theme.text4 : (ev! >= 0 ? Theme.green : Theme.red2)
        return VStack(spacing: Theme.s1) {
            Text(label).font(Theme.ui(9, .semibold)).tracking(0.36).foregroundColor(Theme.text3)
            Text(ev == nil ? "n/a" : Fmt.signedPercent(ev)).font(Theme.mono(18, .bold)).foregroundColor(color)
        }
        .frame(maxWidth: .infinity)
        // 10 -> s3 (12): ties round up.
        .padding(.vertical, Theme.s3)
        .background(highlight ? Theme.primaryHi : Color.clear)
        .clipShape(RoundedRectangle(cornerRadius: Theme.radiusSm, style: .continuous))
    }

    // MARK: Legs

    private var legsCard: some View {
        // A structural zero, deliberately off the spacing scale: the legs are
        // separated by the `Divider()` below, and any gap here would double up
        // with the rows' own vertical padding and float the rules off-centre.
        VStack(spacing: 0) {
            ForEach(Array(slip.bets.enumerated()), id: \.element.betId) { idx, bet in
                // 10 -> s3 (12): ties round up.
                HStack(spacing: Theme.s3) {
                    Text("\(idx + 1)")
                        .font(Theme.mono(12, .bold)).foregroundColor(Theme.primary2)
                        .frame(width: 24, height: 24)
                        .background(Theme.primaryHi).clipShape(Circle())
                    // 2 -> s1 and 6 -> s2 (ties round up), the same pair of calls
                    // SlipCard's leg row makes for the same name-over-prop shape.
                    VStack(alignment: .leading, spacing: Theme.s1) {
                        Text(bet.playerName).font(Theme.ui(14, .semibold)).foregroundColor(Theme.text).lineLimit(1)
                        HStack(spacing: Theme.s2) {
                            SideBadge(side: bet.sideLabel)
                            Text("\(bet.propType) \(Fmt.line(bet.ppLine))")
                                .font(Theme.ui(12)).foregroundColor(Theme.text2).lineLimit(1)
                        }
                    }
                    Spacer()
                    Text(Fmt.percentValue(bet.truePct)).font(Theme.mono(13, .bold)).foregroundColor(Theme.primary2)
                    Button { slip.remove(bet); vm.clearServerResult() } label: {
                        Image(systemName: "xmark").font(.system(size: 11, weight: .bold))
                            .foregroundColor(Theme.text3).frame(width: 26, height: 26)
                            .background(Theme.controlBg).clipShape(Circle())
                    }
                    .buttonStyle(.plain)
                    .accessibilityLabel("Remove \(bet.playerName) from slip")
                }
                .padding(.vertical, Theme.s2)
                if idx < slip.bets.count - 1 { Divider().overlay(Theme.hair) }
            }
        }
        .cpCard()
    }

    // MARK: Server optimize result

    private func optimizeCard(_ sr: SlipResult) -> some View {
        VStack(alignment: .leading, spacing: Theme.s2) {
            HStack {
                Text("CORRELATION-AWARE (SERVER)")
                    // tracking, not kerning, at the same value: 0.6 at 10pt is
                    // .06em, which is what BookBadgeView already tracks at 10pt.
                    // There is no web twin for this label to source a rate from,
                    // so this is an API fix, not a value change.
                    .font(Theme.ui(10, .semibold)).tracking(0.6).foregroundColor(Theme.text3)
                Spacer()
                if let k = sr.optimalK { Text("best \(k)-leg").font(Theme.mono(11)).foregroundColor(Theme.text3) }
            }
            HStack(spacing: Theme.s3) {
                statBlock("BEST PLAY", sr.bestPlayType ?? "—", Theme.text)
                evBlock("BEST EV", sr.bestEvPct, highlight: true)
            }
            if let ids = sr.optimalBetIds, ids.count < n {
                Text("The server's best subset drops \(n - ids.count) leg(s). Apply to trim your slip.")
                    .font(Theme.ui(11)).foregroundColor(Theme.text3)
                Button("Apply server's best subset") { applyOptimal(ids) }
                    .buttonStyle(GhostButtonStyle(fullWidth: true))
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .cpCard()
    }

    private func applyOptimal(_ ids: [String]) {
        let keep = Set(ids)
        for bet in slip.bets where !keep.contains(bet.betId) { slip.remove(bet) }
    }

    // MARK: Actions

    private var actions: some View {
        // 10 -> s3 (12): ties round up.
        VStack(spacing: Theme.s3) {
            Button {
                Task { await vm.optimize(betIds: slip.betIds, client: model.client, model: model) }
            } label: {
                if vm.optimizing { ProgressView().tint(Theme.text) }
                else { Label("Optimize (server)", systemImage: "wand.and.stars") }
            }
            .buttonStyle(GhostButtonStyle(fullWidth: true))
            .disabled(vm.optimizing || n < 2)

            Button {
                Task {
                    let ok = await vm.log(betIds: slip.betIds, client: model.client, model: model)
                    if ok { slip.clear(); vm.clearServerResult() }
                }
            } label: {
                if vm.logging { ProgressView().tint(.white) }
                else { Label("Log \(vm.slipType.apiValue) slip to backtest", systemImage: "tray.and.arrow.down") }
            }
            .buttonStyle(PrimaryButtonStyle())
            .disabled(vm.logging || !(2...6).contains(n))
        }
    }

    @ViewBuilder
    private var bannerView: some View {
        if let banner = vm.banner {
            HStack(spacing: Theme.s2) {
                Image(systemName: banner.isError ? "exclamationmark.triangle" : "checkmark.circle")
                Text(banner.text).font(Theme.ui(13, .medium))
                Spacer()
                Button { vm.banner = nil } label: { Image(systemName: "xmark") }
            }
            .foregroundColor(banner.isError ? Theme.red2 : Theme.green)
            .padding(Theme.s3)
            .background(banner.isError ? Theme.redHi : Theme.greenHi)
            .clipShape(RoundedRectangle(cornerRadius: Theme.radiusSm, style: .continuous))
            // s4 matches the builder's own page gutter, so the banner lines up with
            // the cards it floats over instead of sitting 16 in from a 16 gutter.
            .padding(.horizontal, Theme.s4).padding(.bottom, Theme.s2)
        }
    }
}
