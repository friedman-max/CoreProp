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
                            .foregroundColor(Color(hex: 0xFCA5A5))
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
            VStack(spacing: 14) {
                summaryCard
                legsCard
                if let sr = vm.serverResult { optimizeCard(sr) }
                actions
                Text("EV is an independence estimate mirroring the web calculator. \"Optimize\" runs the server's correlation-aware model.")
                    .font(Theme.ui(11)).foregroundColor(Theme.text3)
                    .multilineTextAlignment(.center).padding(.horizontal, 8)
            }
            .padding(16)
        }
        .safeAreaInset(edge: .bottom) { bannerView }
    }

    // MARK: Summary

    private var summaryCard: some View {
        let power = SlipEV.powerEV(probs)
        let flex = SlipEV.flexEV(probs)
        let combined = probs.reduce(1.0, *)
        let be = SlipEV.breakEven(n: n, type: vm.slipType)
        return VStack(spacing: 14) {
            Picker("Slip type", selection: $vm.slipType) {
                Text("Power").tag(SlipType.power)
                Text("Flex").tag(SlipType.flex)
            }
            .pickerStyle(.segmented)

            HStack(spacing: 12) {
                statBlock("LEGS", "\(n)", Theme.text)
                statBlock("ALL-HIT", Fmt.percent(combined), Theme.text)
                statBlock("BREAK-EVEN/LEG", Fmt.percent(be), Theme.primary2)
            }

            HStack(spacing: 12) {
                evBlock("POWER EV", power, highlight: vm.slipType == .power)
                evBlock("FLEX EV", flex, highlight: vm.slipType == .flex)
            }
        }
        .cpCard()
    }

    private func statBlock(_ label: String, _ value: String, _ color: Color) -> some View {
        VStack(spacing: 4) {
            Text(label).font(Theme.ui(9, .semibold)).kerning(0.5).foregroundColor(Theme.text3)
                .multilineTextAlignment(.center)
            Text(value).font(Theme.mono(16, .bold)).foregroundColor(color)
        }
        .frame(maxWidth: .infinity)
    }

    private func evBlock(_ label: String, _ ev: Double?, highlight: Bool) -> some View {
        let color: Color = ev == nil ? Theme.text4 : (ev! >= 0 ? Theme.green : Color(hex: 0xFCA5A5))
        return VStack(spacing: 4) {
            Text(label).font(Theme.ui(9, .semibold)).kerning(0.5).foregroundColor(Theme.text3)
            Text(ev == nil ? "n/a" : Fmt.signedPercent(ev)).font(Theme.mono(18, .bold)).foregroundColor(color)
        }
        .frame(maxWidth: .infinity)
        .padding(.vertical, 10)
        .background(highlight ? Theme.primaryHi : Color.clear)
        .clipShape(RoundedRectangle(cornerRadius: Theme.radiusSm, style: .continuous))
    }

    // MARK: Legs

    private var legsCard: some View {
        VStack(spacing: 0) {
            ForEach(Array(slip.bets.enumerated()), id: \.element.betId) { idx, bet in
                HStack(spacing: 10) {
                    Text("\(idx + 1)")
                        .font(Theme.mono(12, .bold)).foregroundColor(Theme.primary2)
                        .frame(width: 24, height: 24)
                        .background(Theme.primaryHi).clipShape(Circle())
                    VStack(alignment: .leading, spacing: 2) {
                        Text(bet.playerName).font(Theme.ui(14, .semibold)).foregroundColor(Theme.text).lineLimit(1)
                        HStack(spacing: 6) {
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
                }
                .padding(.vertical, 8)
                if idx < slip.bets.count - 1 { Divider().overlay(Theme.hair) }
            }
        }
        .cpCard()
    }

    // MARK: Server optimize result

    private func optimizeCard(_ sr: SlipResult) -> some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack {
                Text("CORRELATION-AWARE (SERVER)")
                    .font(Theme.ui(10, .semibold)).kerning(0.6).foregroundColor(Theme.text3)
                Spacer()
                if let k = sr.optimalK { Text("best \(k)-leg").font(Theme.mono(11)).foregroundColor(Theme.text3) }
            }
            HStack(spacing: 12) {
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
        VStack(spacing: 10) {
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
            HStack(spacing: 8) {
                Image(systemName: banner.isError ? "exclamationmark.triangle" : "checkmark.circle")
                Text(banner.text).font(Theme.ui(13, .medium))
                Spacer()
                Button { vm.banner = nil } label: { Image(systemName: "xmark") }
            }
            .foregroundColor(banner.isError ? Color(hex: 0xFCA5A5) : Theme.green)
            .padding(12)
            .background(banner.isError ? Theme.redHi : Theme.greenHi)
            .clipShape(RoundedRectangle(cornerRadius: Theme.radiusSm, style: .continuous))
            .padding(.horizontal, 16).padding(.bottom, 8)
        }
    }
}
