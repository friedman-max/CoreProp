import SwiftUI
import CorePropKit

/// Auto-Backtest & slip preferences. Reads `/api/config`, writes via
/// `/api/user/auto-backtest` and `/api/user/slip-prefs`. Validation mirrors the
/// backend: legs 2–6, Flex needs ≥3, min prob strictly in (0,1).
struct SettingsView: View {
    @EnvironmentObject private var model: AppModel

    @State private var loaded = false
    @State private var autoBacktest = false
    @State private var slipType: SlipType = .power
    @State private var legs = 6
    @State private var useCustomMinProb = false
    @State private var minProb = 0.65
    @State private var greenDevils = false
    @State private var saving = false
    @State private var banner: (text: String, error: Bool)?

    var body: some View {
        Form {
            Section("Auto-Backtest") {
                Toggle("Auto-log slips each refresh", isOn: $autoBacktest)
                    .onChange(of: autoBacktest) { newValue in Task { await saveAutoBacktest(newValue) } }
                    .listRowBackground(Theme.card)
            } footer: {
                Text("When on, the server builds and logs a slip for you every refresh, above your minimum leg probability.")
            }

            Section("Slip preferences") {
                Picker("Type", selection: $slipType) {
                    Text("Power").tag(SlipType.power)
                    Text("Flex").tag(SlipType.flex)
                }
                .onChange(of: slipType) { newValue in
                    if newValue == .flex, legs < 3 { legs = 3 }
                }
                .listRowBackground(Theme.card)

                Stepper(value: $legs, in: legRange) {
                    HStack { Text("Legs"); Spacer(); Text("\(legs)").font(Theme.mono(14, .semibold)).foregroundColor(Theme.text) }
                }
                .listRowBackground(Theme.card)

                Toggle("Custom minimum leg probability", isOn: $useCustomMinProb)
                    .listRowBackground(Theme.card)
                if useCustomMinProb {
                    VStack(alignment: .leading) {
                        HStack { Text("Minimum"); Spacer(); Text(Fmt.percent(minProb)).font(Theme.mono(14, .semibold)).foregroundColor(Theme.primary2) }
                        Slider(value: $minProb, in: 0.55...0.90, step: 0.01)
                    }
                    .listRowBackground(Theme.card)
                }

                Toggle("Also auto-log green devils (goblins)", isOn: $greenDevils)
                    .listRowBackground(Theme.card)
            } footer: {
                Text("Flex slips require at least 3 legs. The server enforces a probability floor on auto-logged slips.")
            }

            Section {
                Button {
                    Task { await savePrefs() }
                } label: {
                    HStack {
                        Spacer()
                        if saving { ProgressView() } else { Text("Save preferences").fontWeight(.semibold) }
                        Spacer()
                    }
                }
                .disabled(saving)
                .listRowBackground(Theme.primary)
                .foregroundColor(.white)
                if let banner {
                    Text(banner.text).font(Theme.ui(13))
                        .foregroundColor(banner.error ? Color(hex: 0xFCA5A5) : Theme.green)
                        .listRowBackground(Theme.card)
                }
            }
        }
        .scrollContentBackground(.hidden)
        .background(Theme.bg.ignoresSafeArea())
        .navigationTitle("Automation")
        .navigationBarTitleDisplayMode(.inline)
        .task { if !loaded { await load() } }
    }

    private var legRange: ClosedRange<Int> { slipType == .flex ? 3...6 : 2...6 }

    private func load() async {
        guard let cfg = try? await model.client.userConfig() else { loaded = true; return }
        autoBacktest = cfg.autoBacktest ?? false
        if let t = cfg.autoSlipType, let st = SlipType(rawValue: t) { slipType = st }
        legs = min(6, max(2, cfg.autoSlipLegs ?? 6))
        if slipType == .flex, legs < 3 { legs = 3 }
        if let mp = cfg.autoSlipMinProb { useCustomMinProb = true; minProb = mp }
        greenDevils = cfg.autoBacktestGreenDevils ?? false
        loaded = true
    }

    private func saveAutoBacktest(_ on: Bool) async {
        guard loaded else { return }
        try? await model.client.setAutoBacktest(on)
    }

    private func savePrefs() async {
        saving = true
        banner = nil
        defer { saving = false }
        let prefs = SlipPrefsUpdate(
            autoSlipType: slipType.apiValue,
            autoSlipLegs: legs,
            autoSlipMinProb: useCustomMinProb ? minProb : nil,
            autoBacktestGreenDevils: greenDevils)
        do {
            try await model.client.setSlipPrefs(prefs)
            banner = ("Preferences saved.", false)
        } catch let e as APIError {
            banner = (e.display, true)
        } catch {
            banner = (error.localizedDescription, true)
        }
    }
}
