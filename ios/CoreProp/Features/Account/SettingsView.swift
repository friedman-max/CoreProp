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

    // Auto-place (advanced). Arms the desktop extension to stage slips for a set
    // stake; the extension stages on PrizePicks and the user submits.
    @State private var apStatus: AutoPlaceStatus?
    @State private var apMode = "off"
    @State private var apStake: Double = 5
    @State private var apDailyCap: Double = 25
    @State private var apConsent = false
    @State private var apSaving = false
    @State private var apBanner: (text: String, error: Bool)?

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

            autoPlaceSection

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

    @ViewBuilder
    private var autoPlaceSection: some View {
        if let s = apStatus, s.disabledServerSide {
            Section("Auto-place") {
                Text("Auto-placement isn't enabled on this server.")
                    .font(Theme.ui(13)).foregroundColor(Theme.text3)
                    .listRowBackground(Theme.card)
            }
        } else if apStatus != nil {
            Section("Auto-place (advanced)") {
                Picker("Mode", selection: $apMode) {
                    Text("Off").tag("off")
                    Text("Paper (simulate)").tag("paper")
                    Text("Live").tag("live")
                }
                .listRowBackground(Theme.card)

                if apMode != "off" {
                    Stepper(value: $apStake, in: 1...100, step: 1) {
                        HStack { Text("Stake"); Spacer()
                            Text(Fmt.currency(apStake, maximumFractionDigits: 0))
                                .font(Theme.mono(14, .semibold)).foregroundColor(Theme.text) }
                    }
                    .listRowBackground(Theme.card)

                    VStack(alignment: .leading) {
                        HStack { Text("Daily cap"); Spacer()
                            Text(Fmt.currency(apDailyCap, maximumFractionDigits: 0))
                                .font(Theme.mono(14, .semibold)).foregroundColor(Theme.primary2) }
                        Slider(value: $apDailyCap, in: 0...500, step: 5)
                    }
                    .listRowBackground(Theme.card)

                    if apMode == "live" {
                        Label("Live arms placement for real money on your PrizePicks account.",
                              systemImage: "exclamationmark.triangle.fill")
                            .font(Theme.ui(12)).foregroundColor(Theme.amber)
                            .listRowBackground(Theme.card)
                    }
                    Toggle("I understand and consent", isOn: $apConsent)
                        .listRowBackground(Theme.card)
                }

                Button {
                    Task { await saveAutoPlace() }
                } label: {
                    HStack { Spacer()
                        if apSaving { ProgressView() } else { Text("Save auto-place").fontWeight(.semibold) }
                        Spacer() }
                }
                .disabled(apSaving || (apMode != "off" && !apConsent))
                .listRowBackground(Theme.card)

                if let b = apBanner {
                    Text(b.text).font(Theme.ui(13))
                        .foregroundColor(b.error ? Color(hex: 0xFCA5A5) : Theme.green)
                        .listRowBackground(Theme.card)
                }
            } footer: {
                Text("Placement runs in the CoreProp desktop browser extension, which stages the slip on PrizePicks for this stake — you review and submit each wager. Paper simulates without placing. The server caps every stake and disarms after repeated failures.")
            }
        }
    }

    private func saveAutoPlace() async {
        apSaving = true
        apBanner = nil
        defer { apSaving = false }
        let prefs = AutoPlacePrefsUpdate(
            mode: apMode,
            stake: apMode == "off" ? nil : apStake,
            dailyCap: apMode == "off" ? nil : apDailyCap,
            consent: apConsent)
        do {
            try await model.client.setAutoPlacePrefs(prefs)
            apBanner = ("Auto-place settings saved.", false)
            apStatus = try? await model.client.autoPlaceStatus()
        } catch let e as APIError {
            apBanner = (e.display, true)
        } catch {
            apBanner = (error.localizedDescription, true)
        }
    }

    private var legRange: ClosedRange<Int> { slipType == .flex ? 3...6 : 2...6 }

    private func load() async {
        // Auto-place status is independent of user_config (the server may have
        // the whole feature disabled). Load it first so the section renders.
        if let s = try? await model.client.autoPlaceStatus() {
            apStatus = s
            apMode = s.modeValue
            if let st = s.stake, st > 0 { apStake = st }
            if let dc = s.dailyCap, dc > 0 { apDailyCap = dc }
        }
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
