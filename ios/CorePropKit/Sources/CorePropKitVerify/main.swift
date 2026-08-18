import Foundation
import CorePropKit

// A tiny assertion harness. XCTest is unavailable under bare Command Line
// Tools, so this executable is the runnable verification of CorePropKit:
//   cd ios/CorePropKit && swift run CorePropKitVerify
// It exits non-zero if any check fails.

var failures = 0
var passes = 0

func check(_ cond: Bool, _ msg: String) {
    if cond { passes += 1 }
    else { failures += 1; print("  ✗ FAIL: \(msg)") }
}

func approx(_ a: Double, _ b: Double, _ tol: Double = 1e-9) -> Bool { abs(a - b) <= tol }

func section(_ name: String, _ body: () -> Void) {
    print("• \(name)")
    body()
}

let decoder = JSONDecoder.coreProp()
func decode<T: Decodable>(_ type: T.Type, _ json: String) -> T? {
    guard let data = json.data(using: .utf8) else { return nil }
    do { return try decoder.decode(T.self, from: data) }
    catch { print("  ✗ decode \(T.self) error: \(error)"); return nil }
}

// MARK: - Payout table re-derivation (mirrors test_payout_table_mirror.py)

section("Payout break-even re-derivation") {
    // Power: closed form p = (1/payout)^(1/n).
    for n in Payouts.powerSizes {
        let payout = Payouts.power[n]!
        let derived = pow(1.0 / payout, 1.0 / Double(n))
        let stored = SlipEV.breakEven(n: n, type: .power)
        check(abs(derived - stored) < 0.0006,
              "Power-\(n) break-even: stored \(stored) vs derived \(derived)")
    }
    // Flex: bisection for p where flexEV([p]*n) == 0.
    for n in Payouts.flexSizes {
        func evAt(_ p: Double) -> Double { SlipEV.flexEV(Array(repeating: p, count: n))! }
        var lo = 0.30, hi = 0.80
        check(evAt(lo) < 0 && evAt(hi) > 0, "Flex-\(n) EV brackets zero on [\(lo),\(hi)]")
        for _ in 0..<80 {
            let mid = (lo + hi) / 2
            if evAt(mid) < 0 { lo = mid } else { hi = mid }
        }
        let derived = (lo + hi) / 2
        let stored = SlipEV.breakEven(n: n, type: .flex)
        check(abs(derived - stored) < 0.0006,
              "Flex-\(n) break-even: stored \(stored) vs derived \(derived)")
    }
    // The tables themselves match engine/constants.py exactly.
    check(Payouts.power == [2: 3.0, 3: 6.0, 4: 10.0, 5: 20.0, 6: 37.5], "POWER_PAYOUTS mirror")
    check(Payouts.flex[6]?[6] == 25.0 && Payouts.flex[5]?[3] == 0.4, "FLEX_PAYOUTS spot-check")
}

// MARK: - Slip EV

section("Slip EV formulas") {
    // powerEV = ∏p·payout − 1.
    check(approx(SlipEV.powerEV([0.6, 0.6])!, 0.6 * 0.6 * 3.0 - 1.0), "power-2 EV")
    check(SlipEV.powerEV([0.6]) == nil, "power-1 unsupported → nil")
    check(SlipEV.powerEV([0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5]) == nil, "power-7 unsupported → nil")
    // Break-even legs → EV ≈ 0.
    check(abs(SlipEV.powerEV([0.5774, 0.5774])!) < 0.001, "power-2 at break-even ≈ 0")
    check(abs(SlipEV.flexEV([0.5774, 0.5774, 0.5774])!) < 0.001, "flex-3 at break-even ≈ 0")
    // Flex n==2 short-circuits to Power-2.
    check(SlipEV.flexEV([0.6, 0.6]) == SlipEV.powerEV([0.6, 0.6]), "flex-2 == power-2 (short-circuit)")
    // Full-hit 3-flex EV = Σ P(k)·payout − 1, hand check with equal p.
    let p = 0.7, n = 3
    var expected = -1.0
    for mask in 0..<(1 << n) {
        var prob = 1.0, k = 0
        for i in 0..<n { if (mask >> i) & 1 == 1 { prob *= p; k += 1 } else { prob *= (1 - p) } }
        if let pay = Payouts.flex[n]?[k] { expected += prob * pay }
    }
    check(approx(SlipEV.flexEV([p, p, p])!, expected, 1e-12), "flex-3 enumeration matches hand calc")
    // best() tie-break prefers Power.
    if let b = SlipEV.best([0.6, 0.6]) { check(b.type == .power, "best() tie prefers Power") }
    else { check(false, "best() returned nil for 2 legs") }
    // scoreLeg = p/breakEven − 1.
    check(approx(SlipEV.scoreLeg(0.60, n: 6, type: .power),
                 0.60 / SlipEV.breakEven(n: 6, type: .power) - 1.0), "scoreLeg power-6")
}

// MARK: - Backtest scoring (mirrors btComputeSlipOutcome)

section("Backtest scoring") {
    func leg(_ r: String) -> SlipLeg {
        SlipLeg(slipId: "S", legNum: 1, player: "P", league: "NBA", prop: "Points",
                line: 1.5, side: "over", trueProb: 0.6, resultRaw: r, statActual: nil, gameStart: nil)
    }
    // Power 3/3 hit → payout 6.0, win.
    let power3 = BacktestScoring.outcome(legs: [leg("hit"), leg("hit"), leg("hit")], slipType: "Power")
    check(power3.completed && power3.payout == 6.0 && power3.hits == 3 && power3.isWin, "Power 3/3 → 6.0 win")
    // Power with a miss → 0, loss.
    let powerMiss = BacktestScoring.outcome(legs: [leg("hit"), leg("miss"), leg("hit")], slipType: "Power")
    check(powerMiss.payout == 0 && powerMiss.status == .loss, "Power 2/3 → loss")
    // Flex 4/5 → 2.0.
    let flex5 = BacktestScoring.outcome(
        legs: [leg("hit"), leg("hit"), leg("hit"), leg("hit"), leg("miss")], slipType: "Flex")
    check(flex5.payout == 2.0 && flex5.hits == 4 && flex5.effectiveLegs == 5, "Flex 4/5 → 2.0")
    // Push drops out: 3-leg flex hit,hit,push → effective 2 → power-2 3.0.
    let withPush = BacktestScoring.outcome(legs: [leg("hit"), leg("hit"), leg("push")], slipType: "Flex")
    check(withPush.effectiveLegs == 2 && withPush.payout == 3.0, "push drops leg → power-2 3.0")
    // All void → refund 1.0.
    let allVoid = BacktestScoring.outcome(legs: [leg("push"), leg("dnp")], slipType: "Power")
    check(allVoid.payout == 1.0 && allVoid.effectiveLegs == 0, "all void → refund")
    // Pending → not completed.
    let pending = BacktestScoring.outcome(legs: [leg("hit"), leg("pending")], slipType: "Power")
    check(!pending.completed && pending.status == .pending, "pending → not completed")
}

// MARK: - Decoding real backend JSON

section("Decoding — Bet + CoreBundle") {
    let betJSON = """
    {"bet_id":"NBA_LeBron_Points_over","player_name":"LeBron James","league":"NBA",
     "prop_type":"Points","pp_line":25.5,"fd_line":25.5,"side":"over","true_prob":0.5731,
     "raw_true_prob":0.5623,"market_width":0.031,"team":"LAL","true_odds":-134,"edge":0.0265,
     "individual_ev_pct":0.0603,"over_odds":-120,"under_odds":100,"both_sided":true,
     "start_time":"2026-08-18T23:30:00+00:00","odds_type":"standard","fd_odds_book":-118.0,
     "dk_odds_book":-122.5,"pin_odds_book":-115.0,"nv_odds_book":null,"bet_key":"lebron james|2026-08-18"}
    """
    if let bet = decode(Bet.self, betJSON) {
        check(bet.betId == "NBA_LeBron_Points_over", "bet_id")
        check(bet.playerName == "LeBron James", "player_name")
        check(approx(bet.trueProb, 0.5731), "true_prob")
        check(approx(bet.truePct, 57.31, 1e-6), "truePct derived")
        check(bet.isOver, "isOver")
        check(!bet.isGreenDevil, "not green devil")
        check(bet.bookOdds.count == 3, "bookOdds omits null nv (got \(bet.bookOdds.count))")
        check(bet.bookOdds.first(where: { $0.book == .pinnacle })?.odds == -115, "pin odds rounded")
        check(bet.startDate != nil, "startDate parsed")
    } else { check(false, "Bet failed to decode") }

    let coreJSON = """
    {"bets":[\(betJSON)],"total":1,"is_scraping":false,"last_refresh":"2026-08-18T23:00:00+00:00","interval_min":15}
    """
    if let core = decode(CoreBundle.self, coreJSON) {
        check(core.bets.count == 1, "core bets count")
        check(core.intervalMin == 15, "interval_min")
        check(core.lastRefreshDate != nil, "last_refresh parsed")
    } else { check(false, "CoreBundle failed to decode") }
}

section("Decoding — Backtest slips (string line, null stat_actual)") {
    let json = """
    {"slips":[{"id":"A1B2C3D4","slip_id":"A1B2C3D4","timestamp":"2026-08-17T12:00:00+00:00",
     "slip_type":"Power","n_legs":3,"proj_slip_ev_pct":0.12,"payout":6.0,"hits":3,"completed":true,
     "legs":[
       {"slip_id":"A1B2C3D4","leg_num":1,"player":"A","league":"NBA","prop":"Points","line":"25.5",
        "side":"over","true_prob":0.61,"result":"hit","stat_actual":28,"game_start":"2026-08-16T23:00:00+00:00"},
       {"slip_id":"A1B2C3D4","leg_num":2,"player":"B","league":"NBA","prop":"Assists","line":6.5,
        "side":"under","true_prob":0.58,"result":"hit","stat_actual":null,"game_start":null},
       {"slip_id":"A1B2C3D4","leg_num":3,"player":"C","league":"NHL","prop":"Shots on Goal","line":2.5,
        "side":"over","true_prob":0.57,"result":"hit","stat_actual":3,"game_start":"2026-08-16T23:00:00+00:00"}
     ]}],"total":1}
    """
    if let env = decode(BacktestSlipsEnvelope.self, json) {
        check(env.slips.count == 1, "slips count")
        let slip = env.slips[0]
        check(slip.legs.count == 3, "legs count")
        check(slip.legs[0].line == 25.5, "string line \"25.5\" decoded leniently")
        check(slip.legs[1].statActual == nil, "null stat_actual")
        check(slip.legs[0].result == .hit, "leg result enum")
        let outcome = BacktestScoring.outcome(for: slip)
        check(outcome.payout == 6.0 && outcome.isWin, "recomputed outcome 6.0 win")
        let summary = BacktestSummary.compute(env.slips)
        check(summary.completedSlips == 1 && summary.slipWins == 1, "summary counts")
        check(summary.legHitRate == 1.0, "leg hit rate 1.0")
        check(summary.roi == 5.0, "roi (6-1)/1 = 5.0")
    } else { check(false, "BacktestSlipsEnvelope failed to decode") }
}

section("Decoding — UserConfig (active_leagues dict preserved)") {
    let json = """
    {"interval_min":5,"min_ev_pct":-10.0,"active_leagues":{"NBA":true,"WNBA":false,"MLB":true},
     "auto_backtest":true,"auto_slip_type":"Flex","auto_slip_legs":5,"auto_slip_min_prob":0.6,
     "auto_backtest_green_devils":false}
    """
    if let cfg = decode(UserConfig.self, json) {
        check(cfg.activeLeagues?["NBA"] == true, "active_leagues NBA=true")
        check(cfg.activeLeagues?["WNBA"] == false, "active_leagues WNBA=false")
        check(cfg.autoSlipType == "Flex", "auto_slip_type")
        check(cfg.autoSlipLegs == 5, "auto_slip_legs")
        check(cfg.autoSlipMinProb == 0.6, "auto_slip_min_prob")
    } else { check(false, "UserConfig failed to decode") }
}

section("Decoding — Billing + Coverage + UIConfig") {
    if let b = decode(BillingStatus.self, """
        {"status":"trialing","plan":"monthly","current_period_end":"2026-08-25T00:00:00+00:00",
         "active":true,"comped":false,"enforce":true,"configured":true}
        """) {
        check(b.isUnlocked && b.isTrialing && !b.isComped, "billing status flags")
        check(b.currentPeriodEndDate != nil, "current_period_end parsed")
    } else { check(false, "BillingStatus failed") }

    if let c = decode(Coverage.self, """
        {"prop_source":"PrizePicks","books":["FanDuel","DraftKings","Pinnacle","Novig"],
         "books_noun":"price sources","leagues":["NBA","MLB"],"refresh_minutes":15,"trial_days":7}
        """) {
        check(c.booksCountPhrase == "4 price sources", "books count phrase")
        check(c.leagues == ["NBA", "MLB"], "leagues")
    } else { check(false, "Coverage failed") }

    if let u = decode(UIConfig.self, """
        {"supabase_url":"https://x.supabase.co","supabase_anon_key":"anon","vapid_public_key":"vap"}
        """) {
        check(u.supabaseUrl == "https://x.supabase.co" && u.supabaseAnonKey == "anon", "ui-config")
    } else { check(false, "UIConfig failed") }
}

section("Decoding — Auth session + signup") {
    if let s = decode(AuthSession.self, """
        {"access_token":"abc","token_type":"bearer","expires_in":3600,"expires_at":1900000000,
         "refresh_token":"r1","user":{"id":"u1","email":"a@b.com","user_metadata":{"username":"maxf"},
         "created_at":"2026-01-01T00:00:00+00:00"}}
        """) {
        check(s.accessToken == "abc" && s.refreshToken == "r1", "session tokens")
        check(s.user?.username == "maxf", "user_metadata.username")
        check(!s.isExpired(now: Date(timeIntervalSince1970: 1_800_000_000)), "not expired well before exp")
        check(s.isExpired(now: Date(timeIntervalSince1970: 1_900_000_000)), "expired at exp")
    } else { check(false, "AuthSession failed") }

    if let confirm = decode(SignUpResponse.self, #"{"id":"u2","email":"x@y.com"}"#) {
        check(confirm.needsEmailConfirmation, "signup needs email confirmation")
    } else { check(false, "SignUpResponse (confirm) failed") }

    if let auto = decode(SignUpResponse.self, """
        {"access_token":"t","refresh_token":"r","user":{"id":"u3","email":"z@z.com"}}
        """) {
        check(!auto.needsEmailConfirmation, "signup auto-confirmed → session")
        check(auto.effectiveSession?.accessToken == "t", "signup session token")
    } else { check(false, "SignUpResponse (auto) failed") }
}

section("Decoding — MarketLine variants") {
    if let m = decode(MarketLine.self, """
        {"player_name":"A","league":"NBA","stat_type":"Points","pp_line":25.5,"side":"over",
         "fd_odds":-118.0,"dk_odds":-122.0,"pin_odds":-115.0,"nv_odds":null,"best_odds":-115.0,
         "true_odds":-130.0,"start_time":"2026-08-18T23:00:00+00:00"}
        """) {
        check(m.prop == "Points", "matched prop")
        check(m.lineValue == 25.5, "matched line via pp_line")
        check(m.fd == -118 && m.best == -115, "matched odds rounded")
        check(m.nv == nil, "matched null nv")
    } else { check(false, "MarketLine matched failed") }

    if let s = decode(MarketLine.self, """
        {"player_name":"B","league":"NHL","stat_type":"Saves","line_score":24.5,"side":"under",
         "line_odds":-110.0,"true_odds":-108.0,"start_time":null}
        """) {
        check(s.lineValue == 24.5, "single-book line via line_score")
        check(s.bookOdds == -110, "single-book odds via line_odds")
    } else { check(false, "MarketLine single failed") }
}

section("Decoding — Analytics (calibration + P&L + CLV)") {
    let json = """
    {"brier_score":0.2412,"log_loss":0.66,"n_resolved":120,"n_won":70,"n_lost":50,
     "hit_rate":0.5833,"avg_predicted_prob":0.5712,
     "calibration_buckets":[
       {"bucket":"54-56%","predicted_avg":0.55,"actual_avg":0.58,"count":40},
       {"bucket":"56-58%","predicted_avg":null,"actual_avg":null,"count":0},
       {"bucket":"58-60%","predicted_avg":0.59,"actual_avg":0.55,"count":22}],
     "n_clv_tracked":80,"n_clv_moved":50,"n_clv_stale":30,"clv_plus_rate":0.61,
     "avg_clv_pct":0.021,"avg_clv_pct_moved":0.034,"n_logged_legs":140,"clv_coverage_pct":0.57,
     "pnl_timeline":[
       {"slip_id":"S1","timestamp":"2026-08-10T12:00:00+00:00","pnl":-1.0,"cum_pnl":-1.0},
       {"slip_id":"S2","timestamp":"2026-08-12T12:00:00+00:00","pnl":5.0,"cum_pnl":4.0}],
     "resolved_slips":2,"won_slips":1,"roi_per_slip":2.0,
     "resolved_legs":[{"true_prob":0.61,"outcome":1,"timestamp":"2026-08-10T12:00:00+00:00"}],
     "clv_legs":[{"closing_prob":0.6,"clv_pct":0.02,"timestamp":"2026-08-10T12:00:00+00:00"}]}
    """
    if let a = decode(AnalyticsData.self, json) {
        check(a.nResolved == 120 && a.nWon == 70, "analytics counts")
        check(approx(a.hitRate ?? 0, 0.5833), "analytics hit rate")
        check(a.calibrationBuckets?.count == 3, "all buckets decoded")
        check(a.populatedBuckets.count == 2, "empty bucket filtered out of populatedBuckets")
        check(a.pnlTimeline?.count == 2, "pnl timeline count")
        check(a.pnlTimeline?.last?.cumPnl == 4.0, "cum_pnl last")
        check(a.pnlTimeline?.first?.date != nil, "pnl point date parsed")
        check(a.clvPlusRate == 0.61, "clv plus rate")
        check(a.hasResolvedData, "hasResolvedData true")
        check(a.roiPerSlip == 2.0, "roi per slip")
    } else { check(false, "AnalyticsData failed to decode") }

    // Empty analytics (new user) must decode to a benign, empty state.
    if let empty = decode(AnalyticsData.self, """
        {"brier_score":null,"log_loss":null,"n_resolved":0,"n_won":0,"n_lost":0,
         "hit_rate":null,"avg_predicted_prob":null,"calibration_buckets":[],
         "pnl_timeline":[],"resolved_slips":0,"won_slips":0,"roi_per_slip":null}
        """) {
        check(!empty.hasResolvedData, "empty analytics → hasResolvedData false")
        check(empty.populatedBuckets.isEmpty, "empty analytics → no populated buckets")
    } else { check(false, "empty AnalyticsData failed to decode") }
}

// MARK: - Formatting

section("Formatting") {
    check(Fmt.americanOdds(-118.4) == "-118", "americanOdds negative rounds")
    check(Fmt.americanOdds(150) == "+150", "americanOdds positive prefix")
    check(Fmt.americanOdds(nil as Double?) == "—", "americanOdds nil → dash")
    check(Fmt.percent(0.5731) == "57.3%", "percent")
    check(Fmt.signedPercent(0.0603) == "+6.0%", "signedPercent positive")
    check(Fmt.signedPercent(-0.021) == "-2.1%", "signedPercent negative")
    check(Fmt.line(3.0) == "3", "line whole")
    check(Fmt.line(3.5) == "3.5", "line half")
}

// MARK: - ISO parsing

section("ISO8601 parsing ladder") {
    check(ISO8601Date.parse("2026-08-18T23:30:00+00:00") != nil, "offset")
    check(ISO8601Date.parse("2026-08-18T23:30:00.123456+00:00") != nil, "fractional + offset")
    check(ISO8601Date.parse("2026-08-18T23:30:00Z") != nil, "Z")
    check(ISO8601Date.parse("2026-08-18T23:30:00") != nil, "naive")
    check(ISO8601Date.parse(nil) == nil, "nil → nil")
    check(ISO8601Date.parse("") == nil, "empty → nil")
}

// MARK: - Encoding safety (active_leagues keys must NOT be snake-cased)

section("Encoding — active_leagues key preservation") {
    // Request bodies use a plain encoder (no key strategy) so league keys are
    // sent verbatim. Confirm the plain encoder round-trips the exact keys the
    // server expects (`{"NBA":true,...}`), which is what CoreClient relies on.
    let leagues: [String: Bool] = ["NBA": true, "WNBA": false]
    if let plain = try? JSONEncoder().encode(leagues),
       let str = String(data: plain, encoding: .utf8) {
        check(str.contains("\"NBA\""), "plain encoder preserves \"NBA\"")
        check(!str.contains("\"nba\""), "plain encoder does NOT lowercase to \"nba\"")
    } else { check(false, "plain encode failed") }

    // The snake-case-mapped request bodies (explicit CodingKeys) encode as the
    // server expects: verify one representative body.
    struct PrefsBody: Encodable {
        let autoSlipType: String; let autoSlipLegs: Int
        enum CodingKeys: String, CodingKey { case autoSlipType = "auto_slip_type", autoSlipLegs = "auto_slip_legs" }
    }
    if let data = try? JSONEncoder().encode(PrefsBody(autoSlipType: "Flex", autoSlipLegs: 5)),
       let str = String(data: data, encoding: .utf8) {
        check(str.contains("\"auto_slip_type\"") && str.contains("\"auto_slip_legs\""),
              "explicit CodingKeys produce snake_case body keys")
    } else { check(false, "prefs body encode failed") }
}

// MARK: - Summary

print("")
print("────────────────────────────────────────")
print("CorePropKitVerify: \(passes) passed, \(failures) failed")
if failures == 0 { print("✓ ALL CHECKS PASSED") }
exit(failures == 0 ? 0 : 1)
