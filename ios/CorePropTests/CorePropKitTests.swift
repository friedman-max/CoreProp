import XCTest
@testable import CorePropKit

/// XCTest mirror of `CorePropKitVerify` for use inside Xcode (where XCTest is
/// available). Run with ⌘U. The `CorePropKitVerify` executable covers the same
/// ground on toolchains without XCTest (`swift run CorePropKitVerify`).
final class CorePropKitTests: XCTestCase {

    private let decoder = JSONDecoder.coreProp()

    // MARK: Payout re-derivation

    func testPowerBreakEvenClosedForm() {
        for n in Payouts.powerSizes {
            let payout = Payouts.power[n]!
            let derived = pow(1.0 / payout, 1.0 / Double(n))
            XCTAssertEqual(derived, SlipEV.breakEven(n: n, type: .power), accuracy: 0.0006, "Power-\(n)")
        }
    }

    func testFlexBreakEvenBisection() {
        for n in Payouts.flexSizes {
            func ev(_ p: Double) -> Double { SlipEV.flexEV(Array(repeating: p, count: n))! }
            var lo = 0.30, hi = 0.80
            for _ in 0..<80 { let mid = (lo + hi) / 2; if ev(mid) < 0 { lo = mid } else { hi = mid } }
            XCTAssertEqual((lo + hi) / 2, SlipEV.breakEven(n: n, type: .flex), accuracy: 0.0006, "Flex-\(n)")
        }
    }

    func testPayoutTablesMatchServer() {
        XCTAssertEqual(Payouts.power, [2: 3.0, 3: 6.0, 4: 10.0, 5: 20.0, 6: 37.5])
        XCTAssertEqual(Payouts.flex[6]?[6], 25.0)
        XCTAssertEqual(Payouts.flex[5]?[3], 0.4)
    }

    // MARK: EV

    func testPowerEV() {
        XCTAssertEqual(SlipEV.powerEV([0.6, 0.6])!, 0.6 * 0.6 * 3.0 - 1.0, accuracy: 1e-9)
        XCTAssertNil(SlipEV.powerEV([0.6]))
    }

    func testFlexTwoLegShortCircuitsToPower() {
        XCTAssertEqual(SlipEV.flexEV([0.6, 0.6]), SlipEV.powerEV([0.6, 0.6]))
    }

    func testBestPrefersPowerOnTie() {
        XCTAssertEqual(SlipEV.best([0.6, 0.6])?.type, .power)
    }

    // MARK: Backtest scoring

    func testBacktestScoring() {
        func leg(_ r: String) -> SlipLeg {
            SlipLeg(slipId: "S", legNum: 1, player: "P", league: "NBA", prop: "Points",
                    line: 1.5, side: "over", trueProb: 0.6, resultRaw: r, statActual: nil, gameStart: nil)
        }
        let power = BacktestScoring.outcome(legs: [leg("hit"), leg("hit"), leg("hit")], slipType: "Power")
        XCTAssertEqual(power.payout, 6.0); XCTAssertTrue(power.isWin)

        let push = BacktestScoring.outcome(legs: [leg("hit"), leg("hit"), leg("push")], slipType: "Flex")
        XCTAssertEqual(push.effectiveLegs, 2); XCTAssertEqual(push.payout, 3.0)

        let pending = BacktestScoring.outcome(legs: [leg("hit"), leg("pending")], slipType: "Power")
        XCTAssertFalse(pending.completed)
    }

    // MARK: Decoding

    func testDecodeBet() throws {
        let json = """
        {"bet_id":"b1","player_name":"LeBron James","league":"NBA","prop_type":"Points","pp_line":25.5,
         "fd_line":25.5,"side":"over","true_prob":0.5731,"raw_true_prob":0.5623,"market_width":0.03,
         "team":"LAL","true_odds":-134,"edge":0.0265,"individual_ev_pct":0.06,"over_odds":-120,
         "under_odds":100,"both_sided":true,"start_time":"2026-08-18T23:30:00+00:00","odds_type":"standard",
         "fd_odds_book":-118.0,"dk_odds_book":-122.5,"pin_odds_book":-115.0,"nv_odds_book":null,
         "bet_key":"lebron james|2026-08-18"}
        """
        let bet = try decoder.decode(Bet.self, from: Data(json.utf8))
        XCTAssertEqual(bet.playerName, "LeBron James")
        XCTAssertEqual(bet.bookOdds.count, 3)
        XCTAssertTrue(bet.isOver)
        XCTAssertEqual(bet.truePct, 57.31, accuracy: 1e-6)
    }

    func testDecodeSlipsWithStringLine() throws {
        let json = """
        {"slips":[{"id":"S1","slip_type":"Power","n_legs":2,"proj_slip_ev_pct":0.1,"legs":[
          {"slip_id":"S1","leg_num":1,"player":"A","prop":"Points","line":"25.5","side":"over","true_prob":0.61,"result":"hit","stat_actual":28},
          {"slip_id":"S1","leg_num":2,"player":"B","prop":"Assists","line":6.5,"side":"under","true_prob":0.58,"result":"hit","stat_actual":null}
        ]}],"total":1}
        """
        let env = try decoder.decode(BacktestSlipsEnvelope.self, from: Data(json.utf8))
        XCTAssertEqual(env.slips.first?.legs.first?.line, 25.5)
        XCTAssertNil(env.slips.first?.legs.last?.statActual)
        XCTAssertEqual(BacktestScoring.outcome(for: env.slips[0]).payout, 3.0)
    }

    func testDecodeUserConfigPreservesLeagueKeys() throws {
        let json = #"{"active_leagues":{"NBA":true,"WNBA":false},"auto_slip_type":"Flex","auto_slip_legs":5}"#
        let cfg = try decoder.decode(UserConfig.self, from: Data(json.utf8))
        XCTAssertEqual(cfg.activeLeagues?["NBA"], true)
        XCTAssertEqual(cfg.autoSlipType, "Flex")
    }

    func testDecodeAuthSession() throws {
        let json = """
        {"access_token":"abc","refresh_token":"r1","expires_at":1900000000,
         "user":{"id":"u1","email":"a@b.com","user_metadata":{"username":"maxf"}}}
        """
        let s = try decoder.decode(AuthSession.self, from: Data(json.utf8))
        XCTAssertEqual(s.user?.username, "maxf")
        XCTAssertTrue(s.isExpired(now: Date(timeIntervalSince1970: 1_900_000_100)))
    }

    // MARK: Formatting

    func testFormatting() {
        XCTAssertEqual(Fmt.americanOdds(-118.4), "-118")
        XCTAssertEqual(Fmt.americanOdds(150), "+150")
        XCTAssertEqual(Fmt.signedPercent(0.0603), "+6.0%")
        XCTAssertEqual(Fmt.line(3.0), "3")
        XCTAssertEqual(Fmt.line(3.5), "3.5")
    }
}
