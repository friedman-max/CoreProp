"""
Soccer / World Cup wiring regression tests.

Soccer was removed in May 2026 (commit bca7b40) after single-sided goalscorer
markets and cross-league prop-key collisions produced phantom +EV. It was
re-added for the 2026 World Cup. These tests lock in the pieces that are easy
to silently break:

  1. SOCCER is no longer in EXCLUDED_LEAGUES (else every read path strips it).
  2. PrizePicks points at the live WORLD CUP league (241), not the dormant
     legacy SOCCER league (82, 0 projections off-season).
  3. The FanDuel "N Or More" milestone parser maps ladders to (stat, line) and
     keeps the "To Create" (chances created → Shots Assisted) trap distinct
     from "To Have" (the player's own stat) — the exact mismatch class that
     caused the original phantom +EV.
  4. Team milestone markets are dropped, not mislabeled as player props.
  5. Pinnacle's single-player "X To Score" goalscorer shorthand parses; the
     subjectless futures ("First Goalscorer") do not.
"""
from __future__ import annotations

import unittest

import config as cfg
from engine.constants import PROP_TYPE_MAP, EXCLUDED_LEAGUES, is_excluded_league
from scrapers.fanduel import _parse_soccer_milestone, _soccer_milestone_stat, _normalize_prop_type
from scrapers.pinnacle import _parse_description, LEAGUE_CONFIG as PIN_LEAGUE_CONFIG


class SoccerConfigWiring(unittest.TestCase):
    def test_soccer_not_excluded(self):
        self.assertNotIn("SOCCER", EXCLUDED_LEAGUES)
        self.assertFalse(is_excluded_league("SOCCER"))
        self.assertFalse(is_excluded_league("soccer"))

    def test_prizepicks_points_at_world_cup(self):
        # 241 == live "WORLD CUP"; 82 is the dormant legacy "SOCCER" league.
        self.assertEqual(cfg.PRIZEPICKS_LEAGUE_IDS["SOCCER"], 241)

    def test_soccer_in_active_leagues(self):
        self.assertIn("SOCCER", cfg.ACTIVE_LEAGUES)

    def test_pinnacle_world_cup_league_id(self):
        self.assertEqual(PIN_LEAGUE_CONFIG["SOCCER"]["id"], "2686")


class SoccerPropTypeMap(unittest.TestCase):
    def test_core_mappings(self):
        m = PROP_TYPE_MAP["SOCCER"]
        self.assertEqual(m["shots on target"], "Shots On Target")
        self.assertEqual(m["shots"], "Shots")
        self.assertEqual(m["goalie saves"], "Goalie Saves")
        self.assertEqual(m["saves"], "Goalie Saves")
        self.assertEqual(m["tackles"], "Tackles")
        self.assertEqual(m["passes attempted"], "Passes Attempted")

    def test_saves_does_not_collide_with_nhl(self):
        # NHL "saves" must stay "Saves"; SOCCER "saves" must be "Goalie Saves".
        self.assertEqual(PROP_TYPE_MAP["NHL"]["saves"], "Saves")
        self.assertEqual(PROP_TYPE_MAP["SOCCER"]["saves"], "Goalie Saves")


class FanDuelMilestoneParser(unittest.TestCase):
    def test_have_ladder_maps_to_over_line(self):
        self.assertEqual(_parse_soccer_milestone("Player To Have 3 Or More Shots"), ("Shots", 2.5))
        self.assertEqual(_parse_soccer_milestone("Player To Have 1 Or More Shots On Target"),
                         ("Shots On Target", 0.5))
        self.assertEqual(_parse_soccer_milestone("Player To Have 2 Or More Saves"),
                         ("Goalie Saves", 1.5))
        self.assertEqual(_parse_soccer_milestone("Player To Have 30 Or More Passes"),
                         ("Passes Attempted", 29.5))

    def test_create_is_shots_assisted_not_shots(self):
        # The headline trap: "create N shots" = chances created (PP Shots
        # Assisted), NOT the player's own Shots.
        self.assertEqual(_parse_soccer_milestone("Player To Create 1 Or More Shots"),
                         ("Shots Assisted", 0.5))
        self.assertEqual(_soccer_milestone_stat("create", "shots"), "Shots Assisted")
        self.assertEqual(_soccer_milestone_stat("have", "shots"), "Shots")

    def test_team_markets_dropped(self):
        self.assertIsNone(_parse_soccer_milestone("Team To Have 20 Or More Shots"))

    def test_unknown_stat_dropped(self):
        self.assertIsNone(_parse_soccer_milestone("Player To Have 1 Or More Corners Won"))

    def test_normalize_prop_type_soccer(self):
        self.assertEqual(_normalize_prop_type("Player Shots On Target", "SOCCER"), "Shots On Target")
        self.assertEqual(_normalize_prop_type("Total Shots", "SOCCER"), "Shots")
        self.assertEqual(_normalize_prop_type("Goalkeeper Saves", "SOCCER"), "Goalie Saves")


class PinnacleGoalscorerParsing(unittest.TestCase):
    def test_to_score_shorthand(self):
        self.assertEqual(_parse_description("Lionel Messi To Score"), ("Lionel Messi", "goals"))
        self.assertEqual(_parse_description("Kylian Mbappe To Score"), ("Kylian Mbappe", "goals"))

    def test_subjectless_futures_not_parsed(self):
        # No single-player subject ⇒ must not be matched as a player prop.
        self.assertEqual(_parse_description("First Goalscorer"), (None, None))
        self.assertEqual(_parse_description("Tournament Top Goalscorer"), (None, None))

    def test_classic_parens_still_works(self):
        self.assertEqual(_parse_description("Granit Xhaka (Tackles)"), ("Granit Xhaka", "Tackles"))


if __name__ == "__main__":
    unittest.main()
