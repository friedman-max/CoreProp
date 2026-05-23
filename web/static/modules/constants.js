// Frontend constants. Pure values mirrored from engine/constants.py.
//
// Why here: payout tables and break-even probabilities are used by
// app.js (slip-builder display) and the to-be-extracted sandbox /
// backtest modules. Exporting them once avoids drift between
// duplicate literals scattered across the codebase.

export const POWER_PAYOUTS = { 2: 3.0, 3: 6.0, 4: 10.0, 5: 20.0, 6: 40.0 };

export const FLEX_PAYOUTS = {
  3: { 2: 1.0, 3: 3.0 },
  4: { 3: 1.5, 4: 6.0 },
  5: { 3: 0.4, 4: 2.0, 5: 10.0 },
  6: { 4: 0.4, 5: 2.0, 6: 25.0 },
};

// Per-leg break-even probability — what a leg's true probability must
// be for a slip to break even on EV given the payout. Mirrors
// engine/constants.BREAK_EVEN. Keys are "<n_legs>" string for symmetry
// with the engine side where (n_legs, slip_type) tuples are used.
export const BREAK_EVEN = {
  "2": { power: 0.5774, flex: 0.5774 },
  "3": { power: 0.5503, flex: 0.5454 },
  "4": { power: 0.5623, flex: 0.5230 },
  "5": { power: 0.5493, flex: 0.5266 },
  "6": { power: 0.5407, flex: 0.5224 },
};

// Display order for league chips. Anything not in this list sorts
// alphabetically at the tail.
export const LEAGUE_ORDER = ["NBA", "WNBA", "NCAAB", "MLB", "NHL"];

// Minimum raw consensus probability the observatory accepts. Mirrors
// OBS_MIN_PROB in web/app.py. Used by the sandbox copy that explains
// why low-prob lines are absent.
export const OBS_MIN_PROB = 0.30;
