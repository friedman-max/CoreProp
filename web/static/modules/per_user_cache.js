// localStorage entries scoped to a single user. Purged when the user id
// changes (login / logout / account switch) so the next page paint can't
// render the previous user's cached data.
//
// **Add new entries here** when you write user-scoped data to
// localStorage. Forgetting to register a key re-introduces the
// cross-user data leak this list was created to fix.

export const PER_USER_LOCALSTORAGE_KEYS = [
  "coreprop_backtest_cache_v2",
  "coreprop_analytics_cache_v2",
  "coreprop_auto_backtest",
  "coreprop_slip_type",
  "coreprop_slip_legs",
  "coreprop_slip_min_prob",
];

export function purgePerUserLocalStorage() {
  for (const k of PER_USER_LOCALSTORAGE_KEYS) {
    try { localStorage.removeItem(k); } catch {}
  }
}
