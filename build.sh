#!/usr/bin/env bash
# Precompile the React JSX into plain JS so the browser no longer ships and runs
# @babel/standalone (~600KB gzipped + a main-thread transform of every .jsx on
# each page load). Run this locally BEFORE committing/pushing any .jsx change —
# Render's build env is pip-only (no node), so the compiled output is committed.
#
#   ./build.sh
#
# Output: web/static/dist/<name>.js for every web/static/<name>.jsx.
# The files stay plain global scripts (esbuild --format not set = iife-less
# transform, no module wrapper), preserving the cross-file global-scope model
# the app relies on (components defined in one file, used in another).
set -euo pipefail

cd "$(dirname "$0")"

ESBUILD="npx --yes esbuild@0.28.1"
SRC_DIR="web/static"
OUT_DIR="web/static/dist"

# Load order matters — this MUST match the <script> order in index.html.
FILES=(
  api            # already plain JS, but copied through so all runtime lives in dist/
  tweaks-panel
  components
  landing
  ev-page
  page-boards
  page-backtest
  page-analytics
  pricing
  app-main       # the former inline <script type="text/babel"> App block
)

mkdir -p "$OUT_DIR"

echo "Building ${#FILES[@]} files -> $OUT_DIR/ ..."
for name in "${FILES[@]}"; do
  # api.jsx has no JSX but we still run it through esbuild for uniform minify.
  src="$SRC_DIR/$name.jsx"
  if [[ ! -f "$src" ]]; then
    echo "  ! missing $src" >&2
    exit 1
  fi
  $ESBUILD "$src" \
    --jsx=transform \
    --minify \
    --target=es2019 \
    --outfile="$OUT_DIR/$name.js" \
    --log-level=warning
  printf "  ok  %-16s %6d bytes\n" "$name.js" "$(wc -c < "$OUT_DIR/$name.js")"
done

# ── Cache-bust: stamp a content-derived build id into index.html + sw.js ─────
# The app is 10 plain global scripts whose cross-file globals only line up when
# every bundle is from the SAME build. If a returning client reuses one stale
# bundle alongside newer ones, a global mismatch throws during render and the
# page goes blank. Deriving the ?v= token (and the service-worker cache name)
# from a hash of the freshly built bundles guarantees: (a) any content change
# moves the token so every client re-downloads all bundles together, and (b)
# the SW's activate() purges every prior build's cached bundles, so an offline
# visit can never serve a mixed-version set. Run on every ./build.sh — no manual
# bumping (the old ?v=14d4eaeb went stale exactly this way).
# LC_ALL=C so the glob expands in byte order, matching Python's sorted() in
# tests/api_tests/test_build_stamp.py, which recomputes this same digest and
# asserts it equals the stamps below. A UTF-8 locale collates by dictionary
# rules instead (glibc ignores punctuation; macOS folds case), so the shell's
# order and Python's agree only by luck for the current filenames. Add a bundle
# that collides on case (B.js precedes a.js under C, follows it under
# en_US.UTF-8) or on punctuation-vs-punctuation (a-b.js/a_b.js swap) and the two
# silently disagree — surfacing as a spurious stamp-test failure that points
# nowhere near here. Before deciding this line is unnecessary, note that the
# obvious fixture (page-x.js vs pagex.js) does NOT diverge on macOS: those two
# agree in both locales, so a passing test of that pair proves nothing.
# Scoped to this command substitution's subshell so the perl stamping below
# keeps the ambient locale.
BUILD_ID=$( export LC_ALL=C; cat "$OUT_DIR"/*.js | shasum | cut -c1-10 )
perl -i -pe "s{(/static/dist/[a-z0-9-]+\.js\?v=)[a-z0-9]+}{\${1}$BUILD_ID}g" "$SRC_DIR/index.html"
perl -i -pe "s{(coreprop-shell-)[A-Za-z0-9]+}{\${1}$BUILD_ID}" "$SRC_DIR/sw.js"
echo "Cache-bust id: $BUILD_ID (stamped into index.html ?v= and sw.js CACHE)"

echo "Done. Commit web/static/dist/ + index.html + sw.js alongside the .jsx sources."
