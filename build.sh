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
  data
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

echo "Done. Commit web/static/dist/ alongside the .jsx sources."
