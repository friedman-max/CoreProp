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

# Cache-bust index.html's <script src="...?v=N"> tags.
#
# Cloudflare rewrites our origin's `cache-control: no-cache` to `max-age=14400`
# for .js, so a browser that already has a bundle keeps it for four hours. The
# ?v= query is what forces a refetch — but it was a hand-typed constant (?v=2),
# so rebuilding without remembering to bump it shipped code that no existing
# visitor would see for four hours, while curl against the origin showed the
# new file and everything looked fine.
#
# Deriving it from the content of dist/ makes it impossible to forget: the
# token only changes when the bundles actually change, so this is also a no-op
# on a rebuild that produced identical output.
INDEX="$SRC_DIR/index.html"
if [[ -f "$INDEX" ]]; then
  VER="$(cat "$OUT_DIR"/*.js | shasum | cut -c1-8)"
  # BSD sed (macOS) needs the empty-string argument to -i.
  sed -i '' -E "s|(/static/dist/[a-z-]+\.js)\?v=[^\"']*|\1?v=$VER|g" "$INDEX"
  echo
  echo "Cache-bust token -> ?v=$VER  ($(grep -c "?v=$VER" "$INDEX") script tags in index.html)"
fi

# Drift guard. FILES above is hand-maintained, so a .jsx that nobody added to
# it is never rebuilt — but a stale dist/<name>.js from an earlier build keeps
# sitting there looking current. Editing such a source appears to do nothing,
# and the reason is invisible: the build reports success because it only ever
# looks at the names it was given.
#
# This bit us with auth-page.jsx: dist/auth-page.js was two days staler than
# every sibling, and auth-page.jsx turned out to be unwired entirely (it is not
# in index.html's <script> list and nothing references AuthPage). Left alone,
# the next person to edit it loses an hour.
missing=()
for src in "$SRC_DIR"/*.jsx; do
  name="$(basename "$src" .jsx)"
  found=0
  for f in "${FILES[@]}"; do [[ "$f" == "$name" ]] && found=1 && break; done
  [[ $found -eq 0 ]] && missing+=("$name")
done

if [[ ${#missing[@]} -gt 0 ]]; then
  echo
  echo "WARNING: these .jsx sources are NOT in the build list, so they were not rebuilt:"
  for m in "${missing[@]}"; do
    if [[ -f "$OUT_DIR/$m.js" ]]; then
      echo "  - $m.jsx  (a STALE $OUT_DIR/$m.js exists and will keep being served)"
    else
      echo "  - $m.jsx  (never built)"
    fi
  done
  echo "  Add it to FILES above, or delete the source if it is dead code."
  echo "  Check index.html's <script> tags to see whether it is actually loaded."
fi

echo "Done. Commit web/static/dist/ alongside the .jsx sources."
