#!/usr/bin/env bash
# Package the Chrome extension into the zip the /extension page serves.
#
# Render's build env is pip-only and never runs this, so the built artifact is
# COMMITTED like web/static/dist/. Re-run this after any change under
# coreprop-extension/ or users will keep downloading the previous version.
#
#   ./package-extension.sh
#
# Output: web/static/coreprop-extension.zip
set -euo pipefail

cd "$(dirname "$0")"

SRC="coreprop-extension"
OUT="web/static/coreprop-extension.zip"

VERSION=$(python3 -c "import json;print(json.load(open('$SRC/manifest.json'))['version'])")

# Sanity-check every file the manifest references actually exists — a missing
# content script makes Chrome reject the whole extension at load time with an
# error the user has no way to interpret.
python3 - <<'PY'
import json, sys, pathlib
src = pathlib.Path("coreprop-extension")
m = json.loads((src / "manifest.json").read_text())
missing = []
for cs in m.get("content_scripts", []):
    missing += [f for f in cs.get("js", []) if not (src / f).exists()]
sw = m.get("background", {}).get("service_worker")
if sw and not (src / sw).exists():
    missing.append(sw)
popup = m.get("action", {}).get("default_popup")
if popup and not (src / popup).exists():
    missing.append(popup)
if missing:
    sys.exit("manifest references missing files: " + ", ".join(missing))
PY

rm -f "$OUT"
# Zip the DIRECTORY, not its contents: "Load unpacked" wants a folder, and this
# way the user gets coreprop-extension/ after unzipping rather than loose files
# scattered into whatever directory they extracted to.
zip -r -q "$OUT" "$SRC" \
  -x "*.DS_Store" -x "__MACOSX/*"

echo "Packaged $SRC v$VERSION -> $OUT ($(wc -c < "$OUT" | tr -d ' ') bytes)"
echo "Commit $OUT alongside the extension sources."
