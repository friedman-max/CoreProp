#!/usr/bin/env bash
#
# typecheck-all.sh — type-check all of ios/CoreProp against the macOS SDK.
#
# WHY THIS EXISTS
#   This box has Command Line Tools only: no Xcode, no iOS SDK. The app target
#   therefore cannot be compiled locally and `.github/workflows/ios.yml`
#   (xcodebuild on macos-15) is the only real compile gate — and it runs only on
#   push/PR to main. `swiftc -parse` is not a substitute: it is a parser, so it
#   happily accepts `Theme.s7`, `cpCard(cornerRadius:)`, and a View modifier
#   applied to a Font. This script closes that gap for name and type errors by
#   type-checking the real, unmodified app sources against the *macOS* SDK, with
#   local shims standing in for the iOS-only API.
#
#   Read localcheck/README.md before trusting a green run. It is not xcodebuild.
#
# USAGE
#   ./typecheck-all.sh                  # the gate. non-zero on any error.
#   ./typecheck-all.sh --selftest       # prove it catches 5 classes of defect
#   ./typecheck-all.sh --selftest-reach # prove all N files are individually checked
#   ./typecheck-all.sh --skip-build     # reuse an existing CorePropKit build
#
# THREE FLAGS IN THE swiftc INVOCATION ARE LOAD-BEARING. Do not "simplify" them.
#
#   -enable-batch-mode -driver-batch-count <N>
#       Without batch mode, `swiftc -typecheck` reports diagnostics from ONLY
#       THE FIRST FILE IN COMMAND-LINE ORDER THAT HAS ERRORS, and stays silent
#       about every other file. Measured: three files each containing
#       `let a: Int = "x"` report one error whole-module and three in batch mode.
#       This is not availability-specific — it applies to every diagnostic. A
#       whole-module run that prints two errors may be hiding twenty, so error
#       counts from a non-batch run are not a progress metric. The gate would
#       still be *sound* (clean means clean) but useless to iterate on, and
#       silently non-covering the moment any one file breaks.
#
#   -target <arch>-apple-macos13.0
#       macOS 13 is the platform peer of iOS 16, which is the app's deployment
#       target (project.yml: deploymentTarget.iOS "16.0"). This is what buys the
#       one availability check we get: at macos13.0 the frontend rejects
#       `ContentUnavailableView` and the two-parameter `onChange(of:)` with
#       "only available in macOS 14.0 or newer", because both are iOS 17 /
#       macOS 14. The recipe this script grew out of used macos14.0 — the peer of
#       iOS *17* — which silently accepts every iOS-17-only API. The mapping is a
#       proxy, not a guarantee; see README, "Availability is a proxy".
#
#   (absence of) -Xfrontend -disable-availability-checking
#       That flag makes all 31 files typecheck with zero shims and perfect
#       signature fidelity, which is tempting. It also switches off the
#       availability check above, so iOS-17-only API sails through. The shims in
#       shims/SwiftUIiOSOnly.swift exist to avoid needing it. Adding it back
#       trades the only availability signal this gate has for a shorter shim
#       file.
#
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IOS_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
APP_DIR="$IOS_DIR/CoreProp"
KIT_DIR="$IOS_DIR/CorePropKit"
SHIM_DIR="$SCRIPT_DIR/shims"

MODE="gate"
SKIP_BUILD=0
for arg in "$@"; do
  case "$arg" in
    --selftest)       MODE="selftest" ;;
    --selftest-reach) MODE="reach" ;;
    --skip-build)     SKIP_BUILD=1 ;;
    -h|--help)        sed -n '2,40p' "${BASH_SOURCE[0]}"; exit 0 ;;
    *) echo "unknown argument: $arg" >&2; exit 2 ;;
  esac
done

ARCH="$(uname -m)"
TARGET="$ARCH-apple-macos13.0"
SDK="$(xcrun --show-sdk-path)"
# `${TMPDIR}` on macOS ends in a slash, so the naive
# `mktemp -d "$TMPDIR/cp-localcheck.XXXXXX"` yields a path containing `//`.
# swiftc normalises that away in its diagnostics, so any later attempt to match
# diagnostic paths against "$WORK" silently matches nothing — which is how the
# reach test first reported "0 of 31 files checked" while the compiler was in
# fact reporting all 31. Strip the trailing slash, and match on relative paths
# downstream anyway.
WORK="$(mktemp -d "${TMPDIR:-/tmp}/cp-localcheck.XXXXXX" | sed 's#//*#/#g')"
trap 'rm -rf "$WORK"' EXIT

say() { printf '%s\n' "$*"; }
hr()  { printf -- '--------------------------------------------------------------\n'; }

# ---------------------------------------------------------------- prerequisites

if [ ! -d "$APP_DIR" ]; then
  say "FATAL: no app sources at $APP_DIR"; exit 2
fi

# CorePropKit must be built: the app imports it, and a missing module is a FATAL
# frontend error that aborts the whole invocation before any type-checking
# happens (see the UIKit note below). Without this, the gate reports one error
# and passes over 31 unexamined files.
if [ "$SKIP_BUILD" -eq 0 ]; then
  say "==> swift build CorePropKit"
  if ! (cd "$KIT_DIR" && swift build) >"$WORK/kitbuild.log" 2>&1; then
    say "FATAL: CorePropKit failed to build. The app cannot be type-checked"
    say "       without its module. Log:"
    sed 's/^/       /' "$WORK/kitbuild.log"
    exit 2
  fi
fi
KIT_BIN="$(cd "$KIT_DIR" && swift build --show-bin-path 2>/dev/null)"
if [ -z "$KIT_BIN" ] || [ ! -d "$KIT_BIN" ]; then
  say "FATAL: could not locate CorePropKit build products (try without --skip-build)"
  exit 2
fi

# An EMPTY module named UIKit, so that `import UIKit` resolves.
#
# This is not optional and not cosmetic. `no such module` is a fatal frontend
# error: it aborts the entire invocation in BOTH whole-module and batch mode, so
# the two files that say `import UIKit` blank out diagnostics for all 31. That is
# exactly what the pre-harness baseline looked like — a single error, and no
# information about anything else.
#
# The module is empty on purpose; the actual UIKit declarations live in
# shims/UIKitTypes.swift and are compiled into the app module. See the comment
# at the top of shims/UIKitModule/UIKit.swift for why.
say "==> building empty UIKit stub module"
if ! xcrun swiftc -emit-module -module-name UIKit \
      -sdk "$SDK" -target "$TARGET" \
      -emit-module-path "$WORK/UIKit.swiftmodule" \
      "$SHIM_DIR/UIKitModule/UIKit.swift" >"$WORK/uikit.log" 2>&1; then
  say "FATAL: UIKit stub module failed to build:"
  sed 's/^/       /' "$WORK/uikit.log"
  exit 2
fi

# ------------------------------------------------------------------- type-check

# typecheck <app-source-root> <output-file>
# Returns swiftc's exit status. Diagnostics land in <output-file>.
typecheck() {
  local root="$1" out="$2"
  local shims app n
  # Globbed, not listed. New app files are picked up automatically — a hardcoded
  # file list is how a gate quietly stops covering the file someone just added.
  shims=$(find "$SHIM_DIR" -maxdepth 1 -name '*.swift' | sort)
  app=$(find "$root" -name '*.swift' | sort)
  n=$(printf '%s\n%s\n' "$shims" "$app" | grep -c '\.swift$')
  # shellcheck disable=SC2086
  xcrun swiftc -typecheck \
    -enable-batch-mode -driver-batch-count "$n" \
    -sdk "$SDK" -target "$TARGET" \
    -I "$KIT_BIN/Modules" -I "$KIT_BIN" -I "$WORK" \
    -diagnostic-style=llvm \
    $shims $app >"$out" 2>&1
}

app_file_count() { find "$APP_DIR" -name '*.swift' | wc -l | tr -d ' '; }

# ------------------------------------------------------------------- gate mode

run_gate() {
  local n out
  n=$(app_file_count)
  say "==> type-checking $n app files + $(find "$SHIM_DIR" -maxdepth 1 -name '*.swift' | wc -l | tr -d ' ') shims"
  say "    target=$TARGET  sdk=$SDK"
  out="$WORK/gate.log"
  typecheck "$APP_DIR" "$out"
  local status=$?
  local errs warns
  errs=$(grep -c ': error: ' "$out" || true)
  warns=$(grep -c ': warning: ' "$out" || true)
  hr
  if [ "$errs" -gt 0 ] || [ "$status" -ne 0 ]; then
    grep -E ': (error|warning): ' "$out" | sed "s#$IOS_DIR/##" | sort -u
    hr
    say "FAIL  $errs error(s), $warns warning(s) across $n app files."
    say ""
    say "Reminder: a failing run is NOT a partial result. Because a fatal"
    say "diagnostic (a missing module, a broken shim) aborts the whole"
    say "invocation, the only trustworthy verdict from this gate is a clean one."
    say "Fix these and re-run; do not read the count as a score."
    return 1
  fi
  if [ "$warns" -gt 0 ]; then
    grep ': warning: ' "$out" | sed "s#$IOS_DIR/##" | sort -u
    hr
  fi
  say "PASS  0 errors across $n app files ($warns warning(s))."
  say "      This proves names and types resolve. It does not prove the app"
  say "      builds for iOS, nor anything about layout or runtime. See README."
  return 0
}

# --------------------------------------------------------------- selftest modes

# Copy the real sources into scratch space. The real tree is NEVER mutated:
# a harness that only works by editing the sources it checks is worthless.
scratch_copy() {
  local dest="$WORK/$1"
  rm -rf "$dest"
  mkdir -p "$dest"
  cp -R "$APP_DIR/." "$dest/"
  printf '%s' "$dest"
}

# substitute <dir> <relpath> <literal-old> <literal-new>
# Fails loudly if the anchor is absent, so a source edit upstream turns into a
# broken self-test rather than a silently skipped one.
substitute() {
  local dir="$1" rel="$2" old="$3" new="$4"
  local f="$dir/$rel"
  [ -f "$f" ] || { say "    ANCHOR FILE MISSING: $rel"; return 1; }
  OLD="$old" NEW="$new" python3 - "$f" <<'PY' || return 1
import os, sys
p = sys.argv[1]
old, new = os.environ["OLD"], os.environ["NEW"]
s = open(p).read()
if s.count(old) < 1:
    sys.stderr.write("anchor not found: %r\n" % old); sys.exit(1)
open(p, "w").write(s.replace(old, new, 1))
PY
}

PASSED=0; FAILED=0
# expect_catch <label> <file-substring> <expected-message-substring> <mutator...>
expect_catch() {
  local label="$1" wantfile="$2" wantmsg="$3"; shift 3
  local dir out
  dir=$(scratch_copy "st")
  if ! "$@" "$dir"; then
    say "  ?? $label -- could not inject (anchor drifted); NOT PROVEN"
    FAILED=$((FAILED+1)); return
  fi
  out="$WORK/st.log"
  typecheck "$dir" "$out"
  local status=$?
  if [ "$status" -eq 0 ]; then
    say "  !! $label -- NOT CAUGHT (harness returned success)"
    FAILED=$((FAILED+1)); return
  fi
  local line
  line=$(grep ': error: ' "$out" | grep -F "$wantfile" | grep -F "$wantmsg" | head -1)
  if [ -z "$line" ]; then
    say "  !! $label -- failed, but not for the expected reason:"
    grep ': error: ' "$out" | sed "s#$dir/#    #" | head -4
    FAILED=$((FAILED+1)); return
  fi
  say "  ok $label"
  say "     $(printf '%s' "$line" | sed "s#$dir/##")"
  PASSED=$((PASSED+1))
}

# append <dir> <relpath> <text>
# For defects that do not need to sit at a particular call site. Anchored
# substitution is preferred where it works, because it proves the *real* call
# site is being checked; appending only proves the file is compiled. But an
# anchor is a copy of someone else's source line, and this tree is under active
# refactor — an anchor that drifts turns into "NOT PROVEN", which is correct but
# unhelpful. Anchors are used for the four defects whose value depends on hitting
# real code; appends for the rest.
append() {
  local dir="$1" rel="$2" text="$3"
  local f="$dir/$rel"
  [ -f "$f" ] || { say "    TARGET FILE MISSING: $rel"; return 1; }
  printf '\n%s\n' "$text" >> "$f"
}

m_theme_member() { substitute "$1" Features/Account/SettingsView.swift \
  'Text(banner.text).font(Theme.ui(13))' \
  'Text(banner.text).font(Theme.ui(13)).padding(Theme.s7)'; }

m_wrong_label() { substitute "$1" Features/Auth/AuthView.swift \
  '.cpCard(radius: 18, padding: 20)' \
  '.cpCard(cornerRadius: 18, padding: 20)'; }

# A misplaced closing paren — the classic visual-refactor slip — which applies
# the View modifier `.foregroundColor` to a `Font`.
m_modifier_wrong_type() { substitute "$1" Features/Account/DeveloperView.swift \
  '.font(Theme.mono(12)).foregroundColor(Theme.text2)' \
  '.font(Theme.mono(12).foregroundColor(Theme.text2))'; }

# ContentUnavailableView is iOS 17.0+ / macOS 14.0+. Under -target macos13.0 the
# frontend must reject it. This is the defect class the gate is WEAKEST at --
# it is caught by the macOS-13-is-the-peer-of-iOS-16 proxy, not by any real
# knowledge of iOS availability. See README, "Availability is a proxy".
m_ios17_api() { append "$1" Features/Account/NotificationsView.swift \
'private struct __CPLocalCheckIOS17Probe: View {
    var body: some View { ContentUnavailableView("None", systemImage: "bell") }
}'; }

m_type_error_no_gate_file() { substitute "$1" App/RootView.swift \
  'BrandWordmark(height: 40)' \
  'BrandWordmark(height: "40")'; }

# The remaining two of the seven files that had no local gate at all before this
# harness, each proven individually reachable by a Theme lookup.
m_reach_account() { append "$1" Features/Account/AccountView.swift \
  'private let __cpLocalCheckProbe: CGFloat = Theme.s7'; }
m_reach_subscription() { append "$1" Features/Account/SubscriptionView.swift \
  'private let __cpLocalCheckProbe: CGFloat = Theme.s9'; }

# ---- shim fidelity -----------------------------------------------------------
#
# Every shim in shims/ is a chance to accidentally weaken the gate. A shim typed
# `(_ x: Any)` would make its call site compile no matter what was passed, and
# the gate would then be *worse* than useless there: it would report PASS over a
# genuine typo. These probes assert the opposite -- that each shimmed entry point
# still rejects a misspelled member. If a future shim erases type information,
# the corresponding probe here stops failing and the self-test goes red.
_probe() { append "$1" Features/Account/DeveloperView.swift \
"private struct __CPLocalCheckProbe$2: View {
    var body: some View { $3 }
}"; }

m_shim_title_case()  { _probe "$1" A 'Text("x").navigationBarTitleDisplayMode(.inlin)'; }
m_shim_placement()   { _probe "$1" B 'Text("x").toolbar { ToolbarItem(placement: .navigationBarTrailingg) { Text("t") } }'; }
m_shim_keyboard()    { _probe "$1" C 'TextField("a", text: .constant("")).keyboardType(.url)'; }
m_shim_contenttype() { _probe "$1" D 'TextField("a", text: .constant("")).textContentType(.newPassord)'; }
m_shim_autocap()     { _probe "$1" E 'TextField("a", text: .constant("")).textInputAutocapitalization(.nevr)'; }
m_shim_liststyle()   { _probe "$1" F 'List { Text("x") }.listStyle(.insetGroupedd)'; }

run_selftest() {
  say "==> self-test: inject a defect into a COPY of the sources and require a catch"
  say "    (the real ios/CoreProp tree is never modified)"
  hr
  expect_catch "1. misspelled Theme member (Theme.s7, SettingsView)" \
    "SettingsView.swift" "no member 's7'" m_theme_member
  # Swift words this one "extra argument 'cornerRadius' in call" rather than
  # "incorrect argument label", because cpCard's parameters are all defaulted so
  # the mislabelled call still matches an arity the compiler can reach. The
  # defect is rejected either way; the expectation matches the wording Swift
  # actually emits rather than the wording we assumed.
  expect_catch "2. wrong argument label on cpCard(radius:padding:) (AuthView)" \
    "AuthView.swift" "cornerRadius" m_wrong_label
  expect_catch "3. View modifier applied to a Font (DeveloperView)" \
    "DeveloperView.swift" "foregroundColor" m_modifier_wrong_type
  expect_catch "4. iOS-17-only API under an iOS-16 target (NotificationsView)" \
    "NotificationsView.swift" "ContentUnavailableView" m_ios17_api
  expect_catch "5. type error in a no-local-gate file (RootView)" \
    "RootView.swift" "String" m_type_error_no_gate_file
  expect_catch "6. misspelled Theme member (AccountView)" \
    "AccountView.swift" "no member 's7'" m_reach_account
  expect_catch "7. misspelled Theme member (SubscriptionView)" \
    "SubscriptionView.swift" "no member 's9'" m_reach_subscription
  hr
  say "    shim fidelity -- a typo'd member of a SHIMMED API must still fail:"
  expect_catch "8.  .navigationBarTitleDisplayMode(.inlin)" \
    "DeveloperView.swift" "inlin" m_shim_title_case
  expect_catch "9.  ToolbarItem(placement: .navigationBarTrailingg)" \
    "DeveloperView.swift" "navigationBarTrailingg" m_shim_placement
  expect_catch "10. .keyboardType(.url)   [real case is .URL]" \
    "DeveloperView.swift" "url" m_shim_keyboard
  expect_catch "11. .textContentType(.newPassord)" \
    "DeveloperView.swift" "newPassord" m_shim_contenttype
  expect_catch "12. .textInputAutocapitalization(.nevr)" \
    "DeveloperView.swift" "nevr" m_shim_autocap
  expect_catch "13. .listStyle(.insetGroupedd)" \
    "DeveloperView.swift" "insetGroupedd" m_shim_liststyle
  hr
  if [ "$FAILED" -ne 0 ]; then
    say "SELFTEST FAIL  $PASSED proven, $FAILED not proven."
    say "A harness that cannot be shown to fail must not be trusted when it passes."
    return 1
  fi
  say "SELFTEST PASS  $PASSED/$PASSED defect classes caught."
  return 0
}

# Prove per-file coverage in one invocation: plant the same canary in EVERY app
# file and require that every single file is named in the diagnostics. A clean
# gate run only tells you nothing was wrong; this tells you every file was
# actually looked at. Cheap insurance against a `find` predicate or a batch-count
# bug quietly dropping files.
run_reach() {
  local dir total missing rel
  say "==> reach test: plant a canary error in every app file, require all be reported"
  dir=$(scratch_copy "reach")
  total=$(find "$dir" -name '*.swift' | wc -l | tr -d ' ')
  while IFS= read -r f; do
    printf '\nprivate let __cpLocalCheckCanary: Int = "not an Int"\n' >> "$f"
  done < <(find "$dir" -name '*.swift')
  typecheck "$dir" "$WORK/reach.log"
  # Matched on the path RELATIVE to the scratch root, never on the absolute
  # scratch path: absolute matching is fragile against tmp-path normalisation
  # (see the WORK= comment above) and against /var -> /private/var.
  missing=0
  hr
  while IFS= read -r rel; do
    if ! grep -q "/$rel:" "$WORK/reach.log"; then
      [ "$missing" -eq 0 ] && say "REACH FAIL  these files were NOT type-checked:"
      say "      $rel"
      missing=$((missing+1))
    fi
  done < <(cd "$dir" && find . -name '*.swift' | sed 's#^\./##' | sort)
  say "    files with a canary: $total"
  say "    files reported by the harness: $((total - missing))"
  if [ "$missing" -ne 0 ]; then
    say "REACH FAIL  $missing of $total file(s) unchecked."
    return 1
  fi
  say "REACH PASS  all $total app files are individually type-checked."
  return 0
}

# ------------------------------------------------------------------------- main

case "$MODE" in
  gate)     run_gate ;;
  selftest) run_gate; g=$?; hr; run_selftest; s=$?; [ $g -eq 0 ] && [ $s -eq 0 ] ;;
  reach)    run_reach ;;
esac
