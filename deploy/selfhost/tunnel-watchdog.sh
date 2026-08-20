#!/bin/bash
# Restart the Cloudflare tunnel when it is wedged in a long reconnect backoff.
#
# WHY THIS EXISTS
#   cloudflared retries a dropped edge connection with exponential backoff that
#   caps at ~64s. On this host the WiFi drops briefly ~115 times a day
#   ("sendmsg: network is unreachable" in tunnel.err.log), and 90 of those
#   escalated to the full 64s cap — so a five-second blip becomes a two-minute
#   outage for every user. cloudflared is behaving correctly; the backoff is
#   just tuned for a server with a stable uplink, which this laptop is not.
#
# WHAT IT WILL NOT DO
#   * It never restarts while the app itself is down — that is a different
#     fault and bouncing the tunnel would hide it.
#   * It never restarts while the internet is genuinely unreachable, because
#     reconnecting is impossible and restarting would only thrash.
#   * It respects a cooldown, so a sustained outage triggers one restart per
#     COOLDOWN window rather than a restart every time it runs.
set -uo pipefail

PUBLIC_URL="https://coreprop.me/health"
LOOPBACK_URL="http://127.0.0.1:8010/health"
SERVICE="com.coreprop.tunnel"
STAMP="/tmp/.coreprop-tunnel-watchdog-last-restart"
COOLDOWN=180          # seconds between restarts
LOG="$HOME/Library/Logs/coreprop/watchdog.log"

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') $*" >> "$LOG"; }

code() { curl -s -m 10 -o /dev/null -w '%{http_code}' "$1" 2>/dev/null || echo 000; }

pub=$(code "$PUBLIC_URL")
[ "$pub" = "200" ] && exit 0          # healthy, nothing to do

# Confirm with a second probe — one dropped packet is not an outage.
sleep 5
pub=$(code "$PUBLIC_URL")
[ "$pub" = "200" ] && exit 0

loop=$(code "$LOOPBACK_URL")
if [ "$loop" != "200" ]; then
  log "public=$pub loopback=$loop — app is down, NOT a tunnel fault; leaving it alone"
  exit 0
fi

# Is the internet actually up? If not, cloudflared cannot reconnect no matter
# how many times we restart it, and restarting would only thrash.
#
# Three targets, because a single one produced a false negative in testing and
# cost 30s of extra downtime: this check gates recovery, so a spurious "no
# internet" is not merely noise, it delays the fix. Any one succeeding is proof
# enough. --head keeps it cheap.
online=0
for probe in https://1.1.1.1/ https://www.cloudflare.com/cdn-cgi/trace https://dns.google/; do
  if curl -s -m 6 --head -o /dev/null "$probe" 2>/dev/null; then online=1; break; fi
done
if [ "$online" -ne 1 ]; then
  log "public=$pub but no internet (3 probes failed) — waiting for the network"
  exit 0
fi

now=$(date +%s)
last=$(cat "$STAMP" 2>/dev/null || echo 0)
if [ $((now - last)) -lt "$COOLDOWN" ]; then
  log "public=$pub — restart suppressed, last was $((now - last))s ago"
  exit 0
fi

echo "$now" > "$STAMP"
log "public=$pub loopback=200 internet=up — tunnel wedged, restarting $SERVICE"
launchctl kickstart -k "gui/$(id -u)/$SERVICE" 2>>"$LOG" || \
  log "kickstart failed (is $SERVICE loaded?)"
