/**
 * CoreProp — toolbar popup.
 *
 * Exists so the extension has any surface at all outside the PrizePicks tab.
 * Previously the action had a title but no popup, so clicking the toolbar icon
 * did nothing and there was nowhere to see whether the extension was alive or
 * why the last slip failed.
 */

document.getElementById('ver').textContent = chrome.runtime.getManifest().version;

chrome.runtime.sendMessage({ type: 'coreprop:get-last-result' }, resp => {
  if (chrome.runtime.lastError || !resp || !resp.data) return;
  const r = resp.data;
  document.getElementById('last').textContent = `${r.legs_staged}/${r.legs_total} staged`;

  const box = document.getElementById('fails');
  for (const f of (r.failures || []).slice(0, 6)) {
    const d = document.createElement('div');
    d.className = 'fail';
    d.textContent = `${f.player} ${f.prop} — ${f.reason}`;
    box.appendChild(d);
  }
});
