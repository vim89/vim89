// Fullscreen belongs to the document, so slides and document viewers share one mode.
// Controls are looked up on use: deck modules may add their own (a document viewer's toolbar).
const buttons = () => document.querySelectorAll('[data-fullscreen]');
const statuses = () => document.querySelectorAll('.fullscreen-status');

function updateControls() {
  const label = document.fullscreenElement ? 'Exit fullscreen' : 'Fullscreen';
  for (const button of buttons()) {
    const text = button.querySelector('[data-fullscreen-label]');
    if (text) text.textContent = label;
    button.setAttribute('aria-label', label);
    button.title = `${label} (F)`;
  }
  for (const status of statuses()) status.hidden = true;
}

async function toggleFullscreen() {
  try {
    if (document.fullscreenElement) await document.exitFullscreen();
    else await document.documentElement.requestFullscreen();
  } catch {
    for (const status of statuses()) {
      status.textContent = 'Fullscreen is unavailable here. Open this presentation in a browser window and use its fullscreen command.';
      status.hidden = false;
    }
  }
}

document.addEventListener('fullscreenchange', updateControls);
document.addEventListener('click', event => {
  if (event.target.closest('[data-fullscreen]')) toggleFullscreen();
});
// Let open dialogs handle Escape before the presentation exits fullscreen.
document.addEventListener('keydown', event => {
  if (event.key === 'Escape' && document.querySelector('dialog[open]')) return;
  const exiting = event.key === 'Escape' && document.fullscreenElement;
  if (!exiting && (event.key.toLowerCase() !== 'f' || event.ctrlKey || event.metaKey || event.altKey)) return;
  if (!exiting && event.target.closest('input, textarea, select, [contenteditable]:not([contenteditable="false"])')) return;
  event.preventDefault();
  event.stopImmediatePropagation();
  if (!event.repeat) toggleFullscreen();
}, true);
updateControls();
