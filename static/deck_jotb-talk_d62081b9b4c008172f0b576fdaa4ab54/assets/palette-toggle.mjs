/* Presenter-toggleable brand-color palette. "semantic" (distinct per-brand
 * colors — see jotb.css's own comment above the variable blocks for the
 * full reasoning) is primary/default; "ember" (a unified warm accent
 * blending Spark's and DataFusion's orange) is the backup. Both are real,
 * complete CSS variable sets; this only switches which one applies, via a
 * `data-palette` attribute on <html> — scoped there rather than on
 * `.reveal` so it also reaches `.reveal-viewport`, a sibling element,
 * without duplicating the attribute on both.
 *
 * Two independent ways to switch, both one step: append `?palette=ember`
 * to the URL (a link you can hand someone, or type once), or press "P" at
 * any time while presenting. Either one persists via localStorage, so a
 * plain reload afterward keeps the last explicit choice.
 */
const KEY = 'jotb-palette';
const PALETTES = ['semantic', 'ember'];

function apply(palette) {
  document.documentElement.dataset.palette = palette;
}

export function installPaletteToggle(reveal) {
  const fromUrl = new URLSearchParams(location.search).get('palette');
  const initial = PALETTES.includes(fromUrl) ? fromUrl
    : PALETTES.includes(localStorage.getItem(KEY)) ? localStorage.getItem(KEY)
    : PALETTES[0];
  apply(initial);
  if (fromUrl && fromUrl !== localStorage.getItem(KEY)) localStorage.setItem(KEY, initial);

  const cycle = () => {
    const current = document.documentElement.dataset.palette;
    const next = PALETTES[(PALETTES.indexOf(current) + 1) % PALETTES.length];
    apply(next);
    localStorage.setItem(KEY, next);
  };

  // Registering through Reveal (rather than a bare keydown listener) means
  // the binding also shows up in Reveal's own built-in "?" shortcut-help
  // overlay, for free.
  reveal.addKeyBinding({ keyCode: 80, key: 'P', description: 'Toggle brand color palette (semantic / ember)' }, cycle);
}
