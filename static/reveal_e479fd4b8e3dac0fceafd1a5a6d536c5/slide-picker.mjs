/** Standalone navigation uses the same main/appendix numbering as the player. */
export function installSlidePicker(reveal, container) {
  const tools = container.querySelector('.presentation-tools');
  if (!tools) return;
  const button = document.createElement('button');
  button.type = 'button';
  button.textContent = 'Slides';
  button.title = 'Jump to a slide (G)';
  button.setAttribute('aria-keyshortcuts', 'g');
  button.setAttribute('aria-haspopup', 'dialog');
  tools.prepend(button);

  const dialog = document.createElement('dialog');
  dialog.className = 'slide-picker';
  dialog.setAttribute('aria-label', 'Jump to a slide');
  dialog.innerHTML = `
    <div class="slide-picker-header">
      <h2>Jump to a slide</h2>
      <button type="button" class="slide-picker-close" aria-label="Close slide picker" title="Close (Esc)">×</button>
    </div>
    <input type="search" aria-label="Find a slide" placeholder="Search title, number or slide ID" autocomplete="off" spellcheck="false">
    <p class="slide-picker-status" role="status"></p>
    <nav class="slide-picker-list" aria-label="Slides"></nav>`;
  container.append(dialog);
  const input = dialog.querySelector('input');
  const list = dialog.querySelector('nav');
  const status = dialog.querySelector('[role="status"]');
  let main = 0, appendix = 0, previousControls, opener;
  // Reveal.getSlides() excludes uncounted slides; appendices must remain navigable.
  const entries = [...container.querySelectorAll(':scope > .slides > section')].map(slide => {
    const number = slide.dataset.visibility === 'uncounted' ? `A${++appendix}` : String(++main);
    const heading = slide.querySelector('h1,h2')?.cloneNode(true);
    heading?.querySelectorAll('br').forEach(br => br.replaceWith(' '));
    heading?.querySelectorAll('.anchor-link').forEach(anchor => anchor.remove());
    const title = heading?.textContent.replace(/\s+/g, ' ').trim() || slide.id;
    const link = document.createElement('a');
    link.href = `#/${encodeURIComponent(slide.id)}`;
    const badge = document.createElement('span');
    badge.className = 'slide-picker-number';
    badge.textContent = number;
    const label = document.createElement('span');
    label.textContent = title;
    link.append(badge, label);
    link.addEventListener('click', event => {
      if (event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
      event.preventDefault();
      close();
      const { h, v } = reveal.getIndices(slide);
      reveal.slide(h, v);
      reveal.toggleOverview(false);
    });
    list.append(link);
    return { slide, link, number, search: `${number} ${title} ${slide.id}`.toLowerCase() };
  });

  function filter() {
    const query = input.value.trim().toLowerCase();
    const numbered = /^(?:a)?\d+$/.test(query);
    for (const entry of entries) {
      entry.link.hidden = numbered ? entry.number.toLowerCase() !== query
        : !query.split(/\s+/).every(word => entry.search.includes(word));
    }
    const count = entries.filter(entry => !entry.link.hidden).length;
    status.textContent = count ? `${count} ${count === 1 ? 'slide' : 'slides'} · Enter to jump · Esc to close` : 'No slides match';
    list.scrollTop = 0;
  }

  function open() {
    if (dialog.open || document.querySelector('dialog[open]')) return;
    opener = document.activeElement;
    const config = reveal.getConfig();
    previousControls = { keyboard: config.keyboard, mouseWheel: config.mouseWheel, touch: config.touch };
    reveal.configure({ keyboard: false, mouseWheel: false, touch: false });
    input.value = '';
    for (const { slide, link } of entries) {
      if (slide === reveal.getCurrentSlide()) link.setAttribute('aria-current', 'page');
      else link.removeAttribute('aria-current');
    }
    filter();
    dialog.showModal();
    input.focus();
    list.querySelector('[aria-current]')?.scrollIntoView({ block: 'nearest' });
  }

  function close() {
    if (!dialog.open) return;
    dialog.close();
    reveal.configure(previousControls);
    (opener && opener !== document.body ? opener : button).focus({ preventScroll: true });
  }

  button.addEventListener('click', open);
  reveal.addKeyBinding({ keyCode: 71, key: 'G', description: 'Find and jump to a slide' }, open);
  input.addEventListener('input', filter);
  dialog.querySelector('.slide-picker-close').addEventListener('click', close);
  dialog.addEventListener('cancel', event => { event.preventDefault(); close(); });
  dialog.addEventListener('click', event => { if (event.target === dialog) close(); });
  dialog.addEventListener('keydown', event => {
    event.stopPropagation();
    if (event.ctrlKey || event.metaKey || event.altKey) return;
    const visible = entries.filter(entry => !entry.link.hidden).map(entry => entry.link);
    if (event.key === 'Enter' && event.target === input) {
      event.preventDefault();
      const current = !input.value.trim() && list.querySelector('[aria-current]');
      (current || visible[0])?.click();
    } else if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
      event.preventDefault();
      const index = visible.indexOf(document.activeElement);
      const next = index < 0 ? (event.key === 'ArrowDown' ? 0 : visible.length - 1)
        : index + (event.key === 'ArrowDown' ? 1 : -1);
      if (next < 0) input.focus();
      else visible[Math.min(next, visible.length - 1)]?.focus();
    }
  });
}
