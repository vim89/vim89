import { installSlideFitting } from './slide-fit.mjs';
import { lazyHighlight } from './lazy-highlight.mjs';

/** One custom element per occurrence, including repeated embeds of the same deck.
 * Shadow DOM isolates IDs and CSS; embedded Reveal never owns the host URL.
 */
class RevealDeck extends HTMLElement {
  async connectedCallback() {
    const generation = this.generation = (this.generation || 0) + 1;
    const current = () => this.isConnected && generation === this.generation;
    try {
      const template = this.querySelector('template');
      if (!template) throw new Error('Missing deck template');
      const root = this.shadowRoot || this.attachShadow({ mode: 'open' });
      root.replaceChildren(template.content.cloneNode(true));
      delete this.dataset.error;
      const base = new URL(this.dataset.runtimeAssets || this.dataset.assets, document.baseURI);
      const contentBase = new URL(this.dataset.contentBase, document.baseURI);
      const container = root.querySelector('.reveal');
      container.tabIndex = 0;
      container.setAttribute('role', 'region');
      container.setAttribute('aria-label', this.getAttribute('aria-label') || 'Presentation');
      // Content assets have their own location; a standalone deck page is optional.
      for (const element of container.querySelectorAll('[src], [href], [poster], [data-src]')) {
        for (const attribute of ['src', 'href', 'poster', 'data-src']) {
          const value = element.getAttribute(attribute);
          if (value && !value.startsWith('#')) element.setAttribute(attribute, new URL(value, contentBase).href);
        }
      }
      const styles = [...root.querySelectorAll('link[rel="stylesheet"]')].map(link =>
        link.sheet ? Promise.resolve() : new Promise((resolve, reject) => {
          link.addEventListener('load', resolve, { once: true });
          link.addEventListener('error', () => reject(new Error(`Cannot load ${link.href}`)), { once: true });
        }));
      const [{ default: Reveal }] = await Promise.all([
        import(new URL('vendor/reveal/dist/reveal.mjs', base).href),
        ...styles
      ]);
      if (!current()) return;
      const slides = [...container.querySelectorAll('.slides > section')];
      const main = slides.filter(slide => slide.dataset.visibility !== 'uncounted');
      const appendices = slides.filter(slide => slide.dataset.visibility === 'uncounted');
      const reveal = new Reveal(container, {
        embedded: true,
        width: 1600, height: 900, margin: 0, minScale: 0.05, maxScale: 2,
        center: false, hash: false, history: false, respondToHashChanges: false,
        viewDistance: 1, mobileViewDistance: 1,
        keyboardCondition: () => this.matches(':focus-within'),
        scrollActivationWidth: null,
        controls: true, controlsTutorial: false, progress: true,
        transition: matchMedia('(prefers-reduced-motion: reduce)').matches ? 'none' : 'fade',
        backgroundTransition: 'none',
        slideNumber: false,
        plugins: [lazyHighlight()]
      });
      this.reveal = reveal;
      // Clicking any slide also gives this player keyboard focus.
      container.addEventListener('pointerdown', () => container.focus({ preventScroll: true }));
      await reveal.initialize();
      if (!current()) return;
      // Reveal's built-in counter links to the document hash even with hash:false.
      // A plain counter keeps the containing article's URL entirely host-owned.
      const counter = container.querySelector('.slide-number');
      counter.style.display = 'block';
      const updateCounter = () => {
        const slide = reveal.getCurrentSlide();
        const index = main.indexOf(slide);
        counter.textContent = index >= 0 ? `${index + 1} / ${main.length}` : `A${appendices.indexOf(slide) + 1}`;
      };
      reveal.on('slidechanged', updateCounter);
      updateCounter();
      const disposeFit = await installSlideFitting(reveal, container, root);
      if (!current()) { disposeFit(); return; }
      this.disposeFit = disposeFit;
      this.resize = new ResizeObserver(() => reveal.layout());
      this.resize.observe(this);
      this.dataset.ready = 'true';
    } catch (error) {
      if (!current()) return;
      this.resize?.disconnect();
      this.disposeFit?.();
      this.reveal?.destroy();
      this.dataset.error = 'true';
      // Restore the fallback notice or optional standalone link if enhancement fails.
      this.shadowRoot?.replaceChildren(document.createElement('slot'));
      console.error('Could not initialize embedded presentation', error);
    }
  }

  disconnectedCallback() {
    this.generation = (this.generation || 0) + 1;
    this.resize?.disconnect();
    this.disposeFit?.();
    this.reveal?.destroy();
    this.reveal = undefined;
    delete this.dataset.ready;
  }
}

if (!customElements.get('reveal-deck')) customElements.define('reveal-deck', RevealDeck);
