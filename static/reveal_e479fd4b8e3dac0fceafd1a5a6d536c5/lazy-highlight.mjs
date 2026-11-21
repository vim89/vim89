/** Keep the large syntax library off the initial path until a code slide is shown. */
export function lazyHighlight(target = {}) {
  let reveal, loading;
  const proxy = Object.assign(target, {
    id: 'highlight',
    init(deck) {
      reveal = deck;
      const highlightCurrent = () => {
        const slide = reveal.getCurrentSlide();
        if (slide?.querySelector('pre code')) load().then(plugin => highlightSlides(plugin, [slide])).catch(console.error);
      };
      if (new URLSearchParams(location.search).has('print-pdf')) return load();
      reveal.on('ready', highlightCurrent);
      reveal.on('slidechanged', highlightCurrent);
    },
    async highlightBlock(block) {
      if (loading) {
        const plugin = await loading;
        plugin.highlightBlock(block);
      } else {
        const plugin = await load();
        if (!block.dataset.highlighted) plugin.highlightBlock(block);
      }
    }
  });
  function load() {
    return loading ??= (async () => {
      const { default: Highlight } = await import('./vendor/reveal/dist/plugin/highlight.mjs');
      const plugin = Highlight();
      // Markdown has already escaped code. Annotations can also have added spans
      // before this import resolves; highlight.js reads their textContent.
      reveal.configure({ highlight: { ...reveal.getConfig().highlight, escapeHTML: false, highlightOnLoad: false } });
      await plugin.init(reveal);
      Object.assign(proxy, plugin);
      if (new URLSearchParams(location.search).has('print-pdf')) {
        highlightSlides(plugin, [...reveal.getRevealElement().querySelectorAll('section')]);
      }
      return plugin;
    })();
  }
  function highlightSlides(plugin, slides) {
    const changed = slides.filter(slide => {
      const blocks = [...slide.querySelectorAll('pre code')].filter(block => !block.dataset.highlighted);
      blocks.forEach(block => plugin.highlightBlock(block));
      return blocks.length > 0;
    });
    if (changed.length) reveal.getRevealElement().dispatchEvent(new CustomEvent('code:highlighted', {
      bubbles: true, detail: { slides: changed }
    }));
  }
  return proxy;
}
