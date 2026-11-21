/* Render ```mermaid fences into SVG, offline, with the deck's own palette.
 *
 * The Markdown pipeline already lowers a ```mermaid fence to the conventional
 * <div class="mermaid">…</div> (verified in built output), but the theme ships
 * no mermaid runtime, so nothing renders it. This module supplies one.
 * `code.language-mermaid` is also accepted in case that lowering changes.
 *
 * mermaid is vendored at vendor/mermaid.min.js (UMD, self-contained, 3.4MB) —
 * NOT the ESM build, whose 130 lazy chunks total 43MB and would need network
 * or a much larger vendor tree. A conference room's wifi is not a dependency.
 */

const BUNDLE = new URL('vendor/mermaid.min.js', import.meta.url).href;

function loadMermaid() {
  // UMD sets window.mermaid; a module script can't import it directly.
  return new Promise((resolve, reject) => {
    if (window.mermaid) return resolve(window.mermaid);
    const tag = document.createElement('script');
    tag.src = BUNDLE;
    tag.onload = () => window.mermaid ? resolve(window.mermaid)
                                      : reject(new Error('mermaid did not register'));
    tag.onerror = () => reject(new Error(`could not load ${BUNDLE}`));
    document.head.appendChild(tag);
  });
}

/** Read a palette off a slide so diagrams match whichever slide they sit on.
 *  The theme swaps --ink/--paper/--accent/--muted/--rule on `.dark-slide`, so
 *  resolving against the slide element is what keeps a diagram legible on both.
 */
function paletteFor(element) {
  const style = getComputedStyle(element);
  const dark = element.classList.contains('dark-slide');
  const read = (name, fallback) => (style.getPropertyValue(name).trim() || fallback);
  const ink = read('--ink', '#19242a');
  const paper = read('--paper', '#f5f2eb');
  const accent = read('--accent', '#c92f39');
  const muted = read('--muted', '#58626a');
  const rule = read('--rule', '#c9c8c0');
  return {
    // Keep node fills close to the slide so diagrams read as part of it,
    // not as a pasted-in screenshot with its own white box.
    background: paper,
    darkMode: dark,
    primaryColor: dark ? '#1b4054' : '#e8f2f8',
    primaryTextColor: ink,
    primaryBorderColor: accent,
    secondaryColor: dark ? '#1c504b' : '#e2f3ec',
    secondaryTextColor: ink,
    secondaryBorderColor: rule,
    tertiaryColor: dark ? '#584138' : '#fff0e4',
    tertiaryTextColor: ink,
    tertiaryBorderColor: rule,
    lineColor: muted,
    textColor: ink,
    mainBkg: dark ? '#1b4054' : '#e8f2f8',
    nodeBorder: rule,
    clusterBkg: 'transparent',
    clusterBorder: rule,
    edgeLabelBackground: paper,
    fontFamily: style.getPropertyValue('font-family') || 'inherit',
    fontSize: '27px'
  };
}

function flowClasses(slide) {
  const dark = slide.classList.contains('dark-slide');
  const ink = getComputedStyle(slide).getPropertyValue('--ink').trim() || '#142b42';
  const colors = dark
    ? { input: ['#213f5c', '#83b6e1'], work: ['#1a4f49', '#7de3cb'],
        boundary: ['#594239', '#ffb89c'], result: ['#24564f', '#9ce8ce'] }
    : { input: ['#e8f2fa', '#547899'], work: ['#e1f3ec', '#00796f'],
        boundary: ['#fff0e4', '#bd6b42'], result: ['#d8f0e9', '#00796f'] };
  return Object.entries(colors).map(([name, [fill, stroke]]) =>
    `classDef ${name} fill:${fill},stroke:${stroke},stroke-width:2px,color:${ink};`
  ).join('\n');
}

let counter = 0;

async function renderOne(mermaid, node) {
  // A <div class="mermaid"> is its own host; a fenced code block is wrapped
  // in <pre>, and it is the <pre> that must be replaced.
  const host = node.matches('div.mermaid') ? node : (node.closest('pre') || node);
  const slide = host.closest('section') || document.body;
  const source = node.textContent;
  if (!source.trim()) return;
  const styledSource = /^\s*flowchart\s/m.test(source)
    ? `${source}\n${flowClasses(slide)}`
    : source;

  mermaid.initialize({
    startOnLoad: false,
    securityLevel: 'strict',
    theme: 'base',
    themeVariables: paletteFor(slide),
    // Measure the same bold labels that the deck stylesheet displays.
    themeCSS: '.nodeLabel { font-weight: 600; }',
    flowchart: { curve: 'basis', htmlLabels: true, padding: 12,
                 nodeSpacing: 38, rankSpacing: 52, useMaxWidth: true },
    sequence: { useMaxWidth: true }
  });

  const figure = document.createElement('div');
  figure.className = 'mermaid-figure';
  // Carry the fence's own attribute classes (e.g. {.fragment .step}) across,
  // so a diagram can be click-revealed like any other block.
  for (const name of node.classList) {
    if (!['language-mermaid', 'nohighlight', 'mermaid'].includes(name)) {
      figure.classList.add(name);
    }
  }
  for (const name of host.classList) {
    if (name.startsWith('fragment') || name === 'step' || name === 'verdict') {
      figure.classList.add(name);
    }
  }

  try {
    const { svg } = await mermaid.render(`mmd-${++counter}`, styledSource);
    figure.innerHTML = svg;
    host.replaceWith(figure);
  } catch (error) {
    // A broken diagram must not take the slide with it: leave the source
    // visible and say so, rather than rendering an empty box on stage.
    console.error('mermaid render failed', error);
    host.classList.add('mermaid-failed');
  }
}

export async function installMermaid(root = document) {
  const blocks = [...root.querySelectorAll('div.mermaid, code.language-mermaid')];
  if (!blocks.length) return;
  let mermaid;
  try {
    mermaid = await loadMermaid();
  } catch (error) {
    console.error(error);
    blocks.forEach(block => (block.closest('pre') || block).classList.add('mermaid-failed'));
    return;
  }
  // Sequential: mermaid.initialize is global state, and each diagram needs the
  // palette of the slide it lives on.
  for (const block of blocks) await renderOne(mermaid, block);
  if (window.Reveal?.isReady?.()) window.Reveal.layout();
}
