// Literal code targets keep Markdown fences copyable and syntax highlighting intact.
import { layoutReleaseTimelines } from './release-timeline.mjs';
import './notebook-embed.mjs';
const svgNS = 'http://www.w3.org/2000/svg';
const groupSelector = '.code-walkthrough';
const noteSelector = '.code-note[data-code]';
const highlightSelector = '[data-code-highlight]';
const annotationSelector = `${noteSelector}, ${highlightSelector}`;
const extensions = note => [...note.querySelectorAll('[data-code-extend]')]
  .filter(extension => extension.closest('.code-note') === note);

// Commas remain literal code. Additional disjoint targets are an explicit JSON list.
// Reveal extensions expand the primary target; additional targets stay highlighted.
export function annotationTargets(primary, additional, attribute = 'data-code-also') {
  const initial = primary == null ? [] : [primary];
  if (!additional) return initial;
  const targets = JSON.parse(additional);
  if (!Array.isArray(targets) || targets.some(text => typeof text !== 'string' || !text.length)) {
    throw new TypeError(`${attribute} must be a JSON array of non-empty strings`);
  }
  return [...new Set([...initial, ...targets])];
}

// Each repeated occurrence needs its own connector, even for one literal target.
export function annotationOccurrences(source, text, selection = 1) {
  if (!text) return [];
  if (selection === 'all') {
    return Array.from({ length: source.split(text).length - 1 }, (_, index) => index + 1);
  }
  const occurrence = Number(selection);
  return Number.isInteger(occurrence) && occurrence > 0 ? [occurrence] : [];
}

// Named sources keep note examples from shifting fence references. Legacy slides
// retain their numbered lookup.
export function annotationCode(codes, marker) {
  if (marker.dataset.codeRef) {
    // Excludes `code.language-mermaid`: that class means mermaid-init.mjs
    // hasn't replaced this fence with a rendered `.mermaid-figure` yet. If
    // annotateSlides ever runs in that narrow window, this must not treat
    // raw mermaid source as a code target — "P" appears inside "Parquet"
    // and as a bare node id, so it would silently match the wrong thing
    // instead of correctly falling through to annotationDiagram.
    return codes.find(code => !code.closest('.code-note') && !code.matches('.language-mermaid') &&
      code.closest('[data-walkthrough-code]')?.dataset.walkthroughCode === marker.dataset.codeRef);
  }
  return codes[Number(marker.dataset.codeBlock || 1) - 1];
}

// Diagram targets: this deck's own addition, not in the reference engine
// (which only ever resolves `pre code`). A rendered mermaid diagram has no
// `<code>` inside its `[data-walkthrough-code]` holder — `.mermaid-figure`
// replaces the `<pre>` once mermaid-init.mjs runs — so finding one here
// means `annotationCode` already returned nothing for the same marker.
export function annotationDiagram(group, marker) {
  if (!marker.dataset.codeRef) return undefined;
  return [...group.querySelectorAll('[data-walkthrough-code]')]
    .find(holder => holder.dataset.walkthroughCode === marker.dataset.codeRef
      && holder.querySelector('.mermaid-figure'));
}

function clearAnnotations(slide) {
  slide.querySelectorAll('.annotation-lines').forEach(node => node.remove());
  for (const note of slide.querySelectorAll('[data-code-note-sizes]')) {
    delete note.dataset.codeNoteSizes;
    note.style.removeProperty('--code-note-height');
    note.style.removeProperty('--code-note-full-height');
  }
  for (const mark of slide.querySelectorAll('mark[data-code-anchor]')) {
    const parent = mark.parentNode;
    mark.replaceWith(...mark.childNodes);
    parent.normalize();
  }
  for (const shape of slide.querySelectorAll('[data-code-anchor].diagram-anchor-live')) {
    shape.classList.remove('diagram-anchor-live');
    delete shape.dataset.codeAnchor;
    delete shape.dataset.codeStep;
    delete shape.dataset.codeTarget;
  }
}

// A diagram node has no text run to wrap — the whole mermaid-rendered shape
// becomes the anchor instead of a <mark>. Mermaid node ids are unique per
// render, so (unlike `highlight()`) there is no occurrence/extension concept
// here: one literal is one node id is one shape, always.
function highlightShape(diagram, nodeId, key, step, target) {
  const node = diagram.querySelector(`g[id*="flowchart-${CSS.escape(nodeId)}-"]`);
  if (!node) return false;
  node.classList.add('diagram-anchor-live');
  node.dataset.codeAnchor = key;
  node.dataset.codeStep = step;
  node.dataset.codeTarget = target;
  return true;
}

// A token can cross several highlight.js spans. Wrap only its text-node pieces,
// without flattening syntax spans or changing the copied code, even by one space.
function highlight(code, text, occurrence, key, tone, step = 0, previous, target = 0) {
  if (!text || !Number.isInteger(occurrence) || occurrence < 1) return false;
  const source = code.textContent;
  let start = -1;
  if (previous) {
    // Match the expansion around this occurrence, even when the same shorter
    // target appears elsewhere. Each step must contain the preceding range.
    do {
      start = source.indexOf(text, start + 1);
      if (start < 0 || start > previous.start) return false;
    } while (start + text.length < previous.end);
  } else {
    for (let n = 0; n < occurrence; n++) {
      start = source.indexOf(text, start < 0 ? 0 : start + text.length);
      if (start < 0) return false;
    }
  }
  const end = start + text.length;
  const walker = document.createTreeWalker(code, NodeFilter.SHOW_TEXT);
  const pieces = [];
  let offset = 0;
  while (walker.nextNode()) {
    const node = walker.currentNode;
    const next = offset + node.length;
    if (node.length && offset < end && next > start) pieces.push({ node, from: Math.max(0, start - offset), to: Math.min(node.length, end - offset) });
    offset = next;
  }
  // An expansion may include its own earlier marks, but never another note's.
  const owner = node => node.parentElement.closest('[data-code-anchor]');
  if (pieces.some(({ node }) => owner(node) && owner(node).dataset.codeAnchor !== key)) return false;
  for (const { node, from, to } of pieces) {
    if (owner(node)) continue;
    const selected = node.splitText(from);
    selected.splitText(to - from);
    const mark = document.createElement('mark');
    mark.className = `code-emphasis ${tone}`;
    mark.dataset.codeAnchor = key;
    mark.dataset.codeStep = step;
    mark.dataset.codeTarget = target;
    selected.replaceWith(mark);
    mark.append(selected);
  }
  return { start, end };
}

function svgElement(name, attributes) {
  const element = document.createElementNS(svgNS, name);
  for (const [key, value] of Object.entries(attributes)) element.setAttribute(key, value);
  return element;
}

// Synchronize highlights and note growth without moving the surrounding layout.
// The underlying code stays readable through every fragment state.
export function syncAnnotationVisibility(slide, complete = false) {
  if (!slide) return;
  complete ||= document.documentElement.classList.contains('reveal-print') || !!slide.closest('.reveal.overview');
  for (const [groupIndex, group] of [...slide.querySelectorAll(groupSelector)].entries()) {
    for (const [noteIndex, note] of [...group.querySelectorAll(noteSelector)].entries()) {
      const steps = [note, ...extensions(note)];
      const visible = steps.map(step => complete || !step.closest('.fragment:not(.visible)'));
      if (note.dataset.codeNoteSizes) {
        const sizes = JSON.parse(note.dataset.codeNoteSizes);
        const height = steps.reduce((height, step, index) =>
          visible[index] ? Math.max(height, sizes[index]) : height, sizes[0]);
        note.style.setProperty('--code-note-height', `${height}px`);
      }
      for (const decoration of slide.querySelectorAll(`[data-code-anchor="${groupIndex}-${noteIndex}"]`)) {
        decoration.classList.toggle('annotation-pending', !visible[Number(decoration.dataset.codeStep || 0)]);
        if (decoration.dataset.codeConnectorSteps) {
          const geometry = JSON.parse(decoration.dataset.codeConnectorSteps)[Math.max(0, visible.lastIndexOf(true))];
          decoration.querySelector('path').setAttribute('d', geometry.d);
          const dot = decoration.querySelector('circle');
          dot.setAttribute('cx', geometry.x);
          dot.setAttribute('cy', geometry.y);
        }
      }
    }
  }
}

export function annotateSlides(slides) {
  layoutReleaseTimelines(slides);
  const selected = slides.filter(slide => slide.querySelector(annotationSelector));
  // Also clean up when the final annotation was removed during live editing.
  slides.forEach(clearAnnotations);
  if (!selected.length) return;
  const ruler = document.createElement('div');
  ruler.className = 'reveal fit-measure annotation-measure';
  ruler.setAttribute('aria-hidden', 'true');
  const deckStyle = document.querySelector('body > .reveal')?.style;
  if (deckStyle) for (const property of deckStyle) {
    if (property.startsWith('--')) ruler.style.setProperty(property, deckStyle.getPropertyValue(property));
  }
  const track = ruler.appendChild(document.createElement('div'));
  track.className = 'slides';
  document.body.append(ruler);
  try {
    for (const slide of selected) {
      const groups = [...slide.querySelectorAll(groupSelector)];
      for (const [groupIndex, group] of groups.entries()) {
        const codes = [...group.querySelectorAll('pre code')];
        // Standalone highlights can be declared on a caption in the same group.
        // They stay visible throughout reveals and do not need a connector note.
        for (const [index, marker] of [...group.querySelectorAll(highlightSelector)].entries()) {
          const code = annotationCode(codes, marker);
          if (!code) continue;
          const key = `highlight-${groupIndex}-${index}`;
          const tone = ['error', 'question', 'result'].includes(marker.dataset.codeTone) ? marker.dataset.codeTone : '';
          for (const [target, text] of annotationTargets(marker.dataset.codeHighlight, marker.dataset.codeAlso).entries()) {
            for (const occurrence of annotationOccurrences(code.textContent, text, marker.dataset.codeOccurrence || 1)) {
              highlight(code, text, occurrence, key, tone, 0, undefined, `${target}-${occurrence}`);
            }
          }
        }
        for (const [noteIndex, note] of [...group.querySelectorAll(noteSelector)].entries()) {
          const code = annotationCode(codes, note);
          const tone = note.classList.contains('error') ? 'error' : note.classList.contains('result') ? 'result' : note.classList.contains('question') ? 'question' : '';
          const key = `${groupIndex}-${noteIndex}`;
          if (!code) {
            // Not a code target — try a diagram target instead (this deck's
            // own addition). Each literal is a mermaid node id, matched
            // exactly once; there is no occurrence/extension concept for
            // shapes, unlike the text-mark path below.
            const diagram = annotationDiagram(group, note);
            if (!diagram) continue;
            annotationTargets(note.dataset.code, note.dataset.codeAlso)
              .forEach((nodeId, target) => highlightShape(diagram, nodeId, key, 0, `${target}-1`));
            continue;
          }
          const targets = annotationTargets(note.dataset.code, note.dataset.codeAlso);
          const occurrences = text => annotationOccurrences(code.textContent, text, note.dataset.codeOccurrence || 1);
          for (const [target, text] of targets.entries()) {
            if (target === 0) continue;
            for (const occurrence of occurrences(text)) {
              highlight(code, text, occurrence, key, tone, 0, undefined, `${target}-${occurrence}`);
            }
          }
          for (const occurrence of occurrences(note.dataset.code)) {
            const target = `0-${occurrence}`;
            let range = highlight(code, note.dataset.code, occurrence, key, tone, 0, undefined, target);
            if (range) for (const [step, extension] of extensions(note).entries()) {
              range = highlight(code, extension.dataset.codeExtend, 1, key, tone, step + 1, range, target) || range;
            }
          }
          // Emphasize matching DSL segments inside the note's own example too.
          // These share its reveal and color, without drawing self-connectors.
          for (const example of note.querySelectorAll('pre code')) {
            for (const text of annotationTargets(null, note.dataset.codeLocal, 'data-code-local')) {
              highlight(example, text, 1, key, tone);
            }
          }
        }
      }
      // Use the same fixed canvas as slide fitting. Inactive slides and thumbnail
      // sources then have correct geometry without making them visible or refitting.
      const copy = document.createElement('section');
      copy.className = slide.className;
      copy.append(slide.querySelector('.slide-body').cloneNode(true));
      copy.querySelectorAll('[id]').forEach(node => node.removeAttribute('id'));
      track.replaceChildren(copy);
      const bodyBounds = copy.querySelector('.slide-body').getBoundingClientRect();
      for (const [groupIndex, group] of [...copy.querySelectorAll(groupSelector)].entries()) {
        const bounds = group.getBoundingClientRect();
        if (!bounds.width || !bounds.height) continue;
        const svg = svgElement('svg', {
          class: 'annotation-lines', viewBox: `0 0 ${bounds.width} ${bounds.height}`,
          'aria-hidden': 'true', focusable: 'false', preserveAspectRatio: 'none'
        });
        for (const [noteIndex, note] of [...group.querySelectorAll(noteSelector)].entries()) {
          const label = note.getBoundingClientRect();
          const additions = extensions(note);
          let baseHeight = label.height;
          if (additions.length) {
            const padding = parseFloat(getComputedStyle(note).paddingBottom);
            baseHeight = additions[0].getBoundingClientRect().top - label.top
              - parseFloat(getComputedStyle(additions[0]).marginTop) + padding;
            const sizes = [baseHeight, ...additions.map(addition =>
              Math.min(label.height, addition.getBoundingClientRect().bottom - label.top + padding))];
            const sourceNote = groups[groupIndex].querySelectorAll(noteSelector)[noteIndex];
            sourceNote.dataset.codeNoteSizes = JSON.stringify(sizes);
            sourceNote.style.setProperty('--code-note-full-height', `${label.height}px`);
          }
          const marks = [...group.querySelectorAll(`mark[data-code-anchor="${groupIndex}-${noteIndex}"]`)]
            .filter(mark => !mark.closest('.code-note'));
          const targetIds = [...new Set(marks.map(mark => mark.dataset.codeTarget || '0'))];
          for (const targetId of targetIds) {
            const targetMarks = marks.filter(mark => (mark.dataset.codeTarget || '0') === targetId);
            const rects = targetMarks.flatMap(mark => [...mark.getClientRects()].map(rect => ({ rect, step: Number(mark.dataset.codeStep) })));
            if (!rects.length) continue;
            // Route above or below the highlighted detail before curving through
            // the gutter. Opposite sides separate two targets on the same line.
            const side = Number(targetId.split('-')[0]) === 0
              ? note.dataset.codeSide : note.dataset.codeAlsoSide ?? note.dataset.codeSide;
            const code = marks[0].closest('pre').getBoundingClientRect();
            // The gutter-routing formula below assumes the note sits beside
            // the code (shares its vertical range) — it routes out to the
            // code block's right edge, then curves back through a gutter.
            // Several demo slides (confirmed: "300 - demo-dbt.md", a
            // two-column compact layout with the note stacked directly
            // below its code, not beside it) have no such gutter: the
            // formula still fires, sending the line far right before
            // curving back down and left straight through the note's own
            // text. Detected by vertical-range overlap, not assumed from
            // layout class names, since a slide's CSS can change independent
            // of this script. A direct connector from the mark's own bottom
            // edge to the note avoids the gutter concept entirely.
            const stacked = code.bottom <= label.top + 1;
            const geometries = stacked
              ? [note, ...additions].map((_, step) => {
                  const target = rects.filter(entry => entry.step <= step).at(-1).rect;
                  const x = (target.left + target.right) / 2 - bounds.left;
                  const y = target.bottom - bounds.top + 2;
                  const nx = label.left + 16 - bounds.left;
                  const ny = label.top - bounds.top - 2;
                  const bend = Math.max(10, Math.abs(ny - y) * .35);
                  return { x, y, d: `M ${x} ${y} C ${x} ${y + bend}, ${nx} ${ny - bend}, ${nx} ${ny}` };
                })
              : (() => {
                  const ex = label.left - bounds.left - 2;
                  const ey = label.top - bounds.top + baseHeight / 2;
                  const gutter = code.right - bounds.left + Math.max(12, (label.left - code.right) * .42);
                  const bend = Math.max(8, (ex - gutter) * .6);
                  // Precompute each highlight endpoint once. Fragment navigation
                  // then adjusts this single connector without measuring or
                  // rebuilding it.
                  return [note, ...additions].map((_, step) => {
                    const target = rects.filter(entry => entry.step <= step).at(-1).rect;
                    const x = target.right - bounds.left + 2;
                    const y = (side === 'top' ? target.top - 2 : target.bottom + 2) - bounds.top;
                    return { x, y, d: `M ${x} ${y} H ${code.right - bounds.left + 6} C ${gutter + bend} ${y}, ${gutter - bend} ${ey}, ${ex} ${ey}` };
                  });
                })();
            const tone = note.classList.contains('error') ? 'error' : note.classList.contains('result') ? 'result' : note.classList.contains('question') ? 'question' : '';
            const line = svgElement('g', { class: tone, 'data-code-anchor': `${groupIndex}-${noteIndex}` });
            if (additions.length) line.dataset.codeConnectorSteps = JSON.stringify(geometries);
            line.append(
              svgElement('path', { d: geometries[0].d }),
              svgElement('circle', { cx: geometries[0].x, cy: geometries[0].y, r: 3 })
            );
            svg.append(line);
          }
          // Diagram targets: this deck's own addition. A mermaid node sits
          // wherever the diagram laid it out — above, beside, anywhere — not
          // in a side column next to a code block, so the gutter-routing
          // math above (designed for "note beside a code listing") doesn't
          // apply. A direct two-point bezier between note and shape edges
          // is what this deck's earlier hand-rolled version used and it
          // already reads correctly for a note sitting below a diagram with
          // targets fanning out left/right.
          const shapes = [...group.querySelectorAll(`[data-code-anchor="${groupIndex}-${noteIndex}"].diagram-anchor-live`)];
          const shapeTargetIds = [...new Set(shapes.map(shape => shape.dataset.codeTarget || '0'))];
          for (const targetId of shapeTargetIds) {
            const shape = shapes.find(entry => (entry.dataset.codeTarget || '0') === targetId);
            const target = shape.getBoundingClientRect();
            if (!target.width || !target.height) continue;
            const noteRight = label.right <= target.left;
            const x1 = (noteRight ? label.right : label.left) - bounds.left;
            const y1 = label.top - bounds.top + baseHeight / 2;
            const x2 = (noteRight ? target.left : target.right) - bounds.left;
            const y2 = target.top - bounds.top + target.height / 2;
            const bend = Math.max(18, Math.abs(x2 - x1) * .4);
            const d = `M ${x1} ${y1} C ${x1 + (noteRight ? bend : -bend)} ${y1}, ` +
              `${x2 - (noteRight ? bend : -bend)} ${y2}, ${x2} ${y2}`;
            const tone = note.classList.contains('error') ? 'error' : note.classList.contains('result') ? 'result' : note.classList.contains('question') ? 'question' : '';
            const line = svgElement('g', { class: tone, 'data-code-anchor': `${groupIndex}-${noteIndex}` });
            line.append(svgElement('path', { d }), svgElement('circle', { cx: x2, cy: y2, r: 3 }));
            svg.append(line);
          }
        }
        if (svg.children.length) {
          // Keep decoration out of columns: aligned layouts count their children
          // to establish shared rows. Position the overlay on the slide instead.
          Object.assign(svg.style, {
            left: `${bounds.left - bodyBounds.left}px`, top: `${bounds.top - bodyBounds.top}px`,
            width: `${bounds.width}px`, height: `${bounds.height}px`
          });
          slide.querySelector('.slide-body').append(svg);
        }
      }
      syncAnnotationVisibility(slide);
    }
  } finally { ruler.remove(); }
}

export function installCodeAnnotations(reveal) {
  const container = document.querySelector('body > .reveal > .slides');
  if (!container) return;
  // PDF handouts contain the completed slide, not one page per reveal step.
  reveal.configure({ pdfSeparateFragments: false });
  const fittedStyles = new WeakMap();
  const pending = new Set();
  let frame;
  const render = slides => {
    annotateSlides(slides);
    for (const slide of slides) fittedStyles.set(slide, slide.querySelector('.slide-body')?.getAttribute('style'));
  };
  const flush = () => {
    frame = undefined;
    const slides = [...pending].filter(slide => slide.isConnected);
    pending.clear();
    render(slides);
  };
  const schedule = slides => {
    slides.filter(Boolean).forEach(slide => pending.add(slide));
    if (!frame) frame = requestAnimationFrame(flush);
  };
  // Reveal wraps sections in .pdf-page when preparing a print layout.
  const all = () => [...container.querySelectorAll('section')].filter(slide => slide.querySelector(':scope > .slide-body'));
  const fitting = new MutationObserver(records => {
    for (const { target } of records) {
      if (!target.matches('.slide-body')) continue;
      const slide = target.closest('section');
      if (fittedStyles.get(slide) !== target.getAttribute('style')) schedule([slide]);
    }
  });
  fitting.observe(container, { subtree: true, attributes: true, attributeFilter: ['style', 'data-fitted'] });
  // Fragment steps use premeasured note sizes; never measure or refit the slide.
  for (const event of ['fragmentshown', 'fragmenthidden', 'slidechanged']) {
    reveal.on(event, () => {
      const slide = reveal.getCurrentSlide();
      if (event === 'slidechanged') schedule([slide]);
      syncAnnotationVisibility(slide);
    });
  }
  for (const event of ['overviewshown', 'overviewhidden']) {
    reveal.on(event, () => all().forEach(slide => syncAnnotationVisibility(slide)));
  }
  // Run before the sidebar's bubbling listener clones the updated slide.
  document.addEventListener('preview:updated', event => {
    const changed = new Set(event.detail.changedIds);
    render(all().filter(slide => event.detail.stylesChanged || changed.has(slide.id)));
  }, true);
  document.fonts.addEventListener('loadingdone', () => schedule([reveal.getCurrentSlide()]));
  document.addEventListener('code:highlighted', event => schedule(event.detail.slides));
  reveal.on('pdf-ready', () => schedule(all()));
  window.addEventListener('beforeprint', () => {
    render(all());
    all().forEach(slide => syncAnnotationVisibility(slide, true));
  });
  window.addEventListener('afterprint', () => all().forEach(slide => syncAnnotationVisibility(slide)));
  const ready = () => document.fonts.ready.then(() => schedule(
    new URLSearchParams(location.search).has('print-pdf') ? all() : [reveal.getCurrentSlide()]));
  if (reveal.isReady()) ready(); else reveal.on('ready', ready);
}

if (typeof window !== 'undefined' && window.Reveal) installCodeAnnotations(window.Reveal);

// Live sidebar thumbnails run no scripts: prepare their source slides and static copies.
if (typeof document !== 'undefined') {
  // A restored sidebar can create its first thumbnail before the deck's initial annotation pass.
  document.addEventListener('slide-thumbnail:source', ({ detail: { slide } }) => {
    if (slide.querySelector(annotationSelector) && !slide.querySelector('.annotation-lines')) annotateSlides([slide]);
  });
  document.addEventListener('slide-thumbnail:copy', ({ detail: { copy } }) => {
    syncAnnotationVisibility(copy, true);
    copy.querySelectorAll('[data-code-note-sizes]').forEach(node => node.removeAttribute('data-code-note-sizes'));
  });
}
