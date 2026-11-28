/* Host bootstrap: wires the code-annotation layer once Reveal is live, then
 * renders mermaid fences. Order matters — mermaid replaces <pre> nodes, and
 * code-annotations.mjs's own MutationObserver (watching .slide-body's
 * style/data-fitted attributes) is meant to notice that replacement and
 * reschedule a redraw — but it only fires on the fitted style STRING
 * actually changing, and mermaid rendering a diagram can leave that string
 * byte-identical (same overall fit scale) even though the diagram content
 * changed underneath it. A diagram-note's shapes then never get their
 * first real redraw. Confirmed by direct testing, not assumed: the same
 * reason the original hand-rolled code-arrows.mjs needed an explicit
 * redraw call here too — an explicit one after mermaid finishes closes
 * the gap the observer can miss.
 */
import { installCodeAnnotations, annotateSlides } from './code-annotations.mjs';
import { installMermaid } from './mermaid-init.mjs';
import { installPaletteToggle } from './palette-toggle.mjs';

const reveal = window.Reveal;
if (reveal?.addKeyBinding) installPaletteToggle(reveal);
if (reveal?.on) installCodeAnnotations(reveal);
await installMermaid();
if (reveal?.getCurrentSlide) annotateSlides([reveal.getCurrentSlide()]);
