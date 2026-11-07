// Markdown links resolve through Tiger's asset resolver before becoming iframes.
export function installNotebookEmbeds(root = document) {
  const print = new URLSearchParams(window.location.search).has('print-pdf');
  for (const host of root.querySelectorAll('.notebook-embed')) {
    const link = host.querySelector('a');
    if (!link || host.querySelector('iframe, .notebook-print-preview')) continue;
    const preview = document.createElement('img');
    preview.dataset.src = new URL('preview.png', link.href).href;
    preview.alt = 'Saved view of the dataframe notebook';
    preview.className = 'notebook-print-preview';
    // Print fitting clones slides; never start a live kernel/frame in those copies.
    if (print) {
      preview.src = preview.dataset.src;
      delete preview.dataset.src;
      preview.style.display = 'block';
      host.replaceChildren(preview);
    } else {
      const frame = document.createElement('iframe');
      frame.dataset.src = link.href;
      // Reveal activates data-src frames when their slide is entered. Handle
      // the current slide too when installing after Reveal has initialized.
      if (host.closest('section')?.classList.contains('present')) frame.src = link.href;
      frame.title = 'Interactive Scala dataframe notebook';
      host.replaceChildren(frame, preview);
    }
  }
}
if (typeof document !== 'undefined') {
  installNotebookEmbeds();
  document.addEventListener('preview:updated', () => installNotebookEmbeds());
  document.addEventListener('slide-thumbnail:copy', ({detail: {copy}}) => {
    for (const host of copy.querySelectorAll('.notebook-embed')) {
      host.querySelector('iframe')?.remove();
      const preview = host.querySelector('.notebook-print-preview');
      if (preview) preview.style.display = 'block';
    }
  });
}
