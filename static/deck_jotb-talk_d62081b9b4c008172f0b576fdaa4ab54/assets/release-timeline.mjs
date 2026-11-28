// Positions use UTC calendar dates, so local time zones cannot shift the axis.
export function releaseScale(dates, range = {}) {
  const parseDate = date => {
    const time = Date.parse(`${date}T00:00:00Z`);
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date) || !Number.isFinite(time) || new Date(time).toISOString().slice(0, 10) !== date) {
      throw new Error(`Invalid release date: ${date}`);
    }
    return time;
  };
  const times = dates.map(parseDate);
  if (!times.length) throw new Error('A release timeline needs dates');
  const first = new Date(Math.min(...times)).getUTCFullYear();
  const last = new Date(Math.max(...times)).getUTCFullYear();
  const start = range.start ? parseDate(range.start) : Date.UTC(first, 0, 1);
  const end = range.end ? parseDate(range.end) : Date.UTC(last + 1, 0, 1);
  if (end <= start || times.some(time => time < start || time > end)) {
    throw new Error('Invalid release timeline range');
  }
  const position = time => 100 * (time - start) / (end - start);
  const startYear = new Date(start).getUTCFullYear();
  const endYear = new Date(end).getUTCFullYear();
  const months = [];
  for (let index = startYear * 12 + new Date(start).getUTCMonth(); index <= endYear * 12 + new Date(end).getUTCMonth(); index++) {
    const year = Math.floor(index / 12);
    const month = index % 12;
    const time = Date.UTC(year, month, 1);
    if (time >= start && time <= end) months.push({ year, month, position: position(time) });
  }
  return {
    positions: times.map(position),
    months,
    years: Array.from({ length: endYear - startYear + 1 }, (_, i) => ({
      year: startYear + i, position: position(Date.UTC(startYear + i, 0, 1))
    })).filter(tick => tick.position >= 0 && tick.position < 100)
  };
}

export function layoutReleaseTimelines(slides) {
  for (const slide of slides) for (const list of slide.querySelectorAll('ol.release-timeline')) {
    const items = [...list.querySelectorAll(':scope > li[data-date]')];
    if (!items.length) continue;
    const scale = releaseScale(items.map(item => item.dataset.date), {
      start: list.dataset.start, end: list.dataset.end
    });
    list.classList.add('dated');
    list.querySelectorAll(':scope > .timeline-year, :scope > .timeline-month').forEach(node => node.remove());
    items.forEach((item, i) => {
      item.style.setProperty('--at', `${scale.positions[i]}%`);
      item.title = `${item.textContent.trim()} — ${item.dataset.date}`;
      item.setAttribute('aria-label', item.title);
    });
    const monthly = list.dataset.ticks === 'month';
    list.classList.toggle('monthly', monthly);
    for (const { year, month, position } of monthly ? scale.months : scale.years) {
      const tick = list.ownerDocument.createElement('li');
      tick.className = monthly ? 'timeline-month' : 'timeline-year';
      if (monthly && (position === 0 || position === 100)) {
        tick.classList.add(position === 0 ? 'timeline-start' : 'timeline-end');
      }
      tick.setAttribute('aria-hidden', 'true');
      tick.style.setProperty('--at', `${position}%`);
      tick.textContent = monthly ? ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'][month] : year;
      if (monthly && (month === 0 || position === scale.months[0].position)) {
        const label = list.ownerDocument.createElement('span');
        label.className = 'timeline-month-year';
        label.textContent = year;
        tick.append(label);
      }
      list.append(tick);
    }
  }
}
