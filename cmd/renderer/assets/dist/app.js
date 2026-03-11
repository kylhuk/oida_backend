function asNumber(v) {
  return typeof v === "number" ? v : 0;
}

function fmtNumber(v) {
  return new Intl.NumberFormat("en-US").format(asNumber(v));
}

function fmtPct(v) {
  return `${Math.round((typeof v === "number" ? v : 0) * 100)}%`;
}

function fmtTime(value) {
  if (!value) return "n/a";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return "n/a";
  return parsed.toISOString();
}

function renderSummary(summary) {
  const cards = [
    ["Sources", summary.sources_total],
    ["Enabled", summary.sources_enabled],
    ["Jobs running", summary.jobs_running],
    ["Frontier pending", summary.frontier_pending],
    ["Frontier retry", summary.frontier_retry],
    ["Unresolved", summary.unresolved_open],
    ["Quality open", summary.quality_open],
    ["Sources disabled", summary.sources_disabled],
  ];
  const root = document.getElementById("summaryCards");
  root.innerHTML = cards
    .map(([label, value], index) => `<div class="rounded-2xl border border-slate-800/90 bg-slate-900/75 p-4 shadow-[0_12px_28px_rgba(2,6,23,0.45)]"><div class="mb-2 inline-flex h-6 items-center rounded-full border border-cyan-300/20 bg-cyan-300/10 px-2 text-[10px] font-medium uppercase tracking-[0.16em] text-cyan-200">${String(index + 1).padStart(2, "0")}</div><p class="text-sm text-slate-400">${label}</p><p class="mt-1 text-3xl font-semibold text-slate-50">${fmtNumber(value)}</p></div>`)
    .join("");
}

function renderStorage(storage) {
  const table = document.getElementById("storageTable");
  const rows = Array.isArray(storage.table_rows) ? storage.table_rows : [];
  table.innerHTML = `
    <thead>
      <tr class="text-left text-xs uppercase tracking-[0.12em] text-slate-400">
        <th class="pb-2 pr-3 font-medium">Table</th>
        <th class="pb-2 pr-3 font-medium">Rows</th>
        <th class="pb-2 font-medium">Mode</th>
      </tr>
    </thead>
    <tbody>
      ${rows
        .map(
          (r) => `<tr class="border-t border-slate-800/70"><td class="py-2 pr-3 text-slate-200">${r.table_name}</td><td class="py-2 pr-3 font-medium text-slate-100">${fmtNumber(r.rows)}</td><td class="py-2 text-slate-400">${r.count_mode}</td></tr>`
        )
        .join("")}
    </tbody>
  `;
}

function renderFreshness(quality) {
  const root = document.getElementById("freshnessList");
  const model = quality.freshness || {};
  const threshold = asNumber(model.threshold_seconds);
  const rows = Array.isArray(model.sources) ? model.sources.slice(0, 8) : [];
  root.innerHTML = rows
    .map((row) => {
      const stale = asNumber(row.freshness_seconds) > threshold;
      const pct = Math.min(100, Math.round((asNumber(row.freshness_seconds) / Math.max(1, threshold * 2)) * 100));
      return `<div class="rounded-xl border ${stale ? "border-amber-500/40 bg-amber-500/5" : "border-slate-800/90 bg-slate-900/30"} p-3"><div class="mb-1 flex items-center justify-between"><span class="font-medium text-slate-100">${row.source_id}</span><span class="text-xs ${stale ? "text-amber-300" : "text-slate-400"}">${row.lag_reason}</span></div><div class="mb-2 h-2 overflow-hidden rounded-full bg-slate-800"><div class="h-full ${stale ? "bg-amber-400" : "bg-cyan-400"}" style="width:${pct}%"></div></div><div class="text-xs text-slate-400">${fmtNumber(row.freshness_seconds)}s</div></div>`;
    })
    .join("");
}

function renderParser(quality) {
  const parser = quality.parser_success || {};
  const failures = Array.isArray(parser.failures) ? parser.failures : [];
  const root = document.getElementById("parserStats");
  root.innerHTML = `
    <div class="grid grid-cols-3 gap-2">
      <div class="rounded-lg border border-slate-800/90 bg-slate-900/40 p-3"><p class="text-xs text-slate-400">Window</p><p class="text-lg font-semibold text-slate-50">${fmtNumber(parser.window_minutes)}m</p></div>
      <div class="rounded-lg border border-slate-800/90 bg-slate-900/40 p-3"><p class="text-xs text-slate-400">Runs</p><p class="text-lg font-semibold text-slate-50">${fmtNumber(parser.total_runs)}</p></div>
      <div class="rounded-lg border border-slate-800/90 bg-slate-900/40 p-3"><p class="text-xs text-slate-400">Success</p><p class="text-lg font-semibold text-slate-50">${fmtPct(parser.success_rate)}</p></div>
    </div>
    <div class="space-y-1">
      ${failures
        .map((f) => `<div class="flex items-center justify-between text-sm"><span class="text-slate-300">${f.error_class}</span><span class="text-slate-400">${fmtNumber(f.count)} (${f.example_source})</span></div>`)
        .join("")}
    </div>
  `;
}

function renderOutputs(outputs) {
  const cards = [
    ["Metrics", outputs.metrics_total],
    ["Hotspots", outputs.hotspots_total],
    ["Cross domain", outputs.cross_domain_total],
    ["Latest snapshot", fmtTime(outputs.latest_snapshot_at)],
  ];
  const root = document.getElementById("outputCards");
  root.innerHTML = cards
    .map(([label, value]) => `<div class="rounded-xl border border-slate-800/90 bg-slate-900/40 p-3"><p class="text-xs uppercase tracking-[0.10em] text-slate-400">${label}</p><p class="mt-1 text-sm font-medium text-slate-50">${typeof value === "number" ? fmtNumber(value) : value}</p></div>`)
    .join("");
}

async function boot() {
  const generatedAt = document.getElementById("generatedAt");
  const warningBanner = document.getElementById("warningBanner");
  try {
    const res = await fetch("/stats", { headers: { Accept: "application/json" } });
    if (!res.ok) throw new Error(`stats status ${res.status}`);
    const payload = await res.json();
    const data = payload.data || {};
    renderSummary(data.summary || {});
    renderStorage(data.storage || {});
    renderFreshness(data.quality || {});
    renderParser(data.quality || {});
    renderOutputs(data.outputs || {});
    generatedAt.textContent = `Generated ${fmtTime(data.generated_at || payload.generated_at)}`;
    if (Array.isArray(data.warnings) && data.warnings.length > 0) {
      warningBanner.classList.remove("hidden");
      warningBanner.textContent = data.warnings.join(" | ");
    }
  } catch (err) {
    generatedAt.textContent = "Stats unavailable";
    warningBanner.classList.remove("hidden");
    warningBanner.textContent = `Failed to load dashboard stats: ${err}`;
  }
}

boot();
