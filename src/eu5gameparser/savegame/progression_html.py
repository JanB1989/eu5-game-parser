# ruff: noqa: E501

from __future__ import annotations

import html
import json
import math
from importlib.resources import files
from pathlib import Path
from typing import Any


def write_savegame_progression_html(payload: dict[str, Any], path: str | Path) -> Path:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    clean_payload = _jsonable(payload)
    clean_payload.setdefault("payloadSummary", {})
    payload_json = _json_script(clean_payload)
    clean_payload["payloadSummary"]["jsonBytes"] = len(payload_json.encode("utf-8"))
    payload.setdefault("payloadSummary", {})["jsonBytes"] = clean_payload["payloadSummary"][
        "jsonBytes"
    ]
    payload_json = _json_script(clean_payload)
    output_path.write_text(_html(payload_json), encoding="utf-8")
    return output_path


def _jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def _json_script(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).replace("</", "<\\/")


def _echarts_source() -> str:
    return (
        files("eu5gameparser")
        .joinpath("assets", "echarts.min.js")
        .read_text(encoding="utf-8")
        .replace("</", "<\\/")
    )


def _html(payload_json: str) -> str:
    title = "EU5 Savegame Progression"
    template = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>__TITLE__</title>
  <style>
    * { box-sizing: border-box; }
    html, body { margin: 0; min-height: 100%; }
    body {
      background: #f5f7fb;
      color: #172033;
      font-family: Inter, Segoe UI, system-ui, sans-serif;
      font-size: 13px;
    }
    .shell { display: grid; grid-template-rows: auto auto auto 1fr; min-height: 100vh; }
    header, .tabs, .toolbar {
      background: #fff;
      border-bottom: 1px solid #d8e0ec;
      padding: 10px 16px;
    }
    header { align-items: center; display: flex; gap: 14px; }
    h1 { font-size: 17px; margin: 0; }
    .meta { color: #5d697c; font-size: 12px; }
    .spacer { flex: 1; }
    .tabs, .toolbar { align-items: center; display: flex; flex-wrap: wrap; gap: 8px; }
    button, select, input {
      background: #fff;
      border: 1px solid #c5d0de;
      border-radius: 6px;
      color: #172033;
      font: inherit;
      min-height: 32px;
      padding: 6px 9px;
    }
    button { cursor: pointer; font-weight: 700; }
    button:hover { background: #edf3fb; }
    button.active { background: #172033; border-color: #172033; color: #fff; }
    label { align-items: center; color: #526074; display: inline-flex; gap: 5px; }
    select { min-width: 126px; }
    input { min-width: 170px; }
    main { display: grid; gap: 12px; padding: 12px 16px 22px; }
    .cards { display: grid; gap: 10px; grid-template-columns: repeat(7, minmax(0, 1fr)); }
    .card, .panel {
      background: #fff;
      border: 1px solid #dce4ef;
      border-radius: 8px;
      box-shadow: 0 1px 2px rgba(23, 32, 51, 0.04);
      min-width: 0;
    }
    .card { padding: 10px; }
    .card .label { color: #5d697c; font-size: 11px; font-weight: 800; text-transform: uppercase; }
    .card .value { font-size: 19px; font-weight: 800; margin-top: 4px; overflow-wrap: anywhere; }
    .grid { display: grid; gap: 12px; grid-template-columns: repeat(2, minmax(0, 1fr)); }
    .panel { display: grid; overflow: hidden; }
    .panel.full { grid-column: 1 / -1; }
    .panel-head {
      align-items: center;
      border-bottom: 1px solid #edf1f6;
      display: flex;
      gap: 10px;
      justify-content: space-between;
      min-height: 42px;
      padding: 9px 12px;
    }
    .panel-title { font-weight: 800; }
    .panel-body { min-height: 0; overflow: auto; padding: 10px 12px; }
    .chart { height: 295px; width: 100%; }
    .chart.tall { height: 380px; }
    .sentence {
      align-items: center;
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      line-height: 32px;
    }
    .combobox { display: inline-block; position: relative; }
    .combo-menu {
      background: #fff;
      border: 1px solid #c5d0de;
      border-radius: 7px;
      box-shadow: 0 12px 24px rgba(23, 32, 51, 0.16);
      display: none;
      left: 0;
      max-height: 240px;
      min-width: 220px;
      overflow: auto;
      position: absolute;
      top: calc(100% + 4px);
      z-index: 20;
    }
    .combo-menu.open { display: block; }
    .combo-option { cursor: pointer; padding: 7px 9px; }
    .combo-option:hover, .combo-option.active { background: #edf5ff; }
    .table-wrap { max-height: 420px; overflow: auto; }
    table { border-collapse: collapse; font-size: 12px; min-width: 920px; width: 100%; }
    th, td { border-bottom: 1px solid #eef2f7; padding: 7px 9px; text-align: right; white-space: nowrap; }
    th:first-child, td:first-child {
      background: inherit;
      left: 0;
      position: sticky;
      text-align: left;
      z-index: 1;
    }
    th {
      background: #f8fafc;
      color: #526074;
      cursor: pointer;
      font-size: 11px;
      position: sticky;
      text-transform: uppercase;
      top: 0;
      user-select: none;
      z-index: 2;
    }
    th.sorted { color: #1f6feb; }
    td.numeric { font-feature-settings: "tnum"; font-variant-numeric: tabular-nums; }
    tr:hover { background: #edf5ff; }
    .empty { color: #64748b; padding: 18px; text-align: center; }
    .hidden { display: none !important; }
    @media (max-width: 1180px) {
      .cards { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .grid { grid-template-columns: 1fr; }
      header { align-items: flex-start; flex-direction: column; }
    }
  </style>
</head>
<body>
  <div class="shell">
    <header>
      <div>
        <h1>EU5 Savegame Progression</h1>
        <div class="meta" id="subtitle">Parser-native multi-save dashboard</div>
      </div>
      <div class="spacer"></div>
      <button type="button" id="exportCsv">Export CSV</button>
    </header>
    <nav class="tabs" aria-label="Dashboard sections">
      <button type="button" data-tab="overview" class="active">Overview</button>
      <button type="button" data-tab="explorer">Explorer</button>
    </nav>
    <section class="toolbar" id="overviewToolbar">
      <label>From <select id="overviewStart"></select></label>
      <label>To <select id="overviewEnd"></select></label>
    </section>
    <section class="toolbar hidden" id="explorerToolbar">
      <div class="sentence">
        Show
        <select id="domainSelect"></select>
        <select id="metricSelect"></select>
        for
        <div class="combobox">
          <input id="entityFilter" autocomplete="off" placeholder="all entities">
          <div id="entityMenu" class="combo-menu"></div>
        </div>
        by
        <select id="scopeSelect"></select>
        <select id="dimensionSelect"></select>
        using
        <select id="aggregationSelect"></select>
        ranked
        <select id="rankSelect"><option value="top">Top</option><option value="bottom">Bottom</option></select>
        <select id="limitSelect"><option>5</option><option>10</option><option>25</option><option>50</option></select>
        <label>From <select id="explorerStart"></select></label>
        <label>To <select id="explorerEnd"></select></label>
        <button type="button" id="resetExplorer">Reset</button>
      </div>
    </section>
    <main>
      <section class="cards" id="cards"></section>
      <section class="grid" id="chartGrid"></section>
      <section class="grid" id="tableGrid"></section>
    </main>
  </div>
  <script>__ECHARTS_JS__</script>
  <script>
    const payload = __PAYLOAD_JSON__;
    const state = { tab: "overview", sortKey: "aggregate", sortDir: "desc", currentRows: [] };
    const chartInstances = new Map();
    const snapshots = [...(payload.snapshots || [])].sort((a, b) => Number(a.date_sort || 0) - Number(b.date_sort || 0));
    const snapshotIndex = new Map(snapshots.map((snapshot, index) => [snapshot.snapshot_id, index]));
    const firstSnapshot = snapshots[0] || {};
    const lastSnapshot = snapshots[snapshots.length - 1] || {};
    const explorer = payload.explorer || { metrics: [], dimensions: [], rows: [], aggregations: [] };
    const metricByKey = new Map(explorer.metrics.map(metric => [metric.key, metric]));
    const dimensionByKey = new Map(explorer.dimensions.map(dimension => [dimension.key, dimension]));
    const domainLabels = {
      population: "Population",
      goods: "Goods",
      food: "Food",
      buildings: "Buildings",
      methods: "Production Methods"
    };
    const scopeLabels = {
      world: "World",
      geography: "Geography",
      political: "Political",
      markets: "Markets",
      goods: "Goods",
      production: "Production",
      population: "Population"
    };
    const overviewSeries = payload.overviewSeries || {};

    function $(id) { return document.getElementById(id); }
    function fmt(value, formatter = "whole") {
      if (formatter === "text") return value ?? "";
      if (value === null || value === undefined || value === "") return "n/a";
      const number = Number(value);
      if (!Number.isFinite(number)) return "n/a";
      const digits = formatter === "money" || formatter === "decimal" ? 2 : formatter === "percent" ? 1 : 0;
      return new Intl.NumberFormat(undefined, { maximumFractionDigits: digits, minimumFractionDigits: digits }).format(number);
    }
    function metricMeta(key) { return metricByKey.get(key) || { key, label: key, unit: key, formatter: "decimal", defaultSort: "desc" }; }
    function dateLabel(row) { return row?.date || row?.snapshot_id || ""; }
    function rangeIds(startId, endId) {
      const start = snapshotIndex.get($(startId).value);
      const end = snapshotIndex.get($(endId).value);
      return [start ?? 0, end ?? snapshots.length - 1];
    }
    function inRange(row, startId, endId) {
      const [start, end] = rangeIds(startId, endId);
      const index = snapshotIndex.get(row.snapshot_id);
      return index !== undefined && index >= start && index <= end;
    }
    function rowsInRange(rows, startId, endId) { return rows.filter(row => inRange(row, startId, endId)); }
    function latestRow(rows, startId, endId) {
      const ranged = rowsInRange(rows, startId, endId);
      return ranged[ranged.length - 1] || {};
    }
    function sum(rows, key) {
      return rows.reduce((total, row) => total + Number(row[key] || 0), 0);
    }
    function median(values) {
      const sorted = values.filter(Number.isFinite).sort((a, b) => a - b);
      if (!sorted.length) return 0;
      const mid = Math.floor(sorted.length / 2);
      return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
    }
    function aggregate(values, kind) {
      const numbers = values.filter(Number.isFinite);
      if (!numbers.length) return 0;
      if (kind === "mean") return numbers.reduce((a, b) => a + b, 0) / numbers.length;
      if (kind === "median") return median(numbers);
      if (kind === "min") return Math.min(...numbers);
      if (kind === "max") return Math.max(...numbers);
      return numbers.reduce((a, b) => a + b, 0);
    }
    function setCards(items) {
      const cards = $("cards");
      cards.replaceChildren();
      for (const [label, value, formatter = "whole"] of items) {
        const card = document.createElement("div");
        card.className = "card";
        const display = fmt(value, formatter);
        card.innerHTML = `<div class="label">${label}</div><div class="value" title="${display}">${display}</div>`;
        cards.append(card);
      }
    }
    function panel(id, title, subtitle = "", full = false) {
      const section = document.createElement("section");
      section.className = `panel${full ? " full" : ""}`;
      section.innerHTML = `<div class="panel-head"><div><div class="panel-title">${title}</div><div class="meta">${subtitle}</div></div></div><div class="panel-body"><div id="${id}" class="chart${full ? " tall" : ""}"></div></div>`;
      return section;
    }
    function tablePanel(title) {
      const section = document.createElement("section");
      section.className = "panel full";
      section.innerHTML = `<div class="panel-head"><div><div class="panel-title">${title}</div><div class="meta" id="tableMeta"></div></div></div><div class="panel-body"><div class="table-wrap" id="tableWrap"></div></div>`;
      return section;
    }
    function clearContent() {
      for (const chart of chartInstances.values()) chart.dispose();
      chartInstances.clear();
      $("chartGrid").replaceChildren();
      $("tableGrid").replaceChildren();
      state.currentRows = [];
    }
    function renderChart(id, option) {
      const element = $(id);
      if (!element) return;
      if (!element.clientHeight) element.style.height = id.includes("explorer") ? "380px" : "295px";
      const chart = echarts.init(element, null, { renderer: "canvas" });
      chart.setOption(option, true);
      chartInstances.set(id, chart);
    }
    function lineOption(title, unit, dates, series) {
      return {
        animation: false,
        color: ["#2563eb", "#0891b2", "#7c3aed", "#d97706", "#16a34a", "#dc2626", "#9333ea", "#475569"],
        tooltip: { trigger: "axis", valueFormatter: value => fmt(value, "decimal") },
        legend: { top: 0, type: "scroll" },
        grid: { left: 58, right: 22, top: 42, bottom: 42 },
        xAxis: { type: "category", name: "In-game date", nameLocation: "middle", nameGap: 28, data: dates, axisLabel: { hideOverlap: true } },
        yAxis: { type: "value", name: unit, nameGap: 42, nameLocation: "middle" },
        series: series.map(item => ({ ...item, type: item.type || "line", smooth: false, symbolSize: 5 }))
      };
    }
    function renderOverview() {
      clearContent();
      $("overviewToolbar").classList.remove("hidden");
      $("explorerToolbar").classList.add("hidden");
      const employmentLatest = latestRow(overviewSeries.employment || [], "overviewStart", "overviewEnd");
      const developmentLatest = latestRow(overviewSeries.development || [], "overviewStart", "overviewEnd");
      const taxLatest = latestRow(overviewSeries.tax || [], "overviewStart", "overviewEnd");
      const foodLatest = latestRow(overviewSeries.food || [], "overviewStart", "overviewEnd");
      setCards([
        ["Total Pops", employmentLatest.total_pops, "whole"],
        ["Employed", employmentLatest.employed_pops, "whole"],
        ["Unemployed", employmentLatest.unemployed_pops, "whole"],
        ["Development", developmentLatest.development, "whole"],
        ["Collected Tax", taxLatest.collected_tax, "money"],
        ["Uncollected Tax", taxLatest.uncollected_tax, "money"],
        ["Food", foodLatest.food, "whole"]
      ]);
      $("chartGrid").append(
        panel("popsChart", "Pops by Type", "Pops by in-game date"),
        panel("employmentChart", "Employment", "Pops by in-game date"),
        panel("developmentChart", "Development", "Development by in-game date"),
        panel("taxChart", "Tax", "Gold by in-game date"),
        panel("foodChart", "Food", "Stockpile/capacity and monthly flow", true)
      );
      renderPopsChart();
      renderEmploymentChart();
      renderDevelopmentChart();
      renderTaxChart();
      renderFoodChart();
    }
    function renderPopsChart() {
      const popRows = rowsInRange(overviewSeries.popsByType || [], "overviewStart", "overviewEnd");
      const employmentRows = rowsInRange(overviewSeries.employment || [], "overviewStart", "overviewEnd");
      const dates = employmentRows.map(dateLabel);
      const types = [...new Set(popRows.map(row => row.pop_type))].sort();
      const series = types.map(type => ({
        name: type,
        type: "bar",
        stack: "pops",
        data: dates.map((_, index) => {
          const snapshot = employmentRows[index]?.snapshot_id;
          return sum(popRows.filter(row => row.snapshot_id === snapshot && row.pop_type === type), "value");
        })
      }));
      series.push({ name: "Total Pops", type: "line", data: employmentRows.map(row => row.total_pops) });
      renderChart("popsChart", lineOption("Pops", "Pops", dates, series));
    }
    function renderEmploymentChart() {
      const rows = rowsInRange(overviewSeries.employment || [], "overviewStart", "overviewEnd");
      const dates = rows.map(dateLabel);
      renderChart("employmentChart", lineOption("Employment", "Pops", dates, [
        { name: "Total Pops", data: rows.map(row => row.total_pops) },
        { name: "Employed", data: rows.map(row => row.employed_pops) },
        { name: "Unemployed", data: rows.map(row => row.unemployed_pops) }
      ]));
    }
    function renderDevelopmentChart() {
      const rows = rowsInRange(overviewSeries.development || [], "overviewStart", "overviewEnd");
      renderChart("developmentChart", lineOption("Development", "Development", rows.map(dateLabel), [
        { name: "Development", data: rows.map(row => row.development) }
      ]));
    }
    function renderTaxChart() {
      const rows = rowsInRange(overviewSeries.tax || [], "overviewStart", "overviewEnd");
      const dates = rows.map(dateLabel);
      renderChart("taxChart", lineOption("Tax", "Gold", dates, [
        { name: "Collected", type: "bar", stack: "tax", data: rows.map(row => row.collected_tax) },
        { name: "Uncollected", type: "bar", stack: "tax", data: rows.map(row => row.uncollected_tax) }
      ]));
    }
    function renderFoodChart() {
      const rows = rowsInRange(overviewSeries.food || [], "overviewStart", "overviewEnd");
      const dates = rows.map(dateLabel);
      renderChart("foodChart", {
        animation: false,
        color: ["#2563eb", "#0891b2", "#16a34a", "#d97706", "#dc2626"],
        tooltip: { trigger: "axis", valueFormatter: value => fmt(value, "decimal") },
        legend: { top: 0, type: "scroll" },
        grid: [{ left: 58, right: 22, top: 42, height: "34%" }, { left: 58, right: 22, top: "58%", height: "26%" }],
        xAxis: [
          { type: "category", data: dates, gridIndex: 0, axisLabel: { hideOverlap: true } },
          { type: "category", data: dates, gridIndex: 1, name: "In-game date", nameLocation: "middle", nameGap: 28, axisLabel: { hideOverlap: true } }
        ],
        yAxis: [
          { type: "value", name: "Food", nameLocation: "middle", nameGap: 42, gridIndex: 0 },
          { type: "value", name: "Food/month", nameLocation: "middle", nameGap: 42, gridIndex: 1 }
        ],
        series: [
          { name: "Food", type: "line", xAxisIndex: 0, yAxisIndex: 0, data: rows.map(row => row.food) },
          { name: "Capacity", type: "line", xAxisIndex: 0, yAxisIndex: 0, data: rows.map(row => row.food_max) },
          { name: "Supply", type: "line", xAxisIndex: 1, yAxisIndex: 1, data: rows.map(row => row.food_supply) },
          { name: "Demand", type: "line", xAxisIndex: 1, yAxisIndex: 1, data: rows.map(row => row.food_consumption) },
          { name: "Balance", type: "line", xAxisIndex: 1, yAxisIndex: 1, data: rows.map(row => row.food_balance) }
        ]
      });
    }
    function renderExplorer() {
      clearContent();
      $("overviewToolbar").classList.add("hidden");
      $("explorerToolbar").classList.remove("hidden");
      const queryRows = explorer.rows.filter(row =>
        row.domain === $("domainSelect").value
        && row.metric === $("metricSelect").value
        && row.dimension === $("dimensionSelect").value
        && inRange(row, "explorerStart", "explorerEnd")
      );
      const search = $("entityFilter").value.trim().toLowerCase();
      const filtered = search
        ? queryRows.filter(row => row.entity_key.toLowerCase().includes(search) || row.entity_label.toLowerCase().includes(search))
        : queryRows;
      const ranked = rankedEntities(filtered);
      setCards([
        ["Entities", ranked.length, "whole"],
        ["Rows", filtered.length, "whole"],
        ["Metric", metricMeta($("metricSelect").value).label, "text"],
        ["Aggregation", $("aggregationSelect").value, "text"]
      ]);
      $("chartGrid").append(panel("explorerChart", "Explorer", `${metricMeta($("metricSelect").value).unit} by in-game date`, true));
      $("tableGrid").append(tablePanel("Ranking"));
      renderExplorerChart(filtered, ranked);
      renderExplorerTable(ranked);
      state.currentRows = ranked;
    }
    function rankedEntities(rows) {
      const groups = new Map();
      for (const row of rows) {
        const group = groups.get(row.entity_key) || { entity: row.entity_label, entity_key: row.entity_key, values: [], rows: [] };
        group.values.push(Number(row.value || 0));
        group.rows.push(row);
        groups.set(row.entity_key, group);
      }
      const kind = $("aggregationSelect").value;
      const ranked = [...groups.values()].map(group => {
        const ordered = group.rows.sort((a, b) => Number(a.date_sort || 0) - Number(b.date_sort || 0));
        const values = group.values.filter(Number.isFinite);
        const first = Number(ordered[0]?.value || 0);
        const last = Number(ordered[ordered.length - 1]?.value || 0);
        return {
          entity: group.entity,
          entity_key: group.entity_key,
          aggregate: aggregate(values, kind),
          first,
          last,
          delta: last - first,
          min: values.length ? Math.min(...values) : 0,
          mean: values.length ? aggregate(values, "mean") : 0,
          median: values.length ? aggregate(values, "median") : 0,
          max: values.length ? Math.max(...values) : 0
        };
      });
      const direction = $("rankSelect").value === "bottom" ? 1 : -1;
      return ranked.sort((a, b) => direction * (a.aggregate - b.aggregate)).slice(0, Number($("limitSelect").value || 5));
    }
    function renderExplorerChart(rows, ranked) {
      const metric = metricMeta($("metricSelect").value);
      const selected = new Set(ranked.map(row => row.entity_key));
      const dates = rowsInRange(snapshots, "explorerStart", "explorerEnd").map(dateLabel);
      const byEntity = new Map();
      for (const row of rows) {
        if (!selected.has(row.entity_key)) continue;
        const values = byEntity.get(row.entity_key) || { name: row.entity_label, values: new Map() };
        values.values.set(row.snapshot_id, Number(row.value || 0));
        byEntity.set(row.entity_key, values);
      }
      const series = ranked.map(row => {
        const data = byEntity.get(row.entity_key) || { values: new Map() };
        return { name: row.entity, data: snapshots.filter(snapshot => inRange(snapshot, "explorerStart", "explorerEnd")).map(snapshot => data.values.get(snapshot.snapshot_id) ?? null) };
      });
      renderChart("explorerChart", lineOption(metric.label, metric.unit, dates, series));
    }
    function renderExplorerTable(rows) {
      const metric = metricMeta($("metricSelect").value);
      const formatter = metric.formatter || "decimal";
      const columns = [
        ["entity", "Entity", false],
        ["aggregate", $("aggregationSelect").value, true],
        ["first", "First", true],
        ["last", "Last", true],
        ["delta", "Delta", true],
        ["min", "Min", true],
        ["mean", "Mean", true],
        ["median", "Median", true],
        ["max", "Max", true]
      ];
      const sorted = [...rows].sort((a, b) => {
        if (!state.sortKey) return 0;
        const left = a[state.sortKey];
        const right = b[state.sortKey];
        const delta = typeof left === "number" && typeof right === "number" ? left - right : String(left).localeCompare(String(right));
        return state.sortDir === "asc" ? delta : -delta;
      });
      const table = document.createElement("table");
      const thead = document.createElement("thead");
      const header = document.createElement("tr");
      for (const [key, label] of columns) {
        const th = document.createElement("th");
        th.textContent = label + (state.sortKey === key ? (state.sortDir === "asc" ? " ▲" : " ▼") : "");
        if (state.sortKey === key) th.className = "sorted";
        th.addEventListener("click", () => {
          if (state.sortKey === key) state.sortDir = state.sortDir === "asc" ? "desc" : "asc";
          else { state.sortKey = key; state.sortDir = "desc"; }
          renderExplorerTable(rows);
        });
        header.append(th);
      }
      thead.append(header);
      table.append(thead);
      const tbody = document.createElement("tbody");
      for (const row of sorted) {
        const tr = document.createElement("tr");
        for (const [key, , numeric] of columns) {
          const td = document.createElement("td");
          if (numeric) td.className = "numeric";
          td.textContent = numeric ? fmt(row[key], formatter) : row[key];
          td.title = td.textContent;
          tr.append(td);
        }
        tbody.append(tr);
      }
      table.append(tbody);
      const wrap = $("tableWrap");
      wrap.replaceChildren(rows.length ? table : empty("No matching rows."));
      $("tableMeta").textContent = `${rows.length} ranked entities | ${metric.unit}`;
    }
    function empty(text) {
      const div = document.createElement("div");
      div.className = "empty";
      div.textContent = text;
      return div;
    }
    function fillSnapshotSelects() {
      for (const id of ["overviewStart", "overviewEnd", "explorerStart", "explorerEnd"]) {
        const select = $(id);
        select.replaceChildren();
        for (const snapshot of snapshots) {
          const option = document.createElement("option");
          option.value = snapshot.snapshot_id;
          option.textContent = dateLabel(snapshot);
          select.append(option);
        }
      }
      $("overviewStart").value = firstSnapshot.snapshot_id || "";
      $("explorerStart").value = firstSnapshot.snapshot_id || "";
      $("overviewEnd").value = lastSnapshot.snapshot_id || "";
      $("explorerEnd").value = lastSnapshot.snapshot_id || "";
    }
    function fillExplorerControls() {
      const domainSelect = $("domainSelect");
      domainSelect.replaceChildren();
      for (const domain of [...new Set(explorer.metrics.map(metric => metric.domain))]) {
        const option = document.createElement("option");
        option.value = domain;
        option.textContent = domainLabels[domain] || domain;
        domainSelect.append(option);
      }
      $("domainSelect").value = "population";
      fillMetricOptions();
      $("metricSelect").value = "pops";
      fillScopeOptions();
      $("scopeSelect").value = "world";
      fillDimensionOptions();
      $("dimensionSelect").value = availableDimensions().includes("super_region") ? "super_region" : availableDimensions()[0] || "global";
      const agg = $("aggregationSelect");
      agg.replaceChildren();
      for (const item of explorer.aggregations || ["sum", "mean", "median", "min", "max"]) {
        const option = document.createElement("option");
        option.value = item;
        option.textContent = item;
        agg.append(option);
      }
      agg.value = "sum";
      $("limitSelect").value = "5";
    }
    function fillMetricOptions() {
      const select = $("metricSelect");
      const selected = select.value;
      select.replaceChildren();
      for (const metric of explorer.metrics.filter(metric => metric.domain === $("domainSelect").value)) {
        const option = document.createElement("option");
        option.value = metric.key;
        option.textContent = metric.label;
        select.append(option);
      }
      if ([...select.options].some(option => option.value === selected)) select.value = selected;
    }
    function fillScopeOptions() {
      const select = $("scopeSelect");
      const selected = select.value || "world";
      select.replaceChildren();
      const scopes = ["world", ...new Set(explorer.dimensions.map(dimension => dimension.scope).filter(scope => scope !== "world"))];
      for (const scope of scopes) {
        const option = document.createElement("option");
        option.value = scope;
        option.textContent = scopeLabels[scope] || scope;
        select.append(option);
      }
      select.value = scopes.includes(selected) ? selected : "world";
    }
    function availableDimensions() {
      const domain = $("domainSelect").value;
      const rows = explorer.rows.filter(row => row.domain === domain && row.metric === $("metricSelect").value);
      const present = new Set(rows.map(row => row.dimension));
      const scope = $("scopeSelect").value;
      let candidates = explorer.dimensions.filter(dimension => present.has(dimension.key));
      if (scope !== "world") candidates = candidates.filter(dimension => dimension.scope === scope);
      if (scope === "world") candidates = candidates.filter(dimension => dimension.key !== "global");
      return candidates.sort((a, b) => a.order - b.order).map(dimension => dimension.key);
    }
    function fillDimensionOptions() {
      const select = $("dimensionSelect");
      const selected = select.value;
      const dimensions = availableDimensions();
      select.replaceChildren();
      for (const key of dimensions) {
        const option = document.createElement("option");
        option.value = key;
        option.textContent = dimensionByKey.get(key)?.label || key;
        select.append(option);
      }
      if (dimensions.includes(selected)) select.value = selected;
      else select.value = dimensions.includes("super_region") ? "super_region" : dimensions[0] || "global";
    }
    function updateEntityMenu() {
      const input = $("entityFilter");
      const menu = $("entityMenu");
      const needle = input.value.trim().toLowerCase();
      const options = [...new Map(explorer.rows
        .filter(row => row.domain === $("domainSelect").value && row.metric === $("metricSelect").value && row.dimension === $("dimensionSelect").value)
        .map(row => [row.entity_key, row.entity_label])).entries()]
        .filter(([key, label]) => !needle || key.toLowerCase().includes(needle) || label.toLowerCase().includes(needle))
        .slice(0, 80);
      menu.replaceChildren();
      for (const [key, label] of options) {
        const div = document.createElement("div");
        div.className = "combo-option";
        div.textContent = label;
        div.title = key;
        div.addEventListener("mousedown", event => {
          event.preventDefault();
          input.value = label;
          menu.classList.remove("open");
          render();
        });
        menu.append(div);
      }
      menu.classList.toggle("open", options.length > 0 && document.activeElement === input);
    }
    function resetExplorer() {
      $("domainSelect").value = "population";
      fillMetricOptions();
      $("metricSelect").value = "pops";
      $("scopeSelect").value = "world";
      fillDimensionOptions();
      $("dimensionSelect").value = availableDimensions().includes("super_region") ? "super_region" : availableDimensions()[0] || "global";
      $("aggregationSelect").value = "sum";
      $("rankSelect").value = "top";
      $("limitSelect").value = "5";
      $("entityFilter").value = "";
      $("explorerStart").value = firstSnapshot.snapshot_id || "";
      $("explorerEnd").value = lastSnapshot.snapshot_id || "";
      render();
    }
    function exportCsv() {
      if (!state.currentRows.length) return;
      const columns = Object.keys(state.currentRows[0]);
      const csv = [columns.join(","), ...state.currentRows.map(row => columns.map(col => JSON.stringify(row[col] ?? "")).join(","))].join("\\n");
      const blob = new Blob([csv], { type: "text/csv" });
      const a = document.createElement("a");
      a.href = URL.createObjectURL(blob);
      a.download = `eu5_savegame_${state.tab}.csv`;
      a.click();
      URL.revokeObjectURL(a.href);
    }
    function render() {
      document.querySelectorAll("[data-tab]").forEach(button => button.classList.toggle("active", button.dataset.tab === state.tab));
      if (state.tab === "overview") renderOverview();
      else renderExplorer();
    }
    function init() {
      fillSnapshotSelects();
      fillExplorerControls();
      $("subtitle").textContent = `${snapshots.length} snapshots | ${payload.playthroughId || "all playthroughs"} | payload ${fmt(payload.payloadSummary?.jsonBytes || 0)} bytes`;
      document.querySelectorAll("[data-tab]").forEach(button => button.addEventListener("click", () => { state.tab = button.dataset.tab; render(); }));
      ["overviewStart", "overviewEnd"].forEach(id => $(id).addEventListener("change", render));
      ["explorerStart", "explorerEnd", "aggregationSelect", "rankSelect", "limitSelect", "dimensionSelect"].forEach(id => $(id).addEventListener("change", render));
      $("domainSelect").addEventListener("change", () => { fillMetricOptions(); fillScopeOptions(); fillDimensionOptions(); $("entityFilter").value = ""; render(); });
      $("metricSelect").addEventListener("change", () => { fillDimensionOptions(); $("entityFilter").value = ""; render(); });
      $("scopeSelect").addEventListener("change", () => { fillDimensionOptions(); $("entityFilter").value = ""; render(); });
      $("entityFilter").addEventListener("input", () => { updateEntityMenu(); render(); });
      $("entityFilter").addEventListener("focus", updateEntityMenu);
      document.addEventListener("mousedown", event => { if (!event.target.closest(".combobox")) $("entityMenu").classList.remove("open"); });
      $("resetExplorer").addEventListener("click", resetExplorer);
      $("exportCsv").addEventListener("click", exportCsv);
      window.addEventListener("resize", () => chartInstances.forEach(chart => chart.resize()));
      render();
    }
    init();
  </script>
</body>
</html>
"""
    return (
        template.replace("__TITLE__", html.escape(title))
        .replace("__ECHARTS_JS__", _echarts_source())
        .replace("__PAYLOAD_JSON__", payload_json)
    )
