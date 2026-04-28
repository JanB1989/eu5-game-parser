from __future__ import annotations

import html
import json
from pathlib import Path
from typing import Any

import polars as pl

from eu5gameparser.savegame.exporter import (
    POP_EMPLOYED_COLUMNS,
    POP_UNEMPLOYED_COLUMNS,
    SavegameTables,
)


def write_savegame_explorer_html(tables: SavegameTables, path: str | Path) -> Path:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(_standalone_html(_payload(tables)), encoding="utf-8")
    return output_path


def _payload(tables: SavegameTables) -> dict[str, Any]:
    market_goods = tables.market_goods
    flows = tables.production_method_good_flows
    population_flows = tables.production_method_population_flows
    metadata = tables.save_metadata.to_dicts()[0] if not tables.save_metadata.is_empty() else {}

    return {
        "metadata": metadata,
        "markets": _market_rows(tables.markets),
        "goods": _goods_rows(market_goods, population_flows),
        "marketGoods": _selected_market_goods_rows(market_goods, population_flows),
        "bucketFlows": _bucket_flow_rows(tables.market_good_bucket_flows),
        "flows": _flow_rows(flows, population_flows),
        "rgoFlows": _rgo_flow_rows(tables.rgo_flows, population_flows),
        "populationPools": _population_pool_rows(tables.market_population_pools),
    }


def _market_rows(markets: pl.DataFrame) -> list[dict[str, Any]]:
    if markets.is_empty():
        return []
    return markets.select(
        [
            "market_id",
            "market_center_slug",
            "center_location_id",
            "food",
            "food_max",
            "price",
            "population",
            "capacity",
        ]
    ).sort("market_id").to_dicts()


def _goods_rows(market_goods: pl.DataFrame, population_flows: pl.DataFrame) -> list[dict[str, Any]]:
    if market_goods.is_empty():
        return []
    population_by_good = _population_by_key(population_flows, ["good_id"])
    rows = (
        market_goods.group_by("good_id")
        .agg(
            [
                pl.col("supply").fill_null(0).sum().alias("supply"),
                pl.col("demand").fill_null(0).sum().alias("demand"),
                pl.col("net").fill_null(0).sum().alias("net"),
                pl.col("stockpile").fill_null(0).sum().alias("stockpile"),
                pl.col("price").mean().alias("avg_price"),
                pl.col("default_price").max().alias("default_price"),
            ]
        )
        .sort("good_id")
        .to_dicts()
    )
    return [_merge_population(row, (row["good_id"],), population_by_good) for row in rows]


def _selected_market_goods_rows(
    market_goods: pl.DataFrame, population_flows: pl.DataFrame
) -> list[dict[str, Any]]:
    if market_goods.is_empty():
        return []
    population_by_market_good = _population_by_key(population_flows, ["market_id", "good_id"])
    columns = [
        "market_id",
        "market_center_slug",
        "good_id",
        "price",
        "default_price",
        "supply",
        "demand",
        "net",
        "stockpile",
        "supplied_Production",
        "demanded_Building",
    ]
    selected = [column for column in columns if column in market_goods.columns]
    rows = market_goods.select(selected).sort(["market_id", "good_id"]).to_dicts()
    return [
        _merge_population(row, (row["market_id"], row["good_id"]), population_by_market_good)
        for row in rows
    ]


def _flow_rows(flows: pl.DataFrame, population_flows: pl.DataFrame) -> list[dict[str, Any]]:
    if flows.is_empty():
        return []
    population_by_method = _population_by_key(
        population_flows, ["market_id", "production_method", "building_type"]
    )
    rows = (
        flows.group_by(
            [
                "market_id",
                "market_center_slug",
                "good_id",
                "direction",
                "production_method",
                "building_type",
            ]
        )
        .agg(
            [
                pl.col("allocated_amount").fill_null(0).sum().alias("allocated_amount"),
                pl.col("nominal_amount").fill_null(0).sum().alias("nominal_amount"),
                pl.col("building_count").fill_null(0).sum().alias("building_count"),
                pl.col("level_sum").fill_null(0).sum().alias("level_sum"),
            ]
        )
        .sort(["good_id", "direction", "production_method"])
        .to_dicts()
    )
    return [
        _merge_population(
            row,
            (row["market_id"], row["production_method"], row["building_type"]),
            population_by_method,
        )
        for row in rows
    ]


def _bucket_flow_rows(bucket_flows: pl.DataFrame) -> list[dict[str, Any]]:
    if bucket_flows.is_empty():
        return []
    return bucket_flows.sort(["market_id", "good_id", "direction", "bucket"]).to_dicts()


def _rgo_flow_rows(rgo_flows: pl.DataFrame, population_flows: pl.DataFrame) -> list[dict[str, Any]]:
    if rgo_flows.is_empty():
        return []
    rgo_population = population_flows.filter(pl.col("source_kind") == "rgo")
    population_by_rgo = _population_by_key(rgo_population, ["market_id", "good_id", "location_id"])
    rows = (
        rgo_flows.group_by(
            [
                "market_id",
                "market_center_slug",
                "good_id",
                "location_id",
                "location_slug",
                "raw_material",
            ]
        )
        .agg(
            [
                pl.col("allocated_amount").fill_null(0).sum().alias("allocated_amount"),
                pl.col("nominal_amount").fill_null(0).sum().alias("nominal_amount"),
                pl.col("rgo_employed").fill_null(0).sum().alias("rgo_employed"),
                pl.col("max_raw_material_workers")
                .fill_null(0)
                .sum()
                .alias("max_raw_material_workers"),
            ]
        )
        .sort(["good_id", "allocated_amount"], descending=[False, True])
        .to_dicts()
    )
    return [
        _merge_population(
            row,
            (row["market_id"], row["good_id"], row["location_id"]),
            population_by_rgo,
        )
        for row in rows
    ]


def _population_pool_rows(population_pools: pl.DataFrame) -> list[dict[str, Any]]:
    if population_pools.is_empty():
        return []
    columns = [
        "market_id",
        "market_center_slug",
        "employed_total",
        *POP_EMPLOYED_COLUMNS,
        "unemployed_total",
        *POP_UNEMPLOYED_COLUMNS,
    ]
    selected = [column for column in columns if column in population_pools.columns]
    return population_pools.select(selected).sort("market_id", nulls_last=False).to_dicts()


def _population_by_key(
    population_flows: pl.DataFrame, keys: list[str]
) -> dict[tuple[Any, ...], dict[str, float]]:
    if population_flows.is_empty():
        return {}
    columns = [*keys, "employed_total", *POP_EMPLOYED_COLUMNS]
    selected = [column for column in columns if column in population_flows.columns]
    grouped = (
        population_flows.select(selected)
        .group_by(keys)
        .agg(
            [
                pl.col("employed_total").fill_null(0).sum().alias("employed_total"),
                *[
                    pl.col(column).fill_null(0).sum().alias(column)
                    for column in POP_EMPLOYED_COLUMNS
                    if column in population_flows.columns
                ],
            ]
        )
    )
    result: dict[tuple[Any, ...], dict[str, float]] = {}
    for row in grouped.to_dicts():
        key = tuple(row.get(column) for column in keys)
        result[key] = {
            "employed_total": row.get("employed_total") or 0.0,
            **{column: row.get(column) or 0.0 for column in POP_EMPLOYED_COLUMNS},
        }
    return result


def _merge_population(
    row: dict[str, Any],
    key: tuple[Any, ...],
    population: dict[tuple[Any, ...], dict[str, float]],
) -> dict[str, Any]:
    values = population.get(
        key,
        {"employed_total": 0.0, **{column: 0.0 for column in POP_EMPLOYED_COLUMNS}},
    )
    return {**row, **values}


def _standalone_html(payload: dict[str, Any]) -> str:
    metadata = payload.get("metadata") or {}
    title = "EU5 Savegame Market Explorer"
    save_name = metadata.get("playthrough_name") or metadata.get("save_label") or "Savegame"
    save_date = metadata.get("date") or "unknown date"
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(title)}</title>
  <script src="https://unpkg.com/cytoscape@3.30.4/dist/cytoscape.min.js"></script>
  <script src="https://unpkg.com/dagre@0.8.5/dist/dagre.min.js"></script>
  <script src="https://unpkg.com/cytoscape-dagre@2.5.0/cytoscape-dagre.js"></script>
  <style>
    * {{ box-sizing: border-box; }}
    html, body {{ height: 100%; margin: 0; }}
    body {{
      background: #f6f8fb;
      color: #172033;
      font-family: Inter, Segoe UI, system-ui, sans-serif;
      overflow: hidden;
    }}
    .shell {{
      display: grid;
      grid-template-rows: auto 1fr;
      height: 100vh;
      width: 100vw;
    }}
    header {{
      align-items: center;
      background: #ffffff;
      border-bottom: 1px solid #d7e0ec;
      display: flex;
      gap: 16px;
      min-height: 62px;
      padding: 10px 16px;
    }}
    h1 {{
      font-size: 16px;
      line-height: 1.2;
      margin: 0;
    }}
    .meta {{
      color: #5b677a;
      font-size: 12px;
    }}
    .spacer {{ flex: 1; }}
    .tabs, .controls {{
      align-items: center;
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
    }}
    button, select, input {{
      background: #ffffff;
      border: 1px solid #c5d0de;
      border-radius: 6px;
      color: #172033;
      font: inherit;
      font-size: 13px;
      min-height: 32px;
      padding: 6px 9px;
    }}
    button {{
      cursor: pointer;
      white-space: nowrap;
    }}
    button:hover {{ background: #eef3f8; }}
    button.active {{
      background: #1f6feb;
      border-color: #1f6feb;
      color: #ffffff;
    }}
    input {{ min-width: 220px; }}
    main {{
      display: grid;
      grid-template-columns: clamp(760px, 52vw, 1040px) minmax(0, 1fr);
      min-height: 0;
    }}
    aside {{
      background: #ffffff;
      border-right: 1px solid #d7e0ec;
      display: grid;
      grid-template-rows: auto 1fr;
      min-width: 0;
      overflow: hidden;
    }}
    .panel-head {{
      border-bottom: 1px solid #e4ebf3;
      display: grid;
      gap: 8px;
      padding: 12px;
    }}
    .metrics {{
      display: grid;
      gap: 8px;
      grid-template-columns: repeat(3, minmax(0, 1fr));
    }}
    .metric {{
      background: #f8fafc;
      border: 1px solid #dce5ef;
      border-radius: 6px;
      padding: 8px;
    }}
    .metric-label {{
      color: #5b677a;
      font-size: 11px;
      font-weight: 700;
      text-transform: uppercase;
    }}
    .metric-value {{
      font-size: 16px;
      font-weight: 700;
      margin-top: 3px;
    }}
    .labour-pool {{
      background: #f8fafc;
      border: 1px solid #dce5ef;
      border-radius: 6px;
      display: grid;
      gap: 8px;
      padding: 8px;
    }}
    .pool-totals {{
      display: grid;
      gap: 8px;
      grid-template-columns: repeat(3, minmax(0, 1fr));
    }}
    .pool-breakdown {{
      color: #5b677a;
      font-size: 12px;
      line-height: 1.35;
    }}
    .table-wrap {{
      min-height: 0;
      overflow: auto;
    }}
    table {{
      border-collapse: collapse;
      font-size: 12px;
      min-width: 920px;
      width: 100%;
    }}
    th, td {{
      border-bottom: 1px solid #edf2f7;
      padding: 7px 9px;
      text-align: right;
      white-space: nowrap;
    }}
    th:first-child, td:first-child {{ text-align: left; }}
    th.numeric, td.numeric {{
      font-feature-settings: "tnum";
      font-variant-numeric: tabular-nums;
    }}
    th {{
      background: #f8fafc;
      color: #536173;
      font-size: 11px;
      position: sticky;
      text-transform: uppercase;
      top: 0;
      z-index: 1;
    }}
    th:first-child {{
      left: 0;
      z-index: 3;
    }}
    td:first-child {{
      background: #ffffff;
      left: 0;
      position: sticky;
      z-index: 2;
    }}
    tr:hover td:first-child, tr.selected td:first-child {{
      background: #edf5ff;
    }}
    th.sortable {{
      cursor: pointer;
      user-select: none;
    }}
    .sort-header {{
      align-items: center;
      background: transparent;
      border: 0;
      color: inherit;
      cursor: pointer;
      display: inline-flex;
      font: inherit;
      font-size: inherit;
      font-weight: inherit;
      gap: 4px;
      justify-content: flex-end;
      min-height: 0;
      padding: 0;
      text-transform: inherit;
      width: 100%;
    }}
    th:first-child .sort-header {{
      justify-content: flex-start;
    }}
    tfoot th, tfoot td {{
      background: #f8fafc;
      border-top: 1px solid #cbd5e1;
      bottom: 0;
      font-weight: 700;
      position: sticky;
      z-index: 1;
    }}
    tfoot th:first-child {{
      left: 0;
      z-index: 3;
    }}
    .sort-indicator {{
      color: #1f6feb;
      display: inline-block;
      min-width: 0.8em;
    }}
    tr {{
      cursor: pointer;
    }}
    tr:hover, tr.selected {{
      background: #edf5ff;
    }}
    #cy {{
      height: 100%;
      min-width: 0;
      width: 100%;
    }}
    .hidden {{ display: none; }}
    @media (max-width: 1100px) {{
      body {{ overflow: auto; }}
      .shell {{ min-height: 100vh; height: auto; }}
      main {{ grid-template-columns: 1fr; }}
      aside {{ min-height: 360px; border-right: 0; border-bottom: 1px solid #d7e0ec; }}
      #cy {{ height: 70vh; }}
      header {{ align-items: flex-start; flex-direction: column; }}
      input {{ min-width: 0; width: 100%; }}
    }}
  </style>
</head>
<body>
  <div class="shell">
    <header>
      <div>
        <h1>{html.escape(str(save_name))}</h1>
        <div class="meta">{html.escape(str(save_date))} &middot; Savegame market explorer</div>
      </div>
      <div class="spacer"></div>
      <div class="tabs">
        <button id="overviewTab" type="button" class="active">Overview</button>
        <button id="flowTab" type="button">Good Flow</button>
      </div>
      <div class="controls">
        <input id="goodSearch" list="goodOptions" placeholder="Search goods">
        <datalist id="goodOptions"></datalist>
        <input id="marketSearch" list="marketOptions" placeholder="All markets">
        <datalist id="marketOptions"></datalist>
        <button type="button" onclick="cy.fit(undefined, 70)">Fit</button>
      </div>
    </header>
    <main>
      <aside>
        <div class="panel-head">
          <div class="metrics">
            <div class="metric">
              <div class="metric-label">Supply</div>
              <div class="metric-value" id="supplyMetric">0</div>
            </div>
            <div class="metric">
              <div class="metric-label">Demand</div>
              <div class="metric-value" id="demandMetric">0</div>
            </div>
            <div class="metric">
              <div class="metric-label">Net</div>
              <div class="metric-value" id="netMetric">0</div>
            </div>
          </div>
          <div class="meta" id="scopeLabel"></div>
          <div class="labour-pool" id="labourPool"></div>
        </div>
        <div class="table-wrap">
          <table>
            <thead>
              <tr id="goodsHeaderRow"></tr>
            </thead>
            <tbody id="goodsBody"></tbody>
            <tfoot>
              <tr id="goodsFooterRow"></tr>
            </tfoot>
          </table>
        </div>
      </aside>
      <div id="cy"></div>
    </main>
  </div>
  <script>
    const payload = {json.dumps(payload, ensure_ascii=False)};
    const goods = payload.goods || [];
    const markets = payload.markets || [];
    const marketGoods = payload.marketGoods || [];
    const bucketFlows = payload.bucketFlows || [];
    const flows = payload.flows || [];
    const rgoFlows = payload.rgoFlows || [];
    const populationPools = payload.populationPools || [];
    const popColumns = [
      {{ key: "employed_nobles", label: "nobles" }},
      {{ key: "employed_clergy", label: "clergy" }},
      {{ key: "employed_burghers", label: "burghers" }},
      {{ key: "employed_laborers", label: "laborers" }},
      {{ key: "employed_soldiers", label: "soldiers" }},
      {{ key: "employed_peasants", label: "peasants" }},
      {{ key: "employed_slaves", label: "slaves" }},
      {{ key: "employed_tribesmen", label: "tribesmen" }}
    ];
    const overviewColumns = [
      {{ key: "good_id", label: "Good", numeric: false }},
      {{ key: "supply", label: "Supply", numeric: true }},
      {{ key: "demand", label: "Demand", numeric: true }},
      {{ key: "net", label: "Net", numeric: true }},
      {{ key: "employed_total", label: "Emp", numeric: true }},
      ...popColumns.map(column => ({{
        key: column.key,
        label: column.label[0].toUpperCase() + column.label.slice(1),
        numeric: true
      }}))
    ];
    let overviewSort = {{ key: "net", direction: "desc", absolute: true }};
    let currentView = "overview";
    let selectedGood = goods[0]?.good_id || "";
    let selectedMarketId = null;

    const cy = cytoscape({{
      container: document.getElementById("cy"),
      elements: [],
      minZoom: 0.25,
      maxZoom: 2.5,
      wheelSensitivity: 0.08,
      style: [
        {{
          selector: "node",
          style: {{
            "background-color": "data(color)",
            "border-color": "#172033",
            "border-opacity": 0.18,
            "border-width": 1,
            "color": "#172033",
            "font-size": 11,
            "height": "label",
            "label": "data(label)",
            "padding": "10px",
            "shape": "round-rectangle",
            "text-halign": "center",
            "text-valign": "center",
            "text-wrap": "wrap",
            "width": "label"
          }}
        }},
        {{
          selector: ".good",
          style: {{
            "background-color": "#ffffff",
            "border-color": "#1f6feb",
            "border-width": 3,
            "font-size": 13,
            "font-weight": 700,
            "padding": "14px"
          }}
        }},
        {{
          selector: ".producer",
          style: {{ "background-color": "#dcfce7", "border-color": "#15803d" }}
        }},
        {{
          selector: ".consumer",
          style: {{ "background-color": "#fee2e2", "border-color": "#b91c1c" }}
        }},
        {{
          selector: ".unallocated",
          style: {{
            "background-color": "#f1f5f9",
            "border-style": "dashed",
            "color": "#475569"
          }}
        }},
        {{
          selector: "edge",
          style: {{
            "curve-style": "bezier",
            "font-size": 10,
            "label": "data(label)",
            "line-color": "data(color)",
            "target-arrow-color": "data(color)",
            "target-arrow-shape": "triangle",
            "text-background-color": "#ffffff",
            "text-background-opacity": 0.85,
            "text-background-padding": "2px",
            "width": "data(width)"
          }}
        }}
      ]
    }});
    window.cy = cy;

    function formatNumber(value) {{
      const number = Number(value || 0);
      return number.toLocaleString(undefined, {{ maximumFractionDigits: 2 }});
    }}
    function formatOverviewNumber(value) {{
      const number = Number(value || 0);
      return number.toLocaleString(undefined, {{ maximumFractionDigits: 0 }});
    }}
    function exactNumberTitle(value) {{
      return `Exact: ${{formatNumber(value)}}`;
    }}
    function setOverviewNumber(element, value) {{
      element.textContent = formatOverviewNumber(value);
      element.title = exactNumberTitle(value);
    }}
    function setNumericCell(element, value) {{
      element.textContent = formatOverviewNumber(value);
      element.title = exactNumberTitle(value);
    }}
    function emptyPopulationFields() {{
      const fields = {{ employed_total: 0 }};
      for (const column of popColumns) fields[column.key] = 0;
      return fields;
    }}
    function emptyPopulationPool() {{
      const pool = {{ employed_total: 0, unemployed_total: 0 }};
      for (const column of popColumns) {{
        pool[column.key] = 0;
        pool[column.key.replace("employed_", "unemployed_")] = 0;
      }}
      return pool;
    }}
    function selectedPopulationPool() {{
      const pool = selectedMarketId === null
        ? populationPools.find(row => row.market_id === null)
        : populationPools.find(row => row.market_id === selectedMarketId);
      return pool || emptyPopulationPool();
    }}
    function populationFields(row) {{
      const fields = {{ employed_total: Number(row.employed_total || 0) }};
      for (const column of popColumns) fields[column.key] = Number(row[column.key] || 0);
      return fields;
    }}
    function addPopulationFields(target, row) {{
      target.employed_total += Number(row.employed_total || 0);
      for (const column of popColumns) {{
        target[column.key] += Number(row[column.key] || 0);
      }}
    }}
    function populationDetailLines(row) {{
      return popColumns
        .filter(column => Math.abs(Number(row[column.key] || 0)) > 0.000001)
        .map(column => `${{column.label}}: ${{formatNumber(row[column.key])}}`);
    }}
    function unemployedDetailLines(pool) {{
      return popColumns
        .map(column => ({{
          label: column.label,
          value: Number(pool[column.key.replace("employed_", "unemployed_")] || 0)
        }}))
        .filter(row => Math.abs(row.value) > 0.000001)
        .map(row => `${{row.label}}: ${{formatOverviewNumber(row.value)}}`);
    }}
    function sortableValue(row, key, absolute = false) {{
      const value = row[key];
      if (value === null || value === undefined || value === "") return null;
      if (typeof value === "number") return absolute ? Math.abs(value) : value;
      const numeric = Number(value);
      if (Number.isFinite(numeric)) return absolute ? Math.abs(numeric) : numeric;
      return String(value).toLocaleLowerCase();
    }}
    function compareOverviewRows(left, right) {{
      const leftValue = sortableValue(left, overviewSort.key, overviewSort.absolute);
      const rightValue = sortableValue(right, overviewSort.key, overviewSort.absolute);
      if (leftValue === null && rightValue === null) {{
        return left.good_id.localeCompare(right.good_id);
      }}
      if (leftValue === null) return 1;
      if (rightValue === null) return -1;
      let result = 0;
      if (typeof leftValue === "number" && typeof rightValue === "number") {{
        result = leftValue - rightValue;
      }} else {{
        result = String(leftValue).localeCompare(String(rightValue));
      }}
      if (result === 0) return left.good_id.localeCompare(right.good_id);
      return overviewSort.direction === "asc" ? result : -result;
    }}
    function sortedOverviewRows() {{
      return rowsForScope().slice().sort(compareOverviewRows);
    }}
    function setOverviewSort(key) {{
      if (overviewSort.key === key) {{
        overviewSort = {{
          key,
          direction: overviewSort.direction === "asc" ? "desc" : "asc",
          absolute: false
        }};
      }} else {{
        const column = overviewColumns.find(item => item.key === key);
        overviewSort = {{
          key,
          direction: column && column.numeric ? "desc" : "asc",
          absolute: false
        }};
      }}
      render();
    }}
    function renderTableHeader() {{
      const header = document.getElementById("goodsHeaderRow");
      header.replaceChildren();
      for (const column of overviewColumns) {{
        const th = document.createElement("th");
        th.className = column.numeric ? "numeric sortable" : "sortable";
        const button = document.createElement("button");
        button.className = "sort-header";
        button.type = "button";
        button.addEventListener("click", () => setOverviewSort(column.key));
        const label = document.createElement("span");
        label.textContent = column.label;
        const indicator = document.createElement("span");
        indicator.className = "sort-indicator";
        indicator.textContent = overviewSort.key === column.key
          ? (overviewSort.direction === "asc" ? "^" : "v")
          : "";
        button.append(label, indicator);
        th.append(button);
        header.append(th);
      }}
    }}
    function overviewTotals(rows) {{
      const totals = {{}};
      for (const column of overviewColumns) {{
        if (column.numeric) totals[column.key] = 0;
      }}
      for (const row of rows) {{
        for (const column of overviewColumns) {{
          if (column.numeric) totals[column.key] += Number(row[column.key] || 0);
        }}
      }}
      return totals;
    }}
    function renderTableFooter(rows) {{
      const footer = document.getElementById("goodsFooterRow");
      footer.replaceChildren();
      const totals = overviewTotals(rows);
      for (const column of overviewColumns) {{
        if (!column.numeric) {{
          const th = document.createElement("th");
          th.textContent = "Total";
          th.title = "Visible overview total";
          footer.append(th);
          continue;
        }}
        const td = document.createElement("td");
        td.className = "numeric";
        setNumericCell(td, totals[column.key]);
        footer.append(td);
      }}
    }}
    function marketLabel(market) {{
      if (!market) return "Global";
      return `${{market.market_center_slug || "Market"}} (#${{market.market_id}})`;
    }}
    function selectedMarket() {{
      return markets.find(market => market.market_id === selectedMarketId) || null;
    }}
    function rowsForScope() {{
      if (selectedMarketId === null) return goods;
      const rows = marketGoods.filter(row => row.market_id === selectedMarketId);
      return rows.map(row => ({{
        good_id: row.good_id,
        supply: row.supply || 0,
        demand: row.demand || 0,
        net: row.net || 0,
        stockpile: row.stockpile || 0,
        avg_price: row.price,
        default_price: row.default_price,
        ...populationFields(row)
      }}));
    }}
    function selectedGoodRow() {{
      return rowsForScope().find(row => row.good_id === selectedGood) || null;
    }}
    function fillOptions() {{
      const goodOptions = document.getElementById("goodOptions");
      goodOptions.replaceChildren();
      for (const good of goods) {{
        const option = document.createElement("option");
        option.value = good.good_id;
        goodOptions.append(option);
      }}
      const marketOptions = document.getElementById("marketOptions");
      marketOptions.replaceChildren();
      const all = document.createElement("option");
      all.value = "Global";
      marketOptions.append(all);
      for (const market of markets) {{
        const option = document.createElement("option");
        option.value = marketLabel(market);
        marketOptions.append(option);
      }}
    }}
    function renderTable() {{
      renderTableHeader();
      const body = document.getElementById("goodsBody");
      body.replaceChildren();
      const rows = sortedOverviewRows();
      renderTableFooter(rows);
      for (const row of rows) {{
        const tr = document.createElement("tr");
        if (row.good_id === selectedGood) tr.className = "selected";
        tr.addEventListener("click", () => {{
          selectedGood = row.good_id;
          document.getElementById("goodSearch").value = selectedGood;
          render();
        }});
        for (const column of overviewColumns) {{
          const td = document.createElement("td");
          if (column.numeric) td.className = "numeric";
          if (column.numeric) {{
            setNumericCell(td, row[column.key]);
          }} else {{
            td.textContent = row[column.key];
            td.title = row[column.key] || "";
          }}
          tr.append(td);
        }}
        body.append(tr);
      }}
    }}
    function updateMetrics() {{
      const row = selectedGoodRow() || {{ supply: 0, demand: 0, net: 0 }};
      setOverviewNumber(document.getElementById("supplyMetric"), row.supply);
      setOverviewNumber(document.getElementById("demandMetric"), row.demand);
      setOverviewNumber(document.getElementById("netMetric"), row.net);
      renderLabourPool();
      document.getElementById("scopeLabel").textContent =
        `${{selectedGood || "No good"}} · ${{marketLabel(selectedMarket())}}`;
    }}
    function renderLabourPool() {{
      const pool = selectedPopulationPool();
      const container = document.getElementById("labourPool");
      container.replaceChildren();
      const totals = document.createElement("div");
      totals.className = "pool-totals";
      const employed = Number(pool.employed_total || 0);
      const unemployed = Number(pool.unemployed_total || 0);
      for (const item of [
        ["Employed", employed],
        ["Unemployed", unemployed],
        ["Labour pool", employed + unemployed]
      ]) {{
        const metric = document.createElement("div");
        const label = document.createElement("div");
        label.className = "metric-label";
        label.textContent = item[0];
        const value = document.createElement("div");
        value.className = "metric-value";
        setOverviewNumber(value, item[1]);
        metric.append(label, value);
        totals.append(metric);
      }}
      const breakdown = document.createElement("div");
      breakdown.className = "pool-breakdown";
      const lines = unemployedDetailLines(pool);
      breakdown.textContent = lines.length
        ? `Unemployed by pop: ${{lines.join(" | ")}}`
        : "Unemployed by pop: none";
      container.append(totals, breakdown);
    }}
    function inScope(row) {{
      return row.good_id === selectedGood
        && (selectedMarketId === null || row.market_id === selectedMarketId);
    }}
    function aggregateRows(rows, keyFn, seedFn) {{
      const grouped = new Map();
      for (const row of rows) {{
        const key = keyFn(row);
        const current = grouped.get(key) || seedFn(row);
        current.allocated_amount += Number(row.allocated_amount || row.amount || 0);
        current.nominal_amount += Number(row.nominal_amount || 0);
        current.building_count += Number(row.building_count || 0);
        current.level_sum += Number(row.level_sum || 0);
        current.rgo_employed += Number(row.rgo_employed || 0);
        current.max_raw_material_workers += Number(row.max_raw_material_workers || 0);
        addPopulationFields(current, row);
        if ("location_count" in current) {{
          const hasLocation = row.location_id !== null && row.location_id !== undefined;
          current.location_count += Number(
            row.location_count || (hasLocation ? 1 : 0)
          );
        }}
        grouped.set(key, current);
      }}
      return [...grouped.values()].sort(
        (left, right) => Math.abs(right.allocated_amount) - Math.abs(left.allocated_amount)
      );
    }}
    function bucketRows(direction) {{
      return aggregateRows(
        bucketFlows.filter(row => inScope(row) && row.direction === direction),
        row => `${{row.direction}}:${{row.bucket}}`,
        row => ({{
          bucket: row.bucket,
          direction: row.direction,
          save_column: row.save_column,
          allocated_amount: 0,
          nominal_amount: 0,
          building_count: 0,
          level_sum: 0,
          rgo_employed: 0,
          max_raw_material_workers: 0,
          ...emptyPopulationFields()
        }})
      );
    }}
    function methodDetailRows(direction) {{
      return aggregateRows(
        flows.filter(row => inScope(row) && row.direction === direction),
        row => `${{row.production_method}}:${{row.building_type || ""}}`,
        row => ({{
          production_method: row.production_method,
          building_type: row.building_type,
          direction: row.direction,
          allocated_amount: 0,
          nominal_amount: 0,
          building_count: 0,
          level_sum: 0,
          rgo_employed: 0,
          max_raw_material_workers: 0,
          ...emptyPopulationFields()
        }})
      ).filter(row => Math.abs(row.allocated_amount || 0) > 0.000001);
    }}
    function rgoDetailRows() {{
      return aggregateRows(
        rgoFlows.filter(row => inScope(row)),
        row => row.good_id,
        row => ({{
          good_id: row.good_id,
          raw_material: row.raw_material,
          allocated_amount: 0,
          nominal_amount: 0,
          building_count: 0,
          level_sum: 0,
          rgo_employed: 0,
          max_raw_material_workers: 0,
          location_count: 0,
          ...emptyPopulationFields()
        }})
      ).filter(row => Math.abs(row.allocated_amount || 0) > 0.000001);
    }}
    function graphElements() {{
      const row = selectedGoodRow() || {{}};
      const goodLabel = [
        selectedGood,
        `Supply ${{formatNumber(row.supply)}} · Demand ${{formatNumber(row.demand)}}`,
        `Net ${{formatNumber(row.net)}}`,
        `Price ${{formatNumber(row.avg_price)}} · Base ${{formatNumber(row.default_price)}}`,
        `Stockpile ${{formatNumber(row.stockpile)}}`,
        `Employed ${{formatNumber(row.employed_total)}}`
      ].join("\\n");
      const supplyBuckets = bucketRows("supply");
      const demandBuckets = bucketRows("demand");
      const producers = methodDetailRows("output");
      const rgos = rgoDetailRows();
      const consumers = methodDetailRows("input");
      const nodes = [{{
        data: {{
          id: `good:${{selectedGood}}`,
          label: goodLabel,
          color: "#ffffff"
        }},
        classes: "good"
      }}];
      const edges = [];
      const addEdge = (id, source, target, amount, color) => {{
        edges.push({{
          data: {{
            id,
            source,
            target,
            label: formatNumber(amount),
            color,
            width: Math.max(2, Math.min(10, 1.5 + Math.sqrt(Math.abs(amount || 0))))
          }}
        }});
      }};
      const addBucketNode = (bucket, role) => {{
        const id = `bucket:${{role}}:${{bucket.bucket}}`;
        if (!nodes.some(node => node.data.id === id)) {{
          nodes.push({{
            data: {{
              id,
              label: [
                bucket.bucket,
                formatNumber(bucket.allocated_amount)
              ].join("\\n"),
              color: role === "supply" ? "#dcfce7" : "#fee2e2"
            }},
            classes: role === "supply" ? "producer" : "consumer"
          }});
        }}
        const amount = Math.abs(bucket.allocated_amount || 0);
        if (role === "supply") {{
          addEdge(`edge:${{id}}:good`, id, `good:${{selectedGood}}`, amount, "#15803d");
        }} else {{
          addEdge(`edge:good:${{id}}`, `good:${{selectedGood}}`, id, amount, "#b91c1c");
        }}
        return id;
      }};
      const addMethodNode = (flow, role, bucketId) => {{
        const id = `${{role}}:${{flow.production_method}}:${{flow.building_type || ""}}`;
        if (!nodes.some(node => node.data.id === id)) {{
          const building = flow.building_type ? `\\n${{flow.building_type}}` : "";
          const countLine = `count: ${{formatNumber(flow.building_count)}}`;
          const levelLine = `level sum: ${{formatNumber(flow.level_sum)}}`;
          const employmentLine = `employed: ${{formatNumber(flow.employed_total)}}`;
          const isUnattributed = flow.production_method.startsWith("unattributed");
          nodes.push({{
            data: {{
              id,
              label: [
                `${{flow.production_method}}${{building}}`,
                formatNumber(flow.allocated_amount),
                employmentLine,
                ...populationDetailLines(flow),
                countLine,
                levelLine
              ].join("\\n"),
              color: role === "producer" ? "#dcfce7" : "#fee2e2"
            }},
            classes: `${{role}} ${{isUnattributed ? "unallocated" : ""}}`
          }});
        }}
        const amount = Math.abs(flow.allocated_amount || 0);
        if (role === "producer") {{
          addEdge(`edge:${{id}}:${{bucketId}}`, id, bucketId, amount, "#15803d");
        }} else {{
          addEdge(`edge:${{bucketId}}:${{id}}`, bucketId, id, amount, "#b91c1c");
        }}
      }};
      const addRgoNode = (flow, bucketId) => {{
        const scope = selectedMarketId === null ? "global" : selectedMarketId;
        const id = `rgo:${{scope}}:${{flow.good_id}}`;
        if (!nodes.some(node => node.data.id === id)) {{
          nodes.push({{
            data: {{
              id,
              label: [
                `RGO ${{flow.good_id || flow.raw_material || ""}}`,
                formatNumber(flow.allocated_amount),
                `locations: ${{formatNumber(flow.location_count)}}`,
                `employed: ${{formatNumber(flow.rgo_employed)}}`,
                ...populationDetailLines(flow),
                `max workers: ${{formatNumber(flow.max_raw_material_workers)}}`
              ].join("\\n"),
              color: "#d9f99d"
            }},
            classes: "producer"
          }});
        }}
        addEdge(
          `edge:${{id}}:${{bucketId}}`,
          id,
          bucketId,
          Math.abs(flow.allocated_amount || 0),
          "#4d7c0f"
        );
      }};
      const supplyBucketIds = new Map();
      for (const bucket of supplyBuckets) {{
        supplyBucketIds.set(bucket.bucket, addBucketNode(bucket, "supply"));
      }}
      const demandBucketIds = new Map();
      for (const bucket of demandBuckets) {{
        demandBucketIds.set(bucket.bucket, addBucketNode(bucket, "demand"));
      }}
      const productionBucket = supplyBucketIds.get("Production");
      if (productionBucket) {{
        producers.slice(0, 80).forEach(flow => addMethodNode(flow, "producer", productionBucket));
        rgos.slice(0, 80).forEach(flow => addRgoNode(flow, productionBucket));
      }}
      const buildingBucket = demandBucketIds.get("Building");
      if (buildingBucket) {{
        consumers.slice(0, 80).forEach(flow => addMethodNode(flow, "consumer", buildingBucket));
      }}
      return [...nodes, ...edges];
    }}
    function renderGraph() {{
      cy.elements().remove();
      cy.add(graphElements());
      cy.layout({{
        name: "dagre",
        rankDir: "LR",
        ranker: "network-simplex",
        nodeSep: 70,
        rankSep: 210,
        fit: true,
        padding: 70,
        animate: false
      }}).run();
    }}
    function renderOverviewGraph() {{
      const rows = sortedOverviewRows().slice(0, 35);
      const elements = rows.map(row => ({{
        data: {{
          id: `overview:${{row.good_id}}`,
          label: [
            row.good_id,
            `S ${{formatOverviewNumber(row.supply)}} · D ${{formatOverviewNumber(row.demand)}}`,
            `Net ${{formatOverviewNumber(row.net)}}`,
            `Emp ${{formatOverviewNumber(row.employed_total)}}`
          ].join("\\n"),
          color: (row.net || 0) >= 0 ? "#dcfce7" : "#fee2e2"
        }},
        classes: row.good_id === selectedGood ? "good" : ""
      }}));
      cy.elements().remove();
      cy.add(elements);
      cy.layout({{
        name: "grid",
        fit: true,
        padding: 60,
        animate: false
      }}).run();
      cy.nodes().on("tap", event => {{
        selectedGood = event.target.id().replace("overview:", "");
        document.getElementById("goodSearch").value = selectedGood;
        currentView = "flow";
        syncTabs();
        render();
      }});
    }}
    function syncTabs() {{
      document.getElementById("overviewTab").classList.toggle("active", currentView === "overview");
      document.getElementById("flowTab").classList.toggle("active", currentView === "flow");
    }}
    function render() {{
      renderTable();
      updateMetrics();
      syncTabs();
      if (currentView === "overview") renderOverviewGraph();
      else renderGraph();
    }}
    document.getElementById("goodSearch").addEventListener("change", event => {{
      if (goods.some(good => good.good_id === event.target.value)) {{
        selectedGood = event.target.value;
        render();
      }}
    }});
    document.getElementById("marketSearch").addEventListener("change", event => {{
      const value = event.target.value.trim();
      if (!value || value.toLowerCase() === "global") {{
        selectedMarketId = null;
      }} else {{
        const match = markets.find(market => marketLabel(market) === value);
        selectedMarketId = match ? match.market_id : selectedMarketId;
      }}
      render();
    }});
    document.getElementById("overviewTab").addEventListener("click", () => {{
      currentView = "overview";
      render();
    }});
    document.getElementById("flowTab").addEventListener("click", () => {{
      currentView = "flow";
      render();
    }});
    fillOptions();
    document.getElementById("goodSearch").value = selectedGood;
    render();
  </script>
</body>
</html>
"""
