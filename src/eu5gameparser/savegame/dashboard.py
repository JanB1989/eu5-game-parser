from __future__ import annotations

import argparse
import time
from pathlib import Path
from typing import Any

import plotly.graph_objects as go
from dash import Dash, Input, Output, State, dash_table, dcc, html
from plotly.subplots import make_subplots

from eu5gameparser.load_order import DEFAULT_LOAD_ORDER_PATH
from eu5gameparser.savegame.dashboard_adapter import (
    DashboardQueryResult,
    SavegameDashboardAdapter,
    TemplateQueryResult,
)

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8050
DEFAULT_REFRESH_MS = 5000
_FILTER_CONTROLS = (
    ("good_id", "template-filter-good-id"),
    ("goods_category", "template-filter-goods-category"),
    ("goods_designation", "template-filter-goods-designation"),
    ("market_center_slug", "template-filter-market-center-slug"),
    ("building_type", "template-filter-building-type"),
    ("production_method", "template-filter-production-method"),
    ("country_tag", "template-filter-country-tag"),
    ("pop_type", "template-filter-pop-type"),
    ("religion_name", "template-filter-religion-name"),
)


def create_dashboard_app(
    dataset: str | Path,
    *,
    profile: str = "merged_default",
    load_order_path: str | Path = DEFAULT_LOAD_ORDER_PATH,
    refresh_ms: int = DEFAULT_REFRESH_MS,
) -> Dash:
    adapter = SavegameDashboardAdapter(
        dataset,
        profile=profile,
        load_order_path=load_order_path,
        asset_root=Path(dataset) / "dashboard_assets",
    )
    adapter.asset_root.mkdir(parents=True, exist_ok=True)
    app = Dash(
        __name__,
        title="EU5 Progression Dashboard",
        assets_folder=str(adapter.asset_root),
        suppress_callback_exceptions=False,
    )
    metadata = adapter.template_metadata()
    date_options = adapter.date_options()
    from_value = date_options[0]["value"] if date_options else None
    to_value = date_options[-1]["value"] if date_options else None
    app.layout = _layout(adapter, metadata, date_options, from_value, to_value, refresh_ms)
    _register_callbacks(app, adapter)
    return app


def run_dashboard(
    dataset: str | Path,
    *,
    profile: str = "merged_default",
    load_order_path: str | Path = DEFAULT_LOAD_ORDER_PATH,
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    debug: bool = False,
    refresh_ms: int = DEFAULT_REFRESH_MS,
) -> None:
    started = time.perf_counter()
    app = create_dashboard_app(
        dataset,
        profile=profile,
        load_order_path=load_order_path,
        refresh_ms=refresh_ms,
    )
    url = f"http://{host}:{port}"
    print(f"dashboard: {url}")
    print(f"dataset: {Path(dataset)}")
    print(f"startup_seconds: {time.perf_counter() - started:.2f}")
    app.run(host=host, port=port, debug=debug)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the EU5 progression dashboard.")
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--profile", default="merged_default")
    parser.add_argument("--load-order", type=Path, default=DEFAULT_LOAD_ORDER_PATH)
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--refresh-ms", type=int, default=DEFAULT_REFRESH_MS)
    args = parser.parse_args()
    run_dashboard(
        args.dataset,
        profile=args.profile,
        load_order_path=args.load_order,
        host=args.host,
        port=args.port,
        debug=args.debug,
        refresh_ms=args.refresh_ms,
    )


def _layout(
    adapter: SavegameDashboardAdapter,
    metadata: dict[str, list[dict[str, Any]] | list[str]],
    date_options: list[dict[str, Any]],
    from_value: int | None,
    to_value: int | None,
    refresh_ms: int,
) -> html.Div:
    playthrough_options = adapter.playthrough_options()
    playthrough_value = playthrough_options[0]["value"] if playthrough_options else ""
    return html.Div(
        [
            dcc.Interval(
                id="dashboard-refresh",
                interval=max(1000, int(refresh_ms)),
                n_intervals=0,
            ),
            html.H1("EU5 Progression Dashboard"),
            html.Div(
                [
                    html.Label("Playthrough"),
                    dcc.Dropdown(
                        id="playthrough-select",
                        options=playthrough_options,
                        value=playthrough_value,
                        clearable=False,
                    ),
                    html.Label("From"),
                    dcc.Dropdown(
                        id="from-date",
                        options=date_options,
                        value=from_value,
                        clearable=False,
                    ),
                    html.Label("To"),
                    dcc.Dropdown(
                        id="to-date",
                        options=date_options,
                        value=to_value,
                        clearable=False,
                    ),
                ],
                className="control-row",
            ),
            dcc.Tabs(
                id="main-tabs",
                value="overview",
                children=[
                    dcc.Tab(label="Overview", value="overview", children=_overview_tab()),
                    dcc.Tab(label="Explorer", value="explorer", children=_explorer_tab(metadata)),
                    dcc.Tab(label="Game Data", value="game-data", children=_game_data_tab()),
                ],
            ),
        ],
        className="dashboard-shell",
    )


def _overview_tab() -> html.Div:
    return html.Div(
        [
            html.Div(id="overview-cards", className="metric-cards"),
            dcc.Graph(id="pops-by-type-figure"),
            dcc.Graph(id="employment-figure"),
            dcc.Graph(id="development-figure"),
            dcc.Graph(id="tax-figure"),
            dcc.Graph(id="food-figure"),
        ],
        className="tab-body",
    )


def _explorer_tab(metadata: dict[str, list[dict[str, Any]] | list[str]]) -> html.Div:
    default_metric = _template_metric(metadata, str(metadata["defaultMetric"]))
    default_scope = str(default_metric["defaultScope"])
    default_scope_group = _scope_group_for(metadata, default_scope)
    return html.Div(
        [
            html.Div(
                [
                    _control(
                        "Domain",
                        dcc.Dropdown(
                            id="template-domain",
                            options=metadata["domains"],
                            value=default_metric["domain"],
                            clearable=False,
                        ),
                    ),
                    _control(
                        "Metric",
                        dcc.Dropdown(
                            id="template-metric",
                            options=_metric_options_for_domain(metadata, default_metric["domain"]),
                            value=metadata["defaultMetric"],
                            clearable=False,
                            searchable=True,
                        ),
                    ),
                    _control(
                        "Scope",
                        dcc.Dropdown(
                            id="template-scope-group",
                            options=_scope_group_options(metadata, default_metric),
                            value=default_scope_group,
                            clearable=False,
                        ),
                    ),
                    _control(
                        "Group by",
                        dcc.Dropdown(
                            id="template-group-by",
                            options=_group_by_options(
                                metadata,
                                default_metric,
                                default_scope_group,
                            ),
                            value=default_scope,
                            clearable=False,
                        ),
                    ),
                    _control(
                        "N",
                        dcc.Dropdown(
                            id="template-limit",
                            options=[
                                {"label": str(value), "value": value}
                                for value in metadata["limits"]
                            ],
                            value=5,
                            clearable=False,
                        ),
                    ),
                ],
                className="control-grid",
                style={
                    "display": "grid",
                    "gridTemplateColumns": "repeat(5, minmax(160px, 1fr))",
                    "gap": "8px 12px",
                    "alignItems": "end",
                    "marginBottom": "12px",
                },
            ),
            html.Div(
                id="template-filter-row",
                children=[
                    _filter_control("Good", "template-filter-good-id"),
                    _filter_control("Goods Category", "template-filter-goods-category"),
                    _filter_control("Goods Designation", "template-filter-goods-designation"),
                    _filter_control("Market", "template-filter-market-center-slug"),
                    _filter_control("Building", "template-filter-building-type"),
                    _filter_control("Production Method", "template-filter-production-method"),
                    _filter_control("Country", "template-filter-country-tag"),
                    _filter_control("Pop Type", "template-filter-pop-type"),
                    _filter_control("Religion", "template-filter-religion-name"),
                ],
                style={
                    "display": "grid",
                    "gridTemplateColumns": "repeat(4, minmax(180px, 1fr))",
                    "gap": "8px 12px",
                    "marginBottom": "12px",
                },
            ),
            html.Div(
                id="template-query-chips",
                className="query-chips",
                style={"display": "flex", "gap": "8px", "flexWrap": "wrap"},
            ),
            html.Div(
                [
                    dcc.Graph(id="template-top-sum-figure"),
                    dcc.Graph(id="template-bottom-sum-figure"),
                    dcc.Graph(id="template-top-mean-figure"),
                    dcc.Graph(id="template-bottom-mean-figure"),
                    dcc.Graph(id="template-top-change-figure"),
                    dcc.Graph(id="template-bottom-change-figure"),
                ],
                className="template-chart-grid",
                style={
                    "display": "grid",
                    "gridTemplateColumns": "repeat(2, minmax(0, 1fr))",
                    "gap": "12px",
                },
            ),
            dash_table.DataTable(
                id="template-ranking",
                columns=[
                    {"name": "Entity", "id": "entity_label"},
                    {"name": "Sum", "id": "sum", "type": "numeric"},
                    {"name": "Mean", "id": "mean", "type": "numeric"},
                    {"name": "First", "id": "first", "type": "numeric"},
                    {"name": "Last", "id": "last", "type": "numeric"},
                    {"name": "Absolute Change", "id": "absolute_change", "type": "numeric"},
                    {"name": "Percent Change", "id": "percent_change", "type": "numeric"},
                    {"name": "Min", "id": "min", "type": "numeric"},
                    {"name": "Max", "id": "max", "type": "numeric"},
                ],
                data=[],
                sort_action="native",
                filter_action="native",
                page_size=20,
            ),
        ],
        className="tab-body",
    )


def _game_data_tab() -> html.Div:
    return html.Div(
        [
            html.Div(
                [
                    html.Label("Building search"),
                    dcc.Input(
                        id="building-search",
                        type="search",
                        debounce=True,
                        placeholder="building, category, or pop type",
                    ),
                ],
                className="control-row",
            ),
            html.Div(id="building-reference"),
        ],
        className="tab-body",
    )


def _control(label: str, child: Any, *, style: dict[str, Any] | None = None) -> html.Div:
    return html.Div([html.Label(label), child], style=style or {})


def _filter_control(label: str, component_id: str) -> html.Div:
    return html.Div(
        [
            html.Label(label),
            dcc.Dropdown(
                id=component_id,
                options=[],
                value=None,
                clearable=True,
                searchable=True,
                placeholder=f"Any {label.lower()}",
            ),
        ],
        id=f"{component_id}-control",
        style={"display": "none"},
    )


def _register_callbacks(
    app: Dash,
    adapter: SavegameDashboardAdapter,
) -> None:
    @app.callback(
        Output("playthrough-select", "options"),
        Output("playthrough-select", "value"),
        Input("dashboard-refresh", "n_intervals"),
        State("playthrough-select", "value"),
    )
    def update_playthrough_options(
        _refresh_tick: int,
        current_playthrough_id: str | None,
    ) -> tuple[list[dict[str, str]], str]:
        options = adapter.playthrough_options()
        value = _preserved_dropdown_value(options, current_playthrough_id, default="")
        return options, str(value or "")

    @app.callback(
        Output("from-date", "options"),
        Output("from-date", "value"),
        Output("to-date", "options"),
        Output("to-date", "value"),
        Input("playthrough-select", "value"),
        Input("dashboard-refresh", "n_intervals"),
        State("from-date", "value"),
        State("to-date", "value"),
    )
    def update_date_options(
        playthrough_id: str | None,
        _refresh_tick: int,
        current_from_date_sort: int | None,
        current_to_date_sort: int | None,
    ) -> tuple[list[dict[str, Any]], Any, list[dict[str, Any]], Any]:
        options = adapter.date_options(playthrough_id)
        from_value = _preserved_dropdown_value(
            options,
            current_from_date_sort,
            default=options[0]["value"] if options else None,
        )
        to_value = _preserved_dropdown_value(
            options,
            current_to_date_sort,
            default=options[-1]["value"] if options else None,
        )
        return options, from_value, options, to_value

    @app.callback(
        Output("overview-cards", "children"),
        Output("pops-by-type-figure", "figure"),
        Output("employment-figure", "figure"),
        Output("development-figure", "figure"),
        Output("tax-figure", "figure"),
        Output("food-figure", "figure"),
        Input("playthrough-select", "value"),
        Input("from-date", "value"),
        Input("to-date", "value"),
        Input("dashboard-refresh", "n_intervals"),
    )
    def update_overview(
        playthrough_id: str | None,
        from_date_sort: int | None,
        to_date_sort: int | None,
        _refresh_tick: int,
    ) -> tuple[list[html.Div], go.Figure, go.Figure, go.Figure, go.Figure, go.Figure]:
        overview = adapter.overview(
            playthrough_id=playthrough_id,
            from_date_sort=from_date_sort,
            to_date_sort=to_date_sort,
        )
        latest = _latest_overview_values(overview)
        return (
            _cards(latest),
            _pops_by_type_figure(overview["pops_by_type"]),
            _line_figure(
                "Employment",
                overview["employment"],
                ["total_pops", "employed_pops", "unemployed_pops"],
                "Pops",
            ),
            _line_figure("Development", overview["development"], ["development"], "Development"),
            _tax_figure(overview["tax"]),
            _food_figure(overview["food"]),
        )

    @app.callback(
        Output("template-metric", "options"),
        Output("template-metric", "value"),
        Input("template-domain", "value"),
        Input("dashboard-refresh", "n_intervals"),
        State("template-metric", "value"),
    )
    def update_template_metric_options(
        domain: str,
        _refresh_tick: int,
        current_metric: str | None,
    ) -> tuple[list[dict[str, str]], str]:
        metadata = adapter.template_metadata()
        options = _metric_options_for_domain(metadata, domain)
        values = {item["value"] for item in options}
        value = current_metric if current_metric in values else None
        if value is None and options:
            value = options[0]["value"]
        return options, str(value)

    @app.callback(
        Output("template-scope-group", "options"),
        Output("template-scope-group", "value"),
        Input("template-metric", "value"),
        Input("dashboard-refresh", "n_intervals"),
        State("template-scope-group", "value"),
    )
    def update_template_scope_group_options(
        metric_key: str,
        _refresh_tick: int,
        current_scope_group: str | None,
    ) -> tuple[list[dict[str, str]], str]:
        metadata = adapter.template_metadata()
        metric = _template_metric(metadata, metric_key)
        options = _scope_group_options(metadata, metric)
        values = {item["value"] for item in options}
        value = current_scope_group if current_scope_group in values else None
        if value is None:
            value = _scope_group_for(metadata, metric["defaultScope"])
        if value not in values and options:
            value = options[0]["value"]
        return options, str(value)

    @app.callback(
        Output("template-group-by", "options"),
        Output("template-group-by", "value"),
        Input("template-metric", "value"),
        Input("template-scope-group", "value"),
        Input("dashboard-refresh", "n_intervals"),
        State("template-group-by", "value"),
    )
    def update_template_group_by_options(
        metric_key: str,
        scope_group: str,
        _refresh_tick: int,
        current_group_by: str | None,
    ) -> tuple[list[dict[str, str]], str]:
        metadata = adapter.template_metadata()
        metric = _template_metric(metadata, metric_key)
        options = _group_by_options(metadata, metric, scope_group)
        values = {item["value"] for item in options}
        value = current_group_by if current_group_by in values else None
        if value is None and metric["defaultScope"] in values:
            value = metric["defaultScope"]
        if value is None and options:
            value = options[0]["value"]
        return options, str(value)

    @app.callback(
        Output("template-filter-good-id", "options"),
        Output("template-filter-good-id", "value"),
        Output("template-filter-good-id-control", "style"),
        Output("template-filter-goods-category", "options"),
        Output("template-filter-goods-category", "value"),
        Output("template-filter-goods-category-control", "style"),
        Output("template-filter-goods-designation", "options"),
        Output("template-filter-goods-designation", "value"),
        Output("template-filter-goods-designation-control", "style"),
        Output("template-filter-market-center-slug", "options"),
        Output("template-filter-market-center-slug", "value"),
        Output("template-filter-market-center-slug-control", "style"),
        Output("template-filter-building-type", "options"),
        Output("template-filter-building-type", "value"),
        Output("template-filter-building-type-control", "style"),
        Output("template-filter-production-method", "options"),
        Output("template-filter-production-method", "value"),
        Output("template-filter-production-method-control", "style"),
        Output("template-filter-country-tag", "options"),
        Output("template-filter-country-tag", "value"),
        Output("template-filter-country-tag-control", "style"),
        Output("template-filter-pop-type", "options"),
        Output("template-filter-pop-type", "value"),
        Output("template-filter-pop-type-control", "style"),
        Output("template-filter-religion-name", "options"),
        Output("template-filter-religion-name", "value"),
        Output("template-filter-religion-name-control", "style"),
        Input("template-metric", "value"),
        Input("playthrough-select", "value"),
        Input("dashboard-refresh", "n_intervals"),
    )
    def update_template_filter_options(
        metric_key: str,
        playthrough_id: str | None,
        _refresh_tick: int,
    ) -> tuple[Any, ...]:
        metadata = adapter.template_metadata()
        metric = _template_metric(metadata, metric_key)
        active_filter_keys = _active_filter_keys(metadata, metric)
        output: list[Any] = []
        for filter_key, _component_id in _FILTER_CONTROLS:
            visible = filter_key in active_filter_keys
            options = adapter.template_filter_options(filter_key, playthrough_id=playthrough_id)
            if filter_key == "religion_name" and not options:
                visible = False
            style = {} if visible else {"display": "none"}
            output.extend([options, None, style])
        return tuple(output)

    @app.callback(
        Output("template-query-chips", "children"),
        Output("template-top-sum-figure", "figure"),
        Output("template-bottom-sum-figure", "figure"),
        Output("template-top-mean-figure", "figure"),
        Output("template-bottom-mean-figure", "figure"),
        Output("template-top-change-figure", "figure"),
        Output("template-bottom-change-figure", "figure"),
        Output("template-ranking", "data"),
        Input("template-metric", "value"),
        Input("template-group-by", "value"),
        Input("template-limit", "value"),
        Input("playthrough-select", "value"),
        Input("from-date", "value"),
        Input("to-date", "value"),
        Input("template-filter-good-id", "value"),
        Input("template-filter-goods-category", "value"),
        Input("template-filter-goods-designation", "value"),
        Input("template-filter-market-center-slug", "value"),
        Input("template-filter-building-type", "value"),
        Input("template-filter-production-method", "value"),
        Input("template-filter-country-tag", "value"),
        Input("template-filter-pop-type", "value"),
        Input("template-filter-religion-name", "value"),
        Input("dashboard-refresh", "n_intervals"),
    )
    def update_template(
        metric_key: str,
        group_by: str,
        limit: int,
        playthrough_id: str | None,
        from_date_sort: int | None,
        to_date_sort: int | None,
        good_id: str | None,
        goods_category: str | None,
        goods_designation: str | None,
        market_center_slug: str | None,
        building_type: str | None,
        production_method: str | None,
        country_tag: str | None,
        pop_type: str | None,
        religion_name: str | None,
        _refresh_tick: int,
    ) -> tuple[
        list[html.Span],
        go.Figure,
        go.Figure,
        go.Figure,
        go.Figure,
        go.Figure,
        go.Figure,
        list[dict[str, Any]],
    ]:
        filters = _filters_from_values(
            good_id=good_id,
            goods_category=goods_category,
            goods_designation=goods_designation,
            market_center_slug=market_center_slug,
            building_type=building_type,
            production_method=production_method,
            country_tag=country_tag,
            pop_type=pop_type,
            religion_name=religion_name,
        )
        result = adapter.template_query(
            metric_key=metric_key,
            scope=group_by,
            limit=limit,
            filters=filters,
            playthrough_id=playthrough_id,
            from_date_sort=from_date_sort,
            to_date_sort=to_date_sort,
        )
        return (
            [
                html.Span(
                    chip,
                    className="query-chip",
                    style={
                        "border": "1px solid #d0d7de",
                        "borderRadius": "999px",
                        "padding": "4px 10px",
                        "fontSize": "12px",
                    },
                )
                for chip in result.chips
            ],
            _template_panel_figure(result, "top_sum"),
            _template_panel_figure(result, "bottom_sum"),
            _template_panel_figure(result, "top_mean"),
            _template_panel_figure(result, "bottom_mean"),
            _template_panel_figure(result, "top_change"),
            _template_panel_figure(result, "bottom_change"),
            result.ranking,
        )

    @app.callback(
        Output("building-reference", "children"),
        Input("building-search", "value"),
    )
    def update_building_reference(search: str | None) -> list[html.Div]:
        rows = adapter.building_references(search=search, limit=80)
        if not rows:
            return [html.Div("No buildings found.", className="empty-state")]
        return [_building_card(row) for row in rows]


def _cards(values: dict[str, Any]) -> list[html.Div]:
    labels = [
        ("Total Pops", "total_pops"),
        ("Employed", "employed_pops"),
        ("Unemployed", "unemployed_pops"),
        ("Development", "development"),
        ("Collected Tax", "collected_tax"),
        ("Food", "food"),
    ]
    return [
        html.Div(
            [
                html.Div(label, className="card-label"),
                html.Div(_format_number(values.get(key)), className="card-value"),
            ],
            className="metric-card",
        )
        for label, key in labels
    ]


def _pops_by_type_figure(rows: list[dict[str, Any]]) -> go.Figure:
    figure = go.Figure()
    by_type: dict[str, list[dict[str, Any]]] = {}
    total_by_date: dict[str, float] = {}
    for row in rows:
        by_type.setdefault(str(row["pop_type"]), []).append(row)
        date = str(row["date"])
        total_by_date[date] = total_by_date.get(date, 0.0) + float(row["value"])
    for pop_type, series in sorted(by_type.items()):
        figure.add_bar(
            name=pop_type,
            x=[row["date"] for row in series],
            y=[row["value"] for row in series],
        )
    figure.add_scatter(
        name="Total Pops",
        x=list(total_by_date),
        y=list(total_by_date.values()),
        mode="lines+markers",
        yaxis="y",
    )
    figure.update_layout(
        title="Pops by Type",
        barmode="stack",
        xaxis_title="In-game date",
        yaxis_title="Pops",
        legend_title="Series",
        margin={"l": 64, "r": 24, "t": 48, "b": 48},
    )
    return figure


def _line_figure(
    title: str,
    rows: list[dict[str, Any]],
    columns: list[str],
    unit: str,
) -> go.Figure:
    figure = go.Figure()
    labels = {column: column.replace("_", " ").title() for column in columns}
    for column in columns:
        figure.add_scatter(
            name=labels[column],
            x=[row.get("date") for row in rows],
            y=[row.get(column) for row in rows],
            mode="lines+markers",
        )
    figure.update_layout(
        title=title,
        xaxis_title="In-game date",
        yaxis_title=unit,
        legend_title="Series",
        margin={"l": 64, "r": 24, "t": 48, "b": 48},
    )
    return figure


def _tax_figure(rows: list[dict[str, Any]]) -> go.Figure:
    figure = go.Figure()
    for column, label in [
        ("collected_tax", "Collected Tax"),
        ("uncollected_tax", "Uncollected Tax"),
    ]:
        figure.add_scatter(
            name=label,
            x=[row.get("date") for row in rows],
            y=[row.get(column) for row in rows],
            mode="lines",
            stackgroup="tax",
        )
    figure.update_layout(
        title="Tax",
        xaxis_title="In-game date",
        yaxis_title="Gold",
        legend_title="Series",
        margin={"l": 64, "r": 24, "t": 48, "b": 48},
    )
    return figure


def _food_figure(rows: list[dict[str, Any]]) -> go.Figure:
    figure = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        subplot_titles=("Stockpile and Capacity", "Supply, Demand, and Balance"),
    )
    for column, label in [("food", "Food"), ("food_max", "Capacity")]:
        figure.add_trace(
            go.Scatter(
                name=label,
                x=[row.get("date") for row in rows],
                y=[row.get(column) for row in rows],
                mode="lines+markers",
            ),
            row=1,
            col=1,
        )
    for column, label in [
        ("food_supply", "Supply"),
        ("food_consumption", "Demand"),
        ("food_balance", "Balance"),
    ]:
        figure.add_trace(
            go.Scatter(
                name=label,
                x=[row.get("date") for row in rows],
                y=[row.get(column) for row in rows],
                mode="lines+markers",
            ),
            row=2,
            col=1,
        )
    figure.update_yaxes(title_text="Food", row=1, col=1)
    figure.update_yaxes(title_text="Food/month", row=2, col=1)
    figure.update_xaxes(title_text="In-game date", row=2, col=1)
    figure.update_layout(
        title="Food",
        legend_title="Series",
        margin={"l": 64, "r": 24, "t": 72, "b": 48},
    )
    return figure


def _explorer_figure(result: DashboardQueryResult) -> go.Figure:
    figure = go.Figure()
    if not result.rows:
        figure.update_layout(
            title="Explorer",
            xaxis_title="In-game date",
            yaxis_title=result.metric.get("unit", ""),
        )
        return figure
    by_entity: dict[str, list[dict[str, Any]]] = {}
    for row in result.rows:
        by_entity.setdefault(str(row["entity_label"]), []).append(row)
    for label, rows in by_entity.items():
        ordered = sorted(rows, key=lambda row: row.get("date_sort") or 0)
        figure.add_scatter(
            name=label,
            x=[row.get("date") for row in ordered],
            y=[row.get("value") for row in ordered],
            mode="lines+markers",
        )
    figure.update_layout(
        title=f"{result.metric['label']} by {result.dimension['label']}",
        xaxis_title="In-game date",
        yaxis_title=result.metric.get("unit", ""),
        legend_title=result.dimension["label"],
        margin={"l": 64, "r": 24, "t": 48, "b": 48},
    )
    return figure


def _template_panel_figure(result: TemplateQueryResult, panel_key: str) -> go.Figure:
    panel = result.panels.get(panel_key, {})
    figure = go.Figure()
    rows = panel.get("rows") or []
    if not rows:
        message = result.empty_message or "No rows for this metric and scope."
        figure.update_layout(
            title=panel.get("title") or "No Data",
            xaxis_title="In-game date",
            yaxis_title=result.metric.get("unit", ""),
            annotations=[
                {
                    "text": message,
                    "xref": "paper",
                    "yref": "paper",
                    "x": 0.5,
                    "y": 0.5,
                    "showarrow": False,
                }
            ],
        )
        return figure
    by_entity: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_entity.setdefault(str(row["entity_label"]), []).append(row)
    for label, series in by_entity.items():
        ordered = sorted(series, key=lambda row: row.get("date_sort") or 0)
        figure.add_scatter(
            name=label,
            x=[row.get("date") for row in ordered],
            y=[row.get("value") for row in ordered],
            mode="lines+markers",
        )
    figure.update_layout(
        title=f"{panel['title']} - {result.metric['label']} by {result.scope['label']}",
        xaxis_title="In-game date",
        yaxis_title=result.metric.get("unit", ""),
        legend_title=result.scope["label"],
        margin={"l": 64, "r": 24, "t": 56, "b": 48},
    )
    return figure


def _building_card(row: dict[str, Any]) -> html.Div:
    icon = (
        html.Img(src=row["icon_url"], className="building-icon")
        if row.get("icon_url")
        else html.Div("?", className="building-icon missing")
    )
    details = [
        f"Category: {row.get('category') or 'unknown'}",
        f"Cost: {_format_number(row.get('effective_price_gold'))} gold"
        if row.get("effective_price_gold") is not None
        else "Cost: unresolved",
        f"Pop: {row.get('pop_type') or 'unknown'}",
        f"Employment: {_format_number(row.get('employment_size'))}",
    ]
    return html.Div(
        [
            icon,
            html.Div(
                [
                    html.Div(row.get("name"), className="building-name"),
                    html.Div(" | ".join(details), className="building-details"),
                ]
            ),
        ],
        className="building-card",
    )


def _latest_overview_values(overview: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for row in overview.get("employment", [])[-1:]:
        values.update(row)
    for row in overview.get("development", [])[-1:]:
        values.update(row)
    for row in overview.get("tax", [])[-1:]:
        values.update(row)
    for row in overview.get("food", [])[-1:]:
        values.update(row)
    return values


def _option(row: dict[str, Any]) -> dict[str, str]:
    return {"label": str(row["label"]), "value": str(row.get("value") or row.get("key"))}


def _preserved_dropdown_value(
    options: list[dict[str, Any]],
    current: Any,
    *,
    default: Any,
) -> Any:
    values = {item.get("value") for item in options}
    return current if current in values else default


def _template_metric(metadata: dict[str, Any], key: str) -> dict[str, Any]:
    metrics = list(metadata.get("metrics") or [])
    for metric in metrics:
        if metric["key"] == key:
            return metric
    if metrics:
        return metrics[0]
    return {
        "key": "population:pops",
        "domain": "population",
        "metric": "pops",
        "label": "Pops",
        "unit": "pops",
        "formatter": "number",
        "validScopes": ["super_region"],
        "defaultScope": "super_region",
    }


def _metric_options_for_domain(metadata: dict[str, Any], domain: str) -> list[dict[str, str]]:
    return [
        {"label": str(metric["label"]), "value": str(metric["key"])}
        for metric in metadata["metrics"]
        if metric["domain"] == domain
    ]


def _scope_group_for(metadata: dict[str, Any], scope_key: str) -> str:
    for scope in metadata["scopes"]:
        if scope["key"] == scope_key:
            return str(scope["group"])
    return "Geography"


def _scope_group_options(
    metadata: dict[str, Any],
    metric: dict[str, Any],
) -> list[dict[str, str]]:
    scopes = {scope["key"]: scope for scope in metadata["scopes"]}
    seen: set[str] = set()
    options: list[dict[str, str]] = []
    for key in metric["validScopes"]:
        scope = scopes.get(key)
        if scope is None:
            continue
        group = str(scope["group"])
        if group in seen:
            continue
        seen.add(group)
        options.append({"label": group, "value": group})
    return options


def _group_by_options(
    metadata: dict[str, Any],
    metric: dict[str, Any],
    scope_group: str,
) -> list[dict[str, str]]:
    scopes = {scope["key"]: scope for scope in metadata["scopes"]}
    options = []
    for key in metric["validScopes"]:
        scope = scopes.get(key)
        if scope is None or scope["group"] != scope_group:
            continue
        options.append({"label": str(scope["label"]), "value": str(scope["key"])})
    return options


def _active_filter_keys(metadata: dict[str, Any], metric: dict[str, Any]) -> set[str]:
    domain = str(metric["domain"])
    return {
        str(item["key"])
        for item in metadata["filters"]
        if domain in set(item.get("domains") or [])
    }


def _filters_from_values(**values: str | None) -> dict[str, str]:
    return {key: str(value) for key, value in values.items() if value not in {None, ""}}


def _template_scope_options(
    metadata: dict[str, Any],
    metric: dict[str, Any],
) -> list[dict[str, str]]:
    scopes = {scope["key"]: scope for scope in metadata["scopes"]}
    options = []
    for key in metric["validScopes"]:
        scope = scopes.get(key)
        if scope is None:
            continue
        options.append({"label": f"{scope['group']} / {scope['label']}", "value": scope["key"]})
    return options


def _format_number(value: Any) -> str:
    if value is None:
        return "-"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    if abs(number) >= 1_000_000:
        return f"{number / 1_000_000:.2f}M"
    if abs(number) >= 1_000:
        return f"{number:,.0f}"
    if number.is_integer():
        return f"{number:.0f}"
    return f"{number:.2f}"


if __name__ == "__main__":
    main()
