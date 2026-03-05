"""
app.py — POS Transaction Process Mining — DFG Visualization
============================================================
Dash application that queries gold_dfg_edges and gold_dfg_nodes from Unity
Catalog and renders an interactive Direct-Follow Graph using dash-cytoscape.

Environment variables (required at runtime):
  DATABRICKS_HOST   — workspace URL, e.g. https://adb-1234.azuredatabricks.net
  DATABRICKS_TOKEN  — personal access token or OAuth token (auto-injected by Apps runtime)
  WAREHOUSE_ID      — SQL warehouse ID for queries
  CATALOG           — Unity Catalog name (default: pos_demo)
  SCHEMA            — schema name (default: pos_mining)

Timeout strategy
----------------
The initial HTTP GET returns immediately (no blocking calls at import or layout
time). A dcc.Interval fires 500 ms after the browser renders the page, which
triggers the SQL load asynchronously. This prevents upstream proxy timeouts
caused by slow SQL warehouse warm-up.
"""

import os
import math
import logging
import pandas as pd
import dash
from dash import html, dcc, Input, Output, callback
import dash_cytoscape as cyto
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState, Disposition, Format

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Load extended Cytoscape layouts ──────────────────────────────────────────
cyto.load_extra_layouts()

# ── Configuration ─────────────────────────────────────────────────────────────
# DATABRICKS_HOST and DATABRICKS_TOKEN are injected automatically by the
# Databricks Apps runtime.  WorkspaceClient() picks them up via its standard
# credential chain — no explicit token handling required.
WAREHOUSE_ID = os.environ.get("WAREHOUSE_ID", "")
CATALOG = os.environ.get("CATALOG", "pos_demo")
SCHEMA = os.environ.get("SCHEMA", "pos_mining")

# Lazy-initialised — not created at import time so the page HTML is served
# immediately even before the SDK resolves credentials.
_ws_client: WorkspaceClient | None = None


def _get_client() -> WorkspaceClient:
    global _ws_client
    if _ws_client is None:
        _ws_client = WorkspaceClient()
    return _ws_client


# ── Activity category → color mapping ────────────────────────────────────────
CATEGORY_COLORS = {
    "TRANSACTION_LIFECYCLE": "#1565C0",  # deep blue
    "ITEM_SCAN":             "#2E7D32",  # deep green
    "INCIDENT":              "#C62828",  # deep red
    "PAYMENT":               "#6A1B9A",  # deep purple
}
DEFAULT_COLOR = "#455A64"  # blue-grey for unknown


# ── Data access via SDK Statement Execution API ───────────────────────────────

def _query_df(sql: str) -> pd.DataFrame:
    """
    Execute SQL against a Databricks SQL Warehouse using the SDK's Statement
    Execution API.  The SDK uses the standard credential chain (DATABRICKS_HOST
    + DATABRICKS_TOKEN env vars in Apps, PAT locally) — no OAuth browser flow
    is ever initiated.
    """
    ws = _get_client()
    response = ws.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=sql,
        wait_timeout="50s",          # wait up to 50 s inline before polling
        disposition=Disposition.INLINE,
        format=Format.JSON_ARRAY,
    )

    if response.status.state != StatementState.SUCCEEDED:
        err = response.status.error
        raise RuntimeError(
            f"Statement failed ({response.status.state}): "
            f"{err.error_code if err else 'unknown'} — "
            f"{err.message if err else ''}"
        )

    cols = [c.name for c in (response.manifest.schema.columns or [])]
    rows = response.result.data_array or []
    # data_array rows are lists of string-encoded values; cast via DataFrame
    return pd.DataFrame(rows, columns=cols)


def load_dfg_data():
    """
    Fetch nodes and edges from Unity Catalog Gold tables.

    The Statement Execution API returns all values as strings in JSON_ARRAY
    format.  Cast numeric columns explicitly so downstream math (max, sqrt,
    etc.) works correctly.
    """
    logger.info("Loading DFG data from Unity Catalog...")

    nodes_df = _query_df(f"""
        SELECT
            activity_name,
            occurrence_count,
            case_count,
            start_count,
            end_count,
            COALESCE(avg_activity_duration_ms, 0) AS avg_activity_duration_ms,
            activity_category
        FROM {CATALOG}.{SCHEMA}.gold_dfg_nodes
        ORDER BY occurrence_count DESC
    """)
    for col in ("occurrence_count", "case_count", "start_count",
                "end_count", "avg_activity_duration_ms"):
        nodes_df[col] = pd.to_numeric(nodes_df[col], errors="coerce").fillna(0)

    edges_df = _query_df(f"""
        SELECT
            source_activity,
            target_activity,
            edge_frequency,
            case_count,
            avg_transition_duration_ms,
            min_transition_duration_ms,
            max_transition_duration_ms,
            p50_transition_duration_ms,
            p95_transition_duration_ms
        FROM {CATALOG}.{SCHEMA}.gold_dfg_edges
        ORDER BY edge_frequency DESC
    """)
    for col in ("edge_frequency", "case_count", "avg_transition_duration_ms",
                "min_transition_duration_ms", "max_transition_duration_ms",
                "p50_transition_duration_ms", "p95_transition_duration_ms"):
        edges_df[col] = pd.to_numeric(edges_df[col], errors="coerce").fillna(0)

    logger.info(
        "Loaded %d nodes, %d edges", len(nodes_df), len(edges_df)
    )
    return nodes_df, edges_df


# ── Cytoscape element builder ─────────────────────────────────────────────────

def _edge_width(frequency: int, max_freq: int) -> float:
    """Scale edge width between 1px and 12px based on frequency."""
    if max_freq == 0:
        return 2
    ratio = math.sqrt(frequency / max_freq)
    return round(1 + ratio * 11, 1)


def _format_ms(ms) -> str:
    if ms is None or (isinstance(ms, float) and math.isnan(ms)):
        return "—"
    s = ms / 1000
    if s < 60:
        return f"{s:.1f}s"
    m, sec = divmod(int(s), 60)
    return f"{m}m {sec}s"


def build_cytoscape_elements(nodes_df: pd.DataFrame, edges_df: pd.DataFrame,
                              min_freq: int = 0):
    """Build the list of Cytoscape node + edge element dicts."""
    elements = []

    if nodes_df.empty:
        return elements

    max_freq = int(edges_df["edge_frequency"].max()) if not edges_df.empty else 1

    filtered_edges = edges_df[edges_df["edge_frequency"] >= min_freq]
    active_activities = set(filtered_edges["source_activity"]).union(
        filtered_edges["target_activity"]
    )

    # Always include start/end nodes even if they have no surviving edges
    for _, row in nodes_df.iterrows():
        if row["start_count"] > 0 or row["end_count"] > 0:
            active_activities.add(row["activity_name"])

    for _, row in nodes_df.iterrows():
        name = row["activity_name"]
        if name not in active_activities:
            continue

        category = row.get("activity_category", "")
        color = CATEGORY_COLORS.get(category, DEFAULT_COLOR)

        elements.append({
            "data": {
                "id": name,
                "label": name.replace("_", " ").title(),
                "activity_name": name,
                "occurrence_count": int(row["occurrence_count"]),
                "case_count": int(row["case_count"]),
                "start_count": int(row["start_count"]),
                "end_count": int(row["end_count"]),
                "avg_duration_ms": float(row.get("avg_activity_duration_ms", 0) or 0),
                "category": category,
                "color": color,
                "is_start": row["start_count"] > 0,
                "is_end": row["end_count"] > 0,
            }
        })

    for _, row in filtered_edges.iterrows():
        freq = int(row["edge_frequency"])
        width = _edge_width(freq, max_freq)
        elements.append({
            "data": {
                "id": f"{row['source_activity']}→{row['target_activity']}",
                "source": row["source_activity"],
                "target": row["target_activity"],
                "label": f"{freq:,}",
                "edge_frequency": freq,
                "case_count": int(row["case_count"]),
                "avg_ms": float(row.get("avg_transition_duration_ms") or 0),
                "p50_ms": float(row.get("p50_transition_duration_ms") or 0),
                "p95_ms": float(row.get("p95_transition_duration_ms") or 0),
                "width": width,
            }
        })

    return elements


# ── Stylesheet ────────────────────────────────────────────────────────────────
# Nodes: fixed-size round-rectangles, light-gray fill, category-colored border.
# Edges: taxi (orthogonal elbow) routing.
NODE_W = 160   # px — wide enough for longest wrapped activity label
NODE_H = 60    # px — tall enough for 3 lines at 11px

CYTOSCAPE_STYLE = [
    {
        "selector": "node",
        "style": {
            "label": "data(label)",
            "shape": "round-rectangle",
            "width": NODE_W,
            "height": NODE_H,
            "background-color": "#F5F5F5",   # light gray fill
            "border-color": "data(color)",    # category color on the outline
            "border-width": 3,
            "color": "#2D3748",               # dark text for contrast on gray
            "font-size": "11px",
            "font-family": "Segoe UI, system-ui, sans-serif",
            "text-wrap": "wrap",
            "text-max-width": f"{NODE_W - 16}px",
            "text-valign": "center",
            "text-halign": "center",
            "font-weight": "600",
        },
    },
    # Start nodes: thicker green border
    {
        "selector": "node[?is_start]",
        "style": {
            "border-width": 4,
            "border-color": "#00897B",
        },
    },
    # End nodes: thicker orange border
    {
        "selector": "node[?is_end]",
        "style": {
            "border-width": 4,
            "border-color": "#EF6C00",
        },
    },
    # Selected: yellow highlight ring
    {
        "selector": "node:selected",
        "style": {
            "border-width": 5,
            "border-color": "#FFD600",
            "overlay-color": "#FFD600",
            "overlay-padding": 6,
            "overlay-opacity": 0.15,
        },
    },
    # Edges: orthogonal / elbow routing
    {
        "selector": "edge",
        "style": {
            "label": "data(label)",
            "width": "data(width)",
            "line-color": "#B0BEC5",
            "target-arrow-color": "#B0BEC5",
            "target-arrow-shape": "vee",
            "curve-style": "taxi",
            "taxi-direction": "horizontal",   # horizontal segment first (LR layout)
            "taxi-turn": "50%",               # elbow at midpoint between nodes
            "taxi-turn-min-distance": 20,
            "font-size": "9px",
            "color": "#546E7A",
            "text-background-color": "#FFFFFF",
            "text-background-opacity": 0.85,
            "text-background-padding": "2px",
            "text-rotation": "none",
        },
    },
    {
        "selector": "edge:selected",
        "style": {
            "line-color": "#FFD600",
            "target-arrow-color": "#FFD600",
            "font-weight": "bold",
        },
    },
]

# ── Status banner styles ──────────────────────────────────────────────────────
_BANNER_BASE = {
    "padding": "6px 16px",
    "borderRadius": "4px",
    "fontSize": "12px",
    "marginBottom": "6px",
    "flexShrink": 0,
}
BANNER_LOADING = {**_BANNER_BASE, "backgroundColor": "#E3F2FD", "color": "#1565C0"}
BANNER_OK      = {**_BANNER_BASE, "backgroundColor": "#E8F5E9", "color": "#2E7D32"}
BANNER_ERROR   = {**_BANNER_BASE, "backgroundColor": "#FFEBEE", "color": "#C62828"}
BANNER_HIDDEN  = {**_BANNER_BASE, "display": "none"}


# ── App layout ────────────────────────────────────────────────────────────────
app = dash.Dash(
    __name__,
    title="POS Transaction Process Mining",
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
    external_stylesheets=["https://cdn.jsdelivr.net/npm/bulma@0.9.4/css/bulma.min.css"],
)
server = app.server  # expose WSGI server for gunicorn

# ── App layout ────────────────────────────────────────────────────────────────
# Outer div: full-viewport background.
# Inner div: max 1000px, centered — all visible chrome lives here.
app.layout = html.Div(
    style={"backgroundColor": "#ECEFF1", "minHeight": "100vh",
           "fontFamily": "Segoe UI, system-ui, sans-serif"},
    children=[html.Div(
        style={"maxWidth": "1000px", "margin": "0 auto",
               "height": "100vh", "display": "flex", "flexDirection": "column"},
        children=[

            # ── Navbar (Bulma) ─────────────────────────────────────────────
            html.Nav(
                className="navbar is-dark",
                style={"flexShrink": 0, "paddingLeft": "1rem", "paddingRight": "1rem"},
                children=[
                    html.Div(className="navbar-brand", children=[
                        html.Div(className="navbar-item", children=[
                            html.Div([
                                html.P("POS Transaction Process Mining",
                                       className="title is-5 has-text-white",
                                       style={"margin": 0}),
                                html.P(f"{CATALOG}.{SCHEMA}",
                                       className="subtitle is-7 has-text-grey-light",
                                       style={"margin": 0}),
                            ]),
                        ]),
                    ]),
                    html.Div(className="navbar-end", children=[
                        html.Div(className="navbar-item", children=[
                            html.Button("↻ Refresh", id="refresh-btn",
                                        className="button is-danger is-small"),
                        ]),
                    ]),
                ],
            ),

            # ── Status notification (Bulma) ────────────────────────────────
            html.Div(id="status-banner", style=BANNER_HIDDEN),

            # ── Controls bar ───────────────────────────────────────────────
            html.Div(
                className="box",
                style={
                    "margin": "8px 0",
                    "padding": "0.6rem 1rem",
                    "display": "flex",
                    "alignItems": "center",
                    "gap": "24px",
                    "flexWrap": "wrap",
                    "flexShrink": 0,
                    "borderRadius": "6px",
                },
                children=[
                    # Frequency slider
                    html.Div(style={"display": "flex", "alignItems": "center", "gap": "10px"},
                             children=[
                                 html.Label("Min Frequency",
                                            className="label is-small",
                                            style={"marginBottom": 0, "whiteSpace": "nowrap"}),
                                 html.Div(
                                     dcc.Slider(
                                         id="freq-slider",
                                         min=0, max=1000, step=100, value=0,
                                         marks={0: "0", 500: "500", 1000: "1k"},
                                         tooltip={"placement": "bottom",
                                                  "always_visible": False},
                                         updatemode="mouseup",
                                     ),
                                     style={"width": "220px"},
                                 ),
                             ]),

                    # Layout picker
                    html.Div(style={"display": "flex", "alignItems": "center", "gap": "8px"},
                             children=[
                                 html.Label("Layout",
                                            className="label is-small",
                                            style={"marginBottom": 0}),
                                 dcc.Dropdown(
                                     id="layout-dropdown",
                                     options=[
                                         {"label": "Dagre LR", "value": "dagre"},
                                         {"label": "Breadthfirst", "value": "breadthfirst"},
                                         {"label": "Circle", "value": "circle"},
                                         {"label": "Cose", "value": "cose"},
                                     ],
                                     value="dagre",
                                     clearable=False,
                                     style={"width": "150px", "fontSize": "13px"},
                                 ),
                             ]),

                    # Legend (Bulma tags)
                    html.Div(
                        style={"display": "flex", "alignItems": "center",
                               "flexWrap": "wrap", "gap": "4px"},
                        children=[
                            html.Span("Legend:", className="label is-small",
                                      style={"marginBottom": 0, "marginRight": "4px"}),
                            *[
                                html.Span(
                                    cat.replace("_", " ").title(),
                                    className="tag",
                                    style={"backgroundColor": color,
                                           "color": "#FFF",
                                           "fontSize": "10px"},
                                )
                                for cat, color in CATEGORY_COLORS.items()
                            ],
                            html.Span("▶ Start", className="tag is-success is-light",
                                      style={"fontSize": "10px"}),
                            html.Span("■ End", className="tag is-warning is-light",
                                      style={"fontSize": "10px"}),
                        ],
                    ),
                ],
            ),

            # ── Graph + sidebar row ────────────────────────────────────────
            html.Div(
                style={"display": "flex", "flex": 1, "overflow": "hidden",
                       "gap": "8px", "paddingBottom": "8px"},
                children=[

                    # Graph area — concrete calc() height avoids the 0px
                    # resolved-height problem that occurs when height:"100%"
                    # is used inside dcc.Loading's anonymous wrapper divs.
                    # Breakdown: 100vh - navbar(52) - controls(78) - gaps(30) = ~210px
                    cyto.Cytoscape(
                        id="dfg-graph",
                        elements=[],
                        style={
                            "width": "100%",
                            "height": "calc(100vh - 210px)",
                            "minHeight": "400px",
                            "flex": 1,
                            "backgroundColor": "#FFFFFF",
                            "borderRadius": "6px",
                            "border": "1px solid #DEE2E6",
                        },
                        layout={"name": "dagre", "rankDir": "LR",
                                "nodeSep": 50, "rankSep": 100,
                                "edgeSep": 20},
                        stylesheet=CYTOSCAPE_STYLE,
                        responsive=True,
                        minZoom=0.15,
                        maxZoom=3.0,
                    ),

                    # Sidebar detail panel (Bulma card)
                    html.Div(
                        className="card",
                        style={"width": "240px", "flexShrink": 0,
                               "overflowY": "auto"},
                        children=[
                            html.Div(className="card-header", children=[
                                html.P("Details", className="card-header-title",
                                       style={"fontSize": "13px", "padding": "0.6rem 1rem"}),
                            ]),
                            html.Div(
                                className="card-content",
                                style={"padding": "0.75rem"},
                                children=[
                                    html.Div(id="detail-content", children=[
                                        html.P("Click a node or edge.",
                                               className="has-text-grey is-size-7"),
                                    ]),
                                ],
                            ),
                        ],
                    ),
                ],
            ),

            # ── Hidden stores + interval ───────────────────────────────────
            dcc.Store(id="nodes-store"),
            dcc.Store(id="edges-store"),
            dcc.Interval(id="load-interval", interval=500, max_intervals=1),
        ],
    )],
)


# ── Callbacks ─────────────────────────────────────────────────────────────────

def _slider_config(max_freq: int) -> tuple[int, int, dict]:
    """
    Return (slider_max, step, marks) scaled to the actual data range.

    Targets ~8 usable positions. Step is rounded to a 'nice' number so the
    slider doesn't land on awkward values.  Marks are placed at 0, 25%, 50%,
    75%, and 100% of the range.
    """
    if max_freq <= 0:
        return 100, 10, {0: "0", 100: "100"}

    # Round max up to next multiple of 10 for a clean endpoint
    slider_max = math.ceil(max_freq / 10) * 10

    # Step = ~1/8 of range, rounded to a nice number
    raw_step = slider_max / 8
    magnitude = 10 ** math.floor(math.log10(raw_step)) if raw_step >= 1 else 1
    step = max(1, round(raw_step / magnitude) * magnitude)

    pcts = [0, 0.25, 0.5, 0.75, 1.0]
    marks = {
        int(p * slider_max): (
            f"{int(p * slider_max):,}" if p * slider_max >= 1000
            else str(int(p * slider_max))
        )
        for p in pcts
    }
    return slider_max, step, marks


@callback(
    Output("nodes-store", "data"),
    Output("edges-store", "data"),
    Output("freq-slider", "max"),
    Output("freq-slider", "step"),
    Output("freq-slider", "marks"),
    Output("status-banner", "children"),
    Output("status-banner", "style"),
    # prevent_initial_call=True: neither input fires during the initial HTTP GET,
    # so the page HTML is returned immediately without waiting for SQL.
    Input("load-interval", "n_intervals"),
    Input("refresh-btn", "n_clicks"),
    prevent_initial_call=True,
)
def load_data(_n_intervals, _n_clicks):
    """Load DFG data asynchronously after the page has already been served."""
    try:
        nodes_df, edges_df = load_dfg_data()
        max_freq = int(edges_df["edge_frequency"].max()) if not edges_df.empty else 0
        slider_max, step, marks = _slider_config(max_freq)
        n_cases = int(nodes_df["case_count"].max()) if not nodes_df.empty else 0
        banner_text = (
            f"Loaded {len(nodes_df)} activities · {len(edges_df)} edges · "
            f"{n_cases:,} cases"
        )
        return (
            nodes_df.to_json(orient="records"),
            edges_df.to_json(orient="records"),
            slider_max,
            step,
            marks,
            banner_text,
            BANNER_OK,
        )
    except Exception as exc:
        logger.error("Failed to load DFG data: %s", exc)
        return "[]", "[]", 100, 10, {0: "0", 100: "100"}, f"Error: {exc}", BANNER_ERROR


@callback(
    Output("dfg-graph", "elements"),
    Output("dfg-graph", "layout"),
    Input("nodes-store", "data"),
    Input("edges-store", "data"),
    Input("freq-slider", "value"),
    Input("layout-dropdown", "value"),
)
def update_graph(nodes_json, edges_json, min_freq, layout_name):
    """Re-render the Cytoscape graph when data or filters change."""
    if not nodes_json or nodes_json == "[]":
        return [], {"name": "dagre"}

    nodes_df = pd.read_json(nodes_json, orient="records")
    edges_df = pd.read_json(edges_json, orient="records")

    elements = build_cytoscape_elements(nodes_df, edges_df, min_freq=min_freq or 0)

    layout_cfg = {"name": layout_name}
    if layout_name == "dagre":
        layout_cfg.update({"rankDir": "LR", "nodeSep": 60, "rankSep": 120})
    elif layout_name == "breadthfirst":
        layout_cfg.update({"directed": True, "spacingFactor": 1.4})

    return elements, layout_cfg


@callback(
    Output("detail-content", "children"),
    Input("dfg-graph", "tapNodeData"),
    Input("dfg-graph", "tapEdgeData"),
)
def show_detail(node_data, edge_data):
    """Populate the sidebar with details for the clicked node or edge."""
    ctx = dash.callback_context
    if not ctx.triggered:
        return html.P("Click a node or edge.",
                      style={"color": "#90A4AE", "fontSize": "13px"})

    trigger_id = ctx.triggered[0]["prop_id"]

    if "tapNodeData" in trigger_id and node_data:
        cat = node_data.get("category", "")
        color = CATEGORY_COLORS.get(cat, DEFAULT_COLOR)
        return [
            html.Div(
                node_data.get("activity_name", ""),
                style={
                    "backgroundColor": color,
                    "color": "#FFF",
                    "padding": "8px 12px",
                    "borderRadius": "4px",
                    "fontWeight": "bold",
                    "fontSize": "13px",
                    "marginBottom": "12px",
                    "wordBreak": "break-word",
                },
            ),
            _detail_row("Category", cat.replace("_", " ").title()),
            _detail_row("Occurrences", f"{node_data.get('occurrence_count', 0):,}"),
            _detail_row("Cases", f"{node_data.get('case_count', 0):,}"),
            _detail_row("Start of case", f"{node_data.get('start_count', 0):,}"),
            _detail_row("End of case", f"{node_data.get('end_count', 0):,}"),
            _detail_row(
                "Avg self-duration",
                _format_ms(node_data.get("avg_duration_ms", 0))
                if node_data.get("avg_duration_ms", 0) > 0 else "—",
            ),
        ]

    if "tapEdgeData" in trigger_id and edge_data:
        return [
            html.Div(
                f"{edge_data.get('source', '')} → {edge_data.get('target', '')}",
                style={
                    "backgroundColor": "#37474F",
                    "color": "#FFF",
                    "padding": "8px 12px",
                    "borderRadius": "4px",
                    "fontWeight": "bold",
                    "fontSize": "12px",
                    "marginBottom": "12px",
                    "wordBreak": "break-word",
                },
            ),
            _detail_row("Frequency", f"{edge_data.get('edge_frequency', 0):,}"),
            _detail_row("Cases", f"{edge_data.get('case_count', 0):,}"),
            _detail_row("Avg transition", _format_ms(edge_data.get("avg_ms"))),
            _detail_row("Median (p50)", _format_ms(edge_data.get("p50_ms"))),
            _detail_row("p95 transition", _format_ms(edge_data.get("p95_ms"))),
        ]

    return html.P("Click a node or edge.",
                  style={"color": "#90A4AE", "fontSize": "13px"})


def _detail_row(label: str, value: str):
    return html.Div(
        [
            html.Span(label + ":", style={"color": "#607D8B", "fontSize": "12px",
                                          "display": "inline-block", "width": "130px"}),
            html.Span(value, style={"fontWeight": "bold", "fontSize": "13px"}),
        ],
        style={"marginBottom": "6px"},
    )


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8000)
