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

def _node_size(occurrence_count: int, max_count: int) -> int:
    """Scale node diameter between 40px and 120px based on occurrence_count."""
    if max_count == 0:
        return 60
    ratio = math.sqrt(occurrence_count / max_count)
    return int(40 + ratio * 80)


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

    max_count = int(nodes_df["occurrence_count"].max())
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
        size = _node_size(int(row["occurrence_count"]), max_count)

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
                "size": size,
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
CYTOSCAPE_STYLE = [
    {
        "selector": "node",
        "style": {
            "label": "data(label)",
            "width": "data(size)",
            "height": "data(size)",
            "background-color": "data(color)",
            "color": "#FFFFFF",
            "font-size": "10px",
            "text-wrap": "wrap",
            "text-max-width": "80px",
            "text-valign": "center",
            "text-halign": "center",
            "border-width": 2,
            "border-color": "#FFFFFF",
            "font-weight": "bold",
        },
    },
    {
        "selector": "node[?is_start]",
        "style": {
            "border-width": 4,
            "border-color": "#00E676",
        },
    },
    {
        "selector": "node[?is_end]",
        "style": {
            "border-width": 4,
            "border-style": "double",
            "border-color": "#FF6D00",
        },
    },
    {
        "selector": "node:selected",
        "style": {
            "border-width": 5,
            "border-color": "#FFD600",
            "overlay-color": "#FFD600",
            "overlay-padding": 4,
            "overlay-opacity": 0.2,
        },
    },
    {
        "selector": "edge",
        "style": {
            "label": "data(label)",
            "width": "data(width)",
            "line-color": "#90A4AE",
            "target-arrow-color": "#90A4AE",
            "target-arrow-shape": "vee",
            "curve-style": "bezier",
            "font-size": "9px",
            "color": "#37474F",
            "text-background-color": "#FFFFFF",
            "text-background-opacity": 0.7,
            "text-background-padding": "2px",
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
)
server = app.server  # expose WSGI server for gunicorn

app.layout = html.Div(
    style={"fontFamily": "Segoe UI, sans-serif", "backgroundColor": "#ECEFF1",
           "height": "100vh", "display": "flex", "flexDirection": "column"},
    children=[

        # ── Title bar ─────────────────────────────────────────────────────────
        html.Div(
            style={
                "backgroundColor": "#1C2526",
                "color": "#FFFFFF",
                "padding": "10px 20px",
                "display": "flex",
                "alignItems": "center",
                "justifyContent": "space-between",
                "flexShrink": 0,
            },
            children=[
                html.Div([
                    html.H2("POS Transaction Process Mining",
                            style={"margin": 0, "fontSize": "18px"}),
                    html.Span(f"{CATALOG}.{SCHEMA}",
                              style={"fontSize": "12px", "color": "#90A4AE"}),
                ]),
                html.Button("↻ Refresh Data", id="refresh-btn",
                            style={
                                "backgroundColor": "#FF3621",
                                "color": "#FFF",
                                "border": "none",
                                "borderRadius": "4px",
                                "padding": "6px 16px",
                                "cursor": "pointer",
                                "fontSize": "13px",
                            }),
            ],
        ),

        # ── Status banner (hidden when idle) ──────────────────────────────────
        html.Div(id="status-banner", style=BANNER_HIDDEN),

        # ── Main content area ─────────────────────────────────────────────────
        html.Div(
            style={"display": "flex", "flex": 1, "overflow": "hidden"},
            children=[

                # ── Graph panel ───────────────────────────────────────────────
                html.Div(
                    style={"flex": 1, "position": "relative", "padding": "8px",
                           "display": "flex", "flexDirection": "column"},
                    children=[

                        # Controls bar
                        html.Div(
                            style={
                                "backgroundColor": "#FFFFFF",
                                "borderRadius": "6px",
                                "padding": "8px 16px",
                                "marginBottom": "8px",
                                "display": "flex",
                                "alignItems": "center",
                                "gap": "20px",
                                "flexWrap": "wrap",
                                "boxShadow": "0 1px 3px rgba(0,0,0,0.15)",
                                "flexShrink": 0,
                            },
                            children=[
                                html.Div([
                                    html.Label("Min Edge Frequency",
                                               style={"fontSize": "12px", "marginRight": "8px"}),
                                    html.Div(
                                        dcc.Slider(
                                            id="freq-slider",
                                            min=0, max=100, step=5, value=0,
                                            marks={0: "0", 25: "25", 50: "50",
                                                   75: "75", 100: "100"},
                                            tooltip={"placement": "bottom"},
                                        ),
                                        style={"width": "200px"},
                                    ),
                                ], style={"display": "flex", "alignItems": "center"}),

                                html.Div([
                                    html.Label("Layout",
                                               style={"fontSize": "12px", "marginRight": "8px"}),
                                    dcc.Dropdown(
                                        id="layout-dropdown",
                                        options=[
                                            {"label": "Dagre (left→right)", "value": "dagre"},
                                            {"label": "Breadthfirst", "value": "breadthfirst"},
                                            {"label": "Circle", "value": "circle"},
                                            {"label": "Cose", "value": "cose"},
                                        ],
                                        value="dagre",
                                        clearable=False,
                                        style={"width": "180px", "fontSize": "12px"},
                                    ),
                                ], style={"display": "flex", "alignItems": "center"}),

                                # Legend
                                html.Div(
                                    [
                                        html.Span("Legend: ",
                                                  style={"fontSize": "12px",
                                                         "fontWeight": "bold"}),
                                        *[
                                            html.Span(
                                                cat.replace("_", " ").title(),
                                                style={
                                                    "backgroundColor": color,
                                                    "color": "#FFF",
                                                    "padding": "2px 8px",
                                                    "borderRadius": "3px",
                                                    "fontSize": "11px",
                                                    "marginLeft": "4px",
                                                },
                                            )
                                            for cat, color in CATEGORY_COLORS.items()
                                        ],
                                        html.Span(" ● Start",
                                                  style={"fontSize": "11px",
                                                         "color": "#00C853",
                                                         "marginLeft": "8px"}),
                                        html.Span(" ● End",
                                                  style={"fontSize": "11px",
                                                         "color": "#FF6D00",
                                                         "marginLeft": "4px"}),
                                    ],
                                    style={"display": "flex", "alignItems": "center",
                                           "flexWrap": "wrap"},
                                ),
                            ],
                        ),

                        # Cytoscape graph — fills remaining height
                        dcc.Loading(
                            id="graph-loading",
                            type="circle",
                            color="#FF3621",
                            children=cyto.Cytoscape(
                                id="dfg-graph",
                                elements=[],
                                style={
                                    "width": "100%",
                                    "flex": 1,
                                    "minHeight": "400px",
                                    "backgroundColor": "#FAFAFA",
                                    "borderRadius": "6px",
                                    "boxShadow": "0 1px 3px rgba(0,0,0,0.15)",
                                },
                                layout={"name": "dagre", "rankDir": "LR",
                                        "nodeSep": 60, "rankSep": 120},
                                stylesheet=CYTOSCAPE_STYLE,
                                responsive=True,
                                minZoom=0.2,
                                maxZoom=3.0,
                            ),
                        ),
                    ],
                ),

                # ── Sidebar: detail panel ─────────────────────────────────────
                html.Div(
                    id="detail-panel",
                    style={
                        "width": "300px",
                        "backgroundColor": "#FFFFFF",
                        "padding": "16px",
                        "overflowY": "auto",
                        "boxShadow": "-2px 0 6px rgba(0,0,0,0.1)",
                        "flexShrink": 0,
                    },
                    children=[
                        html.H3("Details", style={"marginTop": 0, "fontSize": "15px",
                                                  "color": "#37474F"}),
                        html.Div(id="detail-content",
                                 children=[
                                     html.P("Click a node or edge to inspect details.",
                                            style={"fontSize": "13px", "color": "#90A4AE"}),
                                 ]),
                    ],
                ),
            ],
        ),

        # ── Hidden stores ─────────────────────────────────────────────────────
        dcc.Store(id="nodes-store"),
        dcc.Store(id="edges-store"),

        # Fires once, 500 ms after page render — triggers the initial data load
        # without blocking the HTTP response that delivers the page HTML.
        dcc.Interval(id="load-interval", interval=500, max_intervals=1),
    ],
)


# ── Callbacks ─────────────────────────────────────────────────────────────────

@callback(
    Output("nodes-store", "data"),
    Output("edges-store", "data"),
    Output("freq-slider", "max"),
    Output("status-banner", "children"),
    Output("status-banner", "style"),
    # load-interval fires once at t+500ms; refresh-btn fires on click.
    # prevent_initial_call=True ensures neither fires during the initial HTTP
    # GET, so the page HTML is returned immediately.
    Input("load-interval", "n_intervals"),
    Input("refresh-btn", "n_clicks"),
    prevent_initial_call=True,
)
def load_data(_n_intervals, _n_clicks):
    """Load DFG data asynchronously after the page has already been served."""
    try:
        nodes_df, edges_df = load_dfg_data()
        max_freq = int(edges_df["edge_frequency"].max()) if not edges_df.empty else 100
        slider_max = max(100, (max_freq // 50 + 1) * 50)
        n_cases = int(nodes_df["case_count"].max()) if not nodes_df.empty else 0
        banner_text = (
            f"Loaded {len(nodes_df)} activities · {len(edges_df)} edges · "
            f"{n_cases:,} cases"
        )
        return (
            nodes_df.to_json(orient="records"),
            edges_df.to_json(orient="records"),
            slider_max,
            banner_text,
            BANNER_OK,
        )
    except Exception as exc:
        print(f"[ERROR] Failed to load DFG data: {exc}")
        return "[]", "[]", 100, f"Error loading data: {exc}", BANNER_ERROR


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
