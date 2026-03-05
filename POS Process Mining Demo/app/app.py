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

Selection highlighting
----------------------
Node selection is tracked via selectedNodeData (supports click-background-to-
deselect). Highlighting is applied by returning a dynamic stylesheet from a
callback rather than by modifying element classes — this avoids triggering a
layout re-run every time the user clicks a node.

Edge curve style
----------------
Uses curve-style: bezier with control-point-step-size to automatically
separate parallel and bidirectional edges. Cytoscape.js (canvas renderer)
does not support edge crossing bumps/bridges; bezier auto-separation is the
recommended alternative for crossing clarity.
"""

import os
import math
import logging
import pandas as pd
import dash
from dash import html, dcc, Input, Output, State, callback
import dash_cytoscape as cyto
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState, Disposition, Format

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Load extended Cytoscape layouts ──────────────────────────────────────────
cyto.load_extra_layouts()

# ── Configuration ─────────────────────────────────────────────────────────────
WAREHOUSE_ID = os.environ.get("WAREHOUSE_ID", "")
CATALOG = os.environ.get("CATALOG", "pos_demo")
SCHEMA = os.environ.get("SCHEMA", "pos_mining")

_ws_client: WorkspaceClient | None = None


def _get_client() -> WorkspaceClient:
    global _ws_client
    if _ws_client is None:
        _ws_client = WorkspaceClient()
    return _ws_client


# ── Activity category → color mapping ────────────────────────────────────────
CATEGORY_COLORS = {
    "TRANSACTION_LIFECYCLE": "#1565C0",
    "ITEM_SCAN":             "#2E7D32",
    "INCIDENT":              "#C62828",
    "PAYMENT":               "#6A1B9A",
}
DEFAULT_COLOR = "#455A64"


# ── Frequency slider — power-of-2 geometric sequence (20 steps) ──────────────
# [0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1k, 2k, 4k, 8k, 16k, 32k, 64k, 128k, 256k]
FREQ_STEPS: list[int] = [0] + [2**i for i in range(19)]


def _fmt_step(v: int) -> str:
    """Abbreviated label for a frequency step value."""
    if v >= 1_000_000:
        return f"{v // 1_000_000}M"
    if v >= 1_000:
        return f"{v // 1_000}k"
    return str(v)


# ── Data access via SDK Statement Execution API ───────────────────────────────

def _query_df(sql: str) -> pd.DataFrame:
    ws = _get_client()
    response = ws.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=sql,
        wait_timeout="50s",
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
    return pd.DataFrame(rows, columns=cols)


def load_dfg_data():
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

    logger.info("Loaded %d nodes, %d edges", len(nodes_df), len(edges_df))
    return nodes_df, edges_df


# ── Cytoscape element builder ─────────────────────────────────────────────────

def _edge_width(frequency: int, max_freq: int) -> float:
    if max_freq == 0:
        return 2
    ratio = math.sqrt(frequency / max_freq)
    return round(1 + ratio * 11, 1)


def _format_ms(ms) -> str:
    if ms is None or (isinstance(ms, float) and math.isnan(ms)):
        return "—"
    ms = float(ms)
    s = ms / 1000
    if s < 60:
        return f"{s:.1f}s"
    m, sec = divmod(int(s), 60)
    return f"{m}m {sec}s"


def build_cytoscape_elements(nodes_df: pd.DataFrame, edges_df: pd.DataFrame,
                              min_freq: int = 0):
    elements = []
    if nodes_df.empty:
        return elements

    max_freq = int(edges_df["edge_frequency"].max()) if not edges_df.empty else 1
    filtered_edges = edges_df[edges_df["edge_frequency"] >= min_freq]
    active_activities = set(filtered_edges["source_activity"]).union(
        filtered_edges["target_activity"]
    )
    for _, row in nodes_df.iterrows():
        if row["start_count"] > 0 or row["end_count"] > 0:
            active_activities.add(row["activity_name"])

    # Detect self-loop activities (edges where source == target)
    self_loop_set: set[str] = set()
    if not filtered_edges.empty:
        loop_mask = (
            filtered_edges["source_activity"] == filtered_edges["target_activity"]
        )
        self_loop_set = set(filtered_edges.loc[loop_mask, "source_activity"].unique())

    for _, row in nodes_df.iterrows():
        name = row["activity_name"]
        if name not in active_activities:
            continue
        category = row.get("activity_category", "")
        color = CATEGORY_COLORS.get(category, DEFAULT_COLOR)
        has_loop = name in self_loop_set
        # Append a circular arrow indicator to the label for self-loop activities
        label = name.replace("_", " ").title()
        if has_loop:
            label += "\n↺"
        elements.append({
            "data": {
                "id": name,
                "label": label,
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
                "has_self_loop": has_loop,
            }
        })

    for _, row in filtered_edges.iterrows():
        freq = int(row["edge_frequency"])
        width = _edge_width(freq, max_freq)
        is_loop = row["source_activity"] == row["target_activity"]
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
                "is_self_loop": is_loop,
            }
        })

    return elements


# ── Base stylesheet ───────────────────────────────────────────────────────────
NODE_W = 180
NODE_H = 60

BASE_STYLESHEET = [
    {
        "selector": "node",
        "style": {
            "label": "data(label)",
            "shape": "round-rectangle",
            "width": NODE_W,
            "height": NODE_H,
            "background-color": "#F5F5F5",
            "border-color": "data(color)",
            "border-width": 3,
            "color": "#2D3748",
            "font-size": "11px",
            "font-family": "Segoe UI, system-ui, sans-serif",
            "text-wrap": "wrap",
            "text-max-width": f"{NODE_W - 16}px",
            "text-valign": "center",
            "text-halign": "center",
            "font-weight": "600",
        },
    },
    {
        "selector": "node[?is_start]",
        "style": {"border-width": 4, "border-color": "#00897B"},
    },
    {
        "selector": "node[?is_end]",
        "style": {"border-width": 4, "border-color": "#EF6C00"},
    },
    {
        # Self-loop nodes: double border to signal the circular connection.
        # The ↺ character in the label provides a second indicator.
        "selector": "node[?has_self_loop]",
        "style": {
            "border-style": "double",
            "border-width": 7,
        },
    },
    {
        "selector": "edge",
        "style": {
            "label": "data(label)",
            "width": "data(width)",
            "line-color": "#B0BEC5",
            "target-arrow-color": "#B0BEC5",
            "target-arrow-shape": "vee",
            # bezier auto-offsets parallel and bidirectional edge pairs so they
            # don't overlap. control-point-step-size controls the spread.
            # Note: Cytoscape.js canvas renderer does not support crossing
            # bumps/bridges — bezier separation achieves the same visual clarity.
            "curve-style": "bezier",
            "control-point-step-size": 40,
            "font-size": "9px",
            "color": "#546E7A",
            "text-background-color": "#FFFFFF",
            "text-background-opacity": 0.85,
            "text-background-padding": "2px",
            "text-rotation": "none",
        },
    },
    {
        # Self-loop edges: orange dashed style for visual distinction.
        # Cytoscape.js renders source==target edges as a loop automatically.
        "selector": "edge[?is_self_loop]",
        "style": {
            "line-color": "#EF6C00",
            "target-arrow-color": "#EF6C00",
            "line-style": "dashed",
            "loop-direction": "-45deg",
            "loop-sweep": "45deg",
        },
    },
]


def _compute_stylesheet(selected_node_id: str | None,
                        edges_json: str | None) -> list:
    """
    Extend BASE_STYLESHEET with highlight rules for the selected node.

    Uses Cytoscape data attribute selectors ([source = '...']) and #id selectors
    rather than element classes — this lets us update only the stylesheet prop
    (no element rebuild) so the layout stays stable while the user clicks around.
    """
    stylesheet = list(BASE_STYLESHEET)
    if not selected_node_id or not edges_json or edges_json == "[]":
        return stylesheet

    try:
        edges_df = pd.read_json(edges_json, orient="records")
    except Exception:
        return stylesheet

    outgoing = edges_df[edges_df["source_activity"] == selected_node_id]
    neighbor_ids = list(outgoing["target_activity"].unique())
    non_dimmed = list(dict.fromkeys([selected_node_id] + neighbor_ids))

    # Dim all nodes that are neither the selected node nor a neighbor.
    not_chain = "".join(f":not(#{nid})" for nid in non_dimmed)
    stylesheet.append({
        "selector": f"node{not_chain}",
        "style": {"opacity": 0.18},
    })

    # Dim all edges except outgoing ones from the selected node
    stylesheet.append({
        "selector": f"edge[source != '{selected_node_id}']",
        "style": {"opacity": 0.07},
    })

    # Highlight outgoing edges (including self-loop if present)
    stylesheet.append({
        "selector": f"edge[source = '{selected_node_id}']",
        "style": {
            "line-color": "#FFB300",
            "target-arrow-color": "#FFB300",
            "line-style": "solid",
            "opacity": 1.0,
        },
    })

    # Highlight neighbor nodes (targets of outgoing edges)
    for nid in neighbor_ids:
        if nid == selected_node_id:
            continue  # skip self-loop — selected node style wins below
        stylesheet.append({
            "selector": f"#{nid}",
            "style": {
                "border-color": "#FF6F00",
                "border-width": 5,
                "background-color": "#FFF3E0",
                "opacity": 1.0,
            },
        })

    # Highlight selected node last so it wins over neighbor styles
    stylesheet.append({
        "selector": f"#{selected_node_id}",
        "style": {
            "border-color": "#FFD600",
            "border-width": 6,
            "background-color": "#FFFDE7",
            "opacity": 1.0,
        },
    })

    return stylesheet


# ── Status banner styles ──────────────────────────────────────────────────────
_BANNER_BASE = {
    "padding": "5px 14px",
    "borderRadius": "4px",
    "fontSize": "12px",
    "margin": "6px 0",
}
BANNER_OK     = {**_BANNER_BASE, "backgroundColor": "#E8F5E9", "color": "#2E7D32"}
BANNER_ERROR  = {**_BANNER_BASE, "backgroundColor": "#FFEBEE", "color": "#C62828"}
BANNER_HIDDEN = {**_BANNER_BASE, "display": "none"}


# ── Helper UI components ───────────────────────────────────────────────────────

def _stat_chip(label: str, value: str):
    """Compact vertical label + value chip for the details bar."""
    return html.Div(
        style={
            "display": "flex",
            "flexDirection": "column",
            "paddingLeft": "12px",
            "borderLeft": "2px solid #E0E0E0",
        },
        children=[
            html.Span(label, style={"fontSize": "10px", "color": "#90A4AE",
                                    "lineHeight": "1.3", "whiteSpace": "nowrap"}),
            html.Span(value, style={"fontSize": "13px", "fontWeight": "bold",
                                    "color": "#2D3748", "lineHeight": "1.4",
                                    "whiteSpace": "nowrap"}),
        ],
    )


# ── Initial slider marks (shown before data loads) ────────────────────────────
_INIT_MARKS = {
    i: {
        "label": _fmt_step(FREQ_STEPS[i]) if i % 2 == 0 else " ",
        "style": {"fontSize": "10px"},
    }
    for i in range(20)
}

# ── App layout ────────────────────────────────────────────────────────────────
app = dash.Dash(
    __name__,
    title="POS Transaction Process Mining",
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
    external_stylesheets=["https://cdn.jsdelivr.net/npm/bulma@0.9.4/css/bulma.min.css"],
)
server = app.server

app.layout = html.Div(
    style={"backgroundColor": "#ECEFF1", "minHeight": "100vh",
           "fontFamily": "Segoe UI, system-ui, sans-serif"},
    children=[html.Div(
        style={"maxWidth": "1200px", "margin": "0 auto", "paddingBottom": "1.5rem"},
        children=[

            # ── Navbar ─────────────────────────────────────────────────────
            html.Nav(
                className="navbar is-dark",
                style={"paddingLeft": "1rem", "paddingRight": "1rem"},
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

            # ── Status banner ───────────────────────────────────────────────
            html.Div(id="status-banner", style=BANNER_HIDDEN),

            # ── Controls bar ────────────────────────────────────────────────
            html.Div(
                className="box",
                style={
                    "margin": "8px 0",
                    "padding": "0.6rem 1rem",
                    "display": "flex",
                    "alignItems": "center",
                    "gap": "24px",
                    "flexWrap": "wrap",
                    "borderRadius": "6px",
                },
                children=[
                    html.Div(
                        style={"display": "flex", "alignItems": "center", "gap": "10px"},
                        children=[
                            html.Label("Min Frequency", className="label is-small",
                                       style={"marginBottom": 0, "whiteSpace": "nowrap"}),
                            html.Div(
                                dcc.Slider(
                                    id="freq-slider",
                                    # Slider uses indices 0–19 that map to FREQ_STEPS.
                                    # Equal visual spacing between geometric steps.
                                    min=0, max=19, step=1, value=0,
                                    marks=_INIT_MARKS,
                                    tooltip={"placement": "bottom", "always_visible": False},
                                    updatemode="mouseup",
                                ),
                                style={"width": "300px"},
                            ),
                        ],
                    ),
                    html.Div(
                        style={"display": "flex", "alignItems": "center", "gap": "8px"},
                        children=[
                            html.Label("Layout", className="label is-small",
                                       style={"marginBottom": 0}),
                            dcc.Dropdown(
                                id="layout-dropdown",
                                options=[
                                    {"label": "Dagre LR",     "value": "dagre"},
                                    {"label": "Breadthfirst", "value": "breadthfirst"},
                                    {"label": "Circle",       "value": "circle"},
                                    {"label": "Cose",         "value": "cose"},
                                ],
                                value="dagre",
                                clearable=False,
                                style={"width": "150px", "fontSize": "13px"},
                            ),
                        ],
                    ),
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
                                           "color": "#FFF", "fontSize": "10px"},
                                )
                                for cat, color in CATEGORY_COLORS.items()
                            ],
                            html.Span("▶ Start", className="tag is-success is-light",
                                      style={"fontSize": "10px"}),
                            html.Span("■ End", className="tag is-warning is-light",
                                      style={"fontSize": "10px"}),
                            html.Span("↺ Loop", className="tag",
                                      style={"backgroundColor": "#EF6C00",
                                             "color": "#FFF", "fontSize": "10px"}),
                        ],
                    ),
                ],
            ),

            # ── Details card (above graph) ──────────────────────────────────
            html.Div(
                className="box",
                style={
                    "margin": "0 0 8px 0",
                    "padding": "0.65rem 1rem",
                    "minHeight": "52px",
                    "display": "flex",
                    "alignItems": "center",
                    "borderRadius": "6px",
                },
                children=[
                    html.Div(id="detail-content", style={"width": "100%"}, children=[
                        html.P("Click a node or edge to see details.",
                               className="has-text-grey is-size-7",
                               style={"margin": 0}),
                    ]),
                ],
            ),

            # ── DFG Graph (full width) ──────────────────────────────────────
            cyto.Cytoscape(
                id="dfg-graph",
                elements=[],
                stylesheet=BASE_STYLESHEET,
                style={
                    "width": "100%",
                    "height": "550px",
                    "backgroundColor": "#FFFFFF",
                    "borderRadius": "6px",
                    "border": "1px solid #DEE2E6",
                },
                layout={"name": "dagre", "rankDir": "LR",
                        "nodeSep": 50, "rankSep": 100, "edgeSep": 20},
                responsive=True,
                minZoom=0.15,
                maxZoom=3.0,
            ),

            # ── Node connection stats panel (below graph) ───────────────────
            html.Div(
                className="box",
                id="node-stats-panel",
                style={"marginTop": "8px", "padding": "0.75rem 1rem",
                       "borderRadius": "6px"},
                children=[
                    html.P("Select a node to see connection statistics.",
                           className="has-text-grey is-size-7",
                           style={"margin": 0}),
                ],
            ),

            # ── Hidden stores + interval ────────────────────────────────────
            dcc.Store(id="nodes-store"),
            dcc.Store(id="edges-store"),
            dcc.Store(id="selected-node-store"),
            dcc.Interval(id="load-interval", interval=500, max_intervals=1),
        ],
    )],
)


# ── Callbacks ─────────────────────────────────────────────────────────────────

def _slider_config(max_freq: int) -> tuple[int, int, dict]:
    """
    Return (slider_max_index, step, marks) for the frequency slider.

    The slider uses *index* positions (0 to N-1) so that the geometric
    FREQ_STEPS sequence gets even visual spacing. update_graph translates the
    index back to the actual frequency value via FREQ_STEPS[index].
    """
    valid_steps = [s for s in FREQ_STEPS if s <= max_freq] or [0]
    n = len(valid_steps)
    show_all = n <= 8
    marks = {}
    for i, v in enumerate(valid_steps):
        show_label = show_all or i % 2 == 0 or i == n - 1
        marks[i] = {
            "label": _fmt_step(v) if show_label else " ",
            "style": {"fontSize": "10px"},
        }
    return n - 1, 1, marks  # max_index, step, marks


@callback(
    Output("nodes-store", "data"),
    Output("edges-store", "data"),
    Output("freq-slider", "max"),
    Output("freq-slider", "step"),
    Output("freq-slider", "marks"),
    Output("freq-slider", "value"),
    Output("status-banner", "children"),
    Output("status-banner", "style"),
    Input("load-interval", "n_intervals"),
    Input("refresh-btn", "n_clicks"),
    prevent_initial_call=True,
)
def load_data(_n_intervals, _n_clicks):
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
            slider_max, step, marks, 0,
            banner_text, BANNER_OK,
        )
    except Exception as exc:
        logger.error("Failed to load DFG data: %s", exc)
        return "[]", "[]", 19, 1, _INIT_MARKS, 0, f"Error: {exc}", BANNER_ERROR


@callback(
    Output("dfg-graph", "elements"),
    Output("dfg-graph", "layout"),
    Input("nodes-store", "data"),
    Input("edges-store", "data"),
    Input("freq-slider", "value"),
    Input("layout-dropdown", "value"),
)
def update_graph(nodes_json, edges_json, freq_idx, layout_name):
    """Rebuild graph elements when data or filter changes. Does NOT touch the
    stylesheet — selection highlighting is handled separately so the layout
    does not re-run every time the user clicks a node."""
    if not nodes_json or nodes_json == "[]":
        return [], {"name": "dagre"}

    # Translate slider index to actual frequency threshold
    idx = int(freq_idx) if freq_idx is not None else 0
    min_freq = FREQ_STEPS[idx] if 0 <= idx < len(FREQ_STEPS) else 0

    nodes_df = pd.read_json(nodes_json, orient="records")
    edges_df = pd.read_json(edges_json, orient="records")
    elements = build_cytoscape_elements(nodes_df, edges_df, min_freq=min_freq)

    layout_cfg = {"name": layout_name}
    if layout_name == "dagre":
        layout_cfg.update({"rankDir": "LR", "nodeSep": 60, "rankSep": 120})
    elif layout_name == "breadthfirst":
        layout_cfg.update({"directed": True, "spacingFactor": 1.4})

    return elements, layout_cfg


@callback(
    Output("selected-node-store", "data"),
    Input("dfg-graph", "selectedNodeData"),
)
def store_selected_node(selected_data):
    """Track the currently selected node. Fires with [] when user clicks
    the graph background, which clears the selection."""
    if not selected_data:
        return None
    return selected_data[0].get("id")


@callback(
    Output("dfg-graph", "stylesheet"),
    Input("selected-node-store", "data"),
    Input("edges-store", "data"),
)
def update_stylesheet(selected_node_id, edges_json):
    """Return a stylesheet that highlights the selected node, its outgoing
    connections, and neighbor nodes. Only the stylesheet prop changes — the
    elements prop is untouched, so the layout stays stable."""
    return _compute_stylesheet(selected_node_id, edges_json)


@callback(
    Output("detail-content", "children"),
    Input("dfg-graph", "tapNodeData"),
    Input("dfg-graph", "tapEdgeData"),
    State("edges-store", "data"),
)
def show_detail(node_data, edge_data, edges_json):
    """Populate the details bar (above the graph) for the clicked node/edge."""
    ctx = dash.callback_context
    if not ctx.triggered:
        return html.P("Click a node or edge to see details.",
                      style={"color": "#90A4AE", "fontSize": "13px", "margin": 0})

    trigger_id = ctx.triggered[0]["prop_id"]

    if "tapNodeData" in trigger_id and node_data:
        cat = node_data.get("category", "")
        color = CATEGORY_COLORS.get(cat, DEFAULT_COLOR)
        is_start = node_data.get("is_start", False)
        is_end = node_data.get("is_end", False)
        has_loop = node_data.get("has_self_loop", False)

        # Count outgoing edges for this node
        n_connections = 0
        if edges_json and edges_json != "[]":
            try:
                edf = pd.read_json(edges_json, orient="records")
                n_connections = int(
                    (edf["source_activity"] == node_data.get("activity_name", "")).sum()
                )
            except Exception:
                pass

        avg_ms = node_data.get("avg_duration_ms", 0) or 0
        chips = [
            html.Span(
                node_data.get("activity_name", ""),
                style={
                    "backgroundColor": color, "color": "#FFF",
                    "padding": "4px 12px", "borderRadius": "4px",
                    "fontWeight": "bold", "fontSize": "13px",
                    "whiteSpace": "nowrap",
                },
            ),
            _stat_chip("Category", cat.replace("_", " ").title()),
            _stat_chip("Occurrences", f"{node_data.get('occurrence_count', 0):,}"),
            _stat_chip("Cases", f"{node_data.get('case_count', 0):,}"),
            _stat_chip("Outgoing Connections", str(n_connections)),
            _stat_chip("Avg Self-Duration", _format_ms(avg_ms) if avg_ms > 0 else "—"),
        ]
        if is_start:
            chips.append(html.Span("START", className="tag is-success is-small",
                                   style={"alignSelf": "center"}))
        if is_end:
            chips.append(html.Span("END", className="tag is-warning is-small",
                                   style={"alignSelf": "center"}))
        if has_loop:
            chips.append(html.Span("↺ SELF-LOOP", className="tag is-small",
                                   style={"alignSelf": "center",
                                          "backgroundColor": "#EF6C00",
                                          "color": "#FFF"}))
        return html.Div(
            style={"display": "flex", "alignItems": "center",
                   "gap": "12px", "flexWrap": "wrap"},
            children=chips,
        )

    if "tapEdgeData" in trigger_id and edge_data:
        is_loop = edge_data.get("is_self_loop", False)
        chips = [
            html.Span(
                f"{edge_data.get('source', '')} → {edge_data.get('target', '')}",
                style={
                    "backgroundColor": "#EF6C00" if is_loop else "#37474F",
                    "color": "#FFF",
                    "padding": "4px 12px", "borderRadius": "4px",
                    "fontWeight": "bold", "fontSize": "13px",
                    "whiteSpace": "nowrap",
                },
            ),
            _stat_chip("Frequency", f"{edge_data.get('edge_frequency', 0):,}"),
            _stat_chip("Cases", f"{edge_data.get('case_count', 0):,}"),
            _stat_chip("Avg Transition", _format_ms(edge_data.get("avg_ms"))),
            _stat_chip("Median (p50)",   _format_ms(edge_data.get("p50_ms"))),
            _stat_chip("p95 Transition", _format_ms(edge_data.get("p95_ms"))),
        ]
        if is_loop:
            chips.append(html.Span("↺ SELF-LOOP", className="tag is-small",
                                   style={"alignSelf": "center",
                                          "backgroundColor": "#EF6C00",
                                          "color": "#FFF"}))
        return html.Div(
            style={"display": "flex", "alignItems": "center",
                   "gap": "12px", "flexWrap": "wrap"},
            children=chips,
        )

    return html.P("Click a node or edge to see details.",
                  style={"color": "#90A4AE", "fontSize": "13px", "margin": 0})


@callback(
    Output("node-stats-panel", "children"),
    Input("selected-node-store", "data"),
    Input("edges-store", "data"),
)
def show_node_stats(selected_node_id, edges_json):
    """
    Show a connection breakdown table below the graph when a node is selected.

    Columns: Target Node | Frequency | % of Outgoing | Cases |
             Avg Time | Median (p50) | p95
    """
    if not selected_node_id:
        return html.P("Select a node to see connection statistics.",
                      className="has-text-grey is-size-7", style={"margin": 0})

    if not edges_json or edges_json == "[]":
        return html.P("No edge data available yet.",
                      className="has-text-grey is-size-7", style={"margin": 0})

    try:
        edges_df = pd.read_json(edges_json, orient="records")
    except Exception:
        return html.P("Error reading edge data.",
                      className="has-text-grey is-size-7", style={"margin": 0})

    outgoing = (
        edges_df[edges_df["source_activity"] == selected_node_id]
        .sort_values("edge_frequency", ascending=False)
    )

    node_label = selected_node_id.replace("_", " ").title()
    header = html.P(
        [html.Strong("Connections from: "), node_label],
        style={"fontSize": "13px", "marginBottom": "10px"},
    )

    if outgoing.empty:
        return html.Div([
            header,
            html.P("No outgoing connections (end node).",
                   className="has-text-grey is-size-7"),
        ])

    total_freq = outgoing["edge_frequency"].sum()

    rows = []
    for _, row in outgoing.iterrows():
        freq = int(row["edge_frequency"])
        pct = freq / total_freq * 100 if total_freq > 0 else 0
        target_label = str(row["target_activity"]).replace("_", " ").title()
        is_self = row["source_activity"] == row["target_activity"]

        # Inline mini progress bar
        bar_width = max(4, int(pct))
        pct_cell = html.Td(
            style={"whiteSpace": "nowrap"},
            children=[
                html.Span(f"{pct:.1f}% ", style={"fontSize": "11px"}),
                html.Span(
                    style={
                        "display": "inline-block",
                        "width": f"{bar_width}px",
                        "height": "6px",
                        "backgroundColor": "#1565C0",
                        "borderRadius": "3px",
                        "verticalAlign": "middle",
                        "maxWidth": "80px",
                    },
                ),
            ],
        )

        target_cell = html.Td(
            style={"fontWeight": "500"},
            children=[
                target_label,
                html.Span(" ↺", style={"color": "#EF6C00", "fontWeight": "bold"})
                if is_self else None,
            ],
        )

        rows.append(html.Tr([
            target_cell,
            html.Td(f"{freq:,}"),
            pct_cell,
            html.Td(f"{int(row.get('case_count', 0)):,}"),
            html.Td(_format_ms(row.get("avg_transition_duration_ms", 0))),
            html.Td(_format_ms(row.get("p50_transition_duration_ms", 0))),
            html.Td(_format_ms(row.get("p95_transition_duration_ms", 0))),
        ]))

    table = html.Table(
        className="table is-narrow is-hoverable is-fullwidth",
        style={"fontSize": "12px"},
        children=[
            html.Thead(html.Tr([
                html.Th("Target Node"),
                html.Th("Frequency"),
                html.Th("% of Outgoing"),
                html.Th("Cases"),
                html.Th("Avg Time"),
                html.Th("Median"),
                html.Th("p95"),
            ])),
            html.Tbody(rows),
        ],
    )

    return html.Div([header, table])


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8000)
