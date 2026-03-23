import json
import re
import uuid
from datetime import datetime

import streamlit as st
import streamlit.components.v1 as components

# ── Signal category tagging from transcript intelligence layer ──
try:
    from utils.transcript_live import (
        SIGNAL_COLORS,
        OUTLOOK_KEYWORDS,
        RISK_KEYWORDS,
        OPPORTUNITY_KEYWORDS,
        INVESTMENT_KEYWORDS,
        PRODUCT_SHIFT_KEYWORDS,
        USER_BEHAVIOR_KEYWORDS,
        MONETIZATION_KEYWORDS,
        STRATEGIC_DIRECTION_KEYWORDS,
        BROADCASTER_THREAT_KEYWORDS,
    )
except ImportError:
    SIGNAL_COLORS = {}
    OUTLOOK_KEYWORDS = RISK_KEYWORDS = OPPORTUNITY_KEYWORDS = []
    INVESTMENT_KEYWORDS = PRODUCT_SHIFT_KEYWORDS = USER_BEHAVIOR_KEYWORDS = []
    MONETIZATION_KEYWORDS = STRATEGIC_DIRECTION_KEYWORDS = BROADCASTER_THREAT_KEYWORDS = []

_SIGNAL_KEYWORD_MAP = {
    "Outlook": OUTLOOK_KEYWORDS,
    "Risks": RISK_KEYWORDS,
    "Opportunities": OPPORTUNITY_KEYWORDS,
    "Investment": INVESTMENT_KEYWORDS,
    "Product Shifts": PRODUCT_SHIFT_KEYWORDS,
    "User Behavior": USER_BEHAVIOR_KEYWORDS,
    "Monetization": MONETIZATION_KEYWORDS,
    "Strategic Direction": STRATEGIC_DIRECTION_KEYWORDS,
    "Broadcaster Threats": BROADCASTER_THREAT_KEYWORDS,
}


def match_signal_category(text: str) -> tuple[str, str]:
    """Match node text against signal keyword maps. Returns (category, border_color) or ("","")."""
    text_lower = text.lower()
    best_cat = ""
    best_score = 0
    for cat, keywords in _SIGNAL_KEYWORD_MAP.items():
        score = sum(1 for kw in keywords if kw.lower() in text_lower)
        if score > best_score:
            best_score = score
            best_cat = cat
    if best_score >= 1 and best_cat in SIGNAL_COLORS:
        return best_cat, SIGNAL_COLORS[best_cat]["border"]
    return "", ""


# ── Depth mode definitions ──
DEPTH_MODES = {
    "focused": {
        "label": "3-4 nodes",
        "icon": "",
        "desc": "",
        "prompt_insert": (
            "MODE: FOCUSED (3-4 nodes). Build a tight CAUSAL CHAIN. "
            "Each step must logically follow the previous one. "
            "Use exactly: [STEP 1] → [STEP 2] → [STEP 3] → [CONCLUSION]. "
            "No branches. Linear reasoning only. Every link should feel inevitable."
        ),
    },
    "balanced": {
        "label": "5-7 nodes",
        "icon": "",
        "desc": "",
        "prompt_insert": (
            "MODE: BALANCED (5-7 nodes). Build a BRANCHING TREE. "
            "Start with 2-3 sequential steps, then FORK into at least one [BRANCH]. "
            "One branch should loop back to challenge an earlier step. "
            "Include ONE surprising lateral connection the user would not expect. "
            "Use: [STEP 1] → [STEP 2] → [BRANCH A] alternative → [STEP 3] → [BRANCH B] risk → [CONCLUSION]."
        ),
    },
    "deep": {
        "label": "8-12 nodes",
        "icon": "",
        "desc": "",
        "prompt_insert": (
            "MODE: DEEP WEB (8-12 nodes). Build a complex WEB of reasoning. "
            "Include SECOND-ORDER effects (what happens because of what happens). "
            "Include at least 2 [BRANCH] nodes that are CONTRARIAN — ideas that push back "
            "on the main thesis or reveal a hidden risk. "
            "Create CROSS-CONNECTIONS between branches (mention earlier steps by name). "
            "Include an [OBSERVATION] that connects two seemingly unrelated data points. "
            "The map should feel like a network, not a line. "
            "Use all marker types: STEP, BRANCH, OBSERVATION, INFERENCE, ANALYSIS, RISK, CONCLUSION."
        ),
    },
}


def _empty_map() -> dict:
    return {"nodes": {}, "edges": [], "root_ids": [], "version": 1}


def _get_map() -> dict:
    if "thought_map" not in st.session_state:
        st.session_state["thought_map"] = _empty_map()
    return st.session_state["thought_map"]


def get_depth_prompt_insert() -> str:
    """Return the prompt insert for the current depth mode."""
    mode = st.session_state.get("thought_map_depth", "balanced")
    return DEPTH_MODES.get(mode, DEPTH_MODES["balanced"])["prompt_insert"]


def render_depth_selector():
    """Render depth selector as clean pill buttons with clear selected state."""
    current = st.session_state.get("thought_map_depth", "balanced")

    # CSS for the depth pills
    st.markdown("""<style>
    .depth-pills { display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; margin: 8px 0 16px 0; }
    .depth-pills div[data-testid="stButton"] > button {
        border-radius: 999px !important;
        border: 1px solid #d9dde5 !important;
        background: rgba(238,240,244,0.15) !important;
        background-image: none !important;
        color: #CBD5E1 !important;
        font-size: 0.9rem !important;
        font-weight: 500 !important;
        padding: 10px 20px !important;
        height: 44px !important;
        transition: all 200ms ease !important;
    }
    .depth-pills div[data-testid="stButton"] > button:hover {
        border-color: #ff5b1f !important;
        background: rgba(255,91,31,0.08) !important;
        background-image: none !important;
        color: #ff8c42 !important;
    }
    .depth-pills div[data-testid="stButton"] > button:active,
    .depth-pills div[data-testid="stButton"] > button:focus,
    .depth-pills div[data-testid="stButton"] > button:focus-visible {
        background-image: none !important;
        outline: none !important;
        box-shadow: none !important;
    }
    .depth-pills div[data-testid="stButton"] > button[kind="primary"] {
        border-color: #ff5b1f !important;
        background: rgba(255,91,31,0.15) !important;
        background-image: none !important;
        color: #ff8c42 !important;
        font-weight: 700 !important;
        box-shadow: 0 0 12px rgba(255,91,31,0.12) !important;
    }
    </style>""", unsafe_allow_html=True)

    st.markdown('<div class="depth-pills">', unsafe_allow_html=True)
    cols = st.columns(len(DEPTH_MODES))
    for i, (key, mode) in enumerate(DEPTH_MODES.items()):
        with cols[i]:
            is_active = key == current
            if st.button(
                mode["label"],
                key=f"depth_{key}",
                type="primary" if is_active else "secondary",
                use_container_width=True,
            ):
                st.session_state["thought_map_depth"] = key
                st.rerun()
    st.markdown('</div>', unsafe_allow_html=True)


STEP_PATTERN = re.compile(
    r"\[(STEP\s*\d+|BRANCH\s*[A-Z0-9]+|CONCLUSION|OBSERVATION|INFERENCE|ANALYSIS|RISK)\]"
    r"\s*(?:([^:\n]+):\s*)?(.+?)(?=\n*\[(?:STEP|BRANCH|CONCLUSION|OBSERVATION|INFERENCE|ANALYSIS|RISK)|$)",
    re.DOTALL | re.IGNORECASE,
)

NODE_TYPE_MAP = {
    "STEP": "step",
    "BRANCH": "branch",
    "CONCLUSION": "conclusion",
    "OBSERVATION": "step",
    "INFERENCE": "step",
    "ANALYSIS": "step",
    "RISK": "branch",
}

NODE_COLORS = {
    "root": "#0073FF",
    "step": "#0073FF",
    "branch": "#F59E0B",
    "conclusion": "#10B981",
    "human": "#8B5CF6",
    "queued": "#ff5b1f",
}


def add_queued_node(text: str, source_type: str = "suggestion", meta: dict = None):
    """Add a queued (pending) node to the thought map. Not yet reasoned about."""
    tm = _get_map()
    node_id = str(uuid.uuid4())[:8]
    label = text[:60] + ("…" if len(text) > 60 else "")
    node = {
        "id": node_id,
        "type": "queued",
        "label": label,
        "content": text,
        "parent_id": None,
        "children": [],
        "depth": 0,
        "collapsed": False,
        "created_at": datetime.now().isoformat(),
        "source_type": source_type,
        "meta": meta or {},
    }
    tm["nodes"][node_id] = node
    tm["root_ids"].append(node_id)
    st.session_state["thought_map"] = tm
    return node_id


def get_queued_nodes() -> list[dict]:
    """Return all queued (pending) nodes from the thought map."""
    tm = _get_map()
    return [n for n in tm["nodes"].values() if n.get("type") == "queued"]


def promote_queued_nodes():
    """Mark all queued nodes as 'root' after AI reasoning starts. Returns the combined prompt text."""
    tm = _get_map()
    queued = [n for n in tm["nodes"].values() if n.get("type") == "queued"]
    texts = []
    for node in queued:
        node["type"] = "root"
        texts.append(node["content"])
    st.session_state["thought_map"] = tm
    return texts


def parse_response_to_nodes(response_text: str, message_index: int) -> list[dict]:
    """Parse a Genie response into structured thought map nodes."""
    matches = list(STEP_PATTERN.finditer(response_text))

    if not matches:
        return [
            {
                "id": str(uuid.uuid4())[:8],
                "type": "root",
                "label": f"Response #{message_index + 1}",
                "content": response_text[:200].strip() + ("…" if len(response_text) > 200 else ""),
                "parent_id": None,
                "children": [],
                "depth": 0,
                "collapsed": False,
                "created_at": datetime.now().isoformat(),
                "source_message_index": message_index,
            }
        ]

    nodes: list[dict] = []
    prev_id = None

    for i, match in enumerate(matches):
        raw_type = match.group(1).strip().upper()
        base_type = re.split(r"\s+\d+|\s+[A-Z]", raw_type)[0].strip()
        node_type = NODE_TYPE_MAP.get(base_type, "step")

        label = (match.group(2) or raw_type).strip()[:60]
        content = match.group(3).strip()

        node_id = str(uuid.uuid4())[:8]
        nodes.append(
            {
                "id": node_id,
                "type": node_type,
                "label": label,
                "content": content,
                "parent_id": prev_id,
                "children": [],
                "depth": i,
                "collapsed": False,
                "created_at": datetime.now().isoformat(),
                "source_message_index": message_index,
            }
        )

        if node_type != "branch":
            prev_id = node_id

    return nodes


def add_nodes_to_map(new_nodes: list[dict], parent_node_id: str = None):
    """Merge parsed nodes into session thought map."""
    tm = _get_map()

    for i, node in enumerate(new_nodes):
        if parent_node_id and i == 0:
            node["parent_id"] = parent_node_id
            if parent_node_id in tm["nodes"]:
                node["depth"] = tm["nodes"][parent_node_id]["depth"] + 1

        tm["nodes"][node["id"]] = node

        if node["parent_id"] is None:
            tm["root_ids"].append(node["id"])
        else:
            parent = tm["nodes"].get(node["parent_id"])
            if parent and node["id"] not in parent["children"]:
                parent["children"].append(node["id"])
            edge = {"from": node["parent_id"], "to": node["id"]}
            if edge not in tm["edges"]:
                tm["edges"].append(edge)

    st.session_state["thought_map"] = tm


def render_thought_map(height: int = 620):
    """Render thought map with Cytoscape.js — animated cascade, edge labels, Smart Focus."""
    tm = _get_map()

    if not tm["nodes"]:
        st.markdown(
            "<div style='padding:40px; text-align:center; color:#64748B; "
            "border:1px dashed rgba(100,116,139,0.3); border-radius:16px; "
            "background: radial-gradient(circle at 50% 50%, rgba(255,91,31,0.04), transparent 60%);'>"
            "<div style='font-size:2.5rem; margin-bottom:12px;'>&#x1f9e0;</div>"
            "<strong style='font-size:1rem; color:#94A3B8;'>Thought Map</strong><br/>"
            "<span style='font-size:0.85rem;'>Select questions and signals below to queue them as nodes.<br/>"
            "Then press <b style='color:#ff5b1f;'>Start Genie Thoughts</b> to let the AI reason through them.</span>"
            "</div>",
            unsafe_allow_html=True,
        )
        return

    cy_nodes = []
    for node in tm["nodes"].values():
        color = NODE_COLORS.get(node["type"], "#64748B")
        sig_cat, sig_color = match_signal_category(
            node.get("label", "") + " " + node.get("content", "")
        )
        if sig_color:
            color = sig_color
        cy_nodes.append(
            {
                "data": {
                    "id": node["id"],
                    "label": node["label"][:30] + ("..." if len(node["label"]) > 30 else ""),
                    "fullLabel": node["label"],
                    "content": node["content"][:150] + ("..." if len(node["content"]) > 150 else ""),
                    "fullContent": node["content"],
                    "type": node["type"],
                    "color": color,
                    "depth": node["depth"],
                    "signalCategory": sig_cat,
                    "sourceType": node.get("source_type", ""),
                }
            }
        )

    # Build edge labels from parent->child type transitions
    cy_edges = []
    for edge in tm["edges"]:
        src = tm["nodes"].get(edge["from"], {})
        tgt = tm["nodes"].get(edge["to"], {})
        src_t = src.get("type", "")
        tgt_t = tgt.get("type", "")
        edge_label = ""
        if tgt_t == "branch":
            edge_label = "branches to"
        elif tgt_t == "conclusion":
            edge_label = "concludes"
        elif src_t == "root" and tgt_t == "step":
            edge_label = "leads to"
        elif src_t == "step":
            edge_label = "then"
        cy_edges.append({
            "data": {
                "id": "e_" + edge["from"] + "_" + edge["to"],
                "source": edge["from"],
                "target": edge["to"],
                "edgeLabel": edge_label,
            }
        })

    elements_json = json.dumps(cy_nodes + cy_edges)

    # Compute badge counts
    type_counts = {}
    for n in tm["nodes"].values():
        t = n["type"]
        type_counts[t] = type_counts.get(t, 0) + 1
    node_count = len(tm["nodes"])
    edge_count = len(tm["edges"])
    queued_count = type_counts.get("queued", 0)
    step_count = type_counts.get("step", 0) + type_counts.get("root", 0)
    branch_count = type_counts.get("branch", 0)
    conclusion_count = type_counts.get("conclusion", 0)

    # Build queued button separately to avoid backslash-in-fstring issue
    queued_btn = ""
    if queued_count:
        queued_btn = (
            '<button class="btn" onclick="filterType(' + "'queued'" + ')">'
            'Queued<span class="badge">' + str(queued_count) + '</span></button>'
        )

    cy_height = height - 44

    # Use regular string concatenation for the HTML template to avoid f-string backslash issues
    html = (
        '<!DOCTYPE html><html><head><meta charset="utf-8">'
        '<script src="https://cdnjs.cloudflare.com/ajax/libs/cytoscape/3.26.0/cytoscape.min.js"></script>'
        '<style>'
        '* { box-sizing: border-box; margin: 0; padding: 0; }'
        'body { background: #0B1220; font-family: -apple-system, BlinkMacSystemFont, "Inter", sans-serif; overflow: hidden; }'
        '#cy { width: 100%; height: ' + str(cy_height) + 'px; '
        '  background: radial-gradient(circle at 20% 20%, rgba(255,107,44,0.06), transparent 28%),'
        '             radial-gradient(circle at 80% 24%, rgba(56,189,248,0.06), transparent 26%),'
        '             radial-gradient(circle at 50% 100%, rgba(139,92,246,0.04), transparent 34%),'
        '             #0B1220; }'
        '#bar { height: 44px; background: #111827; border-bottom: 1px solid rgba(148,163,184,0.15);'
        '  display: flex; align-items: center; padding: 0 10px; gap: 6px; flex-wrap: nowrap; }'
        '.btn { background: rgba(255,255,255,0.08); border: 1px solid rgba(255,255,255,0.15);'
        '  color: #CBD5E1; border-radius: 6px; padding: 4px 8px; cursor: pointer;'
        '  font-size: 0.7rem; font-weight: 600; letter-spacing: 0.02em;'
        '  transition: all 0.15s; white-space: nowrap; }'
        '.btn:hover { background: rgba(0,115,255,0.25); border-color: #0073FF; color:#fff; }'
        '.btn.active { background: rgba(0,115,255,0.35); border-color: #0073FF; color:#fff; }'
        '.badge { background: rgba(255,255,255,0.15); border-radius: 8px; padding: 1px 5px; font-size: 0.6rem; margin-left: 3px; }'
        '#meta { margin-left: auto; font-size: 0.68rem; color: #475569; font-weight: 600; white-space: nowrap; }'
        '.sep { width: 1px; height: 20px; background: rgba(255,255,255,0.1); }'
        '#tooltip { position: fixed; display: none; pointer-events: none; z-index: 1000;'
        '  background: rgba(15,23,42,0.95); border: 1px solid rgba(148,163,184,0.25);'
        '  border-radius: 12px; padding: 14px 16px; max-width: 340px;'
        '  font-size: 0.82rem; color: #E2E8F0;'
        '  box-shadow: 0 12px 36px rgba(0,0,0,0.5); backdrop-filter: blur(12px); }'
        '#tooltip .tt-label { font-weight: 800; color: #60A5FA; margin-bottom: 4px; font-size: 0.9rem; }'
        '#tooltip .tt-type { font-size: 0.65rem; text-transform: uppercase; letter-spacing: 0.08em; color: #94A3B8; margin-bottom: 6px; }'
        '#tooltip .tt-content { line-height: 1.5; color: #CBD5E1; }'
        '#tooltip .tt-badge { display: inline-block; font-size: 0.6rem; background: rgba(99,102,241,0.3); color: #a5b4fc;'
        '  border-radius: 4px; padding: 1px 6px; margin-top: 6px; }'
        '#detail-panel { display: none; position: absolute; bottom: 0; left: 0; right: 0; z-index: 500;'
        '  background: rgba(15,23,42,0.95); border-top: 2px solid #0073FF;'
        '  padding: 14px 18px 16px; max-height: 200px; overflow-y: auto;'
        '  backdrop-filter: blur(12px); animation: slideUp 0.2s ease; }'
        '@keyframes slideUp { from { transform: translateY(100%); opacity:0; } to { transform: translateY(0); opacity:1; } }'
        '#dp-close { float: right; background: none; border: none; color: #64748B; cursor: pointer; font-size: 1.1rem; }'
        '#dp-close:hover { color: #E2E8F0; }'
        '#dp-label { font-weight: 800; color: #60A5FA; font-size: 0.92rem; margin-bottom: 4px; }'
        '#dp-type { display: inline-block; font-size: 0.65rem; text-transform: uppercase;'
        '  letter-spacing: 0.1em; color: #0F172A; font-weight: 700;'
        '  background: #60A5FA; border-radius: 4px; padding: 1px 7px; margin-bottom: 10px; }'
        '#dp-content { line-height: 1.6; color: #CBD5E1; font-size: 0.84rem; white-space: pre-wrap; }'
        '</style></head><body>'
        '<div id="bar">'
        '  <button class="btn" onclick="cy.fit(cy.nodes(),30)">Fit</button>'
        '  <button class="btn" onclick="cy.zoom(Math.min(cy.zoom()*1.3,3))">+</button>'
        '  <button class="btn" onclick="cy.zoom(Math.max(cy.zoom()*0.75,0.2))">-</button>'
        '  <button class="btn" onclick="relayout()">Re-layout</button>'
        '  <span class="sep"></span>'
        '  <button class="btn" id="focusBtn" onclick="toggleFocus()">Smart Focus</button>'
        '  <button class="btn" onclick="filterType(' + "'all'" + ')">All<span class="badge">' + str(node_count) + '</span></button>'
        '  <button class="btn" onclick="filterType(' + "'step'" + ')">Steps<span class="badge">' + str(step_count) + '</span></button>'
        '  <button class="btn" onclick="filterType(' + "'branch'" + ')">Branches<span class="badge">' + str(branch_count) + '</span></button>'
        '  <button class="btn" onclick="filterType(' + "'conclusion'" + ')">End<span class="badge">' + str(conclusion_count) + '</span></button>'
        '  ' + queued_btn +
        '  <div id="meta">' + str(node_count) + ' nodes &middot; ' + str(edge_count) + ' links</div>'
        '</div>'
        '<div id="cy"></div>'
        '<div id="tooltip">'
        '  <div class="tt-label" id="tt-label"></div>'
        '  <div class="tt-type" id="tt-type"></div>'
        '  <div class="tt-content" id="tt-content"></div>'
        '  <div class="tt-badge" id="tt-badge" style="display:none"></div>'
        '</div>'
        '<div id="detail-panel">'
        '  <button id="dp-close" onclick="document.getElementById(' + "'detail-panel'" + ').style.display=' + "'none'" + '">&times;</button>'
        '  <div id="dp-label"></div><div id="dp-type"></div><div id="dp-content"></div>'
        '</div>'
        '<script>'
        'const elements = ' + elements_json + ';'
        'const cy = cytoscape({'
        '  container: document.getElementById("cy"),'
        '  elements: [],'
        '  style: ['
        '    { selector: "node", style: {'
        '        "background-color": "data(color)", "label": "data(label)",'
        '        "color": "#F8FAFC", "font-size": "11px", "font-weight": "700",'
        '        "text-valign": "center", "text-halign": "center",'
        '        "text-wrap": "wrap", "text-max-width": "80px",'
        '        "width": "80px", "height": "80px",'
        '        "border-width": 2, "border-color": "rgba(255,255,255,0.18)",'
        '        "opacity": 1,'
        '        "transition-property": "opacity, background-color, border-color, width, height",'
        '        "transition-duration": "0.3s",'
        '    } },'
        '    { selector: "node[type=\\"branch\\"]", style: { "shape": "diamond", "width": "72px", "height": "72px" } },'
        '    { selector: "node[type=\\"conclusion\\"]", style: { "shape": "round-rectangle", "width": "100px", "height": "56px" } },'
        '    { selector: "node[type=\\"human\\"]", style: {'
        '        "border-width": 3, "border-color": "#8B5CF6", "border-style": "dashed",'
        '        "shape": "round-rectangle", "width": "90px",'
        '    } },'
        '    { selector: "node[type=\\"queued\\"]", style: {'
        '        "border-width": 3, "border-color": "#ff5b1f", "border-style": "dashed",'
        '        "shape": "round-rectangle", "width": "90px", "height": "60px",'
        '        "background-color": "rgba(255,91,31,0.18)", "font-size": "10px", "color": "#ff8c42",'
        '    } },'
        '    { selector: "node[type=\\"root\\"]", style: { "width": "90px", "height": "90px", "font-size": "12px" } },'
        '    { selector: "node:selected", style: { "border-width": 4, "border-color": "#00C2FF" } },'
        '    { selector: ".dimmed", style: { "opacity": 0.15 } },'
        '    { selector: ".focused", style: { "border-width": 4, "border-color": "#00C2FF" } },'
        '    { selector: "edge", style: {'
        '        "width": 2, "line-color": "rgba(100,116,139,0.4)",'
        '        "target-arrow-color": "rgba(100,116,139,0.6)",'
        '        "target-arrow-shape": "triangle", "curve-style": "bezier", "arrow-scale": 0.9,'
        '        "label": "", "font-size": "8px", "color": "#94A3B8",'
        '        "text-rotation": "autorotate", "text-margin-y": -8,'
        '        "opacity": 1,'
        '        "transition-property": "opacity, line-color",'
        '        "transition-duration": "0.3s",'
        '    } },'
        '    { selector: "edge.dimmed", style: { "opacity": 0.08 } },'
        '    { selector: "edge.focused", style: { "width": 3, "line-color": "#0073FF", "target-arrow-color": "#0073FF" } },'
        '  ],'
        '  layout: { name: "breadthfirst", directed: true, spacingFactor: 1.65, padding: 28 },'
        '  userZoomingEnabled: true, userPanningEnabled: true,'
        '  minZoom: 0.15, maxZoom: 3,'
        '});'
        # Animated cascade
        '(function() {'
        '  var nodes = elements.filter(function(e){ return e.data && !e.data.source; });'
        '  var edges = elements.filter(function(e){ return e.data && e.data.source; });'
        '  var delay = 0;'
        '  nodes.forEach(function(n, i) {'
        '    setTimeout(function() {'
        '      cy.add(n);'
        '      edges.forEach(function(edge) {'
        '        if (cy.getElementById(edge.data.id).length === 0 &&'
        '            cy.getElementById(edge.data.source).length > 0 &&'
        '            cy.getElementById(edge.data.target).length > 0) {'
        '          cy.add(edge);'
        '        }'
        '      });'
        '      cy.layout({ name: "breadthfirst", directed: true, spacingFactor: 1.65, padding: 28, animate: true, animationDuration: 200 }).run();'
        '    }, delay);'
        '    delay += 150;'
        '  });'
        '  setTimeout(function(){ cy.fit(cy.nodes(), 30); }, delay + 300);'
        '})();'
        # Relayout
        'function relayout() {'
        '  cy.layout({ name: "breadthfirst", directed: true, spacingFactor: 1.65, padding: 28, animate: true, animationDuration: 300 }).run();'
        '}'
        # Smart Focus
        'var focusActive = false;'
        'function toggleFocus() {'
        '  focusActive = !focusActive;'
        '  document.getElementById("focusBtn").classList.toggle("active", focusActive);'
        '  if (!focusActive) { cy.elements().removeClass("dimmed focused"); return; }'
        '  var conclusions = cy.nodes("[type=\\"conclusion\\"]");'
        '  if (conclusions.length === 0) {'
        '    var important = cy.nodes("[type=\\"root\\"], [type=\\"queued\\"]");'
        '    cy.elements().addClass("dimmed");'
        '    important.removeClass("dimmed").addClass("focused");'
        '    important.connectedEdges().removeClass("dimmed").addClass("focused");'
        '    return;'
        '  }'
        '  var pathNodes = cy.collection();'
        '  var pathEdges = cy.collection();'
        '  conclusions.forEach(function(c) {'
        '    var cur = c;'
        '    pathNodes = pathNodes.union(cur);'
        '    while (true) {'
        '      var incoming = cur.incomers("edge");'
        '      if (incoming.length === 0) break;'
        '      var edge = incoming[0];'
        '      pathEdges = pathEdges.union(edge);'
        '      cur = edge.source();'
        '      pathNodes = pathNodes.union(cur);'
        '    }'
        '  });'
        '  cy.elements().addClass("dimmed");'
        '  pathNodes.removeClass("dimmed").addClass("focused");'
        '  pathEdges.removeClass("dimmed").addClass("focused");'
        '}'
        # Filter by type
        'function filterType(type) {'
        '  if (type === "all") {'
        '    cy.nodes().style("display", "element");'
        '    cy.edges().style("display", "element");'
        '  } else {'
        '    cy.nodes().style("display", "none");'
        '    cy.edges().style("display", "none");'
        '    var matched = cy.nodes("[type=\\"" + type + "\\"]");'
        '    if (type !== "root") cy.nodes("[type=\\"root\\"]").style("display", "element");'
        '    matched.style("display", "element");'
        '    matched.connectedEdges().style("display", "element");'
        '    matched.connectedEdges().connectedNodes().style("display", "element");'
        '  }'
        '  cy.fit(cy.nodes(":visible"), 30);'
        '}'
        # Edge labels on hover
        'cy.on("mouseover", "edge", function(evt) {'
        '  var e = evt.target;'
        '  var label = e.data("edgeLabel");'
        '  if (label) e.style("label", label);'
        '});'
        'cy.on("mouseout", "edge", function(evt) { evt.target.style("label", ""); });'
        # Node tooltip
        'var tooltip = document.getElementById("tooltip");'
        'cy.on("mouseover", "node", function(evt) {'
        '  var n = evt.target;'
        '  document.getElementById("tt-label").textContent = n.data("fullLabel");'
        '  var typeStr = n.data("type").toUpperCase();'
        '  var cat = n.data("signalCategory");'
        '  document.getElementById("tt-type").textContent = typeStr + (cat ? " . " + cat : "");'
        '  document.getElementById("tt-content").textContent = n.data("content");'
        '  var badge = document.getElementById("tt-badge");'
        '  var children = n.outgoers("node").length;'
        '  if (children > 0) { badge.textContent = children + " connected"; badge.style.display = "inline-block"; }'
        '  else { badge.style.display = "none"; }'
        '  tooltip.style.display = "block";'
        '});'
        'cy.on("mousemove", function(evt) {'
        '  if (tooltip.style.display === "block") {'
        '    tooltip.style.left = (evt.originalEvent.clientX + 16) + "px";'
        '    tooltip.style.top = (evt.originalEvent.clientY - 10) + "px";'
        '  }'
        '});'
        'cy.on("mouseout", "node", function() { tooltip.style.display = "none"; });'
        'cy.on("tap", "node", function(evt) {'
        '  var n = evt.target;'
        '  document.getElementById("dp-label").textContent = n.data("fullLabel");'
        '  document.getElementById("dp-type").textContent = n.data("type").toUpperCase();'
        '  document.getElementById("dp-content").textContent = n.data("fullContent") || n.data("content");'
        '  document.getElementById("detail-panel").style.display = "block";'
        '  tooltip.style.display = "none";'
        '});'
        'cy.on("tap", function(evt) {'
        '  if (evt.target === cy) {'
        '    tooltip.style.display = "none";'
        '    document.getElementById("detail-panel").style.display = "none";'
        '  }'
        '});'
        '</script></body></html>'
    )

    components.html(html, height=height, scrolling=False)


def render_thought_map_dashboard():
    """Render a status dashboard below the thought map showing current state."""
    tm = _get_map()
    if not tm["nodes"]:
        return

    type_counts = {}
    for n in tm["nodes"].values():
        t = n["type"]
        type_counts[t] = type_counts.get(t, 0) + 1

    queued = type_counts.get("queued", 0)
    steps = type_counts.get("step", 0) + type_counts.get("root", 0)
    branches = type_counts.get("branch", 0)
    conclusions = type_counts.get("conclusion", 0)
    human = type_counts.get("human", 0)
    total = len(tm["nodes"])
    edges = len(tm["edges"])

    # Build status cards as HTML
    cards = []
    if queued:
        cards.append(f"<div class='tm-stat' style='border-color:#ff5b1f'>"
                     f"<span class='tm-stat-n' style='color:#ff5b1f'>{queued}</span>"
                     f"<span class='tm-stat-l'>Queued</span></div>")
    if steps:
        cards.append(f"<div class='tm-stat' style='border-color:#0073FF'>"
                     f"<span class='tm-stat-n' style='color:#0073FF'>{steps}</span>"
                     f"<span class='tm-stat-l'>Steps</span></div>")
    if branches:
        cards.append(f"<div class='tm-stat' style='border-color:#F59E0B'>"
                     f"<span class='tm-stat-n' style='color:#F59E0B'>{branches}</span>"
                     f"<span class='tm-stat-l'>Branches</span></div>")
    if conclusions:
        cards.append(f"<div class='tm-stat' style='border-color:#10B981'>"
                     f"<span class='tm-stat-n' style='color:#10B981'>{conclusions}</span>"
                     f"<span class='tm-stat-l'>Conclusions</span></div>")
    if human:
        cards.append(f"<div class='tm-stat' style='border-color:#8B5CF6'>"
                     f"<span class='tm-stat-n' style='color:#8B5CF6'>{human}</span>"
                     f"<span class='tm-stat-l'>Notes</span></div>")
    cards.append(f"<div class='tm-stat' style='border-color:#475569'>"
                 f"<span class='tm-stat-n' style='color:#94A3B8'>{edges}</span>"
                 f"<span class='tm-stat-l'>Links</span></div>")

    # Latest activity
    latest_nodes = sorted(tm["nodes"].values(), key=lambda n: n.get("created_at", ""), reverse=True)[:3]
    activity_html = ""
    for n in latest_nodes:
        icon = {"root": "&#x1F535;", "step": "&#x27A1;", "branch": "&#x1F500;", "conclusion": "&#x2705;",
                "human": "&#x1F4AC;", "queued": "&#x1F7E0;"}.get(n["type"], "&#x2022;")
        activity_html += (
            f"<div class='tm-activity'>"
            f"<span>{icon}</span>"
            f"<span class='tm-act-label'>{n['label'][:40]}</span>"
            f"<span class='tm-act-type'>{n['type']}</span>"
            f"</div>"
        )

    st.markdown(f"""<style>
    .tm-dashboard {{ display: flex; gap: 10px; align-items: flex-start; margin-top: 8px; }}
    .tm-stats {{ display: flex; gap: 8px; flex-wrap: wrap; }}
    .tm-stat {{
        background: rgba(15,23,42,0.6); border-left: 3px solid; border-radius: 6px;
        padding: 6px 12px; min-width: 60px; text-align: center;
    }}
    .tm-stat-n {{ display: block; font-size: 1.3rem; font-weight: 800; }}
    .tm-stat-l {{ display: block; font-size: 0.65rem; color: #64748B; text-transform: uppercase; letter-spacing: 0.06em; }}
    .tm-recent {{ flex: 1; background: rgba(15,23,42,0.6); border-radius: 6px; padding: 8px 12px; }}
    .tm-recent-title {{ font-size: 0.7rem; color: #64748B; text-transform: uppercase; letter-spacing: 0.06em; margin-bottom: 4px; }}
    .tm-activity {{ display: flex; align-items: center; gap: 6px; padding: 2px 0; font-size: 0.8rem; color: #CBD5E1; }}
    .tm-act-label {{ flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
    .tm-act-type {{ font-size: 0.6rem; color: #475569; text-transform: uppercase; background: rgba(255,255,255,0.06);
        border-radius: 4px; padding: 1px 5px; }}
    </style>
    <div class="tm-dashboard">
        <div class="tm-stats">{"".join(cards)}</div>
        <div class="tm-recent">
            <div class="tm-recent-title">Latest Activity</div>
            {activity_html}
        </div>
    </div>""", unsafe_allow_html=True)


def render_thought_map_controls():
    """Render annotation/elaboration/export controls below the thought map."""
    tm = _get_map()

    col1, col2, col3 = st.columns([2.5, 1.2, 1.5])

    with col1:
        annotation = st.text_input(
            "Add your own thought node",
            placeholder="e.g. 'But what about subscriber growth offsetting this?'",
            key="tm_annotation_input",
            label_visibility="collapsed",
        )
        if st.button("+ Add to Map", key="tm_add_node_btn") and annotation.strip():
            human_node = {
                "id": str(uuid.uuid4())[:8],
                "type": "human",
                "label": "Your note",
                "content": annotation.strip(),
                "parent_id": None,
                "children": [],
                "depth": 0,
                "collapsed": False,
                "created_at": datetime.now().isoformat(),
                "source_message_index": -1,
            }
            tm["nodes"][human_node["id"]] = human_node
            tm["root_ids"].append(human_node["id"])
            st.session_state["thought_map"] = tm
            st.rerun()

    with col2:
        if st.button("Elaborate Last Node", key="tm_elaborate_btn"):
            if tm["nodes"]:
                candidates = [node for node in tm["nodes"].values() if node["type"] != "human"]
                if candidates:
                    last_node = max(candidates, key=lambda node: node["created_at"])
                    from utils.thought_map import add_queued_node as _aq
                    _aq(
                        "Please elaborate further on this reasoning step:\n\n"
                        f"**{last_node['label']}**\n{last_node['content']}\n\n"
                        "Go deeper — use the [STEP N] format for your reasoning.",
                        source_type="elaboration",
                    )
                    st.session_state["pending_elaboration_node_id"] = last_node["id"]
                    st.rerun()

    with col3:
        if tm["nodes"]:
            _exp_fmt = st.selectbox(
                "Export format",
                ["Markdown", "JSON"],
                key="tm_export_format",
                label_visibility="collapsed",
            )
            if _exp_fmt == "JSON":
                _data = json.dumps(tm, indent=2, default=str)
                _fname = f"thought_map_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
                _mime = "application/json"
            else:
                _data = _map_to_markdown(tm)
                _fname = f"thought_map_{datetime.now().strftime('%Y%m%d_%H%M')}.md"
                _mime = "text/markdown"
            st.download_button(
                f"Export Map ({_exp_fmt})",
                data=_data,
                file_name=_fname,
                mime=_mime,
                key="tm_export_btn",
            )


def _map_to_markdown(tm: dict) -> str:
    lines = ["# Genie Thought Map\n", f"*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}*\n"]

    type_icons = {
        "root": "🔵",
        "step": "➡️",
        "branch": "🔀",
        "conclusion": "✅",
        "human": "💬",
    }

    def render_node(node_id: str, indent: int):
        node = tm["nodes"].get(node_id)
        if not node:
            return
        prefix = "  " * indent
        icon = type_icons.get(node["type"], "•")
        lines.append(f"{prefix}{icon} **{node['label']}**")
        lines.append(f"{prefix}  > {node['content']}\n")
        for child_id in node.get("children", []):
            render_node(child_id, indent + 1)

    for root_id in tm.get("root_ids", []):
        render_node(root_id, 0)

    return "\n".join(lines)
