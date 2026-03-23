"""Enhanced Genie chat UI wired to OpenAI + Thought Map."""

import streamlit as st

from utils.genie_ai import (
    build_genie_messages,
    build_query_transcript_context,
    clean_thought_markers,
    stream_genie_response,
)
from utils.thought_map import add_nodes_to_map, parse_response_to_nodes


def _estimate_token_usage(history: list[dict]) -> int:
    """
    Rough token estimate for warning purposes.
    Uses ~4 chars/token heuristic.
    """
    total_chars = 0
    for message in history:
        total_chars += len(str(message.get("content", "")))
    return total_chars // 4


def render_enhanced_chat_interface(dashboard_state: dict = None, on_new_response=None):
    """
    Full Genie chat interface with OpenAI streaming and thought-map integration.
    """
    st.markdown("<div id='genie-chat-section'></div>", unsafe_allow_html=True)
    st.markdown(
        "<h3 style='margin-bottom:0.5rem;'>🧞 Ask the Genie</h3>"
        "<p style='color:#64748B; font-size:0.9rem; margin-bottom:1rem;'>"
        "Ask anything about the data — company performance, advertising trends, "
        "transcript quotes, macro context, or competitive analysis.</p>",
        unsafe_allow_html=True,
    )

    if "genie_history" not in st.session_state:
        st.session_state["genie_history"] = []

    # ── Primary CTA: Start Genie Thoughts ──
    _pending_q = st.session_state.get("prefill_message", "")
    if _pending_q:
        st.markdown(
            f"<div style='background:rgba(255,91,31,0.08);border:1px solid rgba(255,91,31,0.3);"
            f"border-radius:10px;padding:0.6rem 1rem;margin-bottom:0.8rem;font-size:0.88rem;"
            f"color:#e6edf3;'>"
            f"<strong style='color:#ff8c42;'>Queued:</strong> {_pending_q[:200]}"
            f"{'...' if len(_pending_q) > 200 else ''}</div>",
            unsafe_allow_html=True,
        )

    st.markdown("""<style>
    .genie-start-btn div[data-testid="stButton"] > button {
        background: linear-gradient(135deg, #ff5b1f 0%, #ff8c42 100%) !important;
        color: white !important;
        border: none !important;
        border-radius: 10px !important;
        font-size: 0.95rem !important;
        font-weight: 700 !important;
        padding: 0.6rem 1.5rem !important;
        letter-spacing: 0.02em !important;
        transition: all 0.2s ease !important;
        box-shadow: 0 3px 12px rgba(255,91,31,0.25) !important;
        background-image: none !important;
    }
    .genie-start-btn div[data-testid="stButton"] > button:hover {
        transform: translateY(-1px) !important;
        box-shadow: 0 6px 20px rgba(255,91,31,0.35) !important;
        background-image: none !important;
    }
    .genie-start-btn div[data-testid="stButton"] > button:active,
    .genie-start-btn div[data-testid="stButton"] > button:focus {
        background: linear-gradient(135deg, #ff5b1f 0%, #ff8c42 100%) !important;
        background-image: none !important;
    }
    </style>""", unsafe_allow_html=True)

    _cta_col, _export_col = st.columns([3, 1])
    with _cta_col:
        st.markdown('<div class="genie-start-btn">', unsafe_allow_html=True)
        _start_clicked = st.button(
            "🧞 Start Genie Thoughts",
            key="start_genie_thoughts_btn",
            use_container_width=True,
        )
        st.markdown('</div>', unsafe_allow_html=True)
    with _export_col:
        history = st.session_state.get("genie_history", [])
        if history:
            history_text = "\n\n".join([
                f"**{m.get('role', '').upper()}:** {m.get('content', '')}"
                for m in history
            ])
            st.download_button(
                "⬇ Export",
                data=history_text,
                file_name="genie_conversation.md",
                mime="text/markdown",
                key="export_chat_btn",
            )

    # Display conversation history
    history = st.session_state.get("genie_history", [])
    for message in history:
        role = message.get("role", "assistant")
        content = message.get("content", "")
        avatar = "🧞" if role == "assistant" else "👤"
        with st.chat_message(role, avatar=avatar):
            st.markdown(clean_thought_markers(content) if role == "assistant" else content)

    prefill = st.session_state.pop("prefill_message", None)
    user_input = st.chat_input(
        "Ask about revenue trends, ad market share, transcript quotes...",
        key="genie_chat_input",
    )

    # Start Genie Thoughts button triggers the prefilled message
    if _start_clicked and prefill:
        user_input = prefill
    elif prefill and not user_input:
        user_input = prefill

    if user_input:
        with st.chat_message("user", avatar="👤"):
            st.markdown(user_input)

        st.session_state["genie_history"].append({
            "role": "user",
            "content": user_input,
        })

        runtime_state = dict(dashboard_state or {})
        transcript_context = build_query_transcript_context(user_input, runtime_state)
        if transcript_context:
            runtime_state["matched_transcript_context"] = transcript_context

        messages = build_genie_messages(
            conversation_history=st.session_state["genie_history"][:-1],
            dashboard_state=runtime_state,
            user_message=user_input,
        )

        msg_index = len(st.session_state["genie_history"]) - 1
        response = stream_genie_response(messages)

        st.session_state["genie_history"].append({
            "role": "assistant",
            "content": response,
        })

        parent_node_id = st.session_state.pop("pending_elaboration_node_id", None)
        new_nodes = parse_response_to_nodes(response, msg_index)
        add_nodes_to_map(new_nodes, parent_node_id=parent_node_id)

        if on_new_response:
            on_new_response(response, msg_index)

        st.rerun()

    history = st.session_state.get("genie_history", [])
    if history:
        approx_tokens = _estimate_token_usage(history)
        if approx_tokens >= 120000:
            st.warning("Conversation is getting long. Consider clearing if responses slow down.")


__all__ = ["render_enhanced_chat_interface"]
