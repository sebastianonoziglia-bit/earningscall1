"""
Anthropic Claude API service — replaces OpenAI for all AI generation.
API key from ANTHROPIC_API_KEY env var (set as HuggingFace secret) or session state.

SETUP: HuggingFace Space -> Settings -> Repository secrets -> ANTHROPIC_API_KEY
Get your key at: https://console.anthropic.com
"""
from __future__ import annotations
import os
import logging
import streamlit as st

logger = logging.getLogger(__name__)

MODEL = "claude-sonnet-4-20250514"
API_URL = "https://api.anthropic.com/v1/messages"


def _get_api_key() -> str:
    key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if key:
        return key
    try:
        return str(st.session_state.get("anthropic_api_key", "") or "").strip()
    except Exception:
        return ""


def is_api_available() -> bool:
    try:
        return bool(_get_api_key())
    except Exception:
        return False


def call_claude(
    system_prompt: str,
    user_message: str,
    conversation_history=None,
    max_tokens: int = 1024,
    temperature: float = 0.3,
) -> str:
    """Call Claude API and return text response. Returns empty string on failure."""
    import requests
    api_key = _get_api_key()
    if not api_key:
        return ""
    messages = []
    if conversation_history:
        for msg in conversation_history[-10:]:
            role = str(msg.get("role", "")).lower()
            content = str(msg.get("content", ""))
            if role in {"user", "assistant"} and content:
                messages.append({"role": role, "content": content})
    messages.append({"role": "user", "content": user_message})
    try:
        resp = requests.post(
            API_URL,
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": MODEL,
                "max_tokens": max_tokens,
                "system": system_prompt,
                "messages": messages,
            },
            timeout=30,
        )
        resp.raise_for_status()
        blocks = resp.json().get("content", [])
        return "\n".join(b.get("text", "") for b in blocks if b.get("type") == "text").strip()
    except Exception as e:
        logger.warning("Anthropic API error: %s", e)
        return ""


def generate_segment_insight(
    company: str,
    segment: str,
    revenue_m,
    yoy_pct,
    year: int,
    quarter: str = "",
    transcript_sentence: str = "",
) -> str:
    """Generate a 2-3 sentence segment insight using Claude."""
    system = (
        "You are a senior media & technology financial analyst. "
        "Write concise factual segment insights for an executive dashboard. "
        "Maximum 3 sentences. No bullet points. Be specific about numbers."
    )
    rev_str = ""
    if revenue_m is not None:
        try:
            rev_b = float(revenue_m) / 1000
            rev_str = f"${rev_b:.1f}B" if rev_b >= 1 else f"${float(revenue_m):.0f}M"
            if yoy_pct is not None:
                sign = "+" if float(yoy_pct) >= 0 else ""
                rev_str += f" ({sign}{float(yoy_pct):.1f}% YoY)"
        except Exception:
            pass
    period = f"{year} {quarter}".strip() if quarter else str(year)
    user_msg = (
        f"Company: {company}\nSegment: {segment}\nPeriod: {period}\n"
        f"Revenue: {rev_str or 'not available'}\n"
    )
    if transcript_sentence:
        user_msg += f'Management said: "{transcript_sentence}"\n'
    user_msg += "Write a 2-3 sentence insight for this segment."
    return call_claude(system, user_msg, max_tokens=200)


def generate_editorial_summary(
    company: str,
    transcript_quotes: list,
    company_context: str = "",
) -> str:
    """Generate editorial commentary from transcript quotes."""
    system = (
        "You are a media industry analyst writing editorial commentary for Mediaset's "
        "strategy team. Interpret what management is actually signalling. "
        "2-3 sentences. Be direct and opinionated."
    )
    quotes_str = "\n".join(f'"{q}"' for q in transcript_quotes[:4])
    user_msg = f"Company: {company}\nManagement statements:\n{quotes_str}\n"
    if company_context:
        user_msg += f"Context: {company_context}\n"
    user_msg += "What is management actually signalling? 2-3 sentences."
    return call_claude(system, user_msg, max_tokens=250)
