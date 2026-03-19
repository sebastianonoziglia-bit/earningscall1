"""
Unified AI service — tries DeepSeek, Claude, OpenAI in order.
Uses whichever API key is available in environment.
All models receive same prompt format and return same output format.
"""
import os
import logging
logger = logging.getLogger(__name__)

# API priority order — cheapest/fastest first
_PROVIDER_ORDER = ["deepseek", "anthropic", "openai"]

def _get_available_provider() -> str | None:
    if os.environ.get("DEEPSEEK_API_KEY", "").strip():
        return "deepseek"
    if os.environ.get("ANTHROPIC_API_KEY", "").strip():
        return "anthropic"
    if os.environ.get("OPENAI_API_KEY", "").strip():
        return "openai"
    return None

def call_ai(
    system: str,
    user: str,
    max_tokens: int = 500,
    temperature: float = 0.3,
    provider: str | None = None,
) -> str:
    """
    Call the best available AI API.
    Returns the text response or empty string on failure.
    """
    if provider is None:
        provider = _get_available_provider()
    if provider is None:
        logger.warning("No AI API key available")
        return ""

    try:
        if provider == "deepseek":
            return _call_deepseek(system, user, max_tokens, temperature)
        elif provider == "anthropic":
            return _call_anthropic(system, user, max_tokens, temperature)
        elif provider == "openai":
            return _call_openai(system, user, max_tokens, temperature)
    except Exception as e:
        logger.warning("AI call failed (%s): %s — trying next provider", provider, e)
        # Try next provider in order
        remaining = [p for p in _PROVIDER_ORDER if p != provider]
        for fallback in remaining:
            try:
                if fallback == "deepseek" and os.environ.get("DEEPSEEK_API_KEY"):
                    return _call_deepseek(system, user, max_tokens, temperature)
                elif fallback == "anthropic" and os.environ.get("ANTHROPIC_API_KEY"):
                    return _call_anthropic(system, user, max_tokens, temperature)
                elif fallback == "openai" and os.environ.get("OPENAI_API_KEY"):
                    return _call_openai(system, user, max_tokens, temperature)
            except Exception:
                continue
    return ""


def _call_deepseek(system: str, user: str, max_tokens: int, temperature: float) -> str:
    import requests
    key = os.environ.get("DEEPSEEK_API_KEY", "").strip()
    if not key:
        raise ValueError("No DEEPSEEK_API_KEY")
    # DeepSeek has shorter effective context — truncate long prompts
    if len(user) > 6000:
        user = user[:6000] + "...[truncated]"
    resp = requests.post(
        "https://api.deepseek.com/v1/chat/completions",
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
        json={
            "model": "deepseek-chat",
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "max_tokens": max_tokens,
            "temperature": temperature,
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"].strip()


def _call_anthropic(system: str, user: str, max_tokens: int, temperature: float) -> str:
    import anthropic as _ant
    key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not key:
        raise ValueError("No ANTHROPIC_API_KEY")
    client = _ant.Anthropic(api_key=key)
    msg = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=max_tokens,
        temperature=temperature,
        system=system,
        messages=[{"role": "user", "content": user}],
    )
    return msg.content[0].text.strip()


def _call_openai(system: str, user: str, max_tokens: int, temperature: float) -> str:
    import openai as _oai
    key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not key:
        raise ValueError("No OPENAI_API_KEY")
    client = _oai.OpenAI(api_key=key)
    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        max_tokens=max_tokens,
        temperature=temperature,
    )
    return resp.choices[0].message.content.strip()


def is_ai_available() -> bool:
    return _get_available_provider() is not None


def get_active_provider() -> str:
    p = _get_available_provider()
    return p or "none"
