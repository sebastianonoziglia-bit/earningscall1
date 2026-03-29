"""
Lightweight logo loader for the Welcome page.
This avoids importing AI/chat/data modules that can trigger heavy initialization.
"""

import base64
import logging
import os
from io import BytesIO
from pathlib import Path

import streamlit as st
from PIL import Image

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@st.cache_data(ttl=3600)
def load_company_logos():
    """Load and cache company logos - reusable across pages."""
    try:
        # Use absolute path so it works regardless of cwd (HF vs local)
        _ASSETS = Path(__file__).resolve().parent.parent / "attached_assets"

        def _first_existing(*candidates: str):
            for p in candidates:
                full = _ASSETS / p if not Path(p).is_absolute() else Path(p)
                if full.exists():
                    return str(full)
            return None

        def _a(name: str) -> str:
            """Resolve asset path to absolute."""
            return str(_ASSETS / name)

        logo_files = {
            'Apple': _a('8.png'),
            'Microsoft': _a('msft.png'),
            'Alphabet': _a('10.png'),
            'Netflix': _a('9.png'),
            'Meta Platforms': _a('12.png'),
            'WhatsApp': _first_existing('Whatsapp.png', 'WhatsApp.png', '12.png'),
            'Instagram': _first_existing('Instagram.png', '12.png'),
            'Facebook': _first_existing('Facebook.png', '12.png'),
            'Amazon': _a('Amazon_icon.png'),
            'Disney': _a('icons8-logo-disney-240.png'),
            'Roku': _a('rokudef.png'),
            'Spotify': _a('11.png'),
            'Comcast': _a('6.png'),
            'Paramount': _a('Paramount.png'),
            'Paramount Global': _a('Paramount.png'),
            'Warner Bros. Discovery': _a('adadad.png'),
            'Warner Bros Discovery': _a('adadad.png'),
            'YouTube': _first_existing('Youtube.png', 'youtube.png', 'youtube_logo.png'),
            'Samsung': _first_existing('samsung.png', '1920px-Samsung_logo_blue.png'),
            'Tencent': _first_existing('Tencent.png'),
            'Nvidia': _first_existing('Nvidia.png'),
        }

        for company, logo_path in logo_files.items():
            if logo_path and os.path.exists(logo_path):
                logger.info(f"Logo file exists for {company}: {logo_path}")
            else:
                logger.warning(f"Logo file MISSING for {company}: {logo_path}")

        logos_dict = {}
        for company, logo_path in logo_files.items():
            if not logo_path or not os.path.exists(logo_path):
                continue
            try:
                img = Image.open(logo_path)
                buffered = BytesIO()
                img.save(buffered, format="PNG")
                img_str = base64.b64encode(buffered.getvalue()).decode()
                logos_dict[company] = img_str
                logger.info(f"Successfully loaded logo for {company}")
            except Exception as e:
                logger.error(f"Error loading logo for {company}: {str(e)}")

        return logos_dict
    except Exception as e:
        logger.error(f"Error loading company logos: {str(e)}")
        return {}
