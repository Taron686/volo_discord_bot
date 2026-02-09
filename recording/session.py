from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo


BERLIN_TZ = ZoneInfo("Europe/Berlin")


@dataclass
class SessionContext:
    session_id: str
    session_dir: Path
    chunks_dir: Path
    audio_dir: Path
    chunk_index: int = 0
    transcript_lines: list[str] = field(default_factory=list)
    display_names: dict[int, str] = field(default_factory=dict)


def create_session_id() -> str:
    return datetime.now(BERLIN_TZ).strftime("%Y-%m-%d_%H-%M-%S")


def safe_filename(name: str) -> str:
    sanitized = re.sub(r"[^a-zA-Z0-9._-]+", "_", name.strip())
    sanitized = sanitized.strip("._")
    return sanitized or "unknown"


def init_session(base_dir: str = "/data/sessions") -> SessionContext:
    session_id = create_session_id()
    session_dir = Path(base_dir) / session_id
    chunks_dir = session_dir / "chunks"
    audio_dir = session_dir / "audio"
    chunks_dir.mkdir(parents=True, exist_ok=True)
    audio_dir.mkdir(parents=True, exist_ok=True)
    return SessionContext(
        session_id=session_id,
        session_dir=session_dir,
        chunks_dir=chunks_dir,
        audio_dir=audio_dir,
    )
