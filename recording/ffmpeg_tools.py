from __future__ import annotations

import logging
import shlex
import shutil
import subprocess
from pathlib import Path


logger = logging.getLogger(__name__)


def run_ffmpeg(args: list[str]) -> None:
    cmd = ["ffmpeg", "-y", *args]
    logger.debug("Running ffmpeg command: %s", " ".join(shlex.quote(part) for part in cmd))
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        logger.error("ffmpeg failed: %s", result.stderr.strip())
        raise RuntimeError(f"ffmpeg failed with exit code {result.returncode}")


def write_concat_list(wavs: list[Path], concat_txt: Path) -> None:
    concat_txt.parent.mkdir(parents=True, exist_ok=True)
    lines = [f"file '{wav.resolve().as_posix()}'" for wav in wavs]
    concat_txt.write_text("\n".join(lines) + "\n", encoding="utf-8")


def concat_wavs_to_opus_ogg(wavs: list[Path], out_ogg: Path, bitrate: str = "32k") -> None:
    if not wavs:
        raise ValueError("No wav files supplied for concatenation")

    concat_txt = out_ogg.with_suffix(".concat.txt")
    write_concat_list(wavs, concat_txt)
    run_ffmpeg([
        "-f",
        "concat",
        "-safe",
        "0",
        "-i",
        str(concat_txt),
        "-c:a",
        "libopus",
        "-b:a",
        bitrate,
        "-ac",
        "1",
        str(out_ogg),
    ])


def mix_opus_ogg(inputs: list[Path], out_ogg: Path, bitrate: str = "48k") -> None:
    if not inputs:
        raise ValueError("No input files supplied for mix")
    if len(inputs) == 1:
        shutil.copy2(inputs[0], out_ogg)
        return

    args: list[str] = []
    for input_file in inputs:
        args.extend(["-i", str(input_file)])

    args.extend([
        "-filter_complex",
        f"amix=inputs={len(inputs)}:normalize=0",
        "-c:a",
        "libopus",
        "-b:a",
        bitrate,
        "-ac",
        "1",
        str(out_ogg),
    ])
    run_ffmpeg(args)
