from __future__ import annotations

from pathlib import Path
from typing import Mapping


def require_model_files(node_name: str, required: Mapping[str, Path]) -> None:
    missing = [
        f"- {label}: {path}"
        for label, path in required.items()
        if not path.is_file()
    ]
    if not missing:
        return

    details = "\n".join(missing)
    raise FileNotFoundError(
        f"{node_name} required model files are missing:\n"
        f"{details}\n"
        "See models/MANIFEST.md for provisioning instructions."
    )
