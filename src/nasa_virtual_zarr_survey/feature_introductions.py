"""Reader for `config/feature_introductions.toml` -- hand-curated feature
annotations consumed by the `history` renderer to mark when each feature
shipped on the time-series chart."""

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from datetime import date
from pathlib import Path


class FeatureIntroductionsError(ValueError):
    pass


_VALID_PHASES = {"parse", "dataset", "datatree", "cubability"}


@dataclass(frozen=True)
class FeatureIntroduction:
    key: str
    phases: list[str]
    first_in_vz: str
    introduced: date
    description: str


def load_introductions(path: Path | str) -> list[FeatureIntroduction]:
    """Load feature introductions from TOML, sorted chronologically by `introduced`.

    Returns an empty list if the file does not exist or is empty.
    """
    p = Path(path)
    if not p.exists():
        return []
    raw = tomllib.loads(p.read_text())
    if not raw:
        return []

    intros: list[FeatureIntroduction] = []
    for key, entry in raw.items():
        if not isinstance(entry, dict):
            raise FeatureIntroductionsError(f"feature {key!r} must be a TOML table")
        phases = entry.get("phases")
        if not phases or not isinstance(phases, list):
            raise FeatureIntroductionsError(
                f"feature {key!r} must declare a non-empty phases list"
            )
        for phase in phases:
            if phase not in _VALID_PHASES:
                raise FeatureIntroductionsError(
                    f"feature {key!r}: unknown phase {phase!r} "
                    f"(valid: {sorted(_VALID_PHASES)})"
                )
        first_in_vz = entry.get("first_in_vz")
        if not first_in_vz:
            raise FeatureIntroductionsError(f"feature {key!r} missing first_in_vz")
        introduced_str = entry.get("introduced")
        if not introduced_str:
            raise FeatureIntroductionsError(f"feature {key!r} missing introduced date")
        try:
            introduced = date.fromisoformat(introduced_str)
        except ValueError as e:
            raise FeatureIntroductionsError(
                f"feature {key!r}: introduced must be ISO date; got {introduced_str!r}"
            ) from e
        description = entry.get("description", "")
        intros.append(
            FeatureIntroduction(
                key=key,
                phases=list(phases),
                first_in_vz=first_in_vz,
                introduced=introduced,
                description=description,
            )
        )

    intros.sort(key=lambda i: i.introduced)
    return intros
