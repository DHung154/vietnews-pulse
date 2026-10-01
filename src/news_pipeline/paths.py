"""Path safety checks used before any manifest or output writer runs."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable


def _within(path: Path, parent: Path) -> bool:
    return path == parent or parent in path.parents


def validate_artifact_paths(
    *,
    inputs: Iterable[Path],
    outputs: Iterable[Path],
    evidence: Path,
    staging: Iterable[Path] = (),
) -> None:
    input_paths = [path.resolve() for path in inputs]
    output_paths = [path.resolve() for path in outputs]
    staging_paths = [path.resolve() for path in staging]
    evidence_path = evidence.resolve()
    if evidence_path.exists() and evidence_path.is_dir():
        raise ValueError(f"evidence must be a file path, not a directory: {evidence_path}")
    all_paths = input_paths + output_paths + staging_paths
    for path in all_paths:
        if evidence_path == path:
            raise ValueError("evidence path aliases an input/output/staging path")
    for parent in input_paths + output_paths + staging_paths:
        if _within(evidence_path, parent):
            raise ValueError(f"evidence path must not be inside {parent}")
    groups = ([("input", path) for path in input_paths] + [("output", path) for path in output_paths] + [("staging", path) for path in staging_paths])
    for index, (left_kind, left) in enumerate(groups):
        for right_kind, right in groups[index + 1 :]:
            if left == right or _within(left, right) or _within(right, left):
                raise ValueError(f"{left_kind} overlaps {right_kind}: {left} / {right}")
    if evidence_path.exists():
        for path in input_paths:
            if path.exists() and evidence_path.exists():
                try:
                    if os.path.samefile(path, evidence_path):
                        raise ValueError("evidence path aliases input")
                except OSError:
                    pass


def validate_evidence_only(evidence: Path, *parents: Path) -> None:
    validate_artifact_paths(inputs=(), outputs=parents, evidence=evidence)
