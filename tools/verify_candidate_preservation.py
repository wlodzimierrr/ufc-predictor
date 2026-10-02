"""Hash-only preservation check, separate from candidate training/preprocessing.

Protected outcome/evidence bytes are hashed for preservation, never parsed or
passed to training. This command writes only into the new incomplete run.
"""

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
POINTER_SHA256 = "ff5272e0d2f11f9f6cbc6597f91a2cf9961ae317f8dd47f90bfb1f6e44dd5caa"


def verify(baseline_path: Path, run: Path) -> dict:
    baseline_raw = baseline_path.read_bytes()
    baseline = json.loads(baseline_raw)
    if (not run.resolve().is_relative_to(ROOT / "data/experiments/phase3b_xgb_pre_april_2026")
            or not (run / "INCOMPLETE.json").is_file()):
        raise ValueError("Preservation output must be an incomplete isolated candidate run")
    changed, missing = [], []
    for name, expected in baseline["files"].items():
        path = ROOT / name
        if not path.is_file():
            missing.append(name)
        elif hashlib.sha256(path.read_bytes()).hexdigest() != expected:
            changed.append(name)
    strict_groups = ["models", "data/holdouts", "data/audits",
        "data/experiments/phase3a_pre_april_2026_git_1f477d3",
        "data/experiments/phase3a_pre_april_2026_git_1f477d3_metadata_safe"]
    new_protected = []
    for group in strict_groups:
        for path in (ROOT / group).rglob('*'):
            if path.is_file() and '__pycache__' not in path.parts:
                name = str(path.relative_to(ROOT))
                if name not in baseline["files"]:
                    new_protected.append(name)
    pointer = hashlib.sha256((ROOT / "models/production_model.json").read_bytes()).hexdigest()
    success = not changed and not missing and not new_protected and pointer == POINTER_SHA256
    result = {"status": "VERIFIED" if success else "FAILED", "checked_at": datetime.now(timezone.utc).isoformat(),
        "baseline_sha256": hashlib.sha256(baseline_raw).hexdigest(),
        "existing_files_checked": len(baseline["files"]), "changed": changed, "missing": missing,
        "new_protected_files": sorted(new_protected), "production_pointer_sha256": pointer,
        "meaning": "hash-only independent preservation check; no outcome interpretation"}
    (run / "preservation_check.json").write_text(json.dumps(result, indent=2, sort_keys=True) + '\n')
    if not success:
        raise ValueError(f"Preservation failed: {result}")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", required=True, type=Path)
    parser.add_argument("--run", required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(verify(args.baseline, args.run), indent=2))
