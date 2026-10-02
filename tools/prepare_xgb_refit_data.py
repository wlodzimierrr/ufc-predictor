"""Prepare an isolated archive snapshot; this command cannot train or score."""

import argparse
import hashlib
import importlib.metadata
from pathlib import Path
import sys
import tomllib
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from modeling.refit_preflight import (
    ALGORITHM_VERSION, FEATURE_ORDER, FEATURE_VERSION, ROOT, SCHEMA_VERSION,
    SOURCE_MODE, PreflightError, fold_manifest, load_git_source,
    prepare_training_frame, publish_snapshot, json_bytes, validate_refit_config,
)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--event-cutoff", required=True)
    p.add_argument("--knowledge-cutoff", required=True)
    p.add_argument("--source-mode", required=True, choices=(SOURCE_MODE, "strict_replay"))
    p.add_argument("--source-ref", required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--config", type=Path, default=ROOT / "configs/xgb_refit_pre_april_2026.toml")
    args = p.parse_args()
    started_at = datetime.now(timezone.utc).isoformat()
    if args.output.exists():
        p.error("Output already exists; accepted snapshots are never overwritten")
    if args.source_mode != SOURCE_MODE:
        p.error("Strict replay unavailable: missing per-target source-version history")
    config = tomllib.loads(args.config.read_text())
    validate_refit_config(config)
    if (args.event_cutoff != config["event_cutoff_exclusive"] or
            args.knowledge_cutoff != config["knowledge_cutoff"] or args.source_mode != config["source_mode"]):
        p.error("CLI cutoffs/source mode differ from reviewed draft configuration")
    data, provenance, raw_files = load_git_source(source_ref=args.source_ref,
                                                knowledge_cutoff=args.knowledge_cutoff)
    if provenance["commit"] != config["source_ref"]:
        p.error("Git source differs from configured pinned archive")
    df, summary = prepare_training_frame(data, event_cutoff=args.event_cutoff,
                                         knowledge_cutoff=args.knowledge_cutoff,
                                         source_mode=args.source_mode)
    folds = fold_manifest(df, event_cutoff=args.event_cutoff)
    code_files = ["features/elo.py", "features/replay.py", "features/history.py", "features/snapshot.py",
                  "features/opponent.py", "features/pipeline.py", "features/career.py", "features/rolling.py",
                  "features/decay.py", "features/physical.py", "features/debut_prior.py", "modeling/holdout.py",
                  "modeling/data.py", "modeling/refit_preflight.py", "warehouse/transform.py",
                  "tools/prepare_xgb_refit_data.py"]
    manifest = {"schema_version": SCHEMA_VERSION, "feature_version": FEATURE_VERSION,
                "feature_algorithm_version": ALGORITHM_VERSION, "feature_order": FEATURE_ORDER,
                "event_cutoff_exclusive": args.event_cutoff, "knowledge_cutoff": args.knowledge_cutoff,
                "source_mode": args.source_mode, "source": provenance, "summary": summary,
                "certification_status": "chronological_retrospective_only",
                "strict_replay_ready": False, "phase3b_mode_approval_required": True,
                "label_mapping": {"1": "fighter_1 wins", "0": "fighter_2 wins"},
                "orientation": "archived source fighter_1/fighter_2; never winner-sorted by preparation",
                "decision_policy_version": "probability_band_v1",
                "computation_time": "Phase 3A regeneration; see audit capture, not knowledge availability",
                "code_sha256": {f: hashlib.sha256((ROOT / f).read_bytes()).hexdigest() for f in code_files},
                "config_sha256": hashlib.sha256(args.config.read_bytes()).hexdigest(),
                "package_versions": {name: importlib.metadata.version(name) for name in
                                     ("pandas", "numpy", "psycopg2-binary", "xgboost", "scikit-learn")}}
    hashes = publish_snapshot(df, destination=args.output, manifest=manifest,
                              folds=folds, source_files=raw_files)
    # A separate receipt records actual computation time while keeping the
    # data/fold/source manifests byte-reproducible across runs.
    receipt = {"started_at": started_at, "finished_at": datetime.now(timezone.utc).isoformat(),
               "manifest_sha256": hashes["manifest_sha256"],
               "meaning": "feature computation/publication time; not historical availability"}
    with (args.output / "build-receipt.json").open("xb") as f:
        f.write(json_bytes(receipt))
    (args.output / "build-receipt.json").chmod(0o444)
    print(json_bytes({"output": str(args.output), "summary": summary, "hashes": hashes}).decode())


if __name__ == "__main__":
    main()
