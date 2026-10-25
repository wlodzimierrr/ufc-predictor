"""Build a minimal local refresh-input candidate; this does not approve its inputs.

A new output directory is reserved exclusively. Failures leave an explicit
incomplete inventory; only a finished candidate has manifest.json and no marker.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import stat
import sys

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.automation.prepare_runtime import DATA_FILES, validate_bundle

MANIFEST = "manifest.json"
INCOMPLETE = ".incomplete.json"


class BundleError(ValueError):
    """Fixed failure codes only; source paths and values stay out of logs."""


@dataclass(frozen=True)
class SourceFile:
    source: Path
    relative: Path
    signature: tuple
    sha256: str

    def entry(self) -> dict:
        return {"path": self.relative.as_posix(), "size_bytes": self.signature[3],
                "mode": self.signature[2], "sha256": self.sha256,
                "source_sha256": self.sha256}


def signature(info: os.stat_result) -> tuple:
    if not stat.S_ISREG(info.st_mode):
        raise BundleError("source_not_regular_file")
    return (info.st_dev, info.st_ino, stat.S_IMODE(info.st_mode), info.st_size,
            info.st_mtime_ns, info.st_ctime_ns)


def reject_symlinks(root: Path) -> None:
    if any(path.is_symlink() for path in (root, *root.parents)):
        raise BundleError("source_symlink")
    if not root.is_dir():
        raise BundleError("source_directory_missing")
    if any(path.is_symlink() for path in root.rglob("*")):
        raise BundleError("source_symlink")


def inventory(data: Path, models: Path) -> tuple[dict, dict, Path, dict[Path, Path]]:
    reject_symlinks(data)
    reject_symlinks(models)
    counts, pointer, suffix = validate_bundle(data, models)
    paths = {Path("data") / name: data / name for name in DATA_FILES}
    paths[Path("models/production_model.json")] = models / "production_model.json"
    artifact = models / suffix
    for source in sorted(artifact.rglob("*")):
        if source.is_symlink():
            raise BundleError("source_symlink")
        if source.is_dir():
            continue
        if not source.is_file():
            raise BundleError("source_not_regular_file")
        paths[Path("models") / source.relative_to(models)] = source
    return counts, pointer, suffix, paths


def read_source(source: Path, *, expected: SourceFile | None = None,
                destination: Path | None = None) -> tuple[tuple, str]:
    """Hash a regular source, detecting replacement/changes before and during IO."""
    # O_NOFOLLOW prevents a source file replaced by a link from being read.
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    with os.fdopen(os.open(source, flags), "rb") as stream:
        before = signature(os.fstat(stream.fileno()))
        if expected is not None and before != expected.signature:
            raise BundleError("source_changed")
        hasher = hashlib.sha256()
        output = destination.open("xb") if destination is not None else None
        try:
            while chunk := stream.read(1024 * 1024):
                hasher.update(chunk)
                if output is not None:
                    output.write(chunk)
            if output is not None:
                output.flush()
                os.fchmod(output.fileno(), before[2])
            if signature(os.fstat(stream.fileno())) != before or signature(source.lstat()) != before:
                raise BundleError("source_changed")
        finally:
            if output is not None:
                output.close()
        digest = hasher.hexdigest()
        if expected is not None and digest != expected.sha256:
            raise BundleError("source_changed")
        return before, digest


def copy_checked(item: SourceFile, output: Path) -> None:
    destination = output / item.relative
    destination.parent.mkdir(parents=True, exist_ok=True)
    read_source(item.source, expected=item, destination=destination)
    info, digest = read_source(destination)
    if digest != item.sha256 or info[2:4] != item.signature[2:4]:
        raise BundleError("copied_file_mismatch")


def write_json(path: Path, value: dict) -> None:
    with path.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.write("\n")


def build_input_bundle(data_source: Path, models_source: Path, output: Path) -> dict:
    data_source, models_source, output = map(Path, (data_source, models_source, output))
    if output.is_symlink():
        raise BundleError("output_already_exists")
    # Inspect original paths before resolving so source-root links are rejected.
    reject_symlinks(data_source.absolute())
    reject_symlinks(models_source.absolute())
    data, models, output = (path.resolve() for path in (data_source, models_source, output))
    for source in (data, models):
        if output == source or source in output.parents or output in source.parents:
            raise BundleError("output_source_overlap")
    if output.exists():
        raise BundleError("output_already_exists")
    counts, pointer, suffix, paths = inventory(data, models)
    snapshots = []
    for relative, source in sorted(paths.items()):
        info, digest = read_source(source)
        snapshots.append(SourceFile(source, relative, info, digest))
    manifest = {"schema_version": 1, "status": "candidate", "review_status": "not_reviewed",
                "hash_algorithm": "sha256", "selected_artifact": (Path("models") / suffix).as_posix(),
                "input_rows": {(Path("data") / name).as_posix(): count for name, count in counts.items()},
                "files": [item.entry() for item in snapshots]}
    # mkdir is the reservation: unlike rename, it cannot replace a concurrently
    # created empty directory. Never delete or reuse an output after any failure.
    output.mkdir(parents=True, exist_ok=False)
    write_json(output / INCOMPLETE, dict(manifest, status="incomplete"))
    for item in snapshots:
        copy_checked(item, output)
    final_counts, final_pointer, final_suffix, final_paths = inventory(data, models)
    if (final_counts, final_pointer, final_suffix, final_paths) != (counts, pointer, suffix, paths):
        raise BundleError("source_inventory_changed")
    for item in snapshots:
        read_source(item.source, expected=item)
    copied_counts, copied_pointer, copied_suffix = validate_bundle(output / "data", output / "models")
    if (copied_counts, copied_pointer, copied_suffix) != (counts, pointer, suffix):
        raise BundleError("copied_bundle_mismatch")
    write_json(output / MANIFEST, manifest)
    (output / INCOMPLETE).unlink()
    return manifest


def main() -> int:
    root = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-source", type=Path, default=root / "data")
    parser.add_argument("--models-source", type=Path, default=root / "models")
    parser.add_argument("--output", type=Path, required=True, help="New candidate directory; never overwritten")
    args = parser.parse_args()
    try:
        manifest = build_input_bundle(args.data_source, args.models_source, args.output)
    except Exception as exc:
        code = f" code={exc}" if isinstance(exc, BundleError) else ""
        print(f"bundle status=failed error_category={type(exc).__name__}{code}", file=sys.stderr)
        return 1
    print(f"bundle status=candidate review_status=not_reviewed files={len(manifest['files'])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
