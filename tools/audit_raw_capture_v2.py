"""Offline v2 capture inventory; stdout JSON only, no acquisition or replay.

Use the installed scraper environment. There is deliberately no output-file
option: callers control stdout redirection outside the capture namespace.
"""

import argparse
import json
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.dont_write_bytecode = True
sys.path.insert(0, str(ROOT / "scraper/UFC-Web-Scraping-main/ufc_scraper"))

from ufc_scraper.capture_inventory_v2 import inspect_receipt, inventory  # noqa: E402


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    listing = commands.add_parser("inventory", help="List all reservations, including refused entries")
    listing.add_argument("--root", required=True, type=Path)
    inspection = commands.add_parser("inspect", help="Verify an exact receipt against a caller-supplied pin")
    inspection.add_argument("--root", required=True, type=Path)
    inspection.add_argument("--receipt", required=True, help="Exact relative observations/job/observation/receipt.json")
    inspection.add_argument("--receipt-sha256", required=True, help="Independently trusted receipt SHA-256")
    inspection.add_argument("--forensic", action="store_true", help="Permit failed/cached bytes with their original labels")
    args = parser.parse_args(argv)
    if args.command == "inventory":
        result = inventory(args.root)
    else:
        result = inspect_receipt(args.root, args.receipt, args.receipt_sha256, forensic=args.forensic)
    print(json.dumps(result, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False))
    return 0 if result["status"] in {"INVENTORIED", "NO_OBSERVATIONS", "VERIFIED_CAPTURE_BYTES"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
