"""CLI to seed the database from a JSON events file.

Usage:
    python -m scripts.load_sample_data [path/to/events.json] [--force]
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Make repo root importable when invoked as a script.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.database import init_db  # noqa: E402
from app.seed import seed_from_file  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Seed events into the database.")
    parser.add_argument(
        "path",
        nargs="?",
        default="sample_events.json",
        help="Path to the JSON events file (default: sample_events.json)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Seed even if the events table already contains rows.",
    )
    args = parser.parse_args()

    init_db()
    result = seed_from_file(args.path, force=args.force)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
