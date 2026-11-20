"""Command line entry point: ``python -m generator``."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from generator import config as cfg
from generator.simulation import Settings, parse_month, simulate
from generator.writer import write_dataset


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="python -m generator",
        description="Generate synthetic retail banking source data as CSV files.",
    )
    parser.add_argument("--output-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--seed", type=int, default=42, help="same seed, same data")
    parser.add_argument("--start-month", default="2023-01", metavar="YYYY-MM")
    parser.add_argument("--end-month", default="2025-12", metavar="YYYY-MM")
    parser.add_argument(
        "--scale",
        type=float,
        default=1.0,
        help="multiplier on the number of customers acquired per month",
    )
    parser.add_argument(
        "--no-dirt", action="store_true", help="skip the deliberate data quality problems"
    )
    args = parser.parse_args()

    settings = Settings(
        seed=args.seed,
        start_month=parse_month(args.start_month),
        end_month=parse_month(args.end_month),
        scale=args.scale,
        dirt=cfg.DirtRates.none() if args.no_dirt else cfg.DirtRates(),
    )
    if settings.end_month < settings.start_month:
        parser.error("--end-month must not be earlier than --start-month")

    manifest = write_dataset(simulate(settings), settings, args.output_dir)
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
