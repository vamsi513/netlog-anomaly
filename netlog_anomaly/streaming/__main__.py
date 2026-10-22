"""Lets the package be run as `python -m netlog_anomaly.streaming`."""

from __future__ import annotations

import sys

from netlog_anomaly.streaming.cli import main

if __name__ == "__main__":
    sys.exit(main())
