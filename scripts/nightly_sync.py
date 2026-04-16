#!/usr/bin/env python3
"""Nightly sync CLI wrapper.

Usage:
    python scripts/nightly_sync.py --dsn postgresql://... [--db-path data/graph.db]
    python scripts/nightly_sync.py --dry-run --verbose

See ``semantic_index.nightly_sync`` for implementation.
"""

from semantic_index.nightly_sync import main

if __name__ == "__main__":
    main()
