"""Manual exact-file CLI for materializing one signal-session tick panel."""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Sequence

from india_swing.reference_data.security_master import NseCmSecurityMasterParser

from .signal_tick import (
    LocalForwardPaperSignalTickPanelStore,
    materialize_forward_paper_signal_tick_panel,
)


def _timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp must include an offset")
    return parsed


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Materialize one exact signal-session tick artifact."
    )
    parser.add_argument("--security-master", type=Path, required=True)
    parser.add_argument("--knowledge-time", type=_timestamp, required=True)
    parser.add_argument("--cutoff", type=_timestamp, required=True)
    parser.add_argument("--state-root", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    try:
        arguments = _parser().parse_args(argv)
        master = arguments.security_master.resolve(strict=True)
        root = arguments.state_root.resolve(strict=True)
        parsed = NseCmSecurityMasterParser().parse_bytes(
            master.read_bytes(), original_filename=master.name
        )
        panel = materialize_forward_paper_signal_tick_panel(
            parsed,
            knowledge_time=arguments.knowledge_time,
            cutoff=arguments.cutoff,
        )
        stored = LocalForwardPaperSignalTickPanelStore(root).put(panel)
        print(
            json.dumps(
                {
                    "status": "FORWARD_PAPER_SIGNAL_TICK_READY",
                    "panel_id": stored.panel_id,
                    "signal_session": stored.signal_session.isoformat(),
                    "entry_count": len(stored.entries),
                    "excluded_record_count": stored.excluded_record_count,
                    "source_security_master_id": stored.source_security_master_id,
                    "knowledge_time": stored.knowledge_time.isoformat(),
                    "cutoff": stored.cutoff.isoformat(),
                },
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        return 0
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception:
        print(
            json.dumps(
                {
                    "status": "FAILED",
                    "error_type": "ForwardPaperSignalTickMaterializationFailed",
                },
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
