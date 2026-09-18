"""Read-only, bounded audit of Codex rollout storage.

The audit never emits chat text or file names and reads at most the configured
prefix from each JSONL file.
"""

from __future__ import annotations

import argparse
import json
import time
from collections import Counter
from pathlib import Path


def audit(root: Path, prefix_bytes: int) -> dict[str, object]:
    totals: Counter[str] = Counter()
    providers: Counter[str] = Counter()
    largest = 0
    for area in ("sessions", "archived_sessions"):
        directory = root / area
        if not directory.exists():
            continue
        for path in directory.rglob("*.jsonl"):
            totals[f"{area}_files"] += 1
            try:
                size = path.stat().st_size
                largest = max(largest, size)
                totals["total_bytes"] += size
                if size > 32 * 1024 * 1024:
                    totals["metadata_only_candidates"] += 1
                with path.open("rb") as handle:
                    prefix = handle.read(prefix_bytes)
            except OSError:
                totals["unreadable_files"] += 1
                continue

            complete = prefix if prefix.endswith(b"\n") else prefix.rsplit(b"\n", 1)[0]
            found = False
            for raw_line in complete.splitlines():
                if not raw_line:
                    continue
                try:
                    record = json.loads(raw_line)
                except (UnicodeDecodeError, json.JSONDecodeError):
                    continue
                if record.get("type") != "session_meta":
                    continue
                payload = record.get("payload")
                if not isinstance(payload, dict) or not payload.get("id"):
                    continue
                found = True
                provider = payload.get("model_provider")
                providers[str(provider) if provider else "<missing>"] += 1
                break
            totals["header_session_meta"] += int(found)
            totals["header_missing_session_meta"] += int(not found)

    return {
        **totals,
        "total_gib": round(totals["total_bytes"] / 1024**3, 3),
        "largest_mib": round(largest / 1024**2, 3),
        "providers": dict(providers.most_common()),
        "prefix_limit_kib": prefix_bytes // 1024,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("codex_dir", type=Path)
    parser.add_argument("--prefix-kib", type=int, default=64)
    args = parser.parse_args()
    started = time.perf_counter()
    result = audit(args.codex_dir, max(1, args.prefix_kib) * 1024)
    result["elapsed_ms"] = round((time.perf_counter() - started) * 1000, 3)
    print(
        json.dumps(
            result,
            ensure_ascii=False,
            indent=2,
        )
    )
