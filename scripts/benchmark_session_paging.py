"""Repeatable micro-benchmark for the session-list SQLite access pattern."""

from __future__ import annotations

import json
import argparse
import sqlite3
import statistics
import time
import tracemalloc
from pathlib import Path


SIZES = (100, 1_000, 5_000, 10_000)
RUNS = 9


def build_database(size: int) -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:")
    connection.execute(
        """CREATE TABLE threads (
            id TEXT PRIMARY KEY,
            title TEXT,
            first_user_message TEXT,
            preview TEXT,
            model_provider TEXT,
            model TEXT,
            cwd TEXT,
            rollout_path TEXT,
            updated_at_ms INTEGER,
            archived INTEGER,
            has_user_event INTEGER,
            thread_source TEXT,
            source TEXT
        )"""
    )
    rows = [
        (
            f"session-{index:08d}",
            f"Session {index}",
            f"Question {index}",
            f"Preview {index}",
            "openai",
            "gpt-5.6-sol",
            f"C:/projects/{index % 50}",
            f"C:/sessions/{index}.jsonl",
            2_000_000_000_000 - index,
            0,
            1,
            "subagent" if index % 20 == 0 else "cli",
            None,
        )
        for index in range(size)
    ]
    connection.executemany(
        "INSERT INTO threads VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows
    )
    connection.execute(
        "CREATE INDEX idx_threads_updated_at ON threads(updated_at_ms DESC, id ASC)"
    )
    connection.commit()
    return connection


def measure(operation) -> tuple[float, int]:
    timings = []
    peaks = []
    for _ in range(RUNS):
        tracemalloc.start()
        started = time.perf_counter()
        operation()
        timings.append((time.perf_counter() - started) * 1_000)
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        peaks.append(peak)
    return statistics.median(timings), max(peaks)


def benchmark(size: int) -> dict[str, float | int]:
    connection = build_database(size)

    def old_full_materialization() -> None:
        rows = connection.execute(
            "SELECT * FROM threads ORDER BY updated_at_ms DESC, id ASC"
        ).fetchall()
        rows[:100]

    def new_page_query() -> None:
        connection.execute(
            """SELECT
                   SUM(CASE WHEN lower(trim(thread_source)) = 'subagent' THEN 0 ELSE 1 END),
                   SUM(CASE WHEN lower(trim(thread_source)) = 'subagent' THEN 1 ELSE 0 END)
               FROM threads"""
        ).fetchone()
        connection.execute(
            """SELECT id, title, first_user_message, preview, model_provider, model,
                      cwd, rollout_path, updated_at_ms, archived, has_user_event
               FROM threads
               WHERE lower(trim(thread_source)) <> 'subagent'
               ORDER BY updated_at_ms DESC, id ASC
               LIMIT 101"""
        ).fetchall()

    old_full_materialization()
    new_page_query()
    old_ms, old_peak = measure(old_full_materialization)
    new_ms, new_peak = measure(new_page_query)
    connection.close()
    return {
        "sessions": size,
        "old_median_ms": round(old_ms, 3),
        "new_median_ms": round(new_ms, 3),
        "speedup": round(old_ms / new_ms, 2) if new_ms else 0,
        "old_peak_kib": round(old_peak / 1024, 1),
        "new_peak_kib": round(new_peak / 1024, 1),
        "peak_reduction": round(old_peak / new_peak, 2) if new_peak else 0,
    }


def benchmark_real_database(path: Path) -> dict[str, float | int | str]:
    uri = f"{path.resolve().as_uri()}?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    internal_expression = """
        CASE WHEN NULLIF(TRIM(CAST(t.thread_source AS TEXT)), '') IS NOT NULL
        THEN (LOWER(TRIM(CAST(t.thread_source AS TEXT))) = 'subagent'
              OR INSTR(LOWER(CAST(t.thread_source AS TEXT)), '"subagent"') > 0)
        ELSE CASE WHEN NULLIF(TRIM(CAST(t.source AS TEXT)), '') IS NOT NULL
             THEN (LOWER(TRIM(CAST(t.source AS TEXT))) = 'subagent'
                   OR INSTR(LOWER(CAST(t.source AS TEXT)), '"subagent"') > 0)
             ELSE session_edges.child_thread_id IS NOT NULL END END
    """
    internal_join = """
        LEFT JOIN (SELECT DISTINCT child_thread_id FROM thread_spawn_edges)
        AS session_edges ON session_edges.child_thread_id = t.id
    """

    def old_full_materialization() -> None:
        connection.execute(
            "SELECT * FROM threads ORDER BY updated_at_ms DESC, id ASC"
        ).fetchall()

    def new_page_query() -> None:
        connection.execute(
            f"""SELECT
                    COALESCE(SUM(CASE WHEN ({internal_expression}) THEN 0 ELSE 1 END), 0),
                    COALESCE(SUM(CASE WHEN ({internal_expression}) THEN 1 ELSE 0 END), 0)
                FROM threads AS t {internal_join}"""
        ).fetchone()
        connection.execute(
            f"""SELECT t.id, t.title, t.first_user_message, t.preview,
                      t.model_provider, t.model, t.cwd, t.rollout_path,
                      t.updated_at_ms, t.archived, t.has_user_event
               FROM threads AS t {internal_join}
               WHERE NOT ({internal_expression})
               ORDER BY updated_at_ms DESC, id ASC
               LIMIT 101"""
        ).fetchall()

    total = connection.execute("SELECT COUNT(*) FROM threads").fetchone()[0]
    old_full_materialization()
    new_page_query()
    old_ms, old_peak = measure(old_full_materialization)
    new_ms, new_peak = measure(new_page_query)
    connection.close()
    return {
        "database": str(path),
        "sessions": total,
        "old_median_ms": round(old_ms, 3),
        "new_median_ms": round(new_ms, 3),
        "speedup": round(old_ms / new_ms, 2) if new_ms else 0,
        "old_peak_kib": round(old_peak / 1024, 1),
        "new_peak_kib": round(new_peak / 1024, 1),
        "peak_reduction": round(old_peak / new_peak, 2) if new_peak else 0,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", type=Path)
    args = parser.parse_args()
    result = (
        benchmark_real_database(args.database)
        if args.database
        else [benchmark(size) for size in SIZES]
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
