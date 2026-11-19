"""SQLite 持久化层。

单连接 + 可重入锁：所有读写串行；写事务使用 BEGIN IMMEDIATE，
即使未来跑多进程，也能在数据库层面串行化并发申请，守住多数票。
许可与探活报告均落盘，服务重启后保留。
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
from contextlib import contextmanager
from typing import Iterable

from .models import LatestReport, PermitRecord, now_utc, parse_iso, to_iso

SCHEMA = """
CREATE TABLE IF NOT EXISTS liveness_reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    node_id TEXT NOT NULL,
    sampled_at TEXT NOT NULL,   -- ISO8601 UTC，采样时间
    healthy INTEGER NOT NULL,   -- 1/0
    boot_id TEXT NOT NULL,      -- 启动标识
    received_at TEXT NOT NULL   -- 服务端接收时间（审计用）
);
CREATE INDEX IF NOT EXISTS idx_liveness_node_sampled
    ON liveness_reports(node_id, sampled_at);

CREATE TABLE IF NOT EXISTS permits (
    request_id TEXT PRIMARY KEY,          -- 申请号（幂等键）
    nodes TEXT NOT NULL,                  -- JSON 数组，有序去重
    status TEXT NOT NULL CHECK (status IN ('open', 'finished')),
    approved_at TEXT NOT NULL,
    finished_at TEXT,
    boot_ids_at_approval TEXT NOT NULL    -- JSON 对象 node -> boot_id
);
"""

# 每个节点取采样时间最新的一份；采样时间并列时取后写入的一条。
_LATEST_SQL = """
SELECT node_id, sampled_at, healthy, boot_id
FROM (
    SELECT node_id, sampled_at, healthy, boot_id,
           ROW_NUMBER() OVER (
               PARTITION BY node_id ORDER BY sampled_at DESC, id DESC
           ) AS rn
    FROM liveness_reports
) t
WHERE rn = 1
"""


class Store:
    def __init__(self, db_path: str):
        directory = os.path.dirname(os.path.abspath(db_path))
        os.makedirs(directory, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            db_path, check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA busy_timeout=5000")
            self._conn.executescript(SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    @contextmanager
    def transaction(self):
        """写事务：BEGIN IMMEDIATE 立即拿写锁，并发申请在此串行。"""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
            self._conn.execute("COMMIT")

    # ---- 探活报告 ----

    def add_reports(self, reports: Iterable) -> None:
        received = to_iso(now_utc())
        rows = [
            (r.node_id, to_iso(r.sampled_at), 1 if r.healthy else 0, r.boot_id, received)
            for r in reports
        ]
        with self.transaction():
            self._conn.executemany(
                "INSERT INTO liveness_reports "
                "(node_id, sampled_at, healthy, boot_id, received_at) "
                "VALUES (?,?,?,?,?)",
                rows,
            )

    def latest_reports(self) -> dict[str, LatestReport]:
        with self._lock:
            rows = self._conn.execute(_LATEST_SQL).fetchall()
        return {
            r["node_id"]: LatestReport(
                node_id=r["node_id"],
                sampled_at=parse_iso(r["sampled_at"]),
                healthy=bool(r["healthy"]),
                boot_id=r["boot_id"],
            )
            for r in rows
        }

    # ---- 许可 ----

    @staticmethod
    def _row_to_permit(row: sqlite3.Row) -> PermitRecord:
        return PermitRecord(
            request_id=row["request_id"],
            nodes=tuple(json.loads(row["nodes"])),
            status=row["status"],
            approved_at=parse_iso(row["approved_at"]),
            finished_at=parse_iso(row["finished_at"]) if row["finished_at"] else None,
            boot_ids_at_approval=json.loads(row["boot_ids_at_approval"]),
        )

    def get_permit(self, request_id: str) -> PermitRecord | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM permits WHERE request_id = ?", (request_id,)
            ).fetchone()
        return self._row_to_permit(row) if row else None

    def list_permits(self, status: str | None = None) -> list[PermitRecord]:
        with self._lock:
            if status:
                rows = self._conn.execute(
                    "SELECT * FROM permits WHERE status = ? ORDER BY approved_at, request_id",
                    (status,),
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT * FROM permits ORDER BY approved_at, request_id"
                ).fetchall()
        return [self._row_to_permit(r) for r in rows]

    def open_permits(self) -> list[PermitRecord]:
        return self.list_permits(status="open")

    def insert_permit(self, p: PermitRecord) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO permits "
                "(request_id, nodes, status, approved_at, finished_at, boot_ids_at_approval) "
                "VALUES (?,?,?,?,?,?)",
                (
                    p.request_id,
                    json.dumps(list(p.nodes)),
                    p.status,
                    to_iso(p.approved_at),
                    to_iso(p.finished_at) if p.finished_at else None,
                    json.dumps(p.boot_ids_at_approval),
                ),
            )

    def mark_finished(self, request_id: str, finished_at) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE permits SET status = 'finished', finished_at = ? "
                "WHERE request_id = ?",
                (to_iso(finished_at), request_id),
            )
