import os
import sqlite3
import threading
import time
from pathlib import Path


def default_cache_path():
    if os.name == "nt":
        root = os.environ.get("LOCALAPPDATA") or str(Path.home())
        folder = Path(root) / "USA Lead Finder"
    else:
        folder = Path.home() / ".local" / "share" / "USA Lead Finder"

    folder.mkdir(parents=True, exist_ok=True)
    return folder / "lead_seen_cache.db"


class SeenLeadCache:
    """Persistent query-scoped cache for leads already returned to the user."""

    def __init__(self, path=None, max_rows=20000):
        self.path = str(path or default_cache_path())
        self.max_rows = max(2000, int(max_rows))
        self._lock = threading.RLock()
        self._initialize()

    def _connect(self):
        conn = sqlite3.connect(self.path, timeout=15)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=15000")
        return conn

    def _initialize(self):
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS seen_leads (
                    scope_key TEXT NOT NULL,
                    lead_key TEXT NOT NULL,
                    source TEXT,
                    name TEXT,
                    first_seen INTEGER NOT NULL,
                    last_seen INTEGER NOT NULL,
                    PRIMARY KEY (scope_key, lead_key)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_seen_last ON seen_leads(last_seen)"
            )

    @staticmethod
    def scope_key(keyword, country, city, state, zip_code):
        return "|".join(
            str(value or "").strip().lower()
            for value in [keyword, country, city, state, zip_code]
        )

    def load_scope_keys(self, scope_key):
        """Load once per search, so cache filtering happens in RAM while scraping."""
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                "SELECT lead_key FROM seen_leads WHERE scope_key=?",
                (scope_key,),
            ).fetchall()
        return {row[0] for row in rows}

    def add_many(self, scope_key, leads_and_keys):
        now = int(time.time())
        rows = []

        for lead, lead_key in leads_and_keys:
            if not lead_key:
                continue
            rows.append(
                (
                    scope_key,
                    lead_key,
                    str(lead.get("source") or lead.get("sources") or ""),
                    str(lead.get("name") or ""),
                    now,
                    now,
                )
            )

        if not rows:
            return

        with self._lock, self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO seen_leads(
                    scope_key, lead_key, source, name, first_seen, last_seen
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(scope_key, lead_key)
                DO UPDATE SET
                    last_seen=excluded.last_seen,
                    source=excluded.source,
                    name=excluded.name
                """,
                rows,
            )

            count = conn.execute("SELECT COUNT(*) FROM seen_leads").fetchone()[0]
            if count > self.max_rows:
                conn.execute(
                    """
                    DELETE FROM seen_leads
                    WHERE rowid IN (
                        SELECT rowid FROM seen_leads
                        ORDER BY last_seen ASC
                        LIMIT ?
                    )
                    """,
                    (count - self.max_rows,),
                )

    def count_scope(self, scope_key):
        with self._lock, self._connect() as conn:
            return conn.execute(
                "SELECT COUNT(*) FROM seen_leads WHERE scope_key=?",
                (scope_key,),
            ).fetchone()[0]

    def clear_scope(self, scope_key):
        with self._lock, self._connect() as conn:
            conn.execute("DELETE FROM seen_leads WHERE scope_key=?", (scope_key,))

    def stats(self):
        with self._lock, self._connect() as conn:
            total = conn.execute("SELECT COUNT(*) FROM seen_leads").fetchone()[0]
        return {"total": total, "max_rows": self.max_rows, "path": self.path}
