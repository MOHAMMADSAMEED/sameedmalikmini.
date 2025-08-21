import sqlite3
from datetime import datetime, timedelta

DB_PATH = "events.db"

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        '''
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            event_datetime TEXT NOT NULL,
            description TEXT,
            advance_minutes INTEGER DEFAULT 10,
            notified INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        '''
    )
    conn.commit()
    conn.close()

def add_event(title: str, event_datetime: datetime, description: str = "", advance_minutes: int = 10):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO events(title, event_datetime, description, advance_minutes, notified) VALUES (?, ?, ?, ?, 0)",
        (title, event_datetime.isoformat(timespec="minutes"), description, int(advance_minutes)),
    )
    conn.commit()
    conn.close()

def list_events():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM events ORDER BY event_datetime ASC")
    rows = cur.fetchall()
    conn.close()
    return rows

def delete_event(event_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM events WHERE id = ?", (event_id,))
    conn.commit()
    conn.close()

def mark_notified(event_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE events SET notified = 1 WHERE id = ?", (event_id,))
    conn.commit()
    conn.close()

def reset_notified_for_future():
    """If an event is in the future but was previously notified, allow re-notify."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "UPDATE events SET notified = 0 WHERE datetime(event_datetime) > datetime('now') AND notified = 1"
    )
    conn.commit()
    conn.close()

def get_pending_events():
    """Return events that are not yet notified and have notification time <= now.
    notify_time = event_datetime - advance_minutes
    """
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM events WHERE notified = 0 ORDER BY event_datetime ASC")
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()

    now = datetime.now()
    due = []
    for r in rows:
        try:
            event_dt = datetime.fromisoformat(r["event_datetime"])
        except Exception:
            event_dt = datetime.strptime(r["event_datetime"], "%Y-%m-%d %H:%M")
        notify_time = event_dt - timedelta(minutes=int(r.get("advance_minutes", 10)))
        if now >= notify_time:
            r["_event_dt"] = event_dt
            r["_notify_time"] = notify_time
            due.append(r)
    return due
