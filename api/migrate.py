"""Database migrations for the API.

Run standalone: python -m api.migrate
Adds the closing_lines table and game_start_time column.
"""

import csv
import os
import sqlite3
import sys
from pathlib import Path

# Resolve DB path
_SERVER_DB = "/opt/polymarket-collector/data/polymarket_esports.db"
_LOCAL_DB = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                         "data", "polymarket_esports.db")
DB_PATH = _SERVER_DB if os.path.exists(_SERVER_DB) else _LOCAL_DB


def migrate(db_path: str | None = None):
    """Run all migrations."""
    path = db_path or DB_PATH
    if not os.path.exists(path):
        print(f"Database not found at {path}")
        sys.exit(1)

    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # 1. Add game_start_time column to markets table if missing
    cursor.execute("PRAGMA table_info(markets)")
    existing_cols = {row["name"] for row in cursor.fetchall()}

    if "game_start_time" not in existing_cols:
        print("Adding game_start_time column to markets table...")
        cursor.execute("ALTER TABLE markets ADD COLUMN game_start_time TEXT")
        conn.commit()
        print("  Done.")
    else:
        print("game_start_time column already exists on markets.")

    # 2. Create closing_lines table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS closing_lines (
            id INTEGER PRIMARY KEY,
            game_id TEXT,
            market_id TEXT,
            home_team TEXT,
            away_team TEXT,
            team TEXT,
            is_home INTEGER,
            question TEXT,
            game_start_time TEXT,
            closing_price REAL,
            min_price REAL,
            max_price REAL,
            final_score TEXT,
            team_won INTEGER,
            n_trades INTEGER
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_closing_lines_game_id ON closing_lines(game_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_closing_lines_gst ON closing_lines(game_start_time)")
    conn.commit()
    print("closing_lines table ready.")

    # 3. Import from CSV if table is empty and CSV exists
    row = cursor.execute("SELECT COUNT(*) as cnt FROM closing_lines").fetchone()
    if row["cnt"] == 0:
        csv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                "data", "closing_lines_full.csv")
        if os.path.exists(csv_path):
            print(f"Importing closing lines from {csv_path}...")
            imported = 0
            with open(csv_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row_data in reader:
                    # Convert is_home from True/False string to 0/1
                    is_home = 1 if row_data.get("is_home", "").lower() == "true" else 0
                    # Convert team_won
                    tw_raw = row_data.get("team_won", "").strip()
                    if tw_raw.lower() == "true":
                        team_won = 1
                    elif tw_raw.lower() == "false":
                        team_won = 0
                    else:
                        team_won = None

                    cursor.execute("""
                        INSERT INTO closing_lines
                        (game_id, market_id, home_team, away_team, team,
                         is_home, question, game_start_time,
                         closing_price, min_price, max_price,
                         final_score, team_won, n_trades)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        row_data.get("game_id"),
                        row_data.get("market_id"),
                        row_data.get("home_team"),
                        row_data.get("away_team"),
                        row_data.get("team"),
                        is_home,
                        row_data.get("question"),
                        row_data.get("game_start_time"),
                        float(row_data.get("closing_price", 0)),
                        float(row_data.get("min_price", 0)),
                        float(row_data.get("max_price", 0)),
                        row_data.get("final_score"),
                        team_won,
                        int(row_data.get("n_trades", 0)),
                    ))
                    imported += 1

            conn.commit()
            print(f"  Imported {imported} rows.")
        else:
            print(f"  No CSV found at {csv_path}, skipping import.")
    else:
        print(f"closing_lines table already has {row['cnt']} rows, skipping import.")

    conn.close()
    print("Migration complete.")


if __name__ == "__main__":
    db_arg = sys.argv[1] if len(sys.argv) > 1 else None
    migrate(db_arg)
