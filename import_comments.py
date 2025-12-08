import csv
import hashlib
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import List
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_batch
from connect_to_database_func import connect_db


# ----------------------------
#  Data model
# ----------------------------

@dataclass
class Comment:
    title: str
    grade: int
    description: str
    national_code_hash: str
    mobile_no_hash: str
    created_at: datetime
    date: str | None = None  # currently unused (same as Go code)


# ----------------------------
#  Helpers
# ----------------------------

def hash_string(value: str) -> str:
    """
    Same as Go HashString:
    - if len(input) < 10, left-pad with '0' to length 10
    - then SHA-256 hex digest
    """
    s = value.strip()
    if len(s) < 10:
        s = "0" * (10 - len(s)) + s
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def parse_timestamp(ts: str) -> datetime:
    """
    Python equivalent of Go parseTimestamp with a bit more robustness.
    Supports:
    - "2025-10-09-05.23.59.877774"  (Go-style)
    - "11/29/25 12:05 AM"           (your sample CSV)
    """
    ts = ts.strip()

    # Try Go-style: 2025-10-09-05.23.59.877774
    parts = ts.split("-", 3)
    if len(parts) == 4 and "." in parts[3]:
        year, month, day, time_part = parts
        # replace first 2 dots with colon: 05.23.59.877774 -> 05:23:59.877774
        time_part = time_part.replace(".", ":", 2)
        ts_norm = f"{year}-{month}-{day} {time_part}"
        try:
            return datetime.strptime(ts_norm, "%Y-%m-%d %H:%M:%S.%f")
        except ValueError:
            pass

    # Try format from your example: 11/29/25 12:05 AM
    for fmt in ("%m/%d/%y %I:%M %p", "%m/%d/%Y %I:%M %p"):
        try:
            return datetime.strptime(ts, fmt)
        except ValueError:
            continue

    raise ValueError(f"Unsupported timestamp format: {ts}")


# ----------------------------
#  DB functions
# ----------------------------

# Load environment variables from .env file
load_dotenv()

def get_db_connection():
    return connect_db()


def create_table(conn):
    create_sql = """
    CREATE TABLE IF NOT EXISTS comments (
        id SERIAL PRIMARY KEY,
        title VARCHAR(255),
        grade INTEGER,
        description TEXT,
        national_code_hash VARCHAR(64) NOT NULL,
        mobile_no_hash VARCHAR(64),
        created_at TIMESTAMP NOT NULL,
        date VARCHAR(50),
        imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        sentiment_result TEXT,
        sentiment_score INTEGER,
        second_model_processed BOOLEAN,
        is_repetitive BOOLEAN,
        duplicate_of INTEGER,
        UNIQUE(national_code_hash, created_at)
    );

    CREATE INDEX IF NOT EXISTS idx_comments_national_code_hash ON comments(national_code_hash);
    CREATE INDEX IF NOT EXISTS idx_comments_date ON comments(date);
    CREATE INDEX IF NOT EXISTS idx_comments_grade ON comments(grade);
    """
    with conn.cursor() as cur:
        cur.execute(create_sql)
    conn.commit()
    print("✅ Table 'comments' created or already exists")


# ----------------------------
#  CSV parsing
# ----------------------------

def parse_csv(filename: str) -> List[Comment]:
    """
    Expected columns (like Go):
    TITLE, GRADE, DESCRIPTION, NATIONAL_CODE, REAL_FIRST_NAME,
    REAL_LAST_NAME, MOBILE_NO, CREATED_AT, <EXTRA (Date or Channel)>
    Last column is ignored (same as Go, where Date is commented out).
    """
    comments: List[Comment] = []

    with open(filename, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        print("CSV Header:", header)

        for line_num, row in enumerate(reader, start=2):
            if len(row) < 9:
                print(f"⚠️  Skipping line {line_num}: insufficient columns ({len(row)} < 9)")
                continue

            title = row[0]
            grade_str = row[1]
            description = row[2]
            national_code = row[3]
            # row[4], row[5] = REAL_FIRST_NAME, REAL_LAST_NAME (ignored)
            mobile_no = row[6]
            created_at_str = row[7]
            # row[8] = Date or Channel (ignored for now, like Go)

            # grade
            try:
                grade = int(grade_str)
            except ValueError:
                print(f"⚠️  Line {line_num}: invalid grade '{grade_str}', using 0")
                grade = 0

            # timestamp
            try:
                created_at = parse_timestamp(created_at_str)
            except ValueError as e:
                print(f"⚠️  Line {line_num}: invalid timestamp '{created_at_str}': {e}, skipping")
                continue

            comment = Comment(
                title=title,
                grade=grade,
                description=description,
                national_code_hash=hash_string(national_code),
                mobile_no_hash=hash_string(mobile_no) if mobile_no.strip() else "",
                created_at=created_at,
                date=None,
            )
            comments.append(comment)

    print(f"✅ Successfully parsed {len(comments)} comments")
    return comments


# ----------------------------
#  Insert / upsert
# ----------------------------

def insert_comments(conn, comments: List[Comment]):
    """
    Equivalent to Go insertComments:
    - upsert on (national_code_hash, created_at)
    - skip if two consecutive created_at values differ < 50 ms
    """
    upsert_sql = """
    INSERT INTO comments (
        title, grade, description, national_code_hash, created_at
    ) VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (national_code_hash, created_at)
    DO UPDATE SET
        title = EXCLUDED.title,
        grade = EXCLUDED.grade,
        description = EXCLUDED.description,
        imported_at = CURRENT_TIMESTAMP;
    """

    filtered_rows = []
    for i, c in enumerate(comments):
        if i > 0:
            prev = comments[i - 1]
            delta_ms = abs((c.created_at - prev.created_at).total_seconds() * 1000)
            if delta_ms < 50:
                # same logic as Go: skip if within 50 milliseconds
                continue

        filtered_rows.append(
            (c.title, c.grade, c.description, c.national_code_hash, c.created_at)
        )

    with conn.cursor() as cur:
        execute_batch(cur, upsert_sql, filtered_rows, page_size=1000)
    conn.commit()
    print(f"✅ Completed: {len(filtered_rows)} comments inserted/updated")


