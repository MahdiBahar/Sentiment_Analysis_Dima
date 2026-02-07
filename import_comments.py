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
    channel_code: str | None = None # add channel_code
    # date: str | None = None  # currently unused (same as Go code)


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
    ts = ts.strip()

    # Go-style: 2025-12-24-00.02.26.367590
    if len(ts) >= 23 and ts[10] == "-" and "." in ts:
        try:
            date_part, time_part = ts[:10], ts[11:]
            time_part = time_part.replace(".", ":", 2)
            dt = datetime.strptime(
                f"{date_part} {time_part}",
                "%Y-%m-%d %H:%M:%S.%f"
            )
            return dt.replace(microsecond=0)
        except ValueError:
            pass

    formats = [
        "%m/%d/%y %I:%M %p",
        "%m/%d/%Y %I:%M %p",
        "%m/%d/%y %H:%M",
        "%m/%d/%Y %H:%M",
        "%m/%d/%y %H:%M:%S",
        "%m/%d/%Y %H:%M:%S",
    ]

    for fmt in formats:
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
    CREATE TABLE IF NOT EXISTS dima_comments (
        id SERIAL PRIMARY KEY,
        title VARCHAR(255),
        grade INTEGER,
        description TEXT,
        national_code_hash VARCHAR(64) NOT NULL,
        mobile_no_hash VARCHAR(64),
        created_at TIMESTAMP NOT NULL,
        imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        sentiment_result TEXT,
        sentiment_score INTEGER,
        second_model_processed BOOLEAN,
        is_repetitive BOOLEAN,
        duplicate_of INTEGER,
        channel_code VARCHAR(50),
        UNIQUE(national_code_hash, created_at)
    );

    CREATE INDEX IF NOT EXISTS idx_comments_national_code_hash ON dima_comments(national_code_hash);
    CREATE INDEX IF NOT EXISTS idx_comments_grade ON dima_comments(grade);
    CREATE INDEX IF NOT EXISTS idx_comments_channel_code ON dima_comments(channel_code);
    """
    with conn.cursor() as cur:
        cur.execute(create_sql)
    conn.commit()
    print("✅ Table 'dima_comments' created or already exists")


# ----------------------------
#  CSV parsing
# ----------------------------

def parse_csv(filename: str) -> List[Comment]:
    """
    Expected columns (like Go):
    TITLE, GRADE, DESCRIPTION, NATIONAL_CODE, REAL_FIRST_NAME,
    REAL_LAST_NAME, MOBILE_NO, CREATED_AT, Channel>
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
            channel_code = row[8].strip() if len(row) > 8 else None
            if channel_code == "":
                channel_code = None

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
                channel_code=channel_code,
                # date=None,
            )
            comments.append(comment)

    print(f"✅ Successfully parsed {len(comments)} comments")
    return comments


# ----------------------------
#  Insert / upsert
# ----------------------------

def insert_comments(conn, comments: List[Comment]):
    upsert_sql = """
    INSERT INTO dima_comments (
        title,
        grade,
        description,
        national_code_hash,
        mobile_no_hash,
        channel_code,
        created_at
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (national_code_hash, created_at)
    DO UPDATE SET
        title = EXCLUDED.title,
        grade = EXCLUDED.grade,
        description = EXCLUDED.description,
        mobile_no_hash = EXCLUDED.mobile_no_hash,
        channel_code = COALESCE(dima_comments.channel_code, EXCLUDED.channel_code),

        imported_at = CURRENT_TIMESTAMP;
    """

    rows = [
        (
            c.title,
            c.grade,
            c.description,
            c.national_code_hash,
            c.mobile_no_hash if c.mobile_no_hash else None,
            c.channel_code,
            c.created_at,
        )
        for c in comments
    ]

    with conn.cursor() as cur:
        execute_batch(cur, upsert_sql, rows, page_size=1000)
    conn.commit()
    print(f"✅ Completed: {len(rows)} comments inserted/updated")


