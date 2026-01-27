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
# from import_comments import parse_timestamp

# ----------------------------
#  Data model
# ----------------------------

@dataclass
class AI_assistant:
    user_message: str
    assistant_message: str
    is_liked: bool
    created_at : datetime
    Q1: str | None
    Q2: str | None
    Q3: str | None  # add channel_code
   

# ----------------------------
#  DB functions
# ----------------------------

# Load environment variables from .env file
load_dotenv()


def create_table(conn):
    create_sql = """
    CREATE TABLE IF NOT EXISTS dima_AI_assistant (
    turn_id SERIAL PRIMARY KEY,   
    user_message TEXT,
    assistant_message TEXT,
    imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_liked BOOLEAN,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    Q1 VARCHAR(255),
    Q2 VARCHAR(255),
    Q3 TEXT);
    CREATE INDEX IF NOT EXISTS idx_dima_AI_assistant_is_liked ON dima_AI_assistant(is_liked);
    CREATE INDEX IF NOT EXISTS idx_dima_AI_assistant_Q1 ON dima_AI_assistant(Q1);
    CREATE INDEX IF NOT EXISTS idx_dima_AI_assistant_Q2 ON dima_AI_assistant(Q2);
    CREATE UNIQUE INDEX IF NOT EXISTS unique_message_time
        ON dima_ai_assistant (user_message, created_at);
    """
    with conn.cursor() as cur:
        cur.execute(create_sql)
    conn.commit()
    print("✅ Table 'dima_comments' created or already exists")


# ----------------------------
#  CSV parsing
# ----------------------------

# for converting like and dislike to true and false for the first type of csv data
def parse_is_liked(value: str | None) -> bool | None:
    if not value:
        return None

    value = value.strip().upper()

    if value == "LIKE":
        return True
    if value == "DISLIKE":
        return False

    raise ValueError(f"Invalid is_liked value: {value}")


def parse_csv(filename: str, type_csv=0) -> List[AI_assistant]:
    """
    Expected columns (like Go):
    TITLE, GRADE, DESCRIPTION, NATIONAL_CODE, REAL_FIRST_NAME,
    REAL_LAST_NAME, MOBILE_NO, CREATED_AT, Channel>
    """
    comments: List[AI_assistant] = []

    with open(filename, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        print("CSV Header:", header)

        for line_num, row in enumerate(reader, start=2):
            if len(row) < 8:
                print(f"⚠️  Skipping line {line_num}: insufficient columns ({len(row)} < 8)")
                continue
             
            
            if type_csv == 1:
                # for the first version of input csv file

                user_message = row[1]
                assistant_message = row[2]
                #########################
                try:
                    is_liked = parse_is_liked(row[3])
                except ValueError as e:
                    print(f"⚠️ Line {line_num}: {e}, setting is_liked=NULL")
                    is_liked = None
                #########################
                created_at_str = row[8].strip()
                Q1 = row[5]
                Q2 = row[7]
                Q3 = row[6]
                
                # timestamp
                try:
                    # created_at = parse_timestamp(created_at_str)
                    created_at = created_at_str
                except ValueError as e:
                    print(f"⚠️  Line {line_num}: invalid timestamp '{created_at_str}': {e}, skipping")
                    continue
            elif type_csv ==0: 
                #for the second version of input csv
                user_message = row[0]
                assistant_message = row[1]
                is_liked = row[2].lower() == "true"
                created_at_str = row[7]
                Q1 = row[4]
                Q2 = row[5]
                Q3 = row[6]
                
                # timestamp
                try:
                    # created_at = parse_timestamp(created_at_str)
                    created_at = created_at_str
                except ValueError as e:
                    print(f"⚠️  Line {line_num}: invalid timestamp '{created_at_str}': {e}, skipping")
                    continue


            comment = AI_assistant(
                user_message=user_message,
                assistant_message=assistant_message,
                is_liked = is_liked,
                created_at=created_at,
                Q1=Q1,
                Q2=Q2,
                Q3=Q3,
            )
            comments.append(comment)

    print(f"✅ Successfully parsed {len(comments)} comments")
    return comments


# ----------------------------
#  Insert / upsert
# ----------------------------

def insert_comments(conn, comments: List[AI_assistant]):

    upsert_sql = """
                INSERT INTO dima_AI_assistant (
                    user_message,
                    assistant_message,
                    is_liked,
                    created_at,
                    Q1,
                    Q2,
                    Q3
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_message, created_at)
                DO UPDATE SET
                    assistant_message = EXCLUDED.assistant_message,
                    is_liked = EXCLUDED.is_liked,
                    Q1 = EXCLUDED.Q1,
                    Q2 = EXCLUDED.Q2,
                    Q3 = EXCLUDED.Q3
                """


    rows = [
        (
            c.user_message,
            c.assistant_message,
            c.is_liked,
            c.created_at,
            c.Q1 if c.Q1 else None,
            c.Q2 if c.Q2 else None,
            c.Q3 if c.Q3 else None,
        )
        for c in comments
    ]

    with conn.cursor() as cur:
        execute_batch(cur, upsert_sql, rows, page_size=1000)
    conn.commit()
    print(f"✅ Completed: {len(rows)} comments inserted/updated")


