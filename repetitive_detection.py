import pandas as pd
from connect_to_database_func import connect_db
from datetime import timedelta
import re

def flag_repetitive_comments():
    conn = connect_db()
    query = """
        SELECT id, national_code_hash, description, imported_at, sentiment_result
        FROM comments
        WHERE description IS NOT NULL
        ORDER BY national_code_hash, imported_at;
    """
    df = pd.read_sql(query, conn)
    # Filter out "no comments"
    df = df[df["sentiment_result"] != "no comments"].copy()
    conn.close()

    # Convert imported_at → datetime
    df["imported_at"] = pd.to_datetime(df["imported_at"], errors="coerce")

    # Normalize description for comparison (remove extra spaces and punctuation)
    df["description_norm"] = (
        df["description"]
        .fillna("")
        .apply(lambda x: re.sub(r"\s+", " ", x.strip()))
    )

    # Sort data
    df = df.sort_values(["national_code_hash", "imported_at"]).reset_index(drop=True)
    df["is_repetitive"] = False

    # Process per user
    for user, group in df.groupby("national_code_hash", sort=False):
        prev_desc = None
        prev_time = None

        for i in group.index:
            desc = df.at[i, "description_norm"]
            time = df.at[i, "imported_at"]

            # Compare with previous comment of the same user
            if (
                prev_desc == desc
                and pd.notnull(prev_time)
                and (time - prev_time).total_seconds() <= 3600
            ):
                df.at[i, "is_repetitive"] = True
            else:
                prev_desc = desc
                prev_time = time

    # Write results back to database
    conn = connect_db()
    cur = conn.cursor()

    for _, row in df.iterrows():
        cur.execute(
            "UPDATE comments SET is_repetitive = %s WHERE id = %s;",
            (row["is_repetitive"], row["id"])
        )

    conn.commit()
    cur.close()
    conn.close()

    print("✅ Repetitive comment flagging (by national_code_hash + imported_at) completed.")

    repetitive_count = df["is_repetitive"].sum()
    print(f"✅ Flagged {repetitive_count} repetitive comments.")
    return repetitive_count