import pandas as pd
from connect_to_database_func import connect_db
from datetime import timedelta
import re

def flag_repetitive_comments():
    """Flags repetitive comments within 1 hour per user, using psycopg2 (with duplicate_of support)."""
    try:
        # Step 1️⃣: Reset all repetitive flags
        conn = connect_db()
        cur = conn.cursor()
        cur.execute("UPDATE comments SET is_repetitive = FALSE, duplicate_of = NULL;")
        conn.commit()
        cur.close()
        conn.close()
        print("🔄 Reset all previous repetitive flags to FALSE and duplicate_of to NULL.")

        # Step 2️⃣: Fetch data
        conn = connect_db()
        query = """
            SELECT id, national_code_hash, description, created_at, sentiment_result
            FROM comments
            WHERE description IS NOT NULL
            ORDER BY national_code_hash, created_at;
        """
        df = pd.read_sql(query, conn)
        conn.close()

        # Step 3️⃣: Filter out 'no comments'
        df = df[df["sentiment_result"] != "no comments"].copy()

        # Step 4️⃣: Normalize text
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
        df["description_norm"] = (
            df["description"]
            .fillna("")
            .apply(lambda x: re.sub(r"\s+", " ", x.strip()))
        )

        df.sort_values(["national_code_hash", "created_at"], inplace=True)
        df["is_repetitive"] = False
        df["duplicate_of"] = None

        # Step 5️⃣: Detect duplicates per user
        for user, group in df.groupby("national_code_hash", sort=False):
            prev_desc = None
            prev_time = None
            prev_id = None

            for i in group.index:
                desc = df.at[i, "description_norm"]
                time = df.at[i, "created_at"]

                if (
                    prev_desc == desc
                    and pd.notnull(prev_time)
                    and (time - prev_time).total_seconds() <= 3600
                ):
                    df.at[i, "is_repetitive"] = True
                    df.at[i, "duplicate_of"] = prev_id  # link to the original comment
                else:
                    prev_desc = desc
                    prev_time = time
                    prev_id = df.at[i, "id"]

        # Step 6️⃣: Write results back efficiently
        conn = connect_db()
        conn.autocommit = True
        cur = conn.cursor()

        update_data = [
            (
                bool(row["is_repetitive"]),
                int(row["duplicate_of"]) if pd.notna(row["duplicate_of"]) else None,
                int(row["id"])
            )
            for _, row in df.iterrows()
        ]

        cur.executemany(
            "UPDATE comments SET is_repetitive = %s, duplicate_of = %s WHERE id = %s;",
            update_data
        )

        conn.commit()
        cur.close()
        conn.close()

        repetitive_count = int(df["is_repetitive"].sum())
        print(f"✅ Repetitive detection completed. Flagged {repetitive_count} comments as repetitive.")
        return repetitive_count

    except Exception as e:
        print(f"❌ Error in flag_repetitive_comments: {e}")
        return 0
