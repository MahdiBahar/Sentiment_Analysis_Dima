from connect_to_database_func import connect_db
from dotenv import load_dotenv
from logging_config import setup_logger  # Import logger setup function

load_dotenv()

# # Initialize logger
logger = setup_logger(name="comment_analysis", log_file="comment_sentiment.log")

# Fetch comments that need sentiment analysis for a specific app
def fetch_comments_to_analyze(limit=100):
    logger.info("Fetching comments from 'comments' table for LLM analysis.")
    try:
        conn = connect_db()
        cursor = conn.cursor()

        query = """
            SELECT
                id,
                description,
                sentiment_result,
                created_at
            FROM comments
            WHERE
                is_repetitive IS FALSE
                AND is_analyzed IS FALSE
                AND description IS NOT NULL
                AND LENGTH(TRIM(description)) > 2
            ORDER BY id ASC
            LIMIT %s;
        """

        cursor.execute(query, (limit,))
        rows = cursor.fetchall()

        comments = [
            {
                "comment_id": r[0],
                "comment_text": r[1],
                "sentiment_result": r[2],
                "created_at": r[3],
            }
            for r in rows
        ]

        logger.info(f"Fetched {len(comments)} comments for analysis.")

        cursor.close()
        conn.close()
        return comments

    except Exception as e:
        logger.error(f"Error fetching comments: {e}", exc_info=True)
        return []


#################################################################################\
def upsert_comment_analysis(conn, analysis):
    query = """
        INSERT INTO comment_analysis (
            comment_id,
            created_at,
            sentiment_result,
            type,
            category,
            short_title,
            normalized_title,
            keywords,
            severity,
            priority,
            evidence,
            model,
            processed_at
        )
        VALUES (
            %(comment_id)s,
            %(created_at)s,
            %(sentiment_result)s,
            %(type)s,
            %(category)s,
            %(short_title)s,
            %(normalized_title)s,
            %(keywords)s,
            %(severity)s,
            %(priority)s,
            %(evidence)s,
            %(model)s,
            %(processed_at)s
        )
        ON CONFLICT (comment_id)
        DO UPDATE SET
            sentiment_result = EXCLUDED.sentiment_result,
            type = EXCLUDED.type,
            category = EXCLUDED.category,
            short_title = EXCLUDED.short_title,
            normalized_title = EXCLUDED.normalized_title,
            keywords = EXCLUDED.keywords,
            severity = EXCLUDED.severity,
            priority = EXCLUDED.priority,
            evidence = EXCLUDED.evidence,
            model = EXCLUDED.model,
            processed_at = EXCLUDED.processed_at;
    """

    with conn.cursor() as cur:
        cur.execute(query, analysis)

##########################################################################

def mark_comment_as_analyzed(conn, comment_id):
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE comments SET is_analyzed = TRUE WHERE id = %s;",
            (comment_id,)
        )


