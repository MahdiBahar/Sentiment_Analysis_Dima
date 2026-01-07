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
                "sentiment_group": r[2],
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
