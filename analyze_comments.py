from connect_to_database_func import connect_db
from dotenv import load_dotenv
from cafe_bazar_app.logging_config import setup_logger  # Import logger setup function

load_dotenv()

# # Initialize logger
logger = setup_logger(name="comment_analysis", log_file="dima_comments_analysis.log")

# Fetch dima_comments that need sentiment analysis for a specific app
def fetch_comments_to_analyze():
    logger.info("Fetching comments from 'dima_comments' table for LLM analysis.")
    try:
        conn = connect_db()
        cursor = conn.cursor()

        query = """
            

                SELECT
                c.id,
                c.description,
                c.sentiment_result,
                c.created_at,
                c.title
            FROM dima_comments c
            WHERE
                c.is_repetitive IS FALSE
                AND c.description IS NOT NULL 
                AND TRIM(c.description) != ''
                AND c.sentiment_score IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1
                    FROM dima_comments_analysis a
                    WHERE a.comment_id = c.id
                    AND a.ai_title IS NOT NULL
                )
            ORDER BY c.id Desc;

        """

        # SELECT
        #         id,
        #         description,
        #         sentiment_result,
        #         created_at,
        #         title
        #     FROM dima_comments
        #     WHERE
        #         is_repetitive IS FALSE
        #         AND description IS NOT NULL 
        #         AND TRIM(description) != ''
        #         AND sentiment_score IS NOT NULL
        #         AND created_at > '2026-02-07'
        #     ORDER BY id ASC
            
        #  AND created_at <= '2026-12-25' AND created_at >= '2025-02-05'
            #        -- AND (id = 513720 OR id = 545508 OR id = 473709 OR id = 501915 OR id = 502773 OR id = 537167 OR id = 502668 OR id = 511481 OR id = 507888 OR id = 503077 OR id = 536059)
            #    -- AND (id = 515503 OR id = 501153 OR id = 489193 OR id = 492679 OR id = 494628 OR id = 502342 OR id = 519966 OR id = 516450 OR id = 474548 OR id = 529284 OR id = 541840)
            #     AND (id = 539833 OR id = 533841 OR id = 517043 OR id = 530853 OR id = 545238 OR id = 492321 OR id = 533903 OR id = 535596 OR id = 543121 OR id = 493901 OR id = 506171)
        
# AND is_analyzed IS FALSE
# AND created_at >= '2026-01-25'
#AND (id = 540486 OR id = 537365 OR id = 533010 OR id = 527713 OR id = 526221 OR id = 523844 OR id = 517013 OR id = 520234)
        cursor.execute(query)
        rows = cursor.fetchall()

        comments = [
            {
                "comment_id": r[0],
                "comment_text": r[1],
                "sentiment_result": r[2],
                "created_at": r[3],
                "title": r[4]
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
                INSERT INTO dima_comments_analysis (
            comment_id,
            created_at,
            sentiment_result,
            title,
            type,
            category,
            short_title,
            normalized_title,
            keywords,
            evidence,
            model,
            processed_at,
            ai_title
        )
        VALUES (
            %(comment_id)s,
            %(created_at)s,
            %(sentiment_result)s,
            %(title)s,
            %(type)s,
            %(category)s,
            %(short_title)s,
            %(normalized_title)s,
            %(keywords)s,
            %(evidence)s,
            %(model)s,
            CURRENT_TIMESTAMP,
             %(ai_title)s
        )
        ON CONFLICT (comment_id)
        DO UPDATE SET
            sentiment_result = EXCLUDED.sentiment_result,
            title = EXCLUDED.title,
            type = EXCLUDED.type,
            category = EXCLUDED.category,
            short_title = EXCLUDED.short_title,
            normalized_title = EXCLUDED.normalized_title,
            keywords = EXCLUDED.keywords,
            evidence = EXCLUDED.evidence,
            model = EXCLUDED.model,
            processed_at = CURRENT_TIMESTAMP,
            ai_title = EXCLUDED.ai_title
            ;

    """

    with conn.cursor() as cur:
        cur.execute(query, analysis)

##########################################################################

def mark_comment_as_analyzed(conn, comment_id):
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE dima_comments SET is_analyzed = TRUE WHERE id = %s;",
            (comment_id,)
        )


