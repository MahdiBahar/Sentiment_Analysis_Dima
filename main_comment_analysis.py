from connect_to_database_func import connect_db
from dotenv import load_dotenv
from logging_config import setup_logger  # Import logger setup function
from comment_analysis import fetch_comments_to_analyze,upsert_comment_analysis,mark_comment_as_analyzed
from LLM_function_analysis import call_LLM_single_comment, extract_json, validate_output
from datetime import datetime, timezone

load_dotenv()

# # Initialize logger
logger = setup_logger(name="comment_analysis", log_file="comment_sentiment.log")



def run_comment_analysis_batch(limit=100):
    comments = fetch_comments_to_analyze(limit=limit)

    if not comments:
        logger.info("No comments to analyze.")
        return

    conn = connect_db()

    try:
        for c in comments:
            try:
                logger.info(f"Analyzing comment {c['comment_id']}")

                raw_analysis = call_LLM_single_comment( comment_id=str(c["id"]),
                comment_text=c["description"],
                sentiment_group=c["sentiment_result"],
                created_at=c["created_at"], 
                model = "phi4",
                retries=2
                )

                if not raw_analysis or not raw_analysis.strip():
                    raise RuntimeError("LLM returned empty output")
                

                analysis  = extract_json(raw_analysis)
                analysis["processed_at"] = datetime.now(timezone.utc).isoformat()
                validate_output(analysis, c["description"])

                upsert_comment_analysis(conn, analysis)
                mark_comment_as_analyzed(conn, c["comment_id"])

                conn.commit()

            except Exception as e:
                conn.rollback()
                logger.error(
                    f"Failed to analyze comment {c['comment_id']}: {e}",
                    exc_info=True
                )

    finally:
        conn.close()


