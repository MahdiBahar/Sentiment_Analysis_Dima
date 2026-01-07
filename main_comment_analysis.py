from connect_to_database_func import connect_db
from dotenv import load_dotenv
from analyze_comments import fetch_comments_to_analyze,upsert_comment_analysis,mark_comment_as_analyzed, logger
from LLM_function_analysis import call_LLM_single_comment, extract_json, validate_output
from datetime import datetime, timezone
import json

load_dotenv()


def run_comment_analysis_batch(limit=100):
    comments = fetch_comments_to_analyze(limit=limit)

    if not comments:
        logger.info("No comments to analyze.")
        return

    conn = connect_db()
    DRY_RUN = True
    try:
        for c in comments:
            try:
                logger.info(f"Analyzing comment {c['comment_id']}")
                model = "phi4"
                if c.get("is_analyzed"):
                    logger.info(f"This comment with {c['comment_id']} id is analyzed before")
                    continue

                raw_analysis = call_LLM_single_comment(
                        comment_id=str(c["comment_id"]),
                        comment_text=c["comment_text"],
                        sentiment_result=c["sentiment_result"],
                        created_at=c["created_at"],
                        model=model,
                        retries=2
                    )


                if not raw_analysis or not raw_analysis.strip():
                    raise RuntimeError("LLM returned empty output")

                analysis  = extract_json(raw_analysis)

                # analysis["processed_at"] = datetime.now(timezone.utc).isoformat()

                analysis["processed_at"] = datetime.now(timezone.utc).isoformat()

                analysis["created_at"] = (
                    c["created_at"].isoformat()
                    if hasattr(c["created_at"], "isoformat")
                    else c["created_at"]
                )
                # analysis["created_at"] = c["created_at"]


                analysis["comment_id"] = c["comment_id"]
                
                analysis["sentiment_result"] = c["sentiment_result"]
                analysis["model"] = model
                validate_output(analysis, c["comment_text"])


                logger.info(f"DRY_RUN = {DRY_RUN}")

                if DRY_RUN:
                   print(
                            json.dumps(
                                analysis,
                                ensure_ascii=False,
                                indent=2,
                                default=str   # fallback for unexpected types
                            )
                        )

                else:
                    with conn: 
                        upsert_comment_analysis(conn, analysis)
                        mark_comment_as_analyzed(conn, c["comment_id"])

                        # conn.commit()

            except Exception as e:
                conn.rollback()
                logger.error(
                    f"Failed to analyze comment {c['comment_id']}: {e}",
                    exc_info=True
                )

    finally:
        conn.close()


if __name__ == "__main__":
    logger.info("🚀 Starting comment analysis...")

    run_comment_analysis_batch(limit=50)

    logger.info("✅ comment analysis completed.")
