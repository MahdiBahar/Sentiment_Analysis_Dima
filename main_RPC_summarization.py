from LLM_summarize import call_LLM_summarize_comment, fetch_comments_to_summarize_RPC, update_summarized_result, extract_json
from connect_to_database_func import connect_db
from dotenv import load_dotenv
from cafe_bazar_app.logging_config import setup_logger  # Import logger setup function
from datetime import datetime, timezone
import time

load_dotenv()

# # Initialize logger
logger = setup_logger(name="comment_summarization", log_file="dima_comments_analysis.log")



from datetime import datetime
from datetime import timezone
import math

MAX_COMMENTS_PER_CHUNK = 80   # adjust based on token size


def chunk_list(items, chunk_size):
    for i in range(0, len(items), chunk_size):
        yield items[i:i + chunk_size]



def run_summarization_batch(
    requests , model
):
    conn = connect_db()
    batch_results = []

    for job in requests:
        ## test output
        print("TYPE:", type(requests))
        print("VALUE:", requests)
        print("type_job" , type(job))
        print("job" , job)
        summarized_id = int(job.get("summarized_id"))
        filters = job.get("filter", {})
    
        print("FILTER DATA:", filters)

        print("titles:", filters.get("titles"))
        print("types:", filters.get("types"))
        print("categories:", filters.get("categories"))
        print("sentiments:", filters.get("sentiments"))

        start_time = time.time()
        try:

            final_response = []
            total_comment_count = 0
            titles = filters.get("titles") or []
            types = filters.get("types") or []
            categories = filters.get("categories") or []
            sentiments = filters.get("sentiments") or []
            start_date = filters.get("start_date")
            end_date = filters.get("end_date")
            if not titles or not types or not categories or not sentiments or not start_date or not end_date:
                raise ValueError("One or more required filters are missing")

            for title in titles:
                for type_name in types:
                    for category in categories:
                        for sentiment in sentiments:

                            comments, comment_count = fetch_comments_to_summarize_RPC(
                                title=title,
                                type=type_name,
                                category=category,
                                sentiment_result=sentiment,
                                start_date=start_date,
                                end_date=end_date
                            )
                            total_comment_count += comment_count

                            if comment_count <= 4:
                                raise ValueError("Not enough comments for summarization")

                            comment_texts = [
                                c["normalized_title"]
                                for c in comments
                                if c.get("normalized_title")
                            ]

                            chunk_summaries = []

                            # 🔹 CHUNKING
                            for chunk in chunk_list(comment_texts, MAX_COMMENTS_PER_CHUNK):

                                joined_comments = "\n".join(chunk)

                                llm_output = call_LLM_summarize_comment(
                                    title=title,
                                    category=category,
                                    sentiment_result=sentiment,
                                    type=type_name,
                                    normalized_title=joined_comments,
                                    retries=2,
                                )

                                parsed = extract_json(llm_output)
                                chunk_summaries.append(
                                    parsed.get("summarized_comment", "")
                                )

                            # 🔹 FINAL MERGE
                            if len(chunk_summaries) > 1:

                                merged_text = "\n".join(chunk_summaries)

                                final_llm = call_LLM_summarize_comment(
                                    title=title,
                                    category=category,
                                    sentiment_result=sentiment,
                                    type=type_name,
                                    normalized_title=merged_text,
                                    retries=2,
                                )

                                final_parsed = extract_json(final_llm)
                                final_summary = final_parsed.get("summarized_comment", "")

                            else:
                                final_summary = chunk_summaries[0]

                            final_response.append(final_summary)

            # 🔹 SUCCESS UPDATE
            duration = round(time.time() - start_time, 2)

            update_summarized_result(conn, {
                "summarized_id": summarized_id,
                "summarized_comment": "\n\n".join(final_response),
                "comment_count": total_comment_count,
                "status": "completed",
                "duration_seconds": duration,
                "model": model
            })
            batch_results.append({
                "summarized_id": summarized_id,
                "status": "completed"
            })
            # return {
            #     "message": "Summarization completed",
            #     "data": final_response
            # }
        except Exception as e:

            duration = round(time.time() - start_time, 2)

            # 🔹 FAILURE UPDATE
            update_summarized_result(conn, {
                "summarized_id": summarized_id,
                "summarized_comment": None,
                "comment_count": 0,
                "status": "failed",
                "duration_seconds": duration,
                "model": "your_model_name_here"
            })

            logger.error(f"Summarization failed: {str(e)}")

            batch_results.append({
                "summarized_id": summarized_id,
                "status": "failed",
                "error": str(e)
            }) 
            # return {
            #     "message": "Summarization failed",
            #     "error": str(e)
            # }
    conn.close()

    return {
        "message": "Batch summarization finished",
        "data": batch_results
    }
