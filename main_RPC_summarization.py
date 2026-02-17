from LLM_summarize import call_LLM_summarize_comment, fetch_comments_to_summarize_RPC, extract_json, upsert_summarized_analysis,append_jsonl
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



def run_summarization(
    titles,
    types,
    categories,
    sentiments,
    start_date,
    end_date
):

    final_response = []

    for title in titles:
        for type_name in types:
            for category in categories:
                for sentiment in sentiments:

                    comments,count = fetch_comments_to_summarize_RPC(
                        title=title,
                        type=type_name,
                        category=category,
                        sentiment_result=sentiment,
                        start_date=start_date,
                        end_date=end_date
                    )
                    print("Type of first comment:", type(comments[0]))
                    print("Example comment:", comments[0])

                    if count<=5:
                        return {
                            "message": "In this filter we have a few number of comments for summarizing",
                                "data": []
                            }

                    chunk_summaries = []

                    comment_texts = [
                                c["normalized_title"]
                                for c in comments
                                if c.get("normalized_title")
]

                    # 🔹 CHUNKING
                    for chunk in chunk_list(comment_texts, MAX_COMMENTS_PER_CHUNK):

                        joined_comments = "\n".join(chunk)

                        llm_output = call_LLM_summarize_comment(
                            title=title,
                            category=category,
                            sentiment_result=sentiment,
                            type=type_name,
                            normalized_title=joined_comments,
                            retries=2
                        )

                        parsed = extract_json(llm_output)
                        chunk_summaries.append(
                            parsed.get("summarized_comment", "")
                        )

                    # 🔹 FINAL MERGE SUMMARY
                    if len(chunk_summaries) > 1:

                        merged_text = "\n".join(chunk_summaries)

                        final_llm = call_LLM_summarize_comment(
                            title=title,
                            category=category,
                            sentiment_result=sentiment,
                            type=type_name,
                            normalized_title=merged_text,
                            retries=2
                        )

                        final_parsed = extract_json(final_llm)
                        final_summary = final_parsed.get("summarized_comment", "")

                    else:
                        final_summary = chunk_summaries[0]

                    final_response.append({
                        "title": title,
                        "type": type_name,
                        "category": category,
                        "sentiment": sentiment,
                        "start_date": start_date,
                        "end_date": end_date,
                        "comment_count": len(comments),
                        "summary": final_summary
                    })

    if not final_response:
        return {
            "message": "No data found for given filters and date range",
            "data": []
        }

    return {
    "message": "Summarization completed",
    "data": final_response
}




# if __name__ == "__main__":
#     logger.info("🚀 Starting summarizaing...")

#     titles = ["انتقال وجه"]
#     types = ["issue","suggestion"]
#     categories = ["transfer","card"]
#     sentiments = ["very negative", "negative"]


#     # titles = ["دریافت تسهیلات","انتقال وجه","سایر","مدیریت حساب‌ها","کارت‌ها","دستیار هوشمند","پرداخت قبض","خرید شارژ"]
#     # types = ["issue","suggestion","question","praise","other"]
#     # categories = ["transfer","auth","card","bill","loan","login","ui","performance", "AI", "other"]
#     # sentiments = ["very negative", "negative", "no sentiment expressed","positive", "very positive"]

#     # titles = ["دریافت تسهیلات","انتقال وجه"]
#     # types = ["issue","suggestion"]
#     # categories = ["transfer","auth","card","loan"]
#     # sentiments = ["very negative", "negative"]


#     output_path = "summarized_comments.jsonl"

#     final_summarization(titles, types, categories, sentiments)

#     logger.info("✅ comment summarization completed.")
