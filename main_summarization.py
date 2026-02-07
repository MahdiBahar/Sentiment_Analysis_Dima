from LLM_summarize import call_LLM_summarize_comment, fetch_comments_to_summarize, extract_json, upsert_summarized_analysis,append_jsonl
from connect_to_database_func import connect_db
from dotenv import load_dotenv
from cafe_bazar_app.logging_config import setup_logger  # Import logger setup function
from datetime import datetime, timezone
import time

load_dotenv()

# # Initialize logger
logger = setup_logger(name="comment_summarization", log_file="dima_comments_analysis.log")





def final_summarization(titles, types, categories, sentiments):

    output =[]
    conn = connect_db()
    for title_name in titles:
        for type_name in types:
            for category_name in categories:
                for sentiment_name in sentiments:
                    result_summarized, count = fetch_comments_to_summarize(
                        title= title_name,
                        category= category_name,
                        sentiment_result= sentiment_name,
                        type = type_name
                        )
                    if count <=5:

                        continue

                    else: 
                        Final_result = call_LLM_summarize_comment(
                                title= title_name,
                                category= category_name,
                                sentiment_result= sentiment_name,
                                type = type_name,
                                normalized_title= result_summarized,
                                retries= 2)

                        result = extract_json(Final_result)
                        result["title"] = title_name
                        result["type"] = type_name
                        result["category"] = category_name
                        result["sentiment_result"] = sentiment_name
                        result["processed_at"] = datetime.now(timezone.utc).isoformat()
                        result["count"] = count
                        
                        if 'summarized_comment' not in result:
                            logger.warning(f"Missing 'summarized_comment' in the result. Skipping upsert.")
                        else:
        
                            with conn: 
                                upsert_summarized_analysis(conn, result)
                                logger.info(f"summarized text is inserted to summarized analysis table properly")
                            
                        time.sleep(3)  # Sleep for 1 second between each call

                    append_jsonl(output_path , result )
                    output.append(result)

    conn.close()  



if __name__ == "__main__":
    logger.info("🚀 Starting summarizaing...")

    titles = ["انتقال وجه"]
    types = ["issue","suggestion"]
    categories = ["transfer","card"]
    sentiments = ["very negative", "negative"]


    # titles = ["دریافت تسهیلات","انتقال وجه","سایر","مدیریت حساب‌ها","کارت‌ها","دستیار هوشمند","پرداخت قبض","خرید شارژ"]
    # types = ["issue","suggestion","question","praise","other"]
    # categories = ["transfer","auth","card","bill","loan","login","ui","performance", "AI assistant", "other"]
    # sentiments = ["very negative", "negative", "no sentiment expressed","positive", "very positive"]

    # titles = ["دریافت تسهیلات","انتقال وجه"]
    # types = ["issue","suggestion"]
    # categories = ["transfer","auth","card","loan"]
    # sentiments = ["very negative", "negative"]


    output_path = "summarized_comments.jsonl"

    final_summarization(titles, types, categories, sentiments)

    logger.info("✅ comment summarization completed.")
