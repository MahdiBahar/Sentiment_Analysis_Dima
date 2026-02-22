from connect_to_database_func import connect_db
from dotenv import load_dotenv
from analyze_comments import fetch_comments_to_analyze,upsert_comment_analysis,mark_comment_as_analyzed
from LLM_function_analysis import  extract_json, validate_output, normalize_for_match, call_llm_semantic, call_llm_category
from datetime import datetime, timezone
import json
from cafe_bazar_app.logging_config import setup_logger  # Import logger setup function

load_dotenv()

# def infer_category_from_title(title: str, title_category_map):
#     if not title:
#         return "other"

#     norm_title = normalize_for_match(title)

#     for keyword, category in title_category_map.items():
#         if normalize_for_match(keyword) in norm_title:
#             return category

#     return "other"

def infer_AI_title_from_title(title: str, title_AI_title_map):
    if not title:
        return "other"

    norm_title = normalize_for_match(title)

    for keyword, ai_title in title_AI_title_map.items():
        if normalize_for_match(keyword) in norm_title:
            return ai_title

    return "other"


def infer_type_from_sentiment(sentiment_result: str):
    if not sentiment_result:
        return "other"

    sentiment = sentiment_result.strip().lower()

    if sentiment in ["positive", "very positive"]:
        return "praise"

    if sentiment in ["negative", "very negative"]:
        return "criticism"

    return "other"

neutral_phrases = [
    "بد نیست",
        "بد‌نیست",
        "بد نبود",
        "بدک نیست",
        "بدی نیست",
    "نظری ندارم",
    "نظر خاصی ندارم",
    "ندارم",
    "نظری ندارم",
   "نه خوب نه بعد",
    "معمولی",
]

def force_neutral(text):
    for phrase in neutral_phrases:
        if phrase in text:
            return True
    return False


def run_comment_analysis_batch(logger):
    comments = fetch_comments_to_analyze()

    if not comments:
        logger.info("No comments to analyze.")
        return {"processed": 0, "failed": 0}

    conn = connect_db()
    processed_count = 0
    failed_count = 0
    # TITLE_CATEGORY_MAP = {
    #     "دریافت تسهیلات": "loan",
    #     "انتقال وجه": "transfer",
    #     "کارت‌ها": "card",
    #     "پرداخت قبض": "bill",
    #     "خرید شارژ": "bill",
    #     "دستیار هوشمند": "ai",
    #     "مدیریت حساب‌ها": "account",
    #     "سایر": "other",
    # }
    TITLE_AI_TITLE_MAP = {
        "دریافت تسهیلات": "loan",
        "انتقال وجه": "transfer",
        "کارت‌ها": "card",
        "پرداخت قبض": "bill",
        "خرید شارژ": "top-up",
        "دستیار هوشمند": "ai",
        "مدیریت حساب‌ها": "account",
        "سایر": "other",
        "پروفایل": "profile",
        "خرید اینترنت": "internet package",
        "کلیت اپلیکیشن" : "in general",

    }

    DRY_RUN = False
    try:
        for c in comments:
            try:
                
                if c.get("is_analyzed"):
                    logger.info(f"This comment with {c['comment_id']} id is analyzed before")
                    continue
                
                len_comment = normalize_for_match(text=c.get("comment_text")) 
                if len(len_comment.split())<3:
                    # logger.info(f"The length of this comment with {c['comment_id']} id is {len(len_comment.split())}")
                    logger.info(f"Short comment detected for {c['comment_id']} — using title mapping")

                    ai_title = infer_AI_title_from_title(c.get("title"),TITLE_AI_TITLE_MAP)
                    inferred_type = infer_type_from_sentiment(c.get("sentiment_result"))
                    
                    if force_neutral(c.get("comment_text")):
                        forced_type = "other"
                        logger.info(f"force_neutral detected in short comment for {c['comment_id']} — mapping to others")
                        inferred_type = forced_type


                    analysis = {
                        "comment_id": c["comment_id"],
                        "created_at": (
                            c["created_at"].isoformat()
                            if hasattr(c["created_at"], "isoformat")
                            else c["created_at"]
                        ),
                          "sentiment_result": c["sentiment_result"],
                            "title": c["title"],
                            "type": inferred_type,
                            "category": "other",
                            "short_title": c["title"],
                            "normalized_title": normalize_for_match(c["title"]),
                            "keywords": ["عمومی"],
                            "evidence": c["comment_text"],
                            "model": "rule_based_short_comment",
                            "ai_title" : ai_title
                    }

                else:

                    logger.info(f"Analyzing comment {c['comment_id']}")


                                        # --------------------
                    # Stage 1: Semantic
                    # --------------------
                    raw_semantic = call_llm_semantic(c["comment_text"],app_title=c["title"])
                    semantic_data = extract_json(raw_semantic)
                    
                    if not raw_semantic or not raw_semantic.strip():
                        raise RuntimeError("LLM returned empty output")
                    
                    semantic_data["comment_id"] = c["comment_id"]
                    semantic_data["created_at"] = (
                        c["created_at"].isoformat()
                        if hasattr(c["created_at"], "isoformat")
                        else c["created_at"]
                    )
                    semantic_data["sentiment_result"] = c["sentiment_result"]
                    semantic_data["title"] = c["title"]
                    semantic_data["model"] = "phi4_semantic"
                    if semantic_data["ai_title"] == "ai assistant":

                        semantic_data["ai_title"] = "ai" 
                    
                    if force_neutral(c.get("comment_text")):
                        forced_type = "other"
                        logger.info(f"force_neutral detected for {c['comment_id']} — mapping to others")
                        semantic_data["type"] = forced_type
                    # --------------------
                    # Stage 2: Category
                    # --------------------
                    raw_category = call_llm_category(
                        normalized_title=semantic_data["normalized_title"],
                        type_=semantic_data["type"],
                        ai_title=semantic_data["ai_title"]
                    )

                    category_data = extract_json(raw_category)

                    semantic_data["category"] = category_data.get("category", "other")

                    analysis = semantic_data


                    # model = "phi4"
                    # raw_analysis = call_LLM_single_comment(
                    #         comment_id=str(c["comment_id"]),
                    #         comment_text=c["comment_text"],
                    #         sentiment_result=c["sentiment_result"],
                    #         created_at=c["created_at"],
                    #         model=model,
                    #         retries=2,
                    #         app_title= c["title"]
                    #     )

                    # if not raw_analysis or not raw_analysis.strip():
                    #     raise RuntimeError("LLM returned empty output")
                    

                    # analysis  = extract_json(raw_analysis)

                    # analysis["created_at"] = (
                    #     c["created_at"].isoformat()
                    #     if hasattr(c["created_at"], "isoformat")
                    #     else c["created_at"]
                    # )
                    # # analysis["created_at"] = c["created_at"]
                    # if analysis["ai_title"] == "ai assistant":

                    #     analysis["ai_title"] = "ai"    
                    
                    # if force_neutral(c.get("comment_text")):
                    #     forced_type = "other"
                    #     logger.info(f"force_neutral detected for {c['comment_id']} — mapping to others")
                    #     analysis["type"] = forced_type

                    
                    # analysis["title"] = c["title"]
                    # analysis["comment_id"] = c["comment_id"]
                    
                    # analysis["sentiment_result"] = c["sentiment_result"]
                    # analysis["model"] = model


                validate_output(analysis, c["comment_text"])


                # logger.info(f"DRY_RUN = {DRY_RUN}")

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
                        logger.info(f"comment {c['comment_id']} is inserted to comment analysis table properly")
                        mark_comment_as_analyzed(conn, c["comment_id"])
                        logger.info(f"comment {c['comment_id']} is changed to is_analyzed")

                        processed_count += 1

            except Exception as e:
                conn.rollback()
                failed_count += 1
                logger.error(
                    f"Failed to analyze comment {c['comment_id']}: {e}",
                    exc_info=True
                )

    finally:
        conn.close()
    return {
        "processed": processed_count,
        "failed": failed_count
    }



#########RUN seperately ###############################################

# logger = setup_logger(name="comment_analysis_dima", log_file="analyze_comment_dima.log")

# if __name__ == "__main__":
#     logger.info("🚀 Starting comment analysis...")

#     run_comment_analysis_batch(logger)

#     logger.info("✅ comment analysis completed.")
