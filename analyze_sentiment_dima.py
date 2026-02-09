# Import libraries
import time
# Connect to database
from connect_to_database_func import connect_db
from dotenv import load_dotenv
from cafe_bazar_app.sentiment_model_func import run_first_model, run_second_model, validate_and_score_sentiment

# Load environment variables from .env file
load_dotenv()


# Fetch dima_comments that need sentiment analysis for a specific app
def fetch_comments_to_analyze(logger, limit=100):
    logger.info("Fetching comments from 'dima_comments' table where sentiment not analyzed yet.")
    try:
        conn = connect_db()
        cursor = conn.cursor()
        query = """
            SELECT id, description, grade
            FROM dima_comments
            WHERE sentiment_result IS NULL OR sentiment_result=''
            ORDER BY id ASC
            LIMIT %s;
        """
        cursor.execute(query, (limit,))
        comments = cursor.fetchall()
        logger.info(f"Fetched {len(comments)} comments for analysis from dima_comments.")
        cursor.close()
        conn.close()
        return comments
    except Exception as e:
        logger.error(f"Error fetching comments from dima_comments: {e}", exc_info=True)
        return []


# Update the comment table with the sentiment result and sentiment score
def update_sentiment_dima(logger,id, sentiment_result, sentiment_score, second_model_processed):
    try:
        conn = connect_db()
        cursor = conn.cursor()
        query = """
            UPDATE dima_comments
            SET sentiment_result = %s, sentiment_score = %s, second_model_processed = %s
            WHERE id = %s;
        """
        cursor.execute(query, (sentiment_result, sentiment_score, second_model_processed, id))
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"Error updating sentiment from dima_comments for comment_id: {id}: {e}", exc_info=True)



# Main function to fetch comments for dima application and update sentiments
def analyze_and_update_sentiment(logger, comments):
    logger.info("Starting sentiment analysis from dima_comments")
    for comment_id, comment_text, comment_rating in comments:
        try:
            logger.info(f"Analyzing sentiment for comment_id: {comment_id}")

            # Handle empty or whitespace-only comment
            if not comment_text or comment_text.strip() == "":
                sentiment_result = "no comments"
                sentiment_score = 0
                second_model_processed = False
                logger.info(f"Comment {comment_id} is empty — marked as 'no comments'")
                update_sentiment_dima(logger,comment_id, sentiment_result, sentiment_score, second_model_processed)
                continue  # Skip to next comment

            # Run MT5 model
            sentiment_result = run_first_model(logger,comment_text)
            second_model_processed = False

            # If result is unclear, run fallback
            if sentiment_result.lower() in ["no sentiment expressed", "mixed", "neutral"]:
                logger.debug(f"Running second model for comment_id: {comment_id}")
                second_model_result = run_second_model(logger,comment_text)

                if second_model_result == "NEGATIVE" and comment_rating == 1:
                    sentiment_result = "negative"
                    second_model_processed = True
                    print("second_model is used")
                elif second_model_result == "POSITIVE" and comment_rating == 5:
                    sentiment_result = "positive"
                    second_model_processed = True
                    print("second_model is used")
            SENTIMENT_SCORES = {
                        "very negative": 1,
                        "negative": 2,
                        "neutral": 3,
                        "mixed": 3,
                        "positive": 4,
                        "very positive": 5,
                        "no sentiment expressed": 3
                    }
            sentiment_result, sentiment_score = validate_and_score_sentiment(logger,sentiment_result,SENTIMENT_SCORES)
            # sentiment_result, sentiment_score = validate_and_score_sentiment(logger,sentiment_result)
            update_sentiment_dima (logger,comment_id, sentiment_result, sentiment_score, second_model_processed)

            logger.info(f"Updated comment_id: {comment_id} with sentiment: {sentiment_result}, score: {sentiment_score}")
        except Exception as e:
            logger.error(f"Error processing comment_id: {comment_id}: {e}", exc_info=True)
            update_sentiment_dima (logger,comment_id, "Missed Value", 11, False)
            continue

        time.sleep(20)
