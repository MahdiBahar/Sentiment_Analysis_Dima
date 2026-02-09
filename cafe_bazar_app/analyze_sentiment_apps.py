# Import libraries

import time

# Connect to database
from .connect_to_database_func import connect_db
from dotenv import load_dotenv
from .sentiment_model_func import run_first_model, run_second_model, validate_and_score_sentiment
# Load environment variables from .env file
load_dotenv()



# Fetch comments that need sentiment analysis for a specific app
def fetch_comments_to_analyze_apps(logger, app_id):
    logger.info(f"Fetching comments for app_id: {app_id}")
    try:
        conn = connect_db()
        cursor = conn.cursor()
        query = """
            SELECT comment_id, comment_text, comment_rating
            FROM app_comments
            WHERE app_id = %s AND sentiment_score IS NULL;
        """
        cursor.execute(query, (app_id,))
        comments = cursor.fetchall()
        logger.info(f"Fetched {len(comments)} comments for analysis.")
        cursor.close()
        conn.close()
        return comments
    except Exception as e:
        logger.error(f"Error fetching comments: {e}", exc_info=True)
        return []

# Update the comment table with the sentiment result and sentiment score
def update_sentiment_apps(logger, comment_id, sentiment_result, sentiment_score, second_model_processed):
    # logger.info(f"Updating sentiment for comment_id: {comment_id}")
    try:
        conn = connect_db()
        cursor = conn.cursor()
        query = """
            UPDATE app_comments
            SET sentiment_result = %s, sentiment_score = %s, second_model_processed = %s
            WHERE comment_id = %s;
        """
        cursor.execute(query, (sentiment_result, sentiment_score, second_model_processed, comment_id))
        conn.commit()
        cursor.close()
        conn.close()
        # logger.info(f"Successfully updated comment_id: {comment_id}")
    except Exception as e:
        logger.error(f"Error updating sentiment for comment_id: {comment_id}: {e}", exc_info=True)


# Main function to fetch comments for a specific app_id and update sentiments
def analyze_and_update_sentiment(logger, comments, app_id):
    logger.info(f"Starting sentiment analysis for app_id: {app_id}")
    for comment_id, comment_text, comment_rating in comments:
        try:
            logger.info(f"Analyzing sentiment for comment_id: {comment_id}")
            sentiment_result = run_first_model(logger,comment_text)
            second_model_processed = False
            # If the first model returns "non-sentiment", run the second model
            if sentiment_result.lower() in ["no sentiment expressed", "mixed", "neutral"]:
                logger.debug(f"Running second model for comment_id: {comment_id}")
                second_model_result = run_second_model(logger,comment_text)

            # Apply conditional update logic based on second model result and rating
                if second_model_result == "NEGATIVE" and comment_rating == 1:
                    sentiment_result = "negative"
                    second_model_processed = True
                    print("second_model is used")
                elif second_model_result == "POSITIVE" and comment_rating == 5:
                    sentiment_result = "positive"
                    second_model_processed = True
                    print("second_model is used")
                # Otherwise, retain "no sentiment expressed"

            SENTIMENT_SCORES = {
                        "very negative": -2,
                        "negative": -1,
                        "neutral": 0,
                        "mixed": 0,
                        "positive": 1,
                        "very positive": 2,
                        "no sentiment expressed": 0
                    }
            sentiment_result, sentiment_score = validate_and_score_sentiment(logger,sentiment_result,SENTIMENT_SCORES)
            update_sentiment_apps(logger, comment_id, sentiment_result, sentiment_score, second_model_processed)
            logger.info(f"Updated comment_id: {comment_id} with sentiment: {sentiment_result}, score: {sentiment_score}")
        except Exception as e:
            logger.error(f"Error processing comment_id: {comment_id}: {e}", exc_info=True)
            update_sentiment_apps(logger, comment_id, "Missed Value", 11, False)
            continue
        time.sleep(20)