# Import libraries
from transformers import MT5ForConditionalGeneration, MT5Tokenizer, pipeline
import time
from googletrans import Translator
# Connect to database
from connect_to_database_func import connect_db
from dotenv import load_dotenv
from logging_config import setup_logger  # Import logger setup function
import os

 #Completely remove proxy env vars for this process
for var in ["http_proxy", "https_proxy", "all_proxy", "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY"]:
    os.environ.pop(var, None)

# Create Translator without proxies and without trusting env
translator = Translator(proxies=None, raise_exception=False, http2=False)

# Load environment variables from .env file
load_dotenv()

# Initialize logger
logger = setup_logger(name="sentiment_analysis", log_file="analyze_sentiment.log")

# Load the tokenizer and model
logger.info("Loading MT5 model and tokenizer...")
model_name = "persiannlp/mt5-base-parsinlu-sentiment-analysis"
tokenizer = MT5Tokenizer.from_pretrained(model_name)
model = MT5ForConditionalGeneration.from_pretrained(model_name)

# Load the second model (Hugging Face pipeline)
logger.info("Loading Hugging Face sentiment classifier...")
classifier = pipeline("sentiment-analysis", device=-1)

# Initialize Google Translator
translator = Translator()

# Sentiment mapping for scoring
SENTIMENT_SCORES = {
    "very negative": 1,
    "negative": 2,
    "neutral": 3,
    "mixed": 3,
    "positive": 4,
    "very positive": 5,
    "no sentiment expressed": 3,
    "no comments" : 0
}

# Fetch comments that need sentiment analysis for a specific app
def fetch_comments_to_analyze(limit=100):
    logger.info("Fetching comments from 'comments' table where sentiment not analyzed yet.")
    try:
        conn = connect_db()
        cursor = conn.cursor()
        query = """
            SELECT id, description, grade
            FROM comments
            WHERE sentiment_result IS NULL
            LIMIT %s;
        """
     
        cursor.execute(query, (limit,))
        comments = cursor.fetchall()
        logger.info(f"Fetched {len(comments)} comments for analysis.")
        cursor.close()
        conn.close()
        return comments
    except Exception as e:
        logger.error(f"Error fetching comments: {e}", exc_info=True)
        return []

# Update the comment table with the sentiment result and sentiment score
def update_sentiment(id, sentiment_result, sentiment_score, second_model_processed):
    try:
        conn = connect_db()
        cursor = conn.cursor()
        query = """
            UPDATE comments
            SET sentiment_result = %s, sentiment_score = %s, second_model_processed = %s
            WHERE id = %s;
        """
        cursor.execute(query, (sentiment_result, sentiment_score, second_model_processed, id))
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"Error updating sentiment for comment_id: {id}: {e}", exc_info=True)


# Run the MT5 model to get sentiment
def run_model(context, text_b="نظر شما چیست", **generator_args):
    try:
        logger.debug(f"Running MT5 model for text: {context}")
        input_ids = tokenizer.encode(context + "<sep>" + text_b, return_tensors="pt")
        res = model.generate(input_ids, **generator_args)
        output = tokenizer.batch_decode(res, skip_special_tokens=True)

        if not output:
            raise ValueError("Model returned empty output.")
        logger.info(f"MT5 model output: {output[0]}")
        return output[0]
    except Exception as e:
        logger.error(f"Error in run_model: {e}", exc_info=False)
        return "no sentiment expressed"

def run_second_model(comment_text):
    try:
        logger.debug(f"Running second model for text: {comment_text}")
        translated_text = translator.translate(comment_text, dest="en").text
        if not translated_text:
            raise ValueError("Translation returned empty text.")
        
        result = classifier(translated_text)
        if not result or not isinstance(result, list):
            raise ValueError("Classifier returned invalid result.")
        
        logger.info(f"Second model output: {result[0]['label']}")
        return result[0]["label"]
    except Exception as e:
        logger.error(f"Error in run_second_model: {e}", exc_info=False)
        return "no sentiment expressed"

# Validate sentiment result and assign score
def validate_and_score_sentiment(sentiment_result):
    sentiment_result = sentiment_result.lower()
    if sentiment_result not in SENTIMENT_SCORES:
        sentiment_result = "no sentiment expressed"
    sentiment_score = SENTIMENT_SCORES[sentiment_result]
    logger.debug(f"Validated sentiment: {sentiment_result}, Score: {sentiment_score}")
    return sentiment_result, sentiment_score
# Main function to fetch comments for a specific app_id and update sentiments
def analyze_and_update_sentiment(comments):
    logger.info("Starting sentiment analysis")
    for comment_id, comment_text, comment_rating in comments:
        try:
            logger.info(f"Analyzing sentiment for comment_id: {comment_id}")

            # Handle empty or whitespace-only comment
            if not comment_text or comment_text.strip() == "":
                sentiment_result = "no comments"
                sentiment_score = 0
                second_model_processed = False
                logger.info(f"Comment {comment_id} is empty — marked as 'no comments'")
                update_sentiment(comment_id, sentiment_result, sentiment_score, second_model_processed)
                continue  # Skip to next comment

            # Run MT5 model
            sentiment_result = run_model(comment_text)
            second_model_processed = False

            # If result is unclear, run fallback
            if sentiment_result.lower() in ["no sentiment expressed", "mixed", "neutral"]:
                logger.debug(f"Running second model for comment_id: {comment_id}")
                second_model_result = run_second_model(comment_text)

                if second_model_result == "NEGATIVE" and comment_rating == 1:
                    sentiment_result = "negative"
                    second_model_processed = True
                    print("second_model is used")
                elif second_model_result == "POSITIVE" and comment_rating == 5:
                    sentiment_result = "positive"
                    second_model_processed = True
                    print("second_model is used")

            sentiment_result, sentiment_score = validate_and_score_sentiment(sentiment_result)
            update_sentiment(comment_id, sentiment_result, sentiment_score, second_model_processed)

            logger.info(f"Updated comment_id: {comment_id} with sentiment: {sentiment_result}, score: {sentiment_score}")
        except Exception as e:
            logger.error(f"Error processing comment_id: {comment_id}: {e}", exc_info=True)
            update_sentiment(comment_id, "Missed Value", 11, False)
            continue

        time.sleep(0.3)



if __name__ == "__main__":
#     comments = fetch_comments_to_analyze(limit=100)
#     analyze_and_update_sentiment(comments, app_id=None)  # app_id is not needed anymore
    while True:
        comments = fetch_comments_to_analyze(limit=100)
        if not comments:
            break
        analyze_and_update_sentiment(comments)
        
        logger.info("Sentiment analysis completed.")