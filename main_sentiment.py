from analyze_sentiment import analyze_and_update_sentiment, fetch_comments_to_analyze
# from logging_config import setup_logger
from repetitive_detection import flag_repetitive_comments

# logger = setup_logger(name="sentiment_analysis", log_file="analyze_sentiment.log")
from analyze_sentiment import logger

if __name__ == "__main__":
    logger.info("🚀 Starting sentiment analysis...")

    while True:
        comments = fetch_comments_to_analyze(limit=100)
        if not comments:
            break
        analyze_and_update_sentiment(comments)

    logger.info("✅ Sentiment analysis completed. Now checking for repetitive comments...")

    count = flag_repetitive_comments()
    logger.info(f"✅ Duplicate detection finished. Flagged {count} repetitive comments.")
