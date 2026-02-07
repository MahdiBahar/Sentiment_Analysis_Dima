from analyze_sentiment_dima import analyze_and_update_sentiment, fetch_comments_to_analyze
from cafe_bazar_app.logging_config import setup_logger
from repetitive_detection import flag_repetitive_comments

logger_sentiment_dima = setup_logger(name="sentiment_analysis", log_file="analyze_sentiment_dima.log")
logger_repetitive_dima = setup_logger(name="repetitive_comments", log_file="analyze_sentiment_dima.log")

if __name__ == "__main__":


    logger_repetitive_dima.info("✅ Checking for repetitive new comments...")

    count = flag_repetitive_comments()
    logger_repetitive_dima.info(f"✅ Duplicate detection finished. Flagged {count} new repetitive comments.")

    logger_sentiment_dima.info("🚀 Starting sentiment analysis...")

    while True:
        comments = fetch_comments_to_analyze(logger_sentiment_dima,limit=100)
        if not comments:
            break
        analyze_and_update_sentiment(logger_sentiment_dima,comments)

    logger_sentiment_dima.info(f"✅ Sentiment analysis is finished")
