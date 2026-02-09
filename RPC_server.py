from jsonrpc import JSONRPCResponseManager, dispatcher
# from http.server import BaseHTTPRequestHandler, HTTPServer
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import threading
from cafe_bazar_app.comment_scraper import fetch_app_urls_to_crawl, crawl_comments
from cafe_bazar_app.app_scraper_check import give_information_app, check_and_create_app_id
from cafe_bazar_app.analyze_sentiment_apps import fetch_comments_to_analyze_apps, analyze_and_update_sentiment_apps
from cafe_bazar_app.logging_config import setup_logger
from Ngram import run_ngram_analysis

#####################################################
from analyze_sentiment_dima import (
    analyze_and_update_sentiment as analyze_and_update_sentiment_dima,
    fetch_comments_to_analyze as fetch_comments_to_analyze_dima
)
from repetitive_detection import flag_repetitive_comments

######################################################################################

from main_comment_analysis import run_comment_analysis_batch
##################################################################################
# Setup logger
logger = setup_logger('rpc_server', 'rpc_server.log')
# Initialize logger
logger_sentiment_apps = setup_logger(name="sentiment_analysis_cafe_bazar", log_file="analyze_sentiment_apps.log")
logger_sentiment_dima = setup_logger(
    name="sentiment_analysis_dima",
    log_file="analyze_sentiment_dima.log"
)
logger_comment_analysis_dima = setup_logger(name="comment_analysis_dima", log_file="analyze_comment_dima.log")

# Global dictionary to track tasks
tasks_status = {}
tasks_lock = threading.Lock()

# GPU lock (only one GPU task at a time)
gpu_lock = threading.Lock()

# Event for synchronization
crawl_event = threading.Event()  # Signaled when crawling is complete


class RequestHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        request = self.rfile.read(content_length).decode()
        response = JSONRPCResponseManager.handle(request, dispatcher)
        
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        if response is not None:
            self.wfile.write(response.json.encode('utf-8'))
        else:
            self.wfile.write(b'{"error": "Internal server error"}')


# Helper function to simulate long tasks
def perform_task(task_id, task_function, *args):
    global tasks_status

    with tasks_lock:
        tasks_status[task_id]["status"] = "working"

    try:
        logger.info(f"Starting task {task_id}: {tasks_status[task_id]['description']}")

        # 🔥 Capture result
        result = task_function(*args)

        with tasks_lock:
            tasks_status[task_id]["status"] = "completed"
            tasks_status[task_id]["result"] = result

        logger.info(f"Task {task_id} completed successfully.")

    except Exception as e:
        with tasks_lock:
            tasks_status[task_id]["status"] = "failed"
            tasks_status[task_id]["error"] = str(e)

        logger.error(f"Task {task_id} failed: {e}", exc_info=True)


@dispatcher.add_method
def crawl_comment(app_ids):
    global tasks_status, crawl_event

    crawl_event.clear()
    # task_id = "1"
    task_id = str(len(tasks_status) + 1)

    # Immediately respond that the task has started
    with tasks_lock:
        tasks_status[task_id] = {"status": "started", "description": "Crawling comments"}
    logger.info(f"Task {task_id} started: Crawling comments for app_ids {app_ids}")

    # Start the task in a separate thread
    def wrapped_task():
        try:
            fetch_and_crawl_comments_apps(app_ids)
        finally:
            crawl_event.set()  # Signal that crawling is complete
            logger.info("Crawling comments completed.")

    threading.Thread(target=perform_task, args=(task_id, wrapped_task)).start()
    return {"task_id": task_id, "message": "Task started: Crawling comments"}


@dispatcher.add_method
def sentiment_analysis_apps(app_ids):
    global tasks_status, crawl_event

    # task_id = "2"
    task_id = str(len(tasks_status) + 1)
    with tasks_lock:
        tasks_status[task_id] = {"status": "started", "description": "Performing sentiment analysis"}
    logger.info(f"Task {task_id} started: Performing sentiment analysis from app_comments for app_ids {app_ids}")

    def wrapped_task():
        crawl_event.wait()  # Wait for crawling to complete
        with gpu_lock:
            analyze_sentiments_apps(app_ids)

    threading.Thread(target=perform_task, args=(task_id, wrapped_task)).start()
    return {"task_id": task_id, "message": "Task started: Sentiment analysis from app_comments"}


@dispatcher.add_method
def check_add_url(crawl_url, crawl_app_nickname="unknown"):
    try:
        selected_domain = crawl_url.split("/")[2]

        if selected_domain == "cafebazaar.ir":
            app_data = give_information_app(crawl_app_nickname, crawl_url)
            [long_report, short_report] = check_and_create_app_id(app_data)
            logger.info(f"App URL checked. Report: {long_report}")
        else:
            long_report = f"The {crawl_url} is not related to Cafebazaar or not valid. Please try again"
            short_report = "Bad-URL"
            logger.warning(f"Invalid URL: {long_report}")

        return {"status": short_report, "message": long_report}
    except Exception as e:
        logger.error(f"Error checking URL {crawl_url}: {e}", exc_info=True)
        return {"status": "error", "message": f"An error occurred: {e}"}


@dispatcher.add_method
def check_task_status(task_id):
    global tasks_status

    with tasks_lock:
        task = tasks_status.get(task_id)

        if not task:
            return {"status": "error", "message": "Task ID not found"}

        return {
            "status": task["status"],
            "description": task["description"],
            "result": task.get("result"),
            "error": task.get("error")
        }


@dispatcher.add_method
def ngram_analysis(sentiment=None, start_date=None, end_date=None, top_k=30):

    task_id = str(len(tasks_status) + 1)
    # import uuid
    # task_id = str(uuid.uuid4())
    
    if top_k is not None:
        top_k = int(top_k)

    with tasks_lock:
        tasks_status[task_id] = {
            "status": "started",
            "description": "Ngram analysis",
            "result": None,
            "error": None
        }

    def wrapped_task():
        return run_ngram_analysis(
            sentiment=sentiment,
            start_date=start_date,
            end_date=end_date,
            top_k=top_k
        )

    threading.Thread(
        target=perform_task,
        args=(task_id, wrapped_task)
    ).start()

    return {
        "task_id": task_id,
        "message": "Task started: Ngram analysis"
    }


##########################
@dispatcher.add_method
def sentiment_analysis_dima(limit=100):

    global tasks_status

    task_id = str(len(tasks_status) + 1)

    with tasks_lock:
        tasks_status[task_id] = {
            "status": "started",
            "description": "Performing sentiment analysis for Dima",
            "result": None,
            "error": None
        }

    if limit is not None:
        limit = int(limit)

    def wrapped_task():

        # 🔐 GPU protected block
        with gpu_lock:

            logger_sentiment_dima.info("Checking repetitive comments from dima_comments...")
            count = flag_repetitive_comments()
            logger_sentiment_dima.info(f"Duplicate detection finished. Flagged {count} comments.")

            logger_sentiment_dima.info("Starting Dima sentiment analysis...")

            total_processed = 0

            while True:
                comments = fetch_comments_to_analyze_dima(
                    logger_sentiment_dima,
                    limit=limit
                )

                if not comments:
                    break

                analyze_and_update_sentiment_dima(
                    logger_sentiment_dima,
                    comments
                )

                total_processed += len(comments)

            logger_sentiment_dima.info("Dima sentiment analysis completed.")

            return {
                "processed_comments": total_processed
            }

    threading.Thread(
        target=perform_task,
        args=(task_id, wrapped_task)
    ).start()

    return {
        "task_id": task_id,
        "message": "Task started: Dima sentiment analysis"
    }

############################################################################################################################

@dispatcher.add_method
def comment_analysis_dima():

    global tasks_status

    task_id = str(len(tasks_status) + 1)

    with tasks_lock:
        tasks_status[task_id] = {
            "status": "started",
            "description": "Performing LLM comment analysis for Dima",
            "result": None,
            "error": None
        }

    def wrapped_task():
        try:
            with gpu_lock:
                logger_comment_analysis_dima.info(
                    "Starting LLM comment analysis for Dima..."
                )

                result = run_comment_analysis_batch(
                    logger_comment_analysis_dima
                )

            return result

        except Exception as e:
            logger_comment_analysis_dima.error(
                f"Fatal error in comment analysis task: {e}",
                exc_info=True
            )
            raise

    threading.Thread(
        target=perform_task,
        args=(task_id, wrapped_task)
    ).start()

    return {
        "task_id": task_id,
        "message": "Task started: LLM comment analysis for Dima"
    }


#############################################################################################################################

def fetch_and_crawl_comments_apps(app_ids):
    logger.info("Fetching app URLs and crawling comments from app_comments...")
    apps = fetch_app_urls_to_crawl(app_ids)
    for app_id, app_url in apps:
        try:
            logger.info(f"Starting to crawl comments from app_comments for app_id {app_id} at {app_url}")
            crawl_comments(app_id, app_url)
            logger.info(f"Finished crawling comments from app_comments for app_id {app_id}")
        except Exception as e:
            logger.error(f"Error crawling comments from app_comments for app_id {app_id}: {e}", exc_info=True)


def analyze_sentiments_apps(app_ids):
    logger.info("Starting sentiment analysis from app_comments...")
    for app_id in app_ids:
        try:
            comments = fetch_comments_to_analyze_apps(logger_sentiment_apps,app_id)
            if not comments:
                logger.info(f"No comments left to analyze from app_comments for app_id {app_id}")
                continue
            analyze_and_update_sentiment_apps(logger_sentiment_apps,comments, app_id)
            logger.info(f"Sentiment analysis completed from app_comments for app_id {app_id}")
        except Exception as e:
            logger.error(f"Error during sentiment analysis from app_comments for app_id {app_id}: {e}", exc_info=True)


if __name__ == "__main__":
    logger.info("Server running on port 5000...")
    crawl_event.set()
    # server = HTTPServer(("0.0.0.0", 5000), RequestHandler)
    server = ThreadingHTTPServer(("0.0.0.0", 5000), RequestHandler)

    server.serve_forever()
