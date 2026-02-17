from langchain_community.llms import Ollama
from datetime import datetime, timezone
import json
import re
from analyze_comments import logger
from preprocessing_main import preprocess
from connect_to_database_func import connect_db


llm_summarize = Ollama(
    model="phi4:latest",
    # base_url="http://localhost:11434",
    base_url = "http://192.168.0.10:11434",
    temperature=0.1
)


#### Preprocessing text (using prerocessing_main)

def normalize_for_match(text: str) -> str:
    if not text:
        return ""

    text = preprocess(
        text,
        remove_halfspace=True,
        replace_multiple_spaces=True,
        replace_enter_with_space=True
    )
    return text.strip()


LLM_SUMMARIZE_COMMENT_PROMPT = """

You are an expert in banking bussiness. You should summarize some input shorted comments by consedering their type, category, title and sentiment results and then extract some good summarizations from them.


Rules:
- Output ONLY JSON.
- Clear all previous input before getting the new ones.
- Use the list of {normalized_title} texts in the specific {type}, {category}, {sentiment_result} and {title} as the inputs of summarization.
- See all of input and summarized them as one output.
- The output should be Persian (fa).
- If neccessary, the output could have one or more sentences (Maximum 100 sentences).
- do not generate title. Use the exact input title.
- If neccessary, merge output sentences to give better responses. 
- ALL fields must be in Persian (fa).
- normalized_title MUST be Persian.
- Be sure summarized comment will be filled with sufficient summarization output.   


JSON format:

{{
  
  "type": "{type}",
  "category": "{category}",
  "title" : "{title}"
  "sentiment_result": "{sentiment_result}",
  "summarized_comment": "",

  
}}

"""



def call_LLM_summarize_comment(
    title: str,
    normalized_title: list,
    sentiment_result: str,
    category: str,
    type: str,
    retries: int,
) -> str:
    # preprocessed_comment = normalize_for_match(text = normalized_title)
    prompt = LLM_SUMMARIZE_COMMENT_PROMPT.format(
        title=title,
        category= category,
        sentiment_result=sentiment_result,
        type = type,
        normalized_title= normalized_title
    )
    for i in range(retries + 1):
        raw = llm_summarize.invoke(prompt)

        if raw and raw.strip():
            return raw

        print(f"⚠️ Empty output, retry {i+1}")

    raise RuntimeError("Phi4 returned empty output after retries")


#################################################################################################


def extract_json(raw: str) -> dict:
    if not raw or not raw.strip():
        raise ValueError("Empty LLM output")

    # Remove markdown fences
    raw = re.sub(r"```(?:json)?", "", raw)
    raw = raw.replace("```", "").strip()

    # Extract first JSON object
    # match = re.search(r"\{[\s\S]*\}", raw)
    match = re.search(r"\{[\s\S]*?\}", raw)
    if not match:
        raise ValueError(f"No JSON object found:\n{raw}")

    return json.loads(match.group(0))

def append_jsonl(path: str, obj: dict):
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


###############################################################################################



# Fetch comments that need sentiment analysis for a specific app
def fetch_comments_to_summarize(type,category, title, sentiment_result):
    logger.info("Fetching comments from 'comments' table for LLM analysis.")
    try:
        conn = connect_db()
        cursor = conn.cursor()

        query = """
            SELECT
                type,
                category,
                title,
                sentiment_result,
                normalized_title
            FROM dima_comments_analysis
            WHERE
                type = %s
               AND category = %s
                AND title = %s
                AND sentiment_result = %s        
            ;
            
        """

        cursor.execute(query,(type,category, title,sentiment_result))
        rows = cursor.fetchall()


        comments = [
            {
                "type": r[0],
                "category": r[1],
                "title": r[2],
                "sentiment_result": r[3],
                "normalized_title": r[4]
            }
            for r in rows
        ]
        logger.info(f"Fetched {len(comments)} comments for analysis.")
        count = len(comments)
        cursor.close()
        conn.close()
        return comments , count

    except Exception as e:
        logger.error(f"Error fetching comments: {e}", exc_info=True)
        return []

def fetch_comments_to_summarize_RPC(type,category, title, sentiment_result,start_date, end_date):
    logger.info("Fetching comments from 'comments' table for LLM analysis.")
    try:
        conn = connect_db()
        cursor = conn.cursor()

        query = """
            SELECT
                type,
                category,
                title,
                sentiment_result,
                normalized_title
            FROM dima_comments_analysis
            WHERE
                type = %s
               AND category = %s
                AND title = %s
                AND sentiment_result = %s 
                AND created_at BETWEEN %s AND %s       
            ;
            
        """

        cursor.execute(query,(type,category, title,sentiment_result, start_date, end_date))
        rows = cursor.fetchall()


        comments = [
            {
                "type": r[0],
                "category": r[1],
                "title": r[2],
                "sentiment_result": r[3],
                "normalized_title": r[4],
            }
            for r in rows
        ]
        logger.info(f"Fetched {len(comments)} comments for analysis.")
        count = len(comments)
        cursor.close()
        conn.close()
        return comments , count

    except Exception as e:
        logger.error(f"Error fetching comments: {e}", exc_info=True)
        return []

#################################################################################\
def upsert_summarized_analysis(conn, analysis):
    query = """
        INSERT INTO dima_comments_summarization (
            sentiment_result,
            title,
            type,
            category,
            summarized_comment,
            count,
            processed_at
        )
        VALUES (
            
            %(sentiment_result)s,
            %(title)s,
            %(type)s,
            %(category)s,
            %(summarized_comment)s,
            %(count)s,
            %(processed_at)s
        )
        ON CONFLICT (summarized_id)
        DO UPDATE SET
            sentiment_result = EXCLUDED.sentiment_result,
            title = EXCLUDED.title,
            type = EXCLUDED.type,
            category = EXCLUDED.category,
            summarized_comment = EXCLUDED.summarized_comment,
            count = EXCLUDED.count,
            processed_at = EXCLUDED.processed_at;
    """
    # Ensure the required keys are present in the analysis dictionary
    if 'summarized_comment' not in analysis:
        analysis["summarized_comment"] = "NULL"
        raise KeyError("Missing key: summarized_comment")
    
    with conn.cursor() as cur:
        cur.execute(query, analysis)
