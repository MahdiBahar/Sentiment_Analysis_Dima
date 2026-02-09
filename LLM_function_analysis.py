from langchain_community.llms import Ollama
from datetime import datetime, timezone
import json
import re
from analyze_comments import logger
from preprocessing_main import preprocess


llm = Ollama(
    model="phi4:latest",
    # base_url="http://localhost:11434",
    base_url = "http://192.168.0.10:11434",
    temperature=0
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



def call_LLM_single_comment(
    comment_id: str,
    comment_text: str,
    sentiment_result: str,
    created_at: str,
    model: str,
    retries: int,
) -> str:
    preprocessed_comment = normalize_for_match(text = comment_text)
    prompt = LLM_SINGLE_COMMENT_PROMPT.format(
        # comment_id=comment_id,
        # created_at=created_at,
        # sentiment_result=sentiment_result,
        # model = model,
        comment_text=preprocessed_comment
    )
    for i in range(retries + 1):
        raw = llm.invoke(prompt)

        if raw and raw.strip():
            return raw

        print(f"⚠️ Empty output, retry {i+1}")

    raise RuntimeError("Phi4 returned empty output after retries")

###################################################################################################
    
LLM_SINGLE_COMMENT_PROMPT = """
Analyze ONE Persian user comment and return ONE JSON object.

Rules:
- Output ONLY JSON. No markdown. No explanation.
- Use ONLY the comment text.
- evidence MUST be an exact quote from the comment.
- short_title: max 10 words.
- If type != issue → severity = null
- If type != suggestion → priority = null
- ALL fields must be in Persian (fa).
- normalized_title MUST be Persian.
- keywords MUST be Persian.

Allowed values:

type: issue | suggestion | question | praise | other
category: transfer | auth | card | bill | loan | login | ui | performance | AI | other
severity / priority: high | medium | low | null

- If unsure, use "other" for type and category.

JSON format:

{{
  
  "type": "",
  "category": "",

  "short_title": "",
  "normalized_title": "",

  "keywords": [],

  "severity": null,
  "priority": null,

  "evidence": "",

  
}}

Comment:
{comment_text}
"""

## "comment_id": "{comment_id}",
## "created_at": "{created_at}",
## "sentiment_result": "{sentiment_result}",
##"model": "{model}",
##############################################################################################
ALLOWED_TYPES = {"issue","suggestion","question","praise","other"}
ALLOWED_CATEGORIES = {
    "transfer","auth","card","bill","loan","login","ui","performance", "AI", "other"
}
LEVELS = {"high","medium","low",None}


def validate_output(obj: dict, original_text: str):
    assert obj["type"] in ALLOWED_TYPES
    assert obj["category"] in ALLOWED_CATEGORIES
    assert obj["severity"] in LEVELS
    assert obj["priority"] in LEVELS

    # evidence must be exact substring
    # assert obj["evidence"] in original_text
    norm_evidence = normalize_for_match(obj["evidence"])
    norm_original = normalize_for_match(original_text)

    if norm_evidence not in norm_original:
        logger.warning("Evidence mismatch – repairing")
        obj["evidence"] = original_text.strip()[:300]

    # no English hallucination
    # assert not re.search(r"[A-Za-z]", obj["evidence"])

    if re.search(r"[A-Za-z]", obj["normalized_title"]):
        logger.warning("English detected in normalized_title")

    # for field in ["short_title", "normalized_title"]:
    #     assert not re.search(r"[A-Za-z]", obj[field]), f"English in {field}"
        

    # for kw in obj["keywords"]:
    #     assert not re.search(r"[A-Za-z]", kw), "English keyword detected"
   
    cleaned_keywords = []

    for kw in obj["keywords"]:
        if re.search(r"[A-Za-z]", kw):
            logger.warning(f"English keyword replaced: {kw}")
            continue
        cleaned_keywords.append(kw)

    if not cleaned_keywords:
        cleaned_keywords = ["عمومی"]

    obj["keywords"] = cleaned_keywords[:6]


    # assert 1 <= len(obj["keywords"]) <= 6
    if not (1 <= len(obj["keywords"]) <= 6):
        logger.warning("Invalid keyword count, normalizing")
        obj["keywords"] = obj["keywords"][:6] or ["عمومی"]


    # title length
    assert len(obj["short_title"].split()) <= 20


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