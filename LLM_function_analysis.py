from langchain_community.llms import Ollama
from datetime import datetime, timezone
import json
import re




llm = Ollama(
    model="phi4:latest",
    base_url="http://localhost:11434",
    temperature=0
)


def call_phi4_single_comment(
    comment_id: str,
    comment_text: str,
    sentiment_group: str,
    created_at: str,
    model: str,
    retries: int,
) -> str:
    prompt = LLM_SINGLE_COMMENT_PROMPT.format(
        comment_id=comment_id,
        created_at=created_at,
        sentiment_group=sentiment_group,
        model = model,
        comment_text=comment_text.strip()
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
category: transfer | auth | card | bill | loan | login | ui | performance | other
severity / priority: high | medium | low | null

JSON format:

{{
  "comment_id": "{comment_id}",
  "created_at": "{created_at}",
  "sentiment_group": "{sentiment_group}",

  "type": "",
  "category": "",

  "short_title": "",
  "normalized_title": "",

  "keywords": [],

  "severity": null,
  "priority": null,

  "evidence": "",

  "model": "{model}",
  "processed_at": ""
}}

Comment:
{comment_text}
"""



##############################################################################################
ALLOWED_TYPES = {"issue","suggestion","question","praise","other"}
ALLOWED_CATEGORIES = {
    "transfer","auth","card","bill","loan","login","ui","performance", "AI assistant", "other"
}
LEVELS = {"high","medium","low",None}


def validate_output(obj: dict, original_text: str):
    assert obj["type"] in ALLOWED_TYPES
    assert obj["category"] in ALLOWED_CATEGORIES
    assert obj["severity"] in LEVELS
    assert obj["priority"] in LEVELS

    # evidence must be exact substring
    assert obj["evidence"] in original_text

    # no English hallucination
    assert not re.search(r"[A-Za-z]", obj["evidence"])

    for field in ["short_title", "normalized_title"]:
        assert not re.search(r"[A-Za-z]", obj[field]), f"English in {field}"

    for kw in obj["keywords"]:
        assert not re.search(r"[A-Za-z]", kw), "English keyword detected"


    # title length
    assert len(obj["short_title"].split()) <= 12


def extract_json(raw: str) -> dict:
    if not raw or not raw.strip():
        raise ValueError("Empty LLM output")

    # Remove markdown fences
    raw = re.sub(r"```(?:json)?", "", raw)
    raw = raw.replace("```", "").strip()

    # Extract first JSON object
    match = re.search(r"\{[\s\S]*\}", raw)
    if not match:
        raise ValueError(f"No JSON object found:\n{raw}")

    return json.loads(match.group(0))