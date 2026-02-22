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

def infer_AI_title_from_title(title: str, title_AI_title_map):
    if not title:
        return "other"

    norm_title = normalize_for_match(title)

    for keyword, ai_title in title_AI_title_map.items():
        if normalize_for_match(keyword) in norm_title:
            return ai_title

    return "other"


# def call_LLM_single_comment(
#     comment_id: str,
#     comment_text: str,
#     sentiment_result: str,
#     created_at: str,
#     model: str,
#     retries: int,
#     app_title : str
# ) -> str:
#     preprocessed_comment = normalize_for_match(text = comment_text)
#     prompt = LLM_SINGLE_COMMENT_PROMPT.format(
#         # comment_id=comment_id,
#         # created_at=created_at,
#         # sentiment_result=sentiment_result,
#         # model = model,
#         comment_text=preprocessed_comment
#     )
#     TITLE_AI_TITLE_MAP = {
#         "دریافت تسهیلات": "loan",
#         "انتقال وجه": "transfer",
#         "کارت‌ها": "card",
#         "پرداخت قبض": "bill",
#         "خرید شارژ": "top-up",
#         "دستیار هوشمند": "ai",
#         "مدیریت حساب‌ها": "account",
#         "سایر": "other",
#         "پروفایل": "profile",
#         "خرید اینترنت": "internet package",
#         "کلیت اپلیکیشن" : "in general",

#     }
    
#     hint_ai_title = infer_AI_title_from_title(app_title, TITLE_AI_TITLE_MAP)

#     prompt += f"\nLikely ai_title based on title of app section: {hint_ai_title}"
#     for i in range(retries + 1):
#         raw = llm.invoke(prompt)

#         if raw and raw.strip():
#             return raw

#         print(f"⚠️ Empty output, retry {i+1}")

#     raise RuntimeError("Phi4 returned empty output after retries")

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




def call_llm_semantic(comment_text: str,  app_title : str,retries: int = 2) -> str:
    preprocessed_comment = normalize_for_match(comment_text)

    prompt = LLM_SEMANTIC_PROMPT.format(
        comment_text=preprocessed_comment
    )
    
    hint_ai_title = infer_AI_title_from_title(app_title, TITLE_AI_TITLE_MAP)

    prompt += f"\nLikely ai_title based on title of app section: {hint_ai_title}"
    for i in range(retries + 1):
        raw = llm.invoke(prompt)
        if raw and raw.strip():
            return raw
        logger.warning(f"Empty semantic output retry {i+1}")

    raise RuntimeError("Semantic LLM failed")


def call_llm_category(normalized_title: str, type_: str, ai_title: str, retries: int = 2) -> str:
    
    prompt = LLM_CATEGORY_PROMPT.format(
        normalized_title=normalized_title,
        type=type_,
        ai_title=ai_title
    )
    
    for i in range(retries + 1):
        raw = llm.invoke(prompt)
        if raw and raw.strip():
            return raw
        logger.warning(f"Empty category output retry {i+1}")

    raise RuntimeError("Category LLM failed")

######################################################################################################
LLM_SEMANTIC_PROMPT = """
Analyze ONE Persian user comment and return ONE JSON object.

Rules:
- Output ONLY JSON.
- No markdown.
- No explanation.
- All text fields must be Persian.
- evidence MUST be exact quote from comment.
- short_title max 10 words.
- normalized_title must summarize the issue clearly.
- keywords: 1 to 6 Persian keywords.

Allowed values:

type: issue | suggestion | question | praise | criticism | other

ai_title: transfer | card | bill | loan | in general | ai assistant | account | top-up | internet package | profile | other

You must choose ONE type.
You must choose ONE ai_title.


You must choose ONE type from:

issue : A specific problem or malfunction that needs fixing.
suggestion : A request for improvement or new feature.
question : A request for information or help.
praise : Positive feedback or satisfaction.
criticism : Negative opinion or dissatisfaction without a specific malfunction.
other : Only if none of the above apply.

Choose the MOST appropriate type.
Do NOT invent new types.

If comment contains strong praise phrases like:
"عالیه", "حرف نداره", "دمتون گرم", "سپاس", "خسته نباشید" ,"دستتون درد نکنه"
→ type must be praise.


- ai_title better match the app title context if possible.
- If comment consists of some words like 'اقساط', the ai_title should be loan. 
- If find more than one ai_titles for comment, just choose one and ignore other options.

JSON format:

{{
  "type": "",
  "ai_title": "",
  "short_title": "",
  "normalized_title": "",
  "keywords": [],
  "evidence": ""
}}

Comment:
{comment_text}
"""
##############################################################################################################
LLM_CATEGORY_PROMPT = """
You are classifying technical issue category.

Choose ONE category from:

auth
login
ui
performance
support
security
notification
other

Category definitions:

auth: OTP, verification, password reset, session expiration, face recognition or authentication.
login: cannot log in, login button not working.
ui: layout problems, visual bugs.
performance: crash, lag, slow or bad performance.
support: refund, no response from support.
security: hacking, privacy, unauthorized access.
notification: push notification problems.

You MUST return ONLY this JSON:

{{
  "category": ""
}}

normalized_title:
{normalized_title}

Type:
{type}

Feature:
{ai_title}
"""





###################################################################################################
    
# LLM_SINGLE_COMMENT_PROMPT = """
# Analyze ONE Persian user comment and return ONE JSON object.

# Rules:
# - Output ONLY JSON. No markdown. No explanation.
# - Use ONLY the comment text.
# - evidence MUST be an exact quote from the comment.
# - short_title: max 10 words.
# - ALL fields must be in Persian (fa).
# - normalized_title MUST be Persian.
# - keywords MUST be Persian.

# Allowed values:
# type: issue | suggestion | question | praise | criticism | other
# ai_title: transfer | card | bill | loan   | in general | ai assistant | account | top-up | internet package | profile  | other
# category: auth | login | ui | performance | support | security | notification | other

# You must choose ONE category from the following list:
# auth : Authentication system problems such as OTP, verification, password reset, token/session expiration.
# login : Problems logging into the app (cannot log in, login button not working, stuck on login screen).
# ui : Visual or layout issues in the interface (misaligned buttons, overlapping text, confusing navigation).
# performance : App speed, freezing, crashes, lag, slow loading.
# support : Customer service issues, refund problems, no response from support.
# security : Privacy, hacking concerns, unauthorized access, data leaks.
# notification : Push notification problems, alerts not received, delayed notifications.

# Choose the MOST SPECIFIC category. If find more than one categories for comment, just choose one and ignore other options.



# - ai_title better match the app title context if possible.
# - If comment consists of some words like 'اقساط', the ai_title should be loan. 
# - If find more than one ai_titles for comment, just choose one and ignore other options.


# You must choose ONE type from:

# issue : A specific problem or malfunction that needs fixing.
# suggestion : A request for improvement or new feature.
# question : A request for information or help.
# praise : Positive feedback or satisfaction.
# criticism : Negative opinion or dissatisfaction without a specific malfunction.
# other : Only if none of the above apply.

# Choose the MOST appropriate type.
# Do NOT invent new types.

# - If the comment contains phrases like:
# 'حرف نداره' or 'عالیه' or 'خیلی خوبه' or 'واقعا خوبه' or 'دمتون گرم' or 'دستتون درد نکنه' or 'خسته نباشید' or 'سپاس فراوان' or 'کار راه بنداز' or 'از این بهتر نیست' ,
# It must be classified as praise even if structure contains contrast words.

# JSON format:

# {{
  
#   "type": "",
#   "category": "",
#     "ai_title" : "",
#   "short_title": "",
#   "normalized_title": "",

#   "keywords": [],

#   "evidence": "",

  
# }}

# Comment:
# {comment_text}
#  """

# - If the issue is about entering the account → login
# - If the issue is about verification, session, OTP, password reset → auth
# - If the comment describes a specific technical or functional problem, classify as issue cattegory. If it only expresses dissatisfaction without details, classify as criticism.
# - examples:
# - example 1 : .این برنامه برای بانک ملت لازم بود واقعا که حرف نداره
# - result for type : praise
# - example 2 : نه خوب نه بعد
# - result for type : other
## "comment_id": "{comment_id}",
## "created_at": "{created_at}",
## "sentiment_result": "{sentiment_result}",
##"model": "{model}",
##############################################################################################
ALLOWED_TYPES = {"issue","suggestion","question","praise","criticism","other"}
ALLOWED_CATEGORIES = {
    "auth" ,"login" ,"ui" ,"performance" ,"support" , "security", "notification", "other"
}
ALLOWED_TITLES = {
    "transfer" , "card" , "bill" , "loan"   , "in general" , "ai assistant" , "account" , "top-up" , "internet package" , "profile", "other" 
}
def validate_output(obj: dict, original_text: str):
   
    # assert obj["type"] in ALLOWED_TYPES
    #############################################################
    types = obj.get("type")

    if types not in ALLOWED_TYPES:
        logger.warning(
            f"Invalid type '{types}' – replaced with 'other'"
        )
        obj["type"] = "other"
    ##################################################################

    # assert obj["category"] in ALLOWED_CATEGORIES

    CATEGORY_MAP = {
    "authentication": "auth",
    "customer support": "support",
}

    
    category = obj.get("category", "").strip().lower()

    if category in CATEGORY_MAP:
        category = CATEGORY_MAP[category]
    
    # obj["category"] = obj.get("category", "").strip().lower()
    # category = obj.get("category")

    if category not in ALLOWED_CATEGORIES:
        logger.warning(
            f"Invalid category '{category}' – replaced with 'other'"
        )
        obj["category"] = "other"
    ####################################################################

 # assert obj["category"] in ALLOWED_CATEGORIES

    TITLE_MAP = {
    "ai": "ai assistant",
    "ai_assistant": "ai assistant",
    
}
    ai_title = obj.get("ai_title", "").strip().lower()

    if ai_title in TITLE_MAP:
        ai_title = TITLE_MAP[ai_title]
    
    # obj["category"] = obj.get("category", "").strip().lower()
    # category = obj.get("category")

    if ai_title not in ALLOWED_TITLES:
        logger.warning(
            f"Invalid ai_title '{ai_title}' – replaced with 'other'"
        )
        obj["ai_title"] = "other"


#############################################################################

    # assert obj["severity"] in LEVELS

    # severity = obj.get("severity")

    # if severity not in LEVELS:
    #     logger.warning(
    #         f"Invalid severity '{severity}' – replaced with 'none'"
    #     )
    #     obj["severity"] = "None"


    # # assert obj["priority"] in LEVELS

    # priority = obj.get("priority")
    
    # if priority not in LEVELS:
    #     logger.warning(
    #         f"Invalid priority '{priority}' – replaced with 'none'"
    #     )
    #     obj["priority"] = "None"

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