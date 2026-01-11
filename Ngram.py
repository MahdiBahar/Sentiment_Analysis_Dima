# ngram_analysis.py

# from sklearn.feature_extraction.text import CountVectorizer
# from hazm import Normalizer, word_tokenize, Stemmer, Lemmatizer, stopwords_list
from preprocessing_main import preprocess
from connect_to_database_func import connect_db
import pandas as pd
from nltk.tokenize import word_tokenize
import re
from sklearn.feature_extraction.text import TfidfVectorizer
import numpy as np

# ---------------------------
# 1) Fetch comments from DB
# ---------------------------
def fetch_comments(sentiment=None, min_len=2, limit=None):
    """
    Fetch comments (id, title, grade, description, sentiment_result) from DB.
    Optionally filter by sentiment ('negative', 'positive', etc.) and non-empty text.
    """
    conn = connect_db()
    cur = conn.cursor()

    base = """
        SELECT id, title, grade, description, COALESCE(sentiment_result, '') as sentiment_result
        FROM comments
        WHERE description IS NOT NULL 
            AND is_repetitive = FALSE
           AND sentiment_score >0
    """
    args = []
    if sentiment:
        base += " AND lower(sentiment_result) = lower(%s)"
        args.append(sentiment)
    base += " ORDER BY id ASC"
    if limit:
        base += " LIMIT %s"
        args.append(limit)

    cur.execute(base, tuple(args))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    df = pd.DataFrame(rows, columns=["id", "title", "grade", "description", "sentiment_result"])
    # # drop very short strings
    df = df[df["description"].str.len() >= min_len].reset_index(drop=True)
    return df



import os
def load_stopwords(filepath="stopwords.txt"):
    """Load Persian stopwords from a text file (auto-clean quotes, commas, spaces)."""
    if not os.path.exists(filepath):
        print(f"⚠️ Warning: stopwords file not found at {filepath}")
        return set()

    stopwords = set()
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            word = (
                line.strip()
                .replace("'", "")   # remove single quotes
                .replace(",", "")   # remove commas
                .replace("\u200c", "")  # remove zero-width non-joiner
                .strip()
            )
            if word:
                stopwords.add(word)
    print(f"✅ Loaded {len(stopwords)} clean stopwords.")
    return stopwords



# -----------------------------------
# 2) Preprocess & tokenize
# -----------------------------------
STOPWORDS = load_stopwords("stopwords.txt")

def clean_and_tokenize(text: str):
    if not isinstance(text, str):
        return []

    # Use your custom preprocessing
    cleaned_text = preprocess(
        text,
        convert_farsi_numbers=True,
        convert_arabic_characters=True,
        remove_diacritic=True,
        remove_numbers=True,
        remove_punctuations=True,
        replace_multiple_spaces=True,
        remove_ha_suffix=True
    )

    # Use NLTK tokenizer (works fine for Persian with spacing)
    tokens = word_tokenize(cleaned_text)

    # Keep Persian words only (optional regex filter)
    tokens = [t for t in tokens if re.match(r"^[\u0600-\u06FF]+$", t)]

    # 🔹 Remove stopwords here
    tokens = [t.strip() for t in tokens if t.strip() not in STOPWORDS]
    
    return tokens


# -----------------------------------
# 3) TF-IDF weighted n-grams
# -----------------------------------
def extract_top_ngrams_tfidf(
    texts,
    ngram_range=(2, 3),
    top_k=30,
    min_df=3,
    max_df=0.6,
    max_features=30000
):
    """Extract top TF-IDF weighted n-grams"""
    vectorizer = TfidfVectorizer(
        tokenizer=clean_and_tokenize,
        preprocessor=lambda x: x,
        token_pattern=None,
        ngram_range=ngram_range,
        min_df=min_df,
        max_df=max_df,
        max_features=max_features
    )

    X = vectorizer.fit_transform(texts)
    feature_names = vectorizer.get_feature_names_out()
    tfidf_scores = X.sum(axis=0).A1

    df_tfidf = pd.DataFrame({
        "ngram": feature_names,
        "tfidf": np.round(tfidf_scores,4)
    }).sort_values("tfidf", ascending=False).head(top_k).reset_index(drop=True)

    df_tfidf["n"] = df_tfidf["ngram"].str.count(" ") + 1
    # df_tfidf["n"] = df_tfidf["ngram"].str.count(" ") + 1
    print(df_tfidf.groupby("n").head(10))
    return df_tfidf

# 4) Group sentiments (3 categories)
# ---------------------------------
def group_sentiments(df):
    df = df.copy()
    df["sentiment_group"] = "neutral"  # default group

    df.loc[df["sentiment_result"].str.lower().isin(["negative", "very negative"]), "sentiment_group"] = "negative"
    df.loc[df["sentiment_result"].str.lower().isin(["positive", "very positive"]), "sentiment_group"] = "positive"
    df.loc[df["sentiment_result"].str.lower().isin(["neutral", "mixed", "no sentiment expressed"]), "sentiment_group"] = "neutral"

    return df

