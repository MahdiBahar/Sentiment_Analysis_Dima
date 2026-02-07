
from Ngram import fetch_comments, load_stopwords, clean_and_tokenize, extract_top_ngrams_tfidf, group_sentiments
from preprocessing_main import preprocess
import pandas as pd
import sys
import os
import nltk
# df = fetch_comments()
# df["preprocessed_comments"]= df["description"].apply(lambda x: preprocess(x,convert_arabic_characters=True, remove_numbers=True, replace_multiple_spaces=True, convert_emojis=True, remove_diacritic=True))
# STOPWORDS = load_stopwords("stopwords.txt")
# df["drop_stopword"] = df['description'].apply(lambda x: clean_and_tokenize(x))


# ---------------------------------
# Main analysis pipeline
# ---------------------------------

def main(limit=None, version = 0.0):
    print("Fetching comments from database...")
    df = fetch_comments(limit)
    if df.empty:
        print("No comments found.")
        return

    df = group_sentiments(df)
    print(f"Total comments: {len(df)}")

    # ----- ALL comments
    print("\n🔹 Top n-grams (ALL):")
    df_all_3 = extract_top_ngrams_tfidf(df["description"].tolist(), ngram_range=(3, 3))
    df_all_2 = extract_top_ngrams_tfidf(df["description"].tolist(), ngram_range=(2, 2))

    # Label each type
    df_all_2["type"] = "bigram"
    df_all_3["type"] = "trigram"

    # Combine
    df_all = pd.concat([df_all_2, df_all_3], ignore_index=True)
    print(df_all)

    # ----- NEGATIVE
    df_neg = df[df["sentiment_group"] == "negative"]
    if not df_neg.empty:
        print("\n🔴 Top n-grams (NEGATIVE group):")

        # Extract both bigrams and trigrams
        df_neg_2 = extract_top_ngrams_tfidf(df_neg["description"].tolist(), ngram_range=(2, 2))
        df_neg_3 = extract_top_ngrams_tfidf(df_neg["description"].tolist(), ngram_range=(3, 3))

        # Label each
        df_neg_2["type"] = "bigram"
        df_neg_3["type"] = "trigram"

        # Combine
        df_neg_tfidf = pd.concat([df_neg_2, df_neg_3], ignore_index=True)

        print(df_neg_tfidf)
    else:
        df_neg_tfidf = pd.DataFrame()


    # ----- POSITIVE
    df_pos = df[df["sentiment_group"] == "positive"]
    if not df_pos.empty:
        print("\n🟢 Top n-grams (POSITIVE group):")

        df_pos_2 = extract_top_ngrams_tfidf(df_pos["description"].tolist(), ngram_range=(2, 2))
        df_pos_3 = extract_top_ngrams_tfidf(df_pos["description"].tolist(), ngram_range=(3, 3))

        df_pos_2["type"] = "bigram"
        df_pos_3["type"] = "trigram"

        df_pos_tfidf = pd.concat([df_pos_2, df_pos_3], ignore_index=True)

        print(df_pos_tfidf)
    else:
        df_pos_tfidf = pd.DataFrame()


    # ----- NEUTRAL
    df_neu = df[df["sentiment_group"] == "neutral"]
    if not df_neu.empty:
        print("\n⚪ Top n-grams (NEUTRAL group):")

        df_neu_2 = extract_top_ngrams_tfidf(df_neu["description"].tolist(), ngram_range=(2, 2))
        df_neu_3 = extract_top_ngrams_tfidf(df_neu["description"].tolist(), ngram_range=(3, 3))

        df_neu_2["type"] = "bigram"
        df_neu_3["type"] = "trigram"

        df_neu_tfidf = pd.concat([df_neu_2, df_neu_3], ignore_index=True)

        print(df_neu_tfidf)
    else:
        df_neu_tfidf = pd.DataFrame()


    # ----- Save to CSVs
    # version = "0.2"
    output_dir = "results"
    os.makedirs(output_dir, exist_ok=True)

    df_all.to_csv(os.path.join(output_dir, f"tfidf_all-{version}.csv"), index=False)
    if not df_neg_tfidf.empty:
        df_neg_tfidf.to_csv(os.path.join(output_dir, f"tfidf_negative-{version}.csv"), index=False)
    if not df_pos_tfidf.empty:
        df_pos_tfidf.to_csv(os.path.join(output_dir, f"tfidf_positive-{version}.csv"), index=False)
    if not df_neu_tfidf.empty:
        df_neu_tfidf.to_csv(os.path.join(output_dir, f"tfidf_neutral-{version}.csv"), index=False)

    print("\n✅ TF-IDF n-gram analysis completed. CSV files generated.")



if __name__ == "__main__":
   

    try:
        nltk.data.find("/home/mahdi/nltk_data/tokenizers/punkt")
    except LookupError:
        raise RuntimeError("NLTK punkt tokenizer not installed. Run nltk.download('punkt') once with internet.")

    sys.exit(main(limit=None,version=0.3))  # set limit=1000 for faster testing