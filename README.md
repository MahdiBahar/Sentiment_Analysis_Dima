# Sentiment_Analysis_Dima



Dima User Feedback Analytics Pipeline
Overview

This project implements an end-to-end analytics pipeline for user feedback (comments) collected from the Dima banking application.
The goal is to transform raw user comments into actionable insights for product, engineering, and business teams.

The pipeline covers:

Data ingestion into a database

Sentiment analysis (with fallback logic)

Duplicate / repetitive comment detection

Text analytics (TF-IDF, n-grams)

Representative comment extraction (core, outliers, suggestions)

Structured summarization using a local LLM (Phi-4 via Ollama)

JSON outputs ready for dashboards and UI consumption

High-Level Architecture

                ┌────────────────────┐
                │  Raw User Comments  │
                └─────────┬──────────┘
                          │
                          ▼
                ┌────────────────────┐
                │ PostgreSQL Database │
                │  (comments table)  │
                └─────────┬──────────┘
                          │
          ┌───────────────┼────────────────┐
          │               │                │
          ▼               ▼                ▼
 Sentiment Analysis   Duplicate Detection   Text Analytics
 (MT5 + fallback)   (per user & time)     (TF-IDF, n-grams)
          │               │                │
          └───────────────┼────────────────┘
                          ▼
            Representative Comment Selection
          (core / outliers / suggestions)
                          │
                          ▼
              LLM-Based Structured Summaries
                 (Phi-4 via Ollama)
                          │
                          ▼
               JSON Outputs for UI & BI

Database Schema (Key Columns)

Table: comments

Column name	Description
id	Primary key
title	Feature or functional area
grade	User rating (e.g. 1–5)
description	User comment text
national_code_hash	Hashed user identifier
mobile_no_hash	Hashed mobile number
created_at	Original comment timestamp
imported_at	Ingestion timestamp
sentiment_result	Sentiment label
sentiment_score	Numeric sentiment score
second_model_processed	Fallback model flag
is_repetitive	Duplicate flag
duplicate_of	Reference to original comment
1. Data Ingestion

User comments are ingested from external sources (e.g. app stores).

Data is inserted into PostgreSQL using a stable ingestion script.

All text processing happens after persistence.

2. Sentiment Analysis Pipeline
2.1 Primary Sentiment Model

Model: persiannlp/mt5-base-parsinlu-sentiment-analysis

Output labels:

very negative

negative

neutral

mixed

positive

very positive

no sentiment expressed

2.2 Fallback Model Logic

If the primary model outputs:

neutral

mixed

no sentiment expressed

Then:

A secondary model is executed (via translation + English classifier).

2.3 Empty Comment Handling

If description is null or empty:

Skip all models

Set:

sentiment_result = "no comments"

sentiment_score = 0

2.4 Sentiment Score Mapping
Sentiment label	Score
very negative	1
negative	2
neutral / mixed	3
positive	4
very positive	5
no comments	0
2.5 Batch Processing

Comments are processed in batches (e.g. LIMIT 100)

Only rows with sentiment_result IS NULL are selected

Results are written back to the database

3. Repetitive / Duplicate Comment Detection
Goal

Detect spam or repetitive feedback from the same user.

Rules

A comment is marked as repetitive if:

Same national_code_hash

Same normalized description

Same title (feature)

Time difference ≤ 1 hour

sentiment_result != "no comments"

Output

is_repetitive = TRUE

duplicate_of = <id of original comment>

Reset Strategy

Before each run:

UPDATE comments
SET is_repetitive = FALSE,
    duplicate_of = NULL;

4. Text Analytics (TF-IDF & N-grams)
Preprocessing

Custom Persian preprocessing function

Optional:

number normalization

punctuation removal

diacritics removal

Tokenization using NLTK

Lightweight Persian stopwords

TF-IDF Analysis

Extracts:

Bigrams (2-grams)

Trigrams (3-grams)

Computed separately for:

All comments

Negative group (negative + very negative)

Positive group (positive + very positive)

Neutral group (neutral + mixed + no sentiment expressed)

Output

Printed to console

Optionally exported as CSV or TXT

5. Representative Comment Extraction (Non-LLM)
Objective

Reduce thousands of comments into a small, meaningful subset.

Outputs

Core: most representative comments

Outliers: rare / novel / risky comments

Suggestions: feature requests

Method

TF-IDF vectorization

Centroid similarity for core selection

Low-similarity or unique patterns for outliers

Rule-based + TF-IDF signals for suggestions

6. LLM-Based Structured Summarization
Model

Phi-4, running locally via Ollama

Design Principles

LLM is called after filtering, not on raw data

Input size is controlled (top-N representative comments)

Output is strict JSON, ready for UI consumption

Output Structure
{
  "meta": {
    "app": "dima",
    "language": "fa",
    "model": "phi4:latest",
    "generated_at": "ISO-UTC"
  },
  "top_issues": [...],
  "top_suggestions": [...],
  "outliers": [...],
  "recommended_actions": [...]
}

Evidence Rules

Evidence must be:

Direct quotes

Persian only

Substrings of input comments

Post-processing fixes invalid evidence automatically

7. Design Philosophy & Constraints

Each comment is processed once at ingestion time

Daily volume is low (≈ <100 comments/day)

Pipeline prioritizes:

Stability

Traceability

Explainability

Heavy LLM usage is avoided on raw data

Current Capabilities

✔ Persistent sentiment labeling
✔ Duplicate detection
✔ Feature-level insight extraction
✔ Actionable summaries for business teams
✔ JSON outputs for dashboards and APIs

Next Possible Extensions

Per-title (feature-level) summaries

Trend analysis over time (weekly / monthly)

Alerting on high-risk outliers

Auto-generated Jira / backlog items

Topic clustering over summarized titles











# Issues


## We have a problem in proxy setting which eliminate me to sync the files in github
#### Solution:  
unset http_proxy
unset https_proxy
unset all_proxy
unset HTTP_PROXY
unset HTTPS_PROXY
unset ALL_PROXY
unset ftp_proxy
unset FTP_PROXY

git pull --tags origin main

Last job: Reopen VScode 

### Problem in installing nltk
#This is a new NLTK 3.8+ change: they split the sentence tokenizer model (punkt) into two parts —
👉 punkt (base model)
👉 punkt_tab (language-specific data tables)
# Solution:

import nltk

# Add local path in case it's not found
nltk.data.path.append("/home/mahdi/nltk_data")

# Ensure both punkt and punkt_tab are available
for pkg in ["punkt", "punkt_tab"]:
    try:
        nltk.data.find(f"tokenizers/{pkg}")
    except LookupError:
        nltk.download(pkg, quiet=True)


# How to call insert data function in database?

Ex: python main_import_comments_and_hash.py data/feedback_9-25-test-V0.1.csv


# To solve the error about using google translate library:
We use this:
from deep_translator import GoogleTranslator