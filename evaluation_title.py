import pandas as pd
import numpy as np
from sklearn.metrics import (
    confusion_matrix,
    classification_report,
    accuracy_score,
    precision_score,
    recall_score,
    f1_score
)
import seaborn as sns
import matplotlib.pyplot as plt

# ==============================
# 1️⃣ Load CSV
# ==============================
df = pd.read_csv("/home/mahdi/Sentiment_Analysis_Dima/evaluation/evaluation_title_after_annotation-V0.6.csv")

# Clean data
df = df.dropna(subset=["Annot_title", "ai_title", "title"])

TITLE_AI_TITLE_MAPping = {
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



df["Annot_title"] = df["Annot_title"].astype(str)
df["ai_title"] = df["ai_title"].astype(str)
df["title"] = df["title"].astype(str)
df["title_mapped"] = df["title"].map(TITLE_AI_TITLE_MAPping)

y_true = df["Annot_title"]
y_ai = df["ai_title"]
y_baseline = df["title_mapped"]

# ==============================
# 2️⃣ Evaluation Function
# ==============================

def evaluate_model(y_true, y_pred, model_name):

    accuracy = accuracy_score(y_true, y_pred)
    precision_macro = precision_score(y_true, y_pred, average="macro", zero_division=0)
    recall_macro = recall_score(y_true, y_pred, average="macro", zero_division=0)
    f1_macro = f1_score(y_true, y_pred, average="macro", zero_division=0)

    precision_weighted = precision_score(y_true, y_pred, average="weighted", zero_division=0)
    recall_weighted = recall_score(y_true, y_pred, average="weighted", zero_division=0)
    f1_weighted = f1_score(y_true, y_pred, average="weighted", zero_division=0)

    print(f"\n========== {model_name} ==========")
    print(f"Accuracy: {accuracy:.4f}")
    print(f"F1 Macro: {f1_macro:.4f}")
    print(f"F1 Weighted: {f1_weighted:.4f}")
    print("\nPer-class report:")
    print(classification_report(y_true, y_pred, zero_division=0))

    return {
        "Model": model_name,
        "Accuracy": accuracy,
        "Precision_macro": precision_macro,
        "Recall_macro": recall_macro,
        "F1_macro": f1_macro,
        "Precision_weighted": precision_weighted,
        "Recall_weighted": recall_weighted,
        "F1_weighted": f1_weighted
    }

# ==============================
# 3️⃣ Run Evaluation
# ==============================

results_ai = evaluate_model(y_true, y_ai, "LLM (AI_title)")
results_baseline = evaluate_model(y_true, y_baseline, "Baseline (title)")


# ==============================
# 4️⃣ Comparison Table
# ==============================

comparison_df = pd.DataFrame([results_ai, results_baseline])
print("\n====== Model Comparison ======")
print(comparison_df)


from sklearn.metrics import cohen_kappa_score

kappa_llm = cohen_kappa_score(y_true, y_ai)
kappa_baseline = cohen_kappa_score(y_true, y_baseline)

print("\n===== Cohen's Kappa =====")
print(f"LLM Kappa: {kappa_llm:.4f}")
print(f"Baseline Kappa: {kappa_baseline:.4f}")
# ==============================
# 5️⃣ Confusion Matrices
# ==============================

labels = sorted(list(set(y_true) | set(y_ai) | set(y_baseline)))

def plot_confusion(y_true, y_pred, title_name):
    cm = confusion_matrix(y_true, y_pred, labels=labels)
    plt.figure(figsize=(8,6))
    sns.heatmap(cm,
                annot=True,
                fmt="d",
                cmap="Blues",
                xticklabels=labels,
                yticklabels=labels)
    plt.xlabel("Predicted")
    plt.ylabel("Actual")
    plt.title(title_name)
    plt.xticks(rotation=45)
    plt.yticks(rotation=45)
    plt.tight_layout()
    plt.show()


plot_confusion(y_true, y_ai, "Confusion Matrix - LLM")
plot_confusion(y_true, y_baseline, "Confusion Matrix - Baseline")