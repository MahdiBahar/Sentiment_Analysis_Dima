
# Import libraries
from transformers import MT5ForConditionalGeneration, MT5Tokenizer, pipeline
# from googletrans import Translator
from deep_translator import GoogleTranslator
import os
import time 


def load_models():
    ## For using local models
    os.environ["TRANSFORMERS_OFFLINE"] = "1"
    os.environ["HF_DATASETS_OFFLINE"] = "1"


    
    # Load the tokenizer and model
    model_name = "persiannlp/mt5-base-parsinlu-sentiment-analysis"
    # tokenizer = MT5Tokenizer.from_pretrained(model_name)
    # model = MT5ForConditionalGeneration.from_pretrained(model_name)

    # For localize the first model
    tokenizer = MT5Tokenizer.from_pretrained(
        model_name,
        local_files_only=True
    )

    model = MT5ForConditionalGeneration.from_pretrained(
        model_name,
        local_files_only=True
    )

    # Load the second model (Hugging Face pipeline)
    # classifier = pipeline("sentiment-analysis", device=-1)

    ## For localize the second model
    classifier = pipeline(
        "sentiment-analysis",
        model="/home/mahdi/.cache/huggingface/hub/models--distilbert-base-uncased-finetuned-sst-2-english/snapshots/714eb0fa89d2f80546fda750413ed43d93601a13",
        tokenizer="/home/mahdi/.cache/huggingface/hub/models--distilbert-base-uncased-finetuned-sst-2-english/snapshots/714eb0fa89d2f80546fda750413ed43d93601a13",
        device=-1
    )


    # Initialize Google Translator
    # translator = Translator()
    translator = GoogleTranslator(source="auto", target="en")
    # Sentiment mapping for scoring
    


    return tokenizer, model, classifier, translator

tokenizer, model, classifier, translator = load_models()


def run_first_model(logger,context, text_b="نظر شما چیست", **generator_args):
    try:

        logger.debug(f"Running MT5 model for text: {context}")
        input_ids = tokenizer.encode(context + "<sep>" + text_b, return_tensors="pt")
        res = model.generate(input_ids, **generator_args)
        output = tokenizer.batch_decode(res, skip_special_tokens=True)

        if not output:
            raise ValueError("Model returned empty output.")
        logger.info(f"MT5 model output: {output[0]}")
        return output[0]
    except Exception as e:
        logger.error(f"Error in run_model: {e}", exc_info=False)
        return "no sentiment expressed"

def run_second_model(logger, comment_text):
    try:
        logger.debug(f"Running second model for text: {comment_text}")
        # translated_text = translator.translate(comment_text, dest="en").text
        translated_text = translator.translate(comment_text)
        if not translated_text:
            raise ValueError("Translation returned empty text.")
        
        result = classifier(translated_text)
        if not result or not isinstance(result, list):
            raise ValueError("Classifier returned invalid result.")
        
        logger.info(f"Second model output: {result[0]['label']}")
        return result[0]["label"]
    except Exception as e:
        logger.error(f"Error in run_second_model: {e}", exc_info=False)
        return "no sentiment expressed"

# Validate sentiment result and assign score
def validate_and_score_sentiment(logger, sentiment_result, SENTIMENT_SCORES):
    sentiment_result = sentiment_result.lower()
    if sentiment_result not in SENTIMENT_SCORES:
        sentiment_result = "no sentiment expressed"
    sentiment_score = SENTIMENT_SCORES[sentiment_result]
    logger.debug(f"Validated sentiment: {sentiment_result}, Score: {sentiment_score}")
    return sentiment_result, sentiment_score
