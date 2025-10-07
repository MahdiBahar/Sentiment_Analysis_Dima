# Sentiment_Analysis_Dima

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
