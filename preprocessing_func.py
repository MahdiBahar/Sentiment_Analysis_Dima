import re
from typing import List, Optional
import string

def _multiple_replace(mapping, text):
    pattern = "|".join(map(re.escape, mapping.keys()))
    return re.sub(pattern, lambda m: mapping[m.group()], str(text))

def convert_fa_numbers(input_str):
    mapping = {
        '۰': '0',
        '۱': '1',
        '۲': '2',
        '۳': '3',
        '۴': '4',
        '۵': '5',
        '۶': '6',
        '۷': '7',
        '۸': '8',
        '۹': '9',
        '.': '.',
    }
    return _multiple_replace(mapping, input_str)

def convert_en_numbers(input_str):
    mapping = {
         '0': '۰',
         '1' : '۱',
         '2' :'۲',
        '3'  :'۳',
        '4'  :'۴',
        '5' :'۵',
        '6' :'۶',
        '7' :'۷',
        '8' :'۸',
        '9' :'۹',
        '.' :'.'
    }
    return _multiple_replace(mapping, input_str)

def convert_ar_characters(input_str):
    """
    Converts Arabic chars to related Persian unicode char
    """
    mapping = {
        'ك': 'ک',
        'ى': 'ی',
        'ي': 'ی',
        'ئ':'ی',
        'إ':'ا',
        'أ':'ا',
        'ة':'ه',
        'ؤ':'و'
    }
    return _multiple_replace(mapping, input_str)

#-------------------------------------------------------------------------------------------


def merge_mi_prefix(text):
    zwnj = '\u200C'    # zero-width non-joiner
    # note: replacement is NOT a raw string, so \u200C is interpreted properly
    replacement = r'\1' + zwnj + r'\2'
    return re.sub(r'\b(ن?می)\s+(\S+)', replacement, text)

def remove_diacritics(text):
    # define regex for Persian diacritics (Unicode range: \u064B-\u0652)
    diacritics_pattern = re.compile(r'[\u064B-\u0652]')
    return re.sub(diacritics_pattern, '', text)


def convert_number_to_text(text):
    
    return re.sub(r'(\d)\d*', r'\1', text)

def remove_half_space(text):

    text = text.replace('\u200c', '')
    return text

def remove_extra_charecter(text):

    return re.sub(r'(\w)\1{2,}', r'\1\1',text)

def remove_number(text):

    return re.sub(r' [\d+]', ' ',text)

def remove_punctuation(text):

    return re.sub(r'[^\w\[\]]', ' ', text)

def replace_multiple_space(text):

    return re.sub(r'[\s]{2,}', ' ', text)


def map_num_to_text(text):

    # check if the text is exactly '1', '2', '3' , '4', '5'
    mapping = {'1': 'خیلی بد', '2': 'بد', '3': 'متوسط' , '4': 'خوب' , '5': 'عالی'}
    
    if text in mapping:
        return mapping[text]  
    
    return text 
#--------------------------------------------------------------------------------------------------------

def remove_punctuation_except_keep(
    text: str,
    keep: Optional[List[str]] = None
) -> str:
    # Determine which chars to keep 
    default_keep = []
    # default_keep = ['(', ')', '،', '؟', '؛', '«', '»']
    keep_set = set(keep) if keep is not None else set(default_keep)

    # Build the full punctuation set
    ascii_punct   = set(string.punctuation)                    # !"#$%&'()*+,...@
    persian_punct = set(list("،؟؛«»"))                         # common Persian punctuation
    all_punct     = ascii_punct | persian_punct

    # Compute which to remove
    remove_chars = all_punct - keep_set

    # Build regex and clean 
    # [chars]+ will match any run of unwanted punctuation
    pattern = re.compile("[" + re.escape("".join(remove_chars)) + "]+")
    # replace with a single space, then collapse multiple spaces
    cleaned = pattern.sub(" ", text)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned



# Split sentences on . ! ? followed by whitespace
_SENT_SPLIT = re.compile(r'(?<=[.!?])\s+')

def drop_short_sentences(text: str, min_words: int = 0) -> str:
 
    sentences = _SENT_SPLIT.split(text)
    kept = []
    for sent in sentences:
        sent = sent.strip()
        # count words by splitting on whitespace
        if sent and len(sent.split()) >= min_words:
            kept.append(sent)
        # else: drop this sentence entirely
    return ' '.join(kept)



def remove_phrases(text: str, phrases: List[str] = []) -> str:
    if not phrases:
        return text.strip()
    
    # 1) Escape and join into an alternation
    escaped = [re.escape(p) for p in phrases]
    pattern = re.compile(
        r'\s*(?:' + "|".join(escaped) + r')\s*'
    )
    
    # 2) Delete them
    cleaned = pattern.sub(' ', text)
    # 3) Collapse multiple spaces, trim
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    return cleaned



# English + Persian punctuation to pad
PUNCT_CLASS = r'-\*\.,/"\"!?:;،؟؛«»()\[\]{}"\'…'

def add_space_punc(text: str) -> str:
    # 1) ensure a space BEFORE each punctuation (if not already)
    text = re.sub(rf'(?<!\s)([{PUNCT_CLASS}])', r' \1', text)
    # 2) ensure a space AFTER each punctuation (if not already)
    text = re.sub(rf'([{PUNCT_CLASS}])(?!\s)', r'\1 ', text)
    # 3) clean up
    return re.sub(r'\s+', ' ', text).strip()

def remove_space_after_words(text:str, words: List[str] = []) -> str:
    for word in words:
        # match "word + space(s)" and replace with "word"
        pattern = rf"{word}\s+"
        text = re.sub(pattern, word, text)
    return text


## replace space with half space
def replace_before_spaces_with_halfspace(text:str, words: List[str] = []) -> str:
    for word in words:
        # Match "space + word" and replace with "half-space + word"
        pattern = rf"\s+{word}"
        text = re.sub(pattern, f"\u200c{word}", text)
    return text

# remove ها / های / هایی
def remove_ha_s_suffix(text):

    pattern = r'(?:\s|‌)?ها(?:ی(?:ی)?)?\b'
    return re.sub(pattern, '', text)

# Quick check
# text = """چک برگشتی و ضمانتنامه بلاتکلیف (صرفا ضمانتنامه) نزد سپام می‌باشد ."
# "در (صورت ریز مسدودی و رفع مسدودی) مبلغ مسدودی وجود ندارد اما در (گزارش صورتحساب سی و پنج گردش آخر) مبلغ مسدودی نمایش داده می‌شود . از طریق سامانه بک آفیس » سپرده » انسداد/رفع انسداد گروهی » صورت ریز مسدودی و رفع مسدودی ها بررسی گردد . در (صورت ریز مسدودی و رفع مسدودی) موارد ذیل نمایش داده نمی‌شود : 1-مسدودی بابت حج 2-مسدودی بابت شارژ 3-مسدودی بابت خدمات بورس 4- مسدودی بابت یارانه تکمیلی 5- تامین موجودی چک برگشتی که در (گزارش انسداد/رفع انسداد مبلغ بابت تامین موجودی چک برگشتی) قابل مشاهده می‌باشد . 6- درصورتی که حساب تعداد مسدودی زیادی دارد به دلیل محدودیت نمایش (100 ردیف) ، تاریخ آخرین ردیف مسدودی نمایش داده شده در گزارش 1941 را در فیلد ""از تاریخ"" وارد کرده و تعداد مسدودی های بعدی را نمایش می‌دهد . (اطلاعات مورد نیاز : شماره حساب - مبلغ مسدودی)"
# "در (گزارش صورتحساب سی و پنج گردش آخر) مبلغی با شرح ""برگشت حواله ساتنا/پایا"" نمایش داده می‌شود . پس از صدور حواله ساتنا/پایا ، در صورتی که شبای مقصد دارای استعلام سیاح نامعتبر باشد ، حواله ساتنا/پایا برگشت داده می‌شود . لازم به ذکر است صدور حواله پایا به شبای تسهیلات بدون بررسی استعلام سیاح امکان پذیر می‌باشد . نکته : امکان صدور حواله پایا ، ساتنا برای مشتریان خارجی (حقیقی و حقوقی ) امکان پذیر نمی‌باشد . لازم به ذکر است که صدور حواله از سمت بانک ملت انجام می‌گردد اما از سمت بانک مرکزی رد می‌گردد ."
# "در (گزارش وضعیت استعلام سیاح از بانک مرکزی) پاسخ استعلام با شرح ""در انتظار پاسخ"" نمایش داده می‌شود . در صورتی اطلاعات مشتری اصلاح شود و در سامانه متمرکز نیز تغییرات اعمال گردد ، این اصلاحات از سوی بانک مرکزی در سامانه سیاح تغییر پیدا نمی‌کند . اصلاحاتی از جمله اصلاح کد ملی و . . . برای مشتری حقیقی ، اصلاح شناسه ملی و . . . برای مشتری حقوقی و اصلاح شماره فراگیر و . . . برای مشتری خارجی . جهت رفع مشکل اینگونه موارد می‌بایست حساب مذکور بسته شده و با اطلاعات اصلاح شده نسبت به افتتاح حساب جدید اقدام گردد ."
# جهت پیگیری وندیا های ارسال شده از چه طریق می‌بایست اقدام گردد ؟ """
# print(add_space_punc(')('))                 # -> ') ('
# print(add_space_punc(text))             # -> '( سلام )'
# print(add_space_punc('«سلام»(اطلاعات)'))   # -> '« سلام » ( اطلاعات )'
