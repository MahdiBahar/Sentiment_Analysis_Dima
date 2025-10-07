
from preprocessing_func import convert_fa_numbers, convert_ar_characters, convert_en_numbers,remove_phrases, add_space_punc, remove_space_after_words
from preprocessing_func import  remove_diacritics, map_num_to_text, merge_mi_prefix, replace_multiple_space, remove_punctuation_except_keep
from preprocessing_func import remove_half_space, remove_extra_charecter, remove_number, remove_punctuation, drop_short_sentences, replace_before_spaces_with_halfspace
from preprocessing_func import remove_ha_s_suffix
# import re



def preprocess(text,
               convert_farsi_numbers = False,
               convert_english_numbers = False,
               convert_arabic_characters = False,
               remove_diacritic = False,
               convert_emojis = False,
               remove_halfspace = False,
               remove_removelist = False,
               remove_extra_characters = False,
               remove_numbers = False,
               remove_punctuations = False,
               remove_punctuation_exception_keep = None ,
               replace_multiple_spaces = False,
               handle_prefix = False,
               map_number_to_text = False,
               drop_short_phrases = 0,
               remove_specific_phrases = None,
               add_spaces_punc = False,
               remove_space_after_word = None,
               replace_before_space_with_half_space = None,
               remove_ha_suffix = True
               ):
    
    text = text.strip()

    if drop_short_phrases>0:

        text = drop_short_sentences(text, drop_short_phrases)


    if convert_farsi_numbers:
        text = convert_fa_numbers(text)

    if convert_arabic_characters:
# convert arabic characters to persian
        text = convert_ar_characters(text)

    if remove_diacritic:
        text = remove_diacritics(text)

    if remove_removelist:
        removelist = "<>"
        # text = re.sub(r'[^\w'+removelist+']', ' ', text)
        # text = re.sub(r'[^\w]', ' ', text)
        # text = re.sub(r'((#)[\w]*)','#',text)
    
    if remove_halfspace:
# remove half space
        text = remove_half_space(text)
    
    if remove_extra_characters:
        text = remove_extra_charecter(text)

    if map_number_to_text:
        text = map_num_to_text(text)

# remove numbers
    if remove_numbers:
        text = remove_number(text)

# convert english numbers to persian
    if convert_english_numbers:
        text = convert_en_numbers(text)
    
# remove punctuations
    if remove_punctuations:
        text = remove_punctuation(text)

# remove punctuation exception
    if remove_punctuation_exception_keep:
        text = remove_punctuation_except_keep(text, remove_punctuation_exception_keep)

# replace multiple spaces with one space
    if replace_multiple_spaces:
        text = replace_multiple_space(text)

# prefix
    if handle_prefix:
        text = merge_mi_prefix(text)
   
#remove specific phrases
    if remove_specific_phrases:
        text = remove_phrases(text,remove_specific_phrases)

    if add_spaces_punc:
        text = add_space_punc(text)

#remove space after list of words

    if remove_space_after_word:
        text = remove_space_after_words(text,remove_space_after_word)

#replace before space with half space

    if replace_before_space_with_half_space:
        text = replace_before_spaces_with_halfspace(text,replace_before_space_with_half_space)

# remove ha suffix
    if remove_ha_suffix:
        text = remove_ha_s_suffix(text)

    return(text)