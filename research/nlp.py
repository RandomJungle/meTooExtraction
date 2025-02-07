import json
import re

import pandas as pd
import spacy

from typing import Callable, List, Tuple
from spacy.matcher import Matcher
from tqdm import tqdm

from research.stopwords import japanese_stopwords
from utils.file_utils import read_corpus_generator


def analyze_tweet_generator(input_data_path, model):
    nlp = spacy.load(model)
    for tweet in read_corpus_generator(input_data_path):
        yield nlp(tweet.get('text'))


def tokenize_tweet(
        row: pd.Series,
        model: spacy.Language,
        text_column: str) -> Tuple[List[str], List[str]]:
    # keeping only 'NOUN', ' VERB', 'ADV', 'PROPN', 'ADJ', 'PART', 'NOUN|Polarity=Neg'
    forbidden_pos = [
        'SCONJ', 'PUNCT', 'DET', 'NUM', 'PRON', 'CCONJ',
        'SYM', 'SCONJ|Polarity=Neg', 'ADP', 'AUX', 'SPACE'
    ]
    words_to_keep = ['嫌がらせ', '歳']
    doc = model(row[text_column])
    tokens = [token.text for token in doc]
    filtered_tokens = [
        token.text for token in doc
        if token.text not in japanese_stopwords
           and (token.pos_ not in forbidden_pos or token.text in words_to_keep)
    ]
    return tokens, filtered_tokens


def tokenize_tweets_df(
        dataframe: pd.DataFrame,
        text_column: str = 'text',
        tokens_column: str = 'tokens'):
    nlp = spacy.load('ja_core_news_trf')
    dataframe[[tokens_column, f'{tokens_column}_filtered']] = dataframe.apply(
        lambda row: tokenize_tweet(row, nlp, text_column),
        index=0
    )
    return dataframe


def list_words_per_morpho_tag(data_path: str, text_key: str = "text", hard_limit: int = None):
    nlp = spacy.load('ja_core_news_trf')
    tags = {}
    counter = 0
    for tweet in tqdm(read_corpus_generator(data_path)):
        doc = nlp(tweet.get(text_key))
        for token in doc:
            word_list = tags.get(token.pos_, [])
            word_list.append(token.text)
            tags.update({token.pos_: word_list})
        counter += 1
        if counter > hard_limit:
            break
    return tags


def annotate_lexical_field(
        tweet_list: List,
        lexical_fields_path: str,
        lexical_field_keys: List[str] = None):
    nlp = spacy.load('ja_core_news_trf')
    with open(lexical_fields_path, 'r') as lexical_fields_file:
        lexical_fields_dict = json.loads(lexical_fields_file.read())
    if lexical_field_keys:
        for key in lexical_fields_dict.keys():
            if key not in lexical_field_keys:
                lexical_fields_dict.pop(key)
    tweets = []
    for tweet in tqdm(tweet_list):
        tweet_text = tweet.get('text')
        annotations = []
        for label, keywords in lexical_fields_dict.items():
            annotations.extend(annotate_from_model(keywords, label, tweet_text, nlp))
        if annotations:
            tweet.update({'annotations': annotations})
        else:
            tweet.update({'annotations': []})
        tweets.append(tweet)
    return tweets


def annotate_from_string(keywords: List[str], label: str, text: str):
    annotations = []
    for keyword in keywords:
        for match in re.finditer(keyword, text):
            annotations.append({
                "label": label,
                "start_offset": match.span()[0],
                "end_offset": match.span()[1],
                "text": match.group()})
    return annotations


def annotate_from_model(keywords: List[str], label: str, text: str, model: spacy.Language):
    annotations = []
    doc = model(text)
    matcher = Matcher(model.vocab)
    patterns = [[{"LOWER": keyword}] for keyword in keywords]
    matcher.add("testRule", patterns)
    matches = matcher(doc)
    for match_id, start, end in matches:
        annotations.append({
            "label": label,
            "start_offset": get_start_offset(start, doc),
            "end_offset": get_end_offset(start, end, doc),
            "text": doc[start:end].text})
    return annotations


def get_start_offset(start_position, doc):
    return doc[start_position].idx


def get_end_offset(start_position, end_position, doc):
    return doc[start_position].idx + len(doc[start_position:end_position].text)


if __name__ == "__main__":
    tags = list_words_per_morpho_tag('', "text", hard_limit=200)
    for tag, words in tags.items():
        print(tag)
        print(set(words))
