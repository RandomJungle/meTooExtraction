import spacy
from tqdm import tqdm

from utils.file_utils import read_corpus_generator


def tokenize_all_tweet_texts(data_path: str, stopword_path: str, text_key: str = "text"):
    # keeping only 'NOUN', ' ADP', ' VERB', 'AUX', 'ADV', 'PROPN', 'ADJ', 'PART', 'NOUN|Polarity=Neg'
    # as valid POS for wordcloud
    forbidden_pos = ['SCONJ', 'PUNCT', 'DET', 'NUM', 'PRON', 'CCONJ', 'SYM', 'INTJ', 'SCONJ|Polarity=Neg']
    with open(stopword_path, 'r') as stopword_file:
        stopwords = [line.strip() for line in stopword_file.readlines()]
    nlp = spacy.load('ja_core_news_trf')
    texts = []
    for tweet in tqdm(read_corpus_generator(data_path)):
        if tweet.get('labels').get('category') == 'testimony':
            doc = nlp(tweet.get(text_key))
            texts.append(" ".join([token.text for token in doc
                                   if token.text not in stopwords
                                   and token.pos_ not in forbidden_pos]))
    return " ".join(texts)