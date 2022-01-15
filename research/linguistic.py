import spacy

import utils.paths as paths

from utils.file_utils import read_corpus_generator


def load_large_model():
    return spacy.load("ja_core_news_lg")


def load_transformer_model():
    return spacy.load("ja_core_news_trf")


def analyze_tweet_generator(input_data_path, model):
    nlp = spacy.load(model)
    for tweet in read_corpus_generator(input_data_path):
        yield nlp(tweet.get('text'))


def read_verbs(input_data_path, model="ja_core_news_trf"):
    for document in analyze_tweet_generator(input_data_path, model):
        print([word.text for word in document if word.pos_ == "VERB"])


if __name__ == "__main__":
    read_verbs(paths.FINAL_CORPUS_DIR_PATH)