import os
from collections import OrderedDict, Counter

from utils.converters import file_to_month
from utils.file_utils import read_corpus_generator, build_days_dict
from utils.paths import RAW_DATA_PATH, CLEAN_DATA_PATH, TRANSLATED_DATA_PATH
from utils.tweet_utils import get_day_of_tweet, get_language_of_tweet


def count_corpus(data_path: str):
    total = 0
    for file_name in os.listdir(data_path):
        total += sum(1 for l in open(os.path.join(data_path, file_name)))
    return total


def count_corpus_per_month(data_path: str):
    months = OrderedDict()
    for file_name in sorted(os.listdir(data_path)):
        key = file_to_month.get(file_name)
        months.update({key: sum(1 for l in open(os.path.join(data_path, file_name)))})
    return months


def count_corpus_per_day(data_path: str):
    days = build_days_dict()
    for entry in read_corpus_generator(data_path):
        date = get_day_of_tweet(entry)
        days.update({date: days.get(date, 0) + 1})
    return days


def count_languages(data_path: str):
    languages = {}
    for entry in read_corpus_generator(data_path):
        language = get_language_of_tweet(entry)
        languages.update({language: languages.get(language, 0) + 1})
    return languages


def print_other_languages_tweets(data_path: str):
    for entry in read_corpus_generator(data_path):
        language = get_language_of_tweet(entry)
        if language == "und":
            text = entry.get('text') 
            print(f"{language} : {text}" + '\n' + ('-' * 100) + '\n')
            
            
def count_copycats(data_path: str):
    texts = []
    for tweet in read_corpus_generator(data_path):
        texts.append(tweet.get('text'))
    return len(texts) - len(set(texts))


def print_copycats(data_path: str):
    texts = []
    for tweet in read_corpus_generator(data_path):
        texts.append(tweet.get('english_text'))
    for key, value in Counter(texts).items():
        if value > 1:
            print(f"\n{value} appearances for :\n{key}\n\n" + ("-" * 50))
        

if __name__ == "__main__":
    print_copycats(TRANSLATED_DATA_PATH)
