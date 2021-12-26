import json
import os
from collections import OrderedDict, Counter
from typing import Dict

from utils.converters import file_to_month
from utils.file_utils import read_corpus_generator, build_days_dict, read_file_generator
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


def print_copycats(data_path: str, key="text"):
    texts = []
    for tweet in read_corpus_generator(data_path):
        texts.append(tweet.get(key))
    for key, value in Counter(texts).items():
        if value > 1:
            print(f"\n{value} appearances for :\n{key}\n\n" + ("-" * 50))


def get_all_hashtags(data_path: str):
    hashtags_found = []
    for tweet in read_corpus_generator(data_path):
        hashtags = tweet.get("entities", dict()).get("hashtags", dict())
        for hashtag in hashtags:
            hashtags_found.append(hashtag.get('tag').lower())
    counter = Counter(hashtags_found)
    return {key: value for key, value
            in sorted(counter.items(), key=lambda item: item[1]) if value > 50}


def write_all_hashtags(data_path: str, output_path: str):
    with open(output_path, "w") as output_file:
        for key, value in get_all_hashtags(data_path).items():
            output_file.write(f"{value} tweets found with {key}\n")


def print_tweets_with_hashtag(data_path: str, hashtag: str):
    for tweet in read_corpus_generator(data_path):
        if hashtag in tweet.get('text'):
            print(tweet.get('text') + "\n\n" + ("-" * 50) + "\n\n")


def count_hashtag_per_day(data_path: str, hashtag: str):
    days = build_days_dict()
    for tweet in read_corpus_generator(data_path):
        if hashtag.lower() in tweet.get('text').lower():
            date = get_day_of_tweet(tweet)
            days.update({date: days.get(date, 0) + 1})
    return days


def count_hashtag_per_month(data_path: str, hashtag: str):
    months = OrderedDict()
    for file_name in sorted(os.listdir(data_path)):
        key = file_to_month.get(file_name)
        hashtag_count = 0
        for tweet in read_file_generator(os.path.join(data_path, file_name)):
            if hashtag.lower() in tweet.get('text').lower():
                hashtag_count += 1
        months.update({key: hashtag_count})
    return months


def count_tweet_per_user(data_path: str):
    author_ids = []
    for tweet in read_corpus_generator(data_path):
        author_ids.append(tweet.get('author_id'))
    return Counter(author_ids)


def count_users_tweet_counts(tweet_per_user: Dict):
    counts_list = []
    for value in tweet_per_user.values():
        counts_list.append(value)
    return Counter(counts_list)


def print_outliers_user_ids(data_path: str, threshold: int = 1000):
    counter = count_tweet_per_user(data_path)
    outlier_ids = []
    for key, value in counter.items():
        if value > threshold:
            outlier_ids.append(key)
            print(f"{key}: {value}")
        

if __name__ == "__main__":
    print_outliers_user_ids(CLEAN_DATA_PATH)
