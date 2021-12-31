import os
from collections import OrderedDict, Counter
from typing import Dict

import utils.paths as paths
from utils.converters import file_to_month
from utils.file_utils import read_corpus_generator, build_days_dict, read_file_generator
from utils.tweet_utils import get_day_of_tweet, get_language_of_tweet, get_timestamp_of_tweet, \
    convert_timestamp_to_date, convert_timestamp_to_day


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


def count_testimonies(data_path: str):
    problematic_count = 0
    testimony_count = 0
    not_testimony_count = 0
    no_label_count = 0
    for tweet in read_corpus_generator(data_path):
        if tweet.get("flag"):
            problematic_count += 1
        elif tweet.get('label'):
            if tweet.get('label') == "testimony":
                testimony_count += 1
            if tweet.get('label') == "not_testimony":
                not_testimony_count += 1
        else:
            no_label_count += 1
    print(f"problematic flags : {problematic_count}")
    print(f"testimonies : {testimony_count}")
    print(f"not testimonies : {not_testimony_count}")
    print(f"no label : {no_label_count}")


def write_tweets_to_text(data_path: str, output_path: str):
    problematic_tweets = []
    testimony_tweets = []
    for tweet in read_corpus_generator(data_path):
        if tweet.get("flag"):
            problematic_tweets.append(tweet)
        elif tweet.get('label'):
            if tweet.get('label') == "testimony":
                testimony_tweets.append(tweet)
    with open(output_path, 'w') as output_file:
        output_file.write(f"PROBLEMATIC TWEETS\n\n\n" + ("*" * 100) + "\n\n")
        for tweet in problematic_tweets:
            tweet_en_text = tweet.get('fr_text')
            tweet_ja_text = tweet.get('text')
            output_file.write(f"{tweet_ja_text}" + "\n\n")
            output_file.write(f"{tweet_en_text}\n\n" + ("-" * 40) + "\n\n")
        output_file.write(f"TESTIMONIES\n\n\n" + ("*" * 100) + "\n\n")
        for tweet in testimony_tweets:
            tweet_en_text = tweet.get('fr_text')
            tweet_ja_text = tweet.get('text')
            output_file.write(f"{tweet_ja_text}" + "\n\n")
            output_file.write(f"{tweet_en_text}\n\n" + ("-" * 40) + "\n\n")


def map_files_to_timestamps(data_path: str):
    timestamps = {}
    for file in os.listdir(data_path):
        file_timestamps = []
        for tweet in read_file_generator(os.path.join(data_path, file)):
            file_timestamps.append(get_timestamp_of_tweet(tweet))
        timestamps.update({file: file_timestamps})
    return timestamps


def convert_timestamp_dict_to_day_dict(timestamp_dict: Dict):
    day_dict = {}
    for key, value in timestamp_dict.items():
        day_dict.update({key: [convert_timestamp_to_day(timestamp) for timestamp in value]})
    return day_dict


def extract_day_dict_from_file(data_path: str):
    dates_table = {}
    # all_dates = []
    timestamps_dict = map_files_to_timestamps(data_path)
    day_dict = convert_timestamp_dict_to_day_dict(timestamps_dict)
    # for day_list in day_dict.values():
    #     all_dates.extend(day_list)
    counter = 0
    for key, value in day_dict.items():
        # for date in set(all_dates):
        for date in value:
            dates_table.update({
                f'row_{counter}': [key, date]
            })
            counter += 1
    return dates_table


if __name__ == "__main__":
    time_spans = map_files_to_timestamps(paths.ANNOTATION_CHUNKS)
    print(time_spans)

