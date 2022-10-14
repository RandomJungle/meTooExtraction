import datetime
import json
import os

from collections import OrderedDict, Counter
from typing import Dict, List, Callable

import utils.paths as paths

from utils.converters import file_to_month
from utils.file_utils import read_corpus_generator, read_jsonl_generator, read_variable_dict, read_analysis_csv
from utils.tweet_utils import get_day_of_tweet, get_language_of_tweet, get_hour_of_tweet


def build_days_dict(start_date, end_date):
    dates = {}
    start_date = datetime.date(start_date[0], start_date[1], start_date[2])
    end_date = datetime.date(end_date[0], end_date[1], end_date[2])
    delta = datetime.timedelta(days=1)
    while start_date <= end_date:
        dates.update({f"{start_date}": 0})
        start_date += delta
    return dates


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


def count_corpus_per_day(
        data_path: str,
        start_date=(2017, 10, 1),
        end_date=(2018, 10, 31),
        filter_function=None):
    days = build_days_dict(start_date, end_date)
    for tweet in read_corpus_generator(data_path):
        date = get_day_of_tweet(tweet)
        if (filter_function and filter_function(tweet)) or not filter_function:
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
            in sorted(counter.items(), key=lambda item: item[1]) if value > 1}


def write_all_hashtags(data_path: str, output_path: str):
    with open(output_path, "w") as output_file:
        for key, value in get_all_hashtags(data_path).items():
            output_file.write(f"{value} tweets found with {key}\n")


def print_tweets_with_hashtag(data_path: str, hashtag: str):
    for tweet in read_corpus_generator(data_path):
        if hashtag in tweet.get('text'):
            print(tweet.get('text') + "\n\n" + ("-" * 50) + "\n\n")


def count_hashtag_per_day(
        data_path: str,
        hashtag: str,
        start_date=(2017, 10, 1),
        end_date=(2018, 10, 31),
        filter_function: Callable = None):
    days = build_days_dict(start_date, end_date)
    for tweet in read_corpus_generator(data_path):
        if (filter_function and filter_function(tweet)) or not filter_function:
            if hashtag.lower() in tweet.get('text').lower():
                date = get_day_of_tweet(tweet)
                days.update({date: days.get(date, 0) + 1})
    return days


def count_hashtag_per_month(data_path: str, hashtag: str):
    months = OrderedDict()
    for file_name in sorted(os.listdir(data_path)):
        key = file_to_month.get(file_name)
        hashtag_count = 0
        for tweet in read_jsonl_generator(os.path.join(data_path, file_name)):
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


def count_labels(data_path: str):
    count_dict = {}
    for tweet in read_corpus_generator(data_path):
        label = tweet.get('labels', {}).get('category')
        label = label if label else "not_testimony"
        count_dict.update({label: count_dict.get(label, 0) + 1})
    return count_dict


def count_public_metrics(data_path: str, metric_name: str, filter_function: Callable = None):
    count_dict = {}
    for tweet in read_corpus_generator(data_path):
        if (filter_function and filter_function(tweet)) or not filter_function:
            metric = tweet.get('public_metrics').get(metric_name)
            count_dict.update({metric: count_dict.get(metric, 0) + 1})
    return count_dict


def count_gendered_corpus(data_path: str, filter_function: Callable = None):
    count_dict = {}
    for tweet in read_corpus_generator(data_path):
        if (filter_function and filter_function(tweet)) or not filter_function:
            if tweet.get('labels'):
                genders = tweet.get('labels').get('genders')
                for gender in genders:
                    count_dict.update({gender: count_dict.get(gender, 0) + 1})
    return count_dict


def count_quote_corpus(data_path: str, filter_function: Callable = None):
    count_dict = {}
    for tweet in read_corpus_generator(data_path):
        if (filter_function and filter_function(tweet)) or not filter_function:
            if tweet.get('labels'):
                quote_types = tweet.get('labels').get('quote_types')
                if quote_types:
                    for quote_type in quote_types:
                        count_dict.update({quote_type: count_dict.get(quote_type, 0) + 1})
                else:
                    count_dict.update({"no quote": count_dict.get("has_no_quote", 0) + 1})
    return count_dict


def count_annotations(data_path: str, label_keys: List[str] = None):
    count_dict = {}
    for tweet in read_corpus_generator(data_path):
        annotations = tweet.get("annotations")
        if label_keys:
            annotations = [annotation for annotation in annotations
                           if annotation.get('label') in label_keys]
        for annotation in annotations:
            label = annotation.get('label')
            count_dict.update({label: count_dict.get(label, 0) + 1})
    return count_dict


def count_annotation_texts(data_path: str, label_key: str):
    count_dict = dict()
    for tweet in read_corpus_generator(data_path):
        annotations = [annot for annot in tweet.get("annotations")
                       if annot.get('label') == label_key]
        for annotation in annotations:
            text = annotation.get('text')
            count_dict.update({text: count_dict.get(text, 0) + 1})
    return count_dict


def count_tweets_per_query(data_path: str, query_json_path: str):
    with open(query_json_path, 'r') as query_json_file:
        queries_dict = json.loads(query_json_file.read())
    results = {}
    queries_keywords = {}
    for query_name, query in queries_dict.items():
        query_list = query.replace('(', '').replace(')', '').split(' ')
        keywords = [keyword for keyword in query_list if keyword.startswith('#')]
        negative_keywords = [keyword.replace('-', '') for keyword in query.split(' ') if keyword.startswith('-#')]
        queries_keywords.update({query_name: {'keywords': keywords, 'negative_keywords': negative_keywords}})
    for tweet in read_corpus_generator(data_path):
        tweet_text = tweet.get('text').lower().replace('＃', '#').replace('♯', '#')
        found = False
        for query_name, keywords_dict in queries_keywords.items():
            keywords = keywords_dict.get('keywords')
            negative_keywords = keywords_dict.get('negative_keywords')
            if any([keyword in tweet_text for keyword in keywords]) and not \
                    any([negative in tweet_text for negative in negative_keywords]):
                results.update({query_name: results.get(query_name, 0) + 1})
                found = True
        if not found:
            # discovered that some tweets use an alternate latin alphabet ?
            # print(tweet_text)
            pass
    return results


def count_publication_time(data_path: str, filter_function: Callable = None):
    count_dict = {}
    for tweet in read_corpus_generator(data_path):
        if (filter_function and filter_function(tweet)) or not filter_function:
            hour = get_hour_of_tweet(tweet)
            count_dict.update({hour: count_dict.get(hour, 0) + 1})
    return count_dict


def count_analysis_variable(
        variable_dict_json_path: str,
        analysis_csv_path: str):
    variables_dict = read_variable_dict(variable_dict_json_path)
    analysis_table = read_analysis_csv(analysis_csv_path)
    for variable_name, variable_data in variables_dict.items():
        variable_counts = {}
        for row in analysis_table:
            decoded_entry_name = variable_data.get('labels').get(row.get(variable_name))
            if not decoded_entry_name:
                missing_values_process = variable_data.get("missing_values")
                decoded_entry_name = missing_values_process if missing_values_process != "remove" else None
            if decoded_entry_name:
                variable_counts.update(
                    {decoded_entry_name: variable_counts.get(decoded_entry_name, 0) + 1})
        variable_data.update({"count_dict": variable_counts})
    return variables_dict


if __name__ == "__main__":
    # count_analysis_variable(
    #     paths.VARIABLE_DICT_JSON,
    #     paths.ANALYSIS_WITH_USER_CSV
    # )
    results = count_tweets_per_query(
        data_path=paths.CLEAN_DATA_DIR,
        query_json_path="/home/juliette/Projects/meTooExtraction/info/search/queries.json")
    print(results)
