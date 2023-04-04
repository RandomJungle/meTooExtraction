import csv
from typing import List, Dict

import chardet
import json
import os

from tqdm import tqdm

from utils import paths


def read_corpus_generator(data_path: str):
    for file_name in os.listdir(data_path):
        if file_name.endswith('.jsonl'):
            with open(os.path.join(data_path, file_name), 'r') as data_file:
                for entry in data_file.readlines():
                    yield json.loads(entry)


def read_jsonl_generator(file_path: str):
    with open(file_path, 'r') as data_file:
        for entry in data_file.readlines():
            yield json.loads(entry)


def read_jsonl_list(file_path: str) -> List[Dict]:
    with open(file_path, 'r') as data_file:
        return [json.loads(line) for line in data_file.readlines()]


def read_corpus_list(data_path: str):
    tweets = []
    for file_name in os.listdir(data_path):
        if file_name.endswith('.jsonl'):
            with open(os.path.join(data_path, file_name), 'r') as data_file:
                for tweet in data_file.readlines():
                    tweets.append(json.loads(tweet))
    return tweets


def select_tweets_from_ids_in_corpus(data_path: str, tweet_ids: List[int]):
    tweets = []
    for tweet in read_corpus_generator(data_path):
        if tweet.get('id') in tweet_ids:
            tweets.append(tweet)
    return tweets


def select_tweets_from_ids_in_jsonl(jsonl_path: str, tweet_ids: List[int]):
    tweets = []
    for tweet in read_jsonl_generator(jsonl_path):
        if tweet.get('id') in tweet_ids:
            tweets.append(tweet)
    return tweets


def divide_corpus(data_path: str, output_path, n_folds: int):
    tweets = [tweet for tweet in read_corpus_generator(data_path)]
    size = int(len(tweets) / n_folds) + 1
    chunks = [tweets[x:x + size] for x in range(0, len(tweets), size)]
    for index, chunk in enumerate(chunks):
        with open(os.path.join(output_path, f"sample_{index + 1}_octobre.jsonl"), "w") as output_file:
            for tweet in chunk:
                output_file.write(json.dumps(tweet) + "\n")


def merge_all_problematic_tweets(input_dir_path, output_file_path):
    with open(output_file_path, 'w') as output_file:
        for tweet in read_corpus_generator(input_dir_path):
            if tweet.get('flag') == 'problematic':
                print(tweet.get('en_text'))
                output_file.write(json.dumps(tweet) + "\n")


def merge_corpus(input_dir_path, output_file_path):
    tweets = {}
    for tweet in read_corpus_generator(input_dir_path):
        tweet_id = tweet.get('id')
        if tweet_id not in tweets.keys():
            if tweet.get('label') == "testimony":
                tweets.update({tweet_id: tweet})
    with open(output_file_path, 'w') as output_file:
        for index, tweet in enumerate(tweets.values()):
            output_file.write(json.dumps(tweet) + "\n")
            print(index)


def get_char_encoding_of_file(input_path: str):
    with open(input_path, 'rb') as rawdata:
        result = chardet.detect(rawdata.read(100000))
    print(result)


def read_variable_dict(variable_dict_json_path: str):
    with open(variable_dict_json_path, 'r') as variable_dict_file:
        variable_dict = json.loads(variable_dict_file.read())
    return variable_dict


def read_analysis_csv(analysis_csv_path: str):
    csv_rows = []
    with open(analysis_csv_path, 'r') as analysis_csv_file:
        csv_reader = csv.DictReader(analysis_csv_file)
        csv_rows.extend([row for row in csv_reader])
    return csv_rows


def read_users_csv(user_csv_path: str):
    csv_rows = []
    with open(user_csv_path, 'r') as user_csv_file:
        csv_reader = csv.DictReader(user_csv_file, delimiter=";")
        csv_rows.extend([row for row in csv_reader])
    return csv_rows


def write_tweets_to_jsonl(output_path: str, tweets: List[Dict]):
    with open(output_path, 'w') as output_file:
        for tweet in tqdm(tweets):
            output_file.write(json.dumps(tweet) + "\n")


def write_tweets_to_csv(output_path: str, tweets: List[Dict]):
    with open(output_path, 'w+') as csv_file:
        tweet_keys = (tweets[0].keys())
        writer = csv.DictWriter(csv_file, fieldnames=tweet_keys)
        writer.writeheader()
        for tweet in tweets:
            writer.writerow(tweet)
