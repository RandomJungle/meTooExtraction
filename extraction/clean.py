import json
import os
import re

from tqdm import tqdm

import utils.paths as paths

from utils.file_utils import read_corpus_generator, read_file_generator


def remove_doubles(input_dir: str, output_dir: str, language_filter=None) -> None:
    for file_name in os.listdir(input_dir):
        if file_name.endswith('.jsonl'):
            file_ids = []
            with open(os.path.join(input_dir, file_name), 'r') as input_file, \
                    open(os.path.join(output_dir, file_name), 'w') as output_file:
                for line in tqdm(input_file.readlines()):
                    tweet = json.loads(line)
                    tweet_id = tweet.get('id')
                    tweet_lang = tweet.get('lang')
                    if (tweet_id not in file_ids) and \
                            (not language_filter or tweet_lang == language_filter):
                        output_file.write(line)
                        file_ids.append(tweet_id)


def remove_bots_messages(input_dir: str, output_dir: str, bots_json_path: str) -> None:
    with open(bots_json_path, 'r') as bots_json_file:
        regexes = json.loads(bots_json_file.read()).get('bots_regexes')
    for file_name in os.listdir(input_dir):
        if file_name.endswith('.jsonl'):
            with open(os.path.join(input_dir, file_name), 'r') as input_file, \
                    open(os.path.join(output_dir, file_name), 'w') as output_file:
                for line in tqdm(input_file.readlines()):
                    tweet = json.loads(line)
                    tweet_text = tweet.get('text')
                    if not any([re.match(regex, tweet_text) for regex in regexes]):
                        output_file.write(line)


def remove_bots_ids(input_dir: str, output_dir: str, bots_json_path: str) -> None:
    counter = 0
    with open(bots_json_path, 'r') as bots_json_file:
        bots_user_ids = json.loads(bots_json_file.read()).get('bots_user_ids')
    for file_name in os.listdir(input_dir):
        if file_name.endswith('.jsonl'):
            with open(os.path.join(input_dir, file_name), 'r') as input_file, \
                    open(os.path.join(output_dir, file_name), 'w') as output_file:
                for line in tqdm(input_file.readlines()):
                    tweet = json.loads(line)
                    author_id = tweet.get('author_id')
                    if author_id not in bots_user_ids:
                        output_file.write(line)
                    else:
                        counter += 1


def full_cleaning_pipeline(input_dir: str,
                           output_dir: str,
                           bots_json_path: str,
                           language_filter: str = None):
    counter = 0
    with open(bots_json_path, 'r') as bots_json_file:
        bots_json = json.loads(bots_json_file.read())
        bots_user_ids = bots_json.get('bots_user_ids')
        regexes = bots_json.get('bots_regexes')
    for file_name in os.listdir(input_dir):
        if file_name.endswith('.jsonl'):
            file_ids = []
            with open(os.path.join(input_dir, file_name), 'r') as input_file, \
                    open(os.path.join(output_dir, file_name), 'w') as output_file:
                for line in tqdm(input_file.readlines()):
                    tweet = json.loads(line)
                    tweet_id = tweet.get('id')
                    tweet_lang = tweet.get('lang')
                    tweet_text = tweet.get('text')
                    author_id = tweet.get('author_id')
                    if (tweet_id not in file_ids) \
                            and (not language_filter or tweet_lang == language_filter) \
                            and not any([re.match(regex, tweet_text) for regex in regexes])\
                            and author_id not in bots_user_ids:
                        output_file.write(line)
                        file_ids.append(tweet_id)
                    else:
                        counter += 1
    print(f"Removed {counter} tweets in total")

                        
if __name__ == "__main__":
    
    full_cleaning_pipeline(
        paths.TRANSLATED_DATA_PATH,
        paths.CLEAN_DATA_PATH,
        paths.BOTS_JSON_PATH,
        language_filter="ja"
    )
