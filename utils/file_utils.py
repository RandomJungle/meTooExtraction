import csv
import json
import os

from utils.tweet_utils import is_conspiracy_tweet


def read_corpus_generator(data_path: str):
    for file_name in os.listdir(data_path):
        if file_name.endswith('.jsonl'):
            with open(os.path.join(data_path, file_name), 'r') as data_file:
                for entry in data_file.readlines():
                    yield json.loads(entry)


def read_file_generator(file_path: str):
    with open(file_path, 'r') as data_file:
        for entry in data_file.readlines():
            yield json.loads(entry)


def read_corpus_list(data_path: str):
    tweets = []
    for file_name in os.listdir(data_path):
        if file_name.endswith('.jsonl'):
            with open(os.path.join(data_path, file_name), 'r') as data_file:
                for tweet in data_file.readlines():
                    tweets.append(json.loads(tweet))
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


def convert_jsonl_corpus_to_csv(input_dir_path, output_csv_path):
    with open(output_csv_path, 'w') as output_csv_file:
        csv_writer = csv.writer(output_csv_file, delimiter=";", quotechar='"')
        csv_writer.writerow([
            "tweet id",
            "auteur id",
            "texte en japonais",
            "texte en anglais",
            "texte en français",
            "label",
            "date de création",
            "nombre de retweets",
            "nombre de reply",
            "nombre de likes",
            "nombre de quotes",
            "genre exprimé"
        ])
        for tweet in read_corpus_generator(input_dir_path):
            csv_writer.writerow([
                tweet.get('id'),
                tweet.get('author_id'),
                tweet.get('text'),
                tweet.get('en_text'),
                tweet.get('fr_text'),
                tweet.get('label'),
                tweet.get('created_at'),
                tweet.get('public_metrics').get('retweet_count'),
                tweet.get('public_metrics').get('reply_count'),
                tweet.get('public_metrics').get('like_count'),
                tweet.get('public_metrics').get('quote_count'),
                tweet.get('labels').get('genders')
            ])


def write_tweets_to_text(data_path: str, output_path: str):
    problematic_tweets = []
    testimony_tweets = []
    for tweet in read_corpus_generator(data_path):
        if tweet.get('label'):
            if tweet.get('label') == "testimony":
                testimony_tweets.append(tweet)
        elif tweet.get("flag"):
            problematic_tweets.append(tweet)
    with open(output_path, 'w') as output_file:
        output_file.write(f"PROBLEMATIC TWEETS\n\n\n" + ("*" * 100) + "\n\n")
        for tweet in problematic_tweets:
            tweet_en_text = tweet.get('en_text')
            tweet_ja_text = tweet.get('text')
            output_file.write(f"{tweet_ja_text}" + "\n\n")
            output_file.write(f"{tweet_en_text}\n\n" + ("-" * 40) + "\n\n")
        output_file.write(f"TESTIMONIES\n\n\n" + ("*" * 100) + "\n\n")
        for tweet in testimony_tweets:
            tweet_en_text = tweet.get('en_text')
            tweet_ja_text = tweet.get('text')
            output_file.write(f"{tweet_ja_text}" + "\n\n")
            output_file.write(f"{tweet_en_text}\n\n" + ("-" * 40) + "\n\n")


def create_no_testimony_inclusive_corpus(
        testimony_corpus_path: str,
        no_testimony_corpus_path: str,
        output_path: str,
        keywords_json_path: str):
    with open(keywords_json_path, 'r') as keywords_file:
        conspiracy_keywords = json.loads(keywords_file.read()).get('conspiracy')
    full_tweet_list = []
    done_ids = []
    for tweet in read_corpus_generator(testimony_corpus_path):
        full_tweet_list.append(tweet)
        done_ids.append(tweet.get('id'))
    for tweet in read_corpus_generator(no_testimony_corpus_path):
        tweet_id = tweet.get('id')
        if tweet_id not in done_ids and not is_conspiracy_tweet(tweet, conspiracy_keywords):
            if not tweet.get('label'):
                tweet.update({'label': 'not_testimony'})
            full_tweet_list.append(tweet)
            done_ids.append(tweet_id)
    with open(output_path, 'w+') as output_file:
        for tweet in full_tweet_list:
            output_file.write(json.dumps(tweet) + "\n")


