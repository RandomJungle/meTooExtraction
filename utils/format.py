import csv
import json
import re

import pandas as pd

from utils import paths
from utils.file_utils import read_corpus_list, read_corpus_generator, read_jsonl_generator
from utils.tweet_utils import is_conspiracy_tweet


def find_real_id_in_dataframe(stupid_id: str, user_data: pd.DataFrame):
    if not any([el in stupid_id for el in [".", "E", "+"]]):
        return stupid_id
    else:
        str_id = str(int(float(stupid_id)))
        str_regex = ""
        for index, char in enumerate(str_id[::-1]):
            if char != '0':
                str_regex = str_id[0:-index-1] + ("\d" * (index + 1))
                break
        matching_rows = [entry for entry in user_data.id if re.match(str_regex, entry)]
        if len(matching_rows) == 1:
            return matching_rows[0]
        elif len(matching_rows) > 1:
            print(f"more than one match for : {str_id}")
            return str_id
        else:
            print(f"zero matches for {str_id}")
            return str_id


def clean_up_csv_mess(
        input_csv_path: str,
        input_corpus_dir: str,
        output_csv_path: str):
    tweets = read_corpus_list(input_corpus_dir)
    with open(input_csv_path, 'r') as input_csv_file, open(output_csv_path, 'w+') as output_csv_file:
        csv_reader = csv.DictReader(input_csv_file)
        csv_writer = csv.DictWriter(output_csv_file, fieldnames=csv_reader.fieldnames, restval='NA')
        csv_writer.writeheader()
        for row in csv_reader:
            tweet_text = row.get('TWEET')
            matching_tweets = [tweet for tweet in tweets if tweet.get('text') == tweet_text]
            if len(matching_tweets) != 1:
                matching_tweets = [tweet for tweet in tweets if tweet.get('author_id') == row.get('AUTID')]
                if len(matching_tweets) != 1:
                    matching_tweets = [tweet for tweet in matching_tweets
                                       if tweet.get('created_at') == row.get('DATE')]
                    if len(matching_tweets) != 1:
                        matching_tweets = [tweet for tweet in tweets
                                           if tweet.get('created_at') == row.get('DATE')]
                        if len(matching_tweets) != 1:
                            raise KeyError
            row.update({
                'TWID': matching_tweets[0].get('id'),
                'AUTID': matching_tweets[0].get('author_id'),
                'TWEET': matching_tweets[0].get('text')})
            csv_writer.writerow(row)


def add_user_info_to_clean_csv(
        input_csv_path: str,
        input_user_csv_path: str,
        output_csv_path):
    with open(input_csv_path, 'r') as input_csv_file, \
            open(output_csv_path, 'w+') as output_csv_file, \
            open(input_user_csv_path, 'r') as input_user_csv_file:
        csv_reader = csv.DictReader(input_csv_file)
        user_csv_reader = csv.DictReader(input_user_csv_file, delimiter=";")
        user_entries = [row for row in user_csv_reader]
        csv_writer = csv.DictWriter(output_csv_file, fieldnames=csv_reader.fieldnames, restval='NA')
        csv_writer.writeheader()
        for row in csv_reader:
            if not row.get('username'):
                author_id = row.get('AUTID')
                matching_rows = [r for r in user_entries if r.get('id') == author_id]
                if len(matching_rows) == 1:
                    matching_row = matching_rows[0]
                    row.update({
                        "id": matching_row.get('id'),
                        "created_at": matching_row.get('created_at'),
                        "name": matching_row.get('name'),
                        "username": matching_row.get('username'),
                        "protected": matching_row.get('protected'),
                        "verified": matching_row.get('verified'),
                        "withheld": matching_row.get('withheld'),
                        "profile_image_url": matching_row.get('profile_image_url'),
                        "location": matching_row.get('location'),
                        "url": matching_row.get('url'),
                        "description": matching_row.get('description'),
                        "pinned_tweet_id": matching_row.get('pinned_tweet_id'),
                        "followers": matching_row.get('followers'),
                        "following": matching_row.get('following'),
                        "listed": matching_row.get('listed'),
                        "sample_tweet_count": matching_row.get('sample_tweet_count'),
                        "total_tweet_count": matching_row.get('total_tweet_count'),
                    })
            csv_writer.writerow(row)


def add_user_info_to_csv_with_vague_ids(
        input_table_csv_path: str,
        input_user_csv_path: str,
        output_csv_path: str):
    # read user csv file and cast ids column to string
    user_data = pd.read_csv(
        input_user_csv_path,
        delimiter=";",
        encoding="utf-8",
        quotechar='"',
        dtype={"id": str, "pinned_tweet_id": str})
    # read table data and replace stupid scientific notation
    table_data = pd.read_csv(
        input_table_csv_path,
        delimiter=",",
        encoding="utf-8",
        dtype={"TWID": str, "AUTID": str})
    table_data = table_data.assign(AUTID=[entry.replace(",", ".") for entry in table_data.AUTID])
    # use base number to find ids in user dataframe
    table_data = table_data.assign(real_id=[find_real_id_in_dataframe(entry, user_data)
                                            for entry in table_data.AUTID])
    # merge dataframes on real_id column
    merged = table_data.merge(user_data, left_on='real_id', right_on='id', how='left', validate="many_to_one")
    merged.to_csv(output_csv_path, index=False)


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


def convert_jsonl_user_info_to_csv(input_jsonl_path, output_csv_path):
    with open(output_csv_path, 'w') as output_csv_file:
        csv_writer = csv.writer(output_csv_file, delimiter=",", quotechar='"')
        csv_writer.writerow([
            "id",
            "created_at",
            "name",
            "username",
            "protected",
            "verified",
            "withheld",
            "profile_image_url",
            "location",
            "url",
            "description",
            "pinned_tweet_id",
            "followers_count",
            "following_count",
            "listed_count",
            "tweet_count"
        ])
        for user in read_jsonl_generator(input_jsonl_path):
            csv_writer.writerow([
                user.get('id'),
                user.get('created_at'),
                user.get('name'),
                user.get('username'),
                user.get('protected'),
                user.get('verified'),
                user.get('withheld'),
                user.get('profile_image_url'),
                user.get('location'),
                user.get('url'),
                user.get('description'),
                user.get('pinned_tweet_id'),
                user.get('public_metrics').get('followers_count'),
                user.get('public_metrics').get('following_count'),
                user.get('public_metrics').get('listed_count'),
                user.get('public_metrics').get('tweet_count'),
            ])


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


if __name__ == "__main__":
    convert_jsonl_user_info_to_csv(
        paths.USER_INFO_JSONL,
        paths.USER_INFO_CSV
    )