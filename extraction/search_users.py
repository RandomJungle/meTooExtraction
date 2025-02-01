import requests
import json
import os
from typing import List

import pandas as pd
import plotly.express as px
from dotenv import find_dotenv, load_dotenv
from searchtweets import load_credentials

from utils.file_utils import read_corpus_generator, read_txt_list
from utils.tweet_utils import is_retweet


def chunks_generator(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def retrieve_user_ids(data_path: str):
    user_ids_list = []
    for tweet in read_corpus_generator(data_path):
        user_ids_list.append(tweet.get("author_id"))
    return list(set(user_ids_list))


def import_credentials():
    credentials = load_credentials("~/.twitter_keys.yaml",
                                   yaml_key="search_tweets_api",
                                   env_overwrite=False)
    return credentials


def create_url(user_ids: List):
    user_fields = [
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
        "entities",
        "pinned_tweet_id",
        "public_metrics"
    ]
    user_ids = "ids=" + ",".join(user_ids)
    user_fields = "user.fields=" + ",".join(user_fields)
    url = f"https://api.twitter.com/2/users?{user_ids}&{user_fields}"
    return url


def bearer_oauth(r):
    bearer_token = import_credentials().get('bearer_token')
    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2UserLookupPython"
    return r


def connect_to_endpoint(url):
    response = requests.request("GET", url, auth=bearer_oauth,)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Request returned an error: {} {}".format(
                response.status_code, response.text
            )
        )
    return response.json()


def query_users_info(data_path: str, output_jsonl_path: str, output_csv_path: str):
    jsons = []
    all_user_ids = retrieve_user_ids(data_path)
    for chunk in chunks_generator(all_user_ids, 100):
        url = create_url(chunk)
        json_response = connect_to_endpoint(url)
        jsons.extend(json_response.get('data'))
    with open(output_jsonl_path, 'a+') as output_file:
        for json_user_info in jsons:
            output_file.write(json.dumps(json_user_info, sort_keys=True) + "\n")


def filter_corpus_by_users(data_path: str, user_ids: List[str]):
    tweets = []
    for tweet in read_corpus_generator(data_path):
        if tweet.get('author_id') in user_ids:
            tweets.append(tweet)
    return tweets


def create_filtered_corpus():
    user_ids = read_txt_list(
        '/Users/juliette/Projects/meTooExtraction/info/search/user_ids.txt'
    )
    tweets_2021 = filter_corpus_by_users(
        data_path='/Users/juliette/Desktop/raw_2021',
        user_ids=user_ids
    )
    tweets_2022 = filter_corpus_by_users(
        data_path='/Users/juliette/Desktop/raw_2022',
        user_ids=user_ids
    )
    total_tweets = tweets_2021 + tweets_2022
    total_tweets = [tweet for tweet in total_tweets if not is_retweet(tweet)]
    tweet_set = []
    found_ids = []
    for tweet in total_tweets:
        if tweet.get('id') not in found_ids:
            tweet_set.append(tweet)
            found_ids.append(tweet.get('id'))
    user_tweets = {}
    for user_id in user_ids:
        for tweet in tweet_set:
            if tweet.get('author_id') == user_id:
                if user_id in user_tweets:
                    user_tweets[user_id] = user_tweets[user_id] + [tweet]
                else:
                    user_tweets[user_id] = [tweet]
    final_tweets = []
    for key, value in user_tweets.items():
        value.sort(key=lambda x: x.get('public_metrics').get('retweet_count'), reverse=True)
        final_tweets.extend(value[:5])
    df = pd.DataFrame.from_records(final_tweets)
    for metric in ['like_count', 'quote_count', 'reply_count', 'retweet_count']:
        df[metric] = df['public_metrics'].apply(
            lambda row: row.get(metric)
        )
    df['author_id'] = df['author_id'].astype(str)
    df.to_json(
        '/Users/juliette/Desktop/data_temp.json',
    )
    df.to_csv(
        '/Users/juliette/Desktop/data_temp.csv',
        index=False
    )


if __name__ == "__main__":

    load_dotenv(find_dotenv())

    create_filtered_corpus()

    df = pd.read_json('/Users/juliette/Desktop/data_temp.json')

    df['author_id'] = df['author_id'].astype(str)

    fig = px.histogram(
        df,
        x='created_at',
        color='author_id',
        color_discrete_sequence=px.colors.qualitative.Alphabet
    )
    fig.show()

    fig = px.scatter(
        df,
        x='created_at',
        y='retweet_count',
        color='author_id',
        color_discrete_sequence=px.colors.qualitative.Alphabet
    )
    fig.show()


    '''
    query_users_info(
        data_path=os.environ.get('JAPAN_PUBLIC_METRIC_JSON'),
        output_jsonl_path="/home/juliette/data/meToo_data/japan/public_metric_user_info.jsonl",
        output_csv_path="/home/juliette/data/meToo_data/japan/public_metric_user_info.csv"
    )
    '''
