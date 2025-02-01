import datetime
import json
import logging
import os
import re
from typing import Callable

import pandas
import pandas as pd
from tqdm import tqdm

from utils import converters
from utils.file_utils import (
    read_jsonl_generator,
    write_tweets_to_jsonl,
    select_tweets_from_ids_in_jsonl,
    read_jsonl_list, write_tweets_to_csv
)
from utils.tweet_utils import is_retweet


def find_real_id_in_dataframe(stupid_id: str, user_data: pd.DataFrame):
    if not any([el in stupid_id for el in ['.', 'E', '+']]):
        return stupid_id
    else:
        str_id = str(int(float(stupid_id)))
        str_regex = ''
        for index, char in enumerate(str_id[::-1]):
            if char != '0':
                str_regex = str_id[0:-index-1] + ('\d' * (index + 1))
                break
        matching_rows = [entry for entry in user_data.id if re.match(str_regex, entry)]
        if len(matching_rows) == 1:
            return matching_rows[0]
        elif len(matching_rows) > 1:
            print(f'more than one match for : {str_id}')
            return str_id
        else:
            print(f'zero matches for {str_id}')
            return str_id


def is_most_ancient_data(entry):
    low_timestamp = datetime.datetime(
        year=2017, month=10, day=1, hour=0, minute=0, second=0, tzinfo=datetime.timezone.utc)
    return entry['created_at'] < low_timestamp


def is_middle_data(entry):
    low_timestamp = datetime.datetime(
        year=2017, month=10, day=1, hour=0, minute=0, second=0, tzinfo=datetime.timezone.utc)
    high_timestamp = datetime.datetime(
        year=2019, month=10, day=31, hour=23, minute=59, second=59, tzinfo=datetime.timezone.utc)
    return (entry['created_at'] > low_timestamp) & (entry['created_at'] < high_timestamp)


def is_most_recent_data(entry):
    high_timestamp = datetime.datetime(
        year=2019, month=10, day=31, hour=23, minute=59, second=59, tzinfo=datetime.timezone.utc)
    return entry['created_at'] > high_timestamp


def divide_corpus_into_3_periods(tweet_dir_path: str, corpora_root: str):
    make_corpora_from_tweet_dir(
        tweet_dir_path=tweet_dir_path,
        final_corpus_root=os.path.join(corpora_root, 'JAPAN_2016-sept-2017'),
        filter_function=is_most_ancient_data
    )
    make_corpora_from_tweet_dir(
        tweet_dir_path=tweet_dir_path,
        final_corpus_root=os.path.join(corpora_root, 'JAPAN_oct-2017-oct-2019'),
        filter_function=is_middle_data
    )
    make_corpora_from_tweet_dir(
        tweet_dir_path=tweet_dir_path,
        final_corpus_root=os.path.join(corpora_root, 'JAPAN_nov-2019-2022'),
        filter_function=is_most_recent_data
    )


def make_corpora_from_tweet_dir(tweet_dir_path: str, final_corpus_root: str, filter_function: Callable):
    dataframe = pandas.DataFrame
    for root, dirs, files in os.walk(tweet_dir_path):
        logging.warning(msg=f'PROCESSING ROOT DIR {root}')
        for filename in tqdm(files):
            if filename.endswith('.jsonl'):
                file_size = os.path.getsize(os.path.join(root, filename))/(1 << 30)
                if file_size > 1.6:
                    pass
                    logging.error(msg=f'NOT PROCESSING FILE {filename} BECAUSE OF FILE SIZE {file_size}')
                else:
                    logging.warning(msg=f'Processing file ---> {filename}')
                    file_dataframe = pandas.read_json(os.path.join(root, filename), orient='records', lines=True)
                    filtered_tweets = file_dataframe[file_dataframe.apply(lambda x: filter_function(x), axis=1)]
                    del file_dataframe
                    if dataframe.empty:
                        dataframe = filtered_tweets
                    else:
                        dataframe = pd.concat([dataframe, filtered_tweets])
                    logging.warning(f'Number of rows in Dataframe : {dataframe.shape[0]}')
                    dataframe = dataframe.drop_duplicates(subset='id')
                    logging.warning(f'Number of rows in Dataframe after filtering : {dataframe.shape[0]}')
    if not os.path.exists(final_corpus_root):
        try:
            os.makedirs(final_corpus_root)
        except OSError as error:
            logging.error(error)
    write_dataframe_into_month_jsonl(dataframe, final_corpus_root)


def write_dataframe_into_month_jsonl(dataframe: pandas.DataFrame, output_root_dir: str):
    for year in range(2016, 2024):
        year_dataframe = dataframe[dataframe.apply(
            lambda x: x['created_at'].year == year, axis=1)]
        if not year_dataframe.empty:
            for month in range(1, 13):
                month_dataframe = year_dataframe[year_dataframe.apply(
                    lambda x: x['created_at'].month == month, axis=1)]
                month_str = converters.month_int_to_str[month]
                file_name = f'{year}-{month:02d}-{month_str}.jsonl'
                if not month_dataframe.empty:
                    month_dataframe.to_json(
                        os.path.join(output_root_dir, file_name),
                        orient='records',
                        date_format='iso',
                        lines=True
                    )


def format_dataset_for_network_study(
        tweet_dir_path: str,
        output_dir_path: str,
        threshold: int = 20):
    if output_dir_path == tweet_dir_path:
        raise FileExistsError
    for file in tqdm(os.listdir(tweet_dir_path)):
        input_jsonl_path = os.path.join(tweet_dir_path, file)
        month_tweets = []
        for metric_key in ['reply_count', 'retweet_count', 'quote_count', 'like_count']:
            count_dict = {}
            for tweet in read_jsonl_generator(input_jsonl_path):
                if not is_retweet(tweet):
                    metric_count = tweet.get('public_metrics').get(metric_key)
                    if metric_count:
                        count_dict.update({
                            metric_count: count_dict.get(metric_count, []) + [tweet.get('id')]})
            count_dict = {key: value for key, value in
                          sorted(count_dict.items(), key=lambda item: item[0], reverse=True)}
            sub_dict = dict(list(count_dict.items())[:threshold])
            retrieved_tweets = select_tweets_from_ids_in_jsonl(
                jsonl_path=input_jsonl_path,
                tweet_ids=[item for sublist in sub_dict.values() for item in sublist])
            for tweet in retrieved_tweets:
                tweet.update({'source_inclusion': metric_key})
            month_tweets.extend(retrieved_tweets[0:20])
        write_tweets_to_jsonl(os.path.join(output_dir_path, file), month_tweets)


def format_dataset_to_include_user_info(
        input_data_path: str, users_data_path: str, output_data_path: str):
    users_data = read_jsonl_list(users_data_path)
    for file_name in os.listdir(input_data_path):
        if file_name.endswith('.jsonl'):
            with open(os.path.join(output_data_path, file_name), 'w+') as output_file:
                for tweet in read_jsonl_generator(os.path.join(input_data_path, file_name)):
                    user_id = tweet.get('author_id')
                    user_data = [user_data for user_data in users_data if user_data.get('id') == user_id]
                    if not user_data:
                        logging.warning(f'no user data for id : {user_id}')
                    elif len(user_data) > 1:
                        raise ValueError(f'more than one user data for id : {user_id}')
                    else:
                        user_data = user_data[0]
                        for key, value in user_data.items():
                            tweet.update({f'user_{key}': value})
                    output_file.write(json.dumps(tweet) + '\n')


def flatten_jsonl_dataset(input_data_path: str, output_data_path: str):
    for file_name in tqdm(os.listdir(input_data_path)):
        if file_name.endswith('.jsonl'):
            with open(os.path.join(output_data_path, file_name), 'w+') as output_file:
                for tweet in read_jsonl_generator(os.path.join(input_data_path, file_name)):
                    for metric_name, metric_count in tweet.get('public_metrics').items():
                        tweet.update({metric_name: metric_count})
                    tweet.pop('public_metrics')
                    user_public_metrics = tweet.get('user_public_metrics')
                    if user_public_metrics:
                        for metric_name, metric_count in user_public_metrics.items():
                            tweet.update({metric_name: metric_count})
                        tweet.pop('user_public_metrics')
                    output_file.write(json.dumps(tweet) + '\n')


def format_jsonl_dataset_to_csv(input_path: str, output_path):
    for file_name in tqdm(os.listdir(input_path)):
        if file_name.endswith('.jsonl'):
            tweets = read_jsonl_list(os.path.join(input_path, file_name))
            csv_file_name = file_name.replace('.jsonl', '.csv')
            write_tweets_to_csv(
                output_path=os.path.join(output_path, csv_file_name),
                tweets=tweets)
            


if __name__ == '__main__':

    format_jsonl_dataset_to_csv(
        input_path=os.environ.get('DATA_FLATTEN'),
        output_path='public_metric_csv'
    )
