import os
from functools import reduce
from typing import List, Optional, Dict

import pandas as pd
from dotenv import load_dotenv, find_dotenv

from utils.file_utils import read_json_dataframe


def df_pipeline(dataframe, functions):
    """
    Pipeline function to chain functions on a dataframe
    """
    return reduce(lambda d, f: f(d), functions, dataframe)


def basic_pipeline(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms applied to dataframe additional (no column removal)
    """
    df_pipeline(
        dataframe=dataframe,
        functions=[
            add_month_column,
            add_day_column,
            add_quote_reply_retweet_columns,
            add_reference_type_column,
            add_hashtags_column
        ])
    return dataframe


def add_month_column(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Create column for months in pandas dataframe, all date will be rounded
    to the first day of the month
    """
    dataframe['month'] = dataframe['created_at'].dt.to_period('M').dt.to_timestamp()
    return dataframe


def add_day_column(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Create column for days in pandas dataframe, with no time information
    """
    dataframe['day'] = dataframe['created_at'].dt.to_period('D').dt.to_timestamp()
    return dataframe


def extract_hashtags_from_row(row: pd.Series) -> List[str]:
    """
    Row-level function to extract all hashtags from the entities cell
    """
    if isinstance(row, dict):
        hashtags = row.get('hashtags')
        if hashtags:
            return [hashtag.get('tag') for hashtag in hashtags]
    return []


def add_hashtags_column(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Create a column for the hashtags identified in a tweet
    """
    dataframe['hashtags'] = dataframe['entities'].apply(
        lambda row: extract_hashtags_from_row(row)
    )
    return dataframe


def explode_lowered_hashtags(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Explode and lower the text of the column of hashtags
    """
    dataframe = dataframe.explode(
        column='hashtags',
        ignore_index=True
    )
    dataframe['hashtags'] = dataframe['hashtags'].str.lower()
    return dataframe


def filter_hashtags_by_count(
        dataframe: pd.DataFrame,
        min_count: Optional[int] = 20) -> List[str]:
    """
    Filter hashtags dataframe by count with a min_count threshold
    """
    count = dataframe.groupby('hashtags').count()
    count = count[count['id'] > min_count]
    return count.index.tolist()


def is_label_in_referenced(referenced_list: List[Dict], label: str) -> bool:
    """
    Row-level function to find labels in the list of referenced info
    """
    if isinstance(referenced_list, list):
        return any([label == ref.get('type') for ref in referenced_list])
    return False


def add_quote_reply_retweet_columns(
        dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Add boolean columns if tweet is a reply / quote / retweet
    """
    column_mapping = {
        'reply': 'replied_to',
        'quote': 'quoted',
        'retweet': 'retweeted'
    }
    for column_name, label in column_mapping.items():
        dataframe[column_name] = dataframe.apply(
            lambda x: is_label_in_referenced(
                referenced_list=x['referenced_tweets'],
                label=label
            ),
            axis=1
        )
    return dataframe


def determine_reference_label(row: pd.Series) -> str:
    """
    Row-level function to determine the type of referencing of a tweet
    from boolean columns
    """
    if row['retweet']:
        return 'retweet'
    elif row['quote']:
        return 'quote'
    elif row['reply']:
        return 'reply'
    return 'original'


def add_reference_type_column(
        dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Add column based on the four reference types {original, quote, reply, retweet}
    """
    dataframe['reference_type'] = dataframe.apply(
        determine_reference_label, axis=1
    )
    return dataframe


def hashtag_frequency_table(
        dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Pivot a df of tweets to a df of hashtags frequencies in tweets
    Includes the total count of hashtag in all users, and a column
    for each user_username with individual hashtag counts
    """
    if not 'hashtags' in dataframe.columns:
        dataframe = add_hashtags_column(dataframe)
    dataframe = explode_lowered_hashtags(dataframe)
    count_df = dataframe.groupby(
        by=['hashtags'],
        as_index=False
    ).count()
    count_df['total_count'] = count_df['id']
    users_df = pd.pivot_table(
        dataframe,
        values='id',
        index=['hashtags'],
        columns=['user_username'],
        aggfunc='count'
    )
    dataframe = pd.merge(
        left=count_df[['hashtags', 'total_count']],
        right=users_df,
        how='left',
        left_on='hashtags',
        right_index=True
    )
    return dataframe


def tweet_per_user_per_month_table(
        dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Table of the number of tweet per user_username per month
    """
    dataframe = add_month_column(dataframe)
    dataframe = pd.pivot_table(
        dataframe,
        values='id',
        index=['month'],
        columns=['user_username'],
        aggfunc='count'
    )
    return dataframe


def tweet_per_user_per_day_table(
        dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Table of the number of tweet per user_username per day
    """
    dataframe = add_day_column(dataframe)
    dataframe = pd.pivot_table(
        dataframe,
        values='id',
        index=['day'],
        columns=['user_username'],
        aggfunc='count'
    )
    return dataframe


if __name__ == '__main__':

    load_dotenv(find_dotenv())

    df = read_json_dataframe(
        file_path=os.environ.get('USERS_DATA_PATH'),
        remove_duplicates=True
    )
    df = tweet_per_user_per_month_table(df)
    df.to_csv(
        'tweet_per_user_per_month_table.csv',
        index=True
    )