import os
import re
from functools import reduce
from typing import List, Optional, Dict

import numpy as np
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
            add_hashtags_column,
            add_public_metrics_column,
            add_identity_column
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


def add_public_metrics_column(
        dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Add column based on the public metrics {retweet_count, quote_count, reply_count, like_count}
    """
    metrics = ['retweet_count', 'quote_count', 'reply_count', 'like_count']
    for metric in metrics:
        dataframe[metric] = dataframe['public_metrics'].apply(
            lambda x: x.get(metric)
        )
    return dataframe


def add_identities_booleans(
        row: pd.Series, columns: List[str]) -> List[str]:
    identities = []
    for column in columns:
        if row[column]:
            identities.append(column)
    return identities


def add_identity_column(
        dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Add column based on the public metrics {retweet_count, quote_count, reply_count, like_count}
    """
    identity_columns = [
        'media', 'journalist', 'victim', 'youtuber', 'academic', 'writer_editor', 'nationalist',
        'personality', 'politicized', 'activist', 'political', 'feminist_activist', 'jurist',
        'translator_interpret', 'group_organization', 'meninist_activist'
    ]
    dataframe['identities'] = dataframe.apply(
        lambda row: add_identities_booleans(row, identity_columns),
        axis=1
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


def chunk_dataframe(
        dataframe: pd.DataFrame,
        num_chunks: int) -> List[pd.DataFrame]:
    """
    Split dataframe into n chunks (mainly for LLM querying)
    """
    if num_chunks <= 1:
        chunks = [dataframe]
    else:
        rows_per_chunk = len(dataframe) // num_chunks
        chunks = [
            dataframe[i * rows_per_chunk:(i + 1) * rows_per_chunk]
            for i in range(num_chunks)
        ]
        if len(dataframe) % num_chunks != 0:
            chunks.append(dataframe[num_chunks * rows_per_chunk:])
    return chunks


def replace_punctuation_with_line_break(text: str) -> str:
    return text.replace(
        '、', '、<br>'
    ).replace(
        '。', '。<br>'
    ).replace(
        '！', '！<br>'
    ).replace(
        '？', '？<br>'
    ).replace(
        ',', ',<br>'
    ).replace(
        '.', '.<br>'
    ).replace(
        '!', '!<br>'
    ).replace(
        '?', '?<br>'
    )


def add_line_break_text_column(
        dataframe: pd.DataFrame,
        text_column: Optional[str] = 'text',
        language: Optional[str] = 'JA') -> pd.DataFrame:
    if language.lower() == 'ja':
        dataframe[f'{text_column}_br'] = dataframe[text_column].apply(
            lambda x: replace_punctuation_with_line_break(
                re.sub(
                    '[\u0020\n\u3000\u00A0\u0009\u000B\u000D\u2009\u200A\u202F]',
                    repl='<br>',
                    string=x
                )
            )
        )
    else:
        dataframe[f'{text_column}_br'] = dataframe[text_column].apply(
            lambda x: replace_punctuation_with_line_break(
                x.replace('\n', '<br>')
            )
        )
    return dataframe


def extract_urls(entities: float | Dict[str, List[Dict]]) -> float | List[str]:
    if not entities or isinstance(entities, float):
        return np.NaN
    urls = entities.get('urls')
    if not urls:
        return np.NaN
    long_urls = [url.get('expanded_url') for url in urls]
    return [url for url in long_urls if not url.startswith('https://twitter')]


def add_full_urls_column(
        dataframe: pd.DataFrame,
        url_column: Optional[str] = 'entities'):
    dataframe['urls'] = dataframe[url_column].apply(
        lambda x: extract_urls(x)
    )
    return dataframe


if __name__ == '__main__':

    load_dotenv(find_dotenv())

    df = read_json_dataframe(
        file_path=os.environ.get('LATEST_DATASET_PATH'),
        remove_duplicates=True
    )
    df = add_full_urls_column(df)
    df.to_json(
        os.path.join(
            os.environ.get('DATASETS_DIR_PATH'),
            'tweets_2017_2019_url.json'
        ),
        orient='table'
    )