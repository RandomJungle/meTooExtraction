from typing import List, Optional

import pandas as pd


def add_month_column(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Create column for months in pandas dataframe, all date will be rounded
    to the first day of the month
    """
    dataframe['month'] = dataframe['created_at'].dt.to_period('M').dt.to_timestamp()
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
