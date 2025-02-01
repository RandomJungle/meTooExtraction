import matplotlib.pyplot as plt
import os
from typing import Callable, Optional

import pandas as pd
import plotly.express as px
from dotenv import find_dotenv, load_dotenv
from plotly.graph_objs import Figure
from wordcloud import WordCloud

from research import nlp
from utils.df_transform import (
    add_month_column,
    add_hashtags_column,
    explode_lowered_hashtags,
    filter_hashtags_by_count,
    add_quote_reply_retweet_columns,
    add_reference_type_column,
    df_pipeline
)
from utils.file_utils import read_json_dataframe


load_dotenv(find_dotenv())


def export_plotly_image(figure: Figure, output_path: str | None) -> None:
    if output_path:
        if output_path.endswith('html'):
            figure.write_html(output_path)
        else:
            figure.write_image(output_path)
    else:
        figure.show()


def word_cloud_from_tokenized_text(
        text: str, output_path: str) -> None:
    """
    Creates a wordcloud from raw text

    Args:
        text: raw text
        output_path: full path to save figure to

    Returns:
        None, save to file
    """
    plt.rcParams["font.family"] = "Noto Sans CJK JP"
    wordcloud = WordCloud(
        font_path=os.environ.get('JAPANESE_FONT_PATH'),
        background_color="white",
        max_font_size=200,
        width=800,
        height=400).generate(text)
    plt.figure(figsize=(20, 10))
    plt.imshow(wordcloud, interpolation="bilinear")
    plt.axis("off")
    plt.savefig(output_path)


def plot_word_cloud(
        data_path: str,
        figure_name: str,
        filter_function: Callable = None) -> None:
    text = nlp.tokenize_all_tweet_texts(
        data_path, os.environ.get('JAPANESE_STOP_WORDS'), "text", filter_function)
    word_cloud_from_tokenized_text(text, figure_name)


def plot_tweet_count_per_month(
        dataframe: pd.DataFrame,
        output_path: Optional[str] = None) -> None:
    """
    Histogram of tweet count per month in a dataframe
    """
    fig = px.histogram(
        dataframe,
        x='created_at',
        color='user_username',
        width=1200,
        height=600,
        color_discrete_sequence=px.colors.qualitative.G10 + px.colors.qualitative.Vivid
    )
    export_plotly_image(fig, output_path)


def plot_hashtags_evolution(
        dataframe: pd.DataFrame,
        min_count_threshold: int = 20,
        output_path: Optional[str] = None) -> None:
    """
    Line plot of hashtag use grouped by month
    """
    dataframe = df_pipeline(
        dataframe=dataframe,
        functions=[add_month_column, add_hashtags_column, explode_lowered_hashtags])
    hashtags = filter_hashtags_by_count(dataframe, min_count_threshold)
    dataframe = dataframe[dataframe['hashtags'].isin(hashtags)]
    dataframe = dataframe.groupby(
        by=['month', 'hashtags'],
        as_index=False
    ).apply(lambda x: pd.Series({
        'hashtag count': x['id'].count(),
    }))
    fig = px.line(
        dataframe,
        x='month',
        y='hashtag count',
        color='hashtags',
        color_discrete_sequence=px.colors.qualitative.Bold,
        width = 1200,
        height = 600
    )
    export_plotly_image(fig, output_path)


def plot_tweet_type(
        dataframe: pd.DataFrame,
        color_column: Optional[str] = None,
        pattern_column: Optional[str] = None,
        output_path: Optional[str] = None) -> None:
    """
    Simple histogram of distribution of tweet type (OG, quote, reply and RT)
    """
    dataframe = df_pipeline(
        dataframe=dataframe,
        functions=[add_quote_reply_retweet_columns, add_reference_type_column])
    fig = px.histogram(
        dataframe,
        x='reference_type',
        color=color_column,
        pattern_shape=pattern_column
    )
    export_plotly_image(fig, output_path)


if __name__ == "__main__":

    df = read_json_dataframe(
        file_path=os.environ.get('USERS_DATA_PATH'),
        remove_duplicates=True
    )
    plot_tweet_type(
        df,
        color_column='group_organization',
        pattern_column='journalist'
    )