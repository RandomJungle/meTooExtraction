import matplotlib.pyplot as plt
import os
from typing import Callable, Optional, Tuple, List

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dotenv import find_dotenv, load_dotenv
from wordcloud import WordCloud

from research import nlp
from utils.df_transform import (
    add_month_column,
    add_hashtags_column,
    explode_lowered_hashtags,
    filter_hashtags_by_count,
    add_quote_reply_retweet_columns,
    add_reference_type_column,
    df_pipeline, add_day_column, basic_pipeline
)
from utils.file_utils import read_json_dataframe


load_dotenv(find_dotenv())


def export_plotly_image(figure: go.Figure, output_path: str | None) -> None:
    if output_path:
        if output_path.endswith('html'):
            figure.write_html(output_path)
        else:
            figure.write_image(output_path)
    else:
        figure.show()


def define_width_and_height(output_path: str) -> Tuple[int | None, int | None]:
    """
    Define output image width and height according to output path
    """
    if not output_path:
        return None, None
    elif output_path.endswith('html'):
        return None, None
    return 1200, 600


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
    width, height = define_width_and_height(output_path)
    fig = px.histogram(
        dataframe,
        x='created_at',
        color='user_username',
        color_discrete_sequence=px.colors.qualitative.G10 + px.colors.qualitative.Vivid,
        width = width,
        height = height
    )
    export_plotly_image(fig, output_path)


def plot_hashtags_timeline(
        dataframe: pd.DataFrame,
        min_count_threshold: int = 20,
        output_path: Optional[str] = None) -> None:
    """
    Line plot of hashtag use grouped by month
    """
    width, height = define_width_and_height(output_path)
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
        width = width,
        height = height
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
    fig = px.histogram(
        dataframe,
        x='reference_type',
        color=color_column,
        pattern_shape=pattern_column
    )
    export_plotly_image(fig, output_path)


def plot_tweet_timeline_hist(
        dataframe: pd.DataFrame,
        color_column: str,
        output_path: Optional[str] = None) -> None:
    """
    Line plot of hashtag use grouped by month
    """
    width, height = define_width_and_height(output_path)
    fig = px.histogram(
        dataframe,
        x='created_at',
        color=color_column,
        color_discrete_sequence=px.colors.qualitative.Bold,
        width=width,
        height=height
    )
    export_plotly_image(fig, output_path)


def plot_pie_chart(
        dataframe: pd.DataFrame,
        values_column: Optional[str] = None,
        names_column: Optional[str] = None,
        title: Optional[str] = None,
        output_path: Optional[str] = None) -> None:
    """
    Generic pie chart adaptable to the dataframe
    """
    width, height = define_width_and_height(output_path)
    fig = px.pie(
        dataframe,
        values=values_column,
        names=names_column,
        color_discrete_sequence=px.colors.qualitative.G10 + px.colors.qualitative.Vivid,
        title=title,
        width = width,
        height = height
    )
    text_pos = 'inside' if not output_path or output_path.endswith('.html') else 'outside'
    fig.update_traces(textposition=text_pos, textinfo='percent+label')
    export_plotly_image(fig, output_path)


def plot_multi_column_histogram(
        dataframe: pd.DataFrame,
        columns: List[str],
        title: Optional[str] = None,
        output_path: Optional[str] = None) -> None:
    # width, height = define_width_and_height(output_path)
    fig = go.Figure()
    for column in columns:
        fig.add_trace(
            go.Histogram(
                # histfunc='count',
                x=dataframe[column],
                name=f'{column} count'
            )
        )
    export_plotly_image(fig, output_path)


if __name__ == "__main__":

    df = read_json_dataframe(
        file_path=os.environ.get('USERS_DATA_PATH'),
        remove_duplicates=True
    )
    plot_multi_column_histogram(
        dataframe=df,
        columns=['activist', 'journalist', 'media'],
        title=''
    )