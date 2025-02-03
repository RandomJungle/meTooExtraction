import matplotlib.pyplot as plt
import os
from typing import Callable, Optional, Tuple, List

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dotenv import find_dotenv, load_dotenv
from plotly.subplots import make_subplots
from wordcloud import WordCloud

from research import nlp
from utils.df_transform import (
    add_month_column,
    add_hashtags_column,
    explode_lowered_hashtags,
    filter_hashtags_by_count,
    df_pipeline,
    basic_pipeline
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
    return 1200, 800


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


def plot_tweet_count_per_month_histogram(
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


def plot_tweet_type_histogram(
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


def plot_tweet_timeline_histogram(
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


def plot_identities_pie_chart(
        dataframe: pd.DataFrame,
        values_column: Optional[str] = None,
        names_column: Optional[str] = 'identities',
        title: Optional[str] = None,
        output_path: Optional[str] = None) -> None:
    """
    Pie chart generated by exploding columns of identities (overlapping)
    """
    width, height = define_width_and_height(output_path)
    dataframe = dataframe.explode(
        column=names_column,
        ignore_index=True
    )
    fig = px.pie(
        dataframe,
        values=values_column,
        names=names_column,
        color_discrete_sequence=px.colors.qualitative.G10 + px.colors.qualitative.Vivid,
        title=title,
        width=width,
        height=height
    )
    text_pos = 'inside' if not output_path or output_path.endswith('.html') else 'outside'
    fig.update_traces(textposition=text_pos, textinfo='percent+label')
    export_plotly_image(fig, output_path)


def plot_public_metrics_boxplot(
        dataframe: pd.DataFrame,
        title: Optional[str] = None,
        output_path: Optional[str] = None) -> None:
    """
    Boxplot of all four public metrics distributions
    """
    width, height = define_width_and_height(output_path)
    fig = go.Figure()
    for metric in ['reply_count', 'retweet_count', 'quote_count', 'like_count']:
        fig.add_trace(
            go.Box(
                x=dataframe[metric],
                name=metric,
                boxpoints='all'
            )
        )
    fig.update_layout(
        title_text=title,
        width=width,
        height=height
    )
    export_plotly_image(fig, output_path)


def plot_public_metrics_per_user_boxplot(
        dataframe: pd.DataFrame,
        metric: str,
        title: Optional[str] = None,
        output_path: Optional[str] = None) -> None:
    """
    Boxplot of all four public metrics distributions
    """
    width, height = define_width_and_height(output_path)
    fig = go.Figure()
    for user in dataframe['user_username'].dropna().unique():
        fig.add_trace(
            go.Box(
                x=dataframe[dataframe['user_username'] == user][metric],
                name=user,
                boxpoints=False
            )
        )
    fig.update_layout(
        title_text=title,
        width=width,
        height=height
    )
    export_plotly_image(fig, output_path)


def plot_average_metric_per_tweet_per_user(
        dataframe: pd.DataFrame,
        title: Optional[str] = None,
        output_path: Optional[str] = None) -> None:
    """
    Histogram of averages for all public metrics per user
    """
    layout = dict(
        hoversubplots='axis',
        title=dict(text=title),
        hovermode='x',
        grid=dict(rows=4, columns=1),
        autosize=False,
        width=1200,
        height=1000,
        legend=dict(
            orientation='h',
            yanchor='bottom',
            y=1.02,
            xanchor='left',
            x=0.0
        )
    )
    data = []
    for index, metric in enumerate(['reply', 'retweet', 'quote', 'like']):
        y_axis = 'y' if index == 0 else f'y{index + 1}'
        data.append(
            go.Histogram(
                x=dataframe['user_username'],
                y=dataframe[f'{metric}_count'],
                texttemplate='%{y:.2f}',
                histfunc='avg',
                xaxis='x',
                yaxis=y_axis,
                name=f'average number of {metric} per tweet'
            )
        )
    fig = go.Figure(data=data, layout=layout)
    export_plotly_image(fig, output_path)


if __name__ == "__main__":

    df = read_json_dataframe(
        file_path=os.environ.get('USERS_DATA_PATH'),
        remove_duplicates=True
    )
    df = basic_pipeline(df)
    plot_average_metric_per_tweet_per_user(
        dataframe=df,
        title='Mean metrics per tweet per user',
        output_path=os.path.join(
            os.environ.get('OUTPUT_PLOT_DIR'),
            'avg_metrics_count_per_user_hist.png'
        )
    )