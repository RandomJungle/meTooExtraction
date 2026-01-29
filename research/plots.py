from collections import OrderedDict

import matplotlib.pyplot as plt
import os
from typing import Optional, Tuple, List, Dict

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dotenv import find_dotenv, load_dotenv
from wordcloud import WordCloud

from research.clustering import agglomerative_clustering, kmeans_clustering
from research.nlp import tokenize_tweets_df, tf_idf_clusters
from utils.df_transform import (
    add_month_column,
    add_hashtags_column,
    explode_lowered_hashtags,
    filter_hashtags_by_count,
    df_pipeline,
)
from utils.file_utils import read_json_dataframe


def export_plotly_image(
        figure: go.Figure, output_path: str | None) -> None:
    if output_path:
        if output_path.endswith('html'):
            figure.write_html(output_path)
        else:
            figure.write_image(output_path)
    else:
        figure.show()


def define_width_and_height(
        output_path: str) -> Tuple[int | None, int | None]:
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
        dataframe: pd.DataFrame,
        output_path: Optional[str] = None) -> None:
    if not 'tokens_filtered' in dataframe.columns:
        dataframe = tokenize_tweets_df(
            dataframe=dataframe
        )
    dataframe['tokens_s'] = dataframe['tokens_filtered'].apply(
        lambda x: ' '.join(x)
    )
    text = ' '.join(dataframe['tokens_s'].tolist())
    word_cloud_from_tokenized_text(text, output_path)


def plot_word_clouds_clusters(
        dataframe: pd.DataFrame,
        cluster_labels_column: str,
        output_path: Optional[str] = None) -> None:
    plt.figure(figsize=(8, 10))
    if not 'tokens_filtered' in dataframe.columns:
        dataframe = tokenize_tweets_df(
            dataframe=dataframe
        )
    dataframe['tokens_str'] = dataframe['tokens_filtered'].apply(
        lambda x: ' '.join(x)
    )
    # iterate through set of cluster labels to create word clouds for each cluster
    dataframe[cluster_labels_column] = dataframe[cluster_labels_column].astype('category')
    unique_labels = set(dataframe[cluster_labels_column].cat.categories)
    for index, cluster_label in enumerate(sorted(unique_labels, key=lambda x: int(x))):
        ax = plt.subplot(int(len(unique_labels)/2) + 1, 2, index + 1)
        cluster_subset = dataframe[dataframe[cluster_labels_column] == cluster_label]
        text = ' '.join(cluster_subset['tokens_str'].tolist())
        wordcloud = WordCloud(
            font_path=os.environ.get('JAPANESE_FONT_PATH'),
            background_color="white",
            max_font_size=200,
            width=1200,
            height=400
        ).generate(text)
        ax.imshow(wordcloud, interpolation="bilinear")
        ax.axis("off")
        ax.title.set_text(f'Cluster n° {cluster_label}')
    if output_path:
        plt.savefig(output_path)
    else:
        plt.show()


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


def plot_2d_scatter(
        dataframe: pd.DataFrame,
        embedding_column: str,
        color_column: Optional[str] = None,
        title: Optional[str] = None,
        output_path: Optional[str] = None) -> None:
    width, height = define_width_and_height(output_path)
    fig = px.scatter(
        dataframe,
        x=f'{embedding_column}_x',
        y=f'{embedding_column}_y',
        color=color_column,
        width=width,
        height=height,
        title=title,
        custom_data=[
            'user_username',
            'created_at',
            'text_br',
            'text_en_br'
        ],
        color_discrete_sequence=px.colors.qualitative.G10 + px.colors.qualitative.Vivid,
    )
    fig.update_traces(
        hovertemplate=(
            '<b>%{customdata[0]}</b><br><br>' +
            'Published : %{customdata[1]|%Y-%m-%d}<br><br>' +
            '%{customdata[2]}<br><br>' +
            '%{customdata[3]}'
        )
    )
    export_plotly_image(fig, output_path)


def plot_tf_idf_cluster_table(
        dataframe: pd.DataFrame,
        tokens_column: Optional[str] = 'tokens',
        cluster_labels_column: Optional[str] = 'labels',
        max_terms: Optional[int] = None) -> None:
    tf_idf_matrix = tf_idf_clusters(
        dataframe=dataframe,
        cluster_labels_column=cluster_labels_column,
        tokens_column=tokens_column,
        max_terms=max_terms
    )
    tf_idf_matrix_ordered = OrderedDict(
        sorted(
            [(key, value) for key, value in tf_idf_matrix.items()],
            key=lambda x: int(x[0])
        )
    )
    values = [
        [key for key in tf_idf_matrix_ordered.keys()],
        [
            ', '.join([
                f'{top_term[0]} ({round(top_term[1], 2)})'
                for top_term in cluster_top_terms])
                for cluster_top_terms in tf_idf_matrix_ordered.values()
        ]
    ]
    fig = go.Figure(
        data=[
            go.Table(
                columnorder=[1, 2],
                columnwidth=[80, 400],
                header=dict(
                    values=[
                        ['<b>CLUSTER</b>'],
                        ['<b>TOP TERMS ASSOCIATED (TF-IDF adjusted for document length)</b>']
                    ],
                    line_color='darkslategray',
                    fill_color='royalblue',
                    align=['left', 'center'],
                    font=dict(color='white', size=12),
                    height=40
                ),
                cells=dict(
                    values=values,
                    line_color='darkslategray',
                    fill=dict(color=['paleturquoise', 'white']),
                    align=['left', 'center'],
                    font_size=12,
                    height=30)
            )
        ]
    )
    fig.show()


if __name__ == "__main__":

    load_dotenv(find_dotenv())

    distance = 3

    df = read_json_dataframe(
        file_path=os.environ.get('LATEST_DATASET_PATH'),
        remove_duplicates=True
    )
    df, _ = agglomerative_clustering(
        dataframe=df,
        embeddings_column='mistral-embed_embeddings',
        n_clusters=None,
        distance_threshold=distance
    )
    plot_word_clouds_clusters(
        dataframe=df,
        cluster_labels_column=f'clustering_agglo_dist{distance}',
        output_path=os.path.join(
            os.getenv('PLOT_DIR_PATH'),
            'wordcloud_clusters.png'
        )
    )
    # mistral_model = 'mistral-embed'
    # openai_model = 'text-embedding-3-large'
    # model = mistral_model
    # dim_red = 'umap'
    # plot_tsne_scatter(
    #     dataframe=df,
    #     embedding_column=f'{dim_red}_{model}_embeddings',
    #     title=f'{dim_red} scatter plot of {model} embeddings',
    #     # output_path=os.path.join(
    #     #     os.environ.get('OUTPUT_PLOT_DIR'),
    #     #     'avg_metrics_count_per_user_hist.png'
    #     # )
    # )