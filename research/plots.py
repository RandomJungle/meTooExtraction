import matplotlib.pyplot as plt
import numpy as np
import os

import seaborn as sns

from typing import List, Dict, Callable, Tuple

from tqdm import tqdm
from wordcloud import WordCloud

from research.stats import count_time_of_tweets
from utils import paths
from research import stats, nlp
from utils.tweet_utils import is_testimony, is_not_retweet


def create_plot_file_name(file_name: str):
    return os.path.join(paths.VISUALS_DIR, file_name)


def pie_chart_from_count_dict(count_dict: Dict, figure_name: str, title: str = None):
    """
    Plot and saves a pie chart from a counting dict mapping label -> frequency

    Args:
        count_dict: Dict of counted values
        figure_name: Name to give the figure for saving
        title: optional title to give to the figure

    Returns:
        None, save to file
    """
    lists = count_dict.items()
    x, y = zip(*lists)
    fig, ax = plt.subplots(figsize=(10, 5))
    fig.suptitle(title)
    ax.pie(y, labels=x,
           autopct=lambda p: f'{p:.2f}%, {p*sum(y)/100 :.0f}',
           labeldistance=None, startangle=90)
    ax.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    plt.legend(loc='lower left')
    plt.savefig(create_plot_file_name(figure_name))


def labeled_pie_chart_from_count_dict(count_dict: Dict, figure_name: str, title: str = None):
    """
    Plot a pie chart labeled with arrows from a counting dict mapping label -> frequency

    Args:
        count_dict: Dict of counted values
        figure_name: Name to give the figure for saving
        title: optional title to give to the figure

    Returns:
        None, save to file
    """
    lists = count_dict.items()
    labels, data = zip(*lists)
    fig, ax = plt.subplots(figsize=(12, 7), subplot_kw=dict(aspect="equal"))
    fig.suptitle(title)
    wedges, texts = ax.pie(data, wedgeprops=dict(width=0.5), startangle=-40)
    bbox_props = dict(boxstyle="square,pad=0.3", fc="w", ec="k", lw=0.72)
    kw = dict(arrowprops=dict(arrowstyle="-"),
              bbox=bbox_props, zorder=0, va="center")
    for i, p in enumerate(wedges):
        ang = (p.theta2 - p.theta1) / 2. + p.theta1
        y = np.sin(np.deg2rad(ang))
        x = np.cos(np.deg2rad(ang))
        horizontalalignment = {-1: "right", 1: "left"}[int(np.sign(x))]
        connectionstyle = "angle,angleA=0,angleB={}".format(ang)
        kw["arrowprops"].update({"connectionstyle": connectionstyle})
        ax.annotate(labels[i], xy=(x, y), xytext=(1.35 * np.sign(x), 1.4 * y),
                    horizontalalignment=horizontalalignment, **kw)
    plt.savefig(create_plot_file_name(figure_name))


def histogram_from_count_dict(count_dict: Dict, figure_name: str):
    """
    Plot a histogram bar plot from a counting dict mapping label -> frequency

    Args:
        count_dict: Dict of counted values
        figure_name: Name to give the figure for saving

    Returns:
        None, save to file
    """
    plt.bar(list(count_dict.keys()), count_dict.values())
    plt.savefig(create_plot_file_name(figure_name))


def word_cloud_from_tokenized_text(text: str, figure_name: str):
    """
    Creates a wordcloud from raw text

    Args:
        text: raw text
        figure_name: name of figure to save

    Returns:
        None, save to file
    """
    plt.rcParams["font.family"] = "Noto Sans CJK JP"
    wordcloud = WordCloud(
        font_path=paths.JAPANESE_FONT_PATH,
        background_color="white",
        max_font_size=200,
        width=800,
        height=400).generate(text)
    plt.figure(figsize=(20, 10))
    plt.imshow(wordcloud, interpolation="bilinear")
    plt.axis("off")
    plt.savefig(create_plot_file_name(figure_name))


def days_histogram_from_day_dict(month_dict: Dict, figure_name: str, every_nth: int = 15):
    lists = month_dict.items()
    x, y = zip(*lists)
    fig, ax = plt.subplots()
    fig.set_size_inches(12, 8)
    plt.plot(x, y)
    plt.xticks(rotation=45)
    for n, label in enumerate(ax.xaxis.get_ticklabels()):
        if n % every_nth != 0:
            label.set_visible(False)
    plt.savefig(create_plot_file_name(figure_name))


def scatter_from_count_dict(count_dict: Dict, x_label: str, y_label: str, figure_name: str):
    lists = count_dict.items()
    x, y = zip(*lists)
    fig, ax = plt.subplots()
    ax.scatter(x, y, s=5)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.savefig(create_plot_file_name(figure_name))


def plot_labels(data_path: str, save_path: str):
    count_dict = stats.count_labels(data_path)
    pie_chart_from_count_dict(
        count_dict,
        save_path,
        title="Répartition des tweets entre les annotations de témoignage")


def plot_month_counts(data_path: str, figure_name: str):
    fig, ax = plt.subplots()
    month_dict = stats.count_corpus_per_month(data_path)
    lists = month_dict.items()
    x, y = zip(*lists)
    plt.plot(x, y)
    plt.xticks(rotation=45)
    fig.set_size_inches(8, 10)
    plt.savefig(create_plot_file_name(figure_name))
    
    
def plot_days_counts(
        data_path: str,
        figure_name: str,
        filter_function: Callable = None,
        start_date: Tuple = (2017, 10, 1),
        end_date: Tuple = (2019, 11, 1),
        every_nth: int = 15):
    month_dict = stats.count_corpus_per_day(
        data_path,
        start_date,
        end_date,
        filter_function)
    days_histogram_from_day_dict(month_dict, figure_name, every_nth)


def plot_days_count_testimonies(data_path: str, figure_name: str, every_nth: int = 15):
    full_dict = stats.count_corpus_per_day(data_path, (2017, 10, 1), (2017, 11, 1))
    testimony_dict = stats.count_corpus_per_day(data_path, (2017, 10, 1), (2017, 11, 1), is_testimony)
    x_full, y_full = zip(*full_dict.items())
    x_testimony, y_testimony = zip(*testimony_dict.items())
    fig, ax = plt.subplots()
    fig.set_size_inches(10, 8)
    full_line, = ax.plot(x_full, y_full)
    full_line.set_label('Entire Corpus')
    plt.fill_between(x_full, y_full)
    testimony_line, = plt.plot(x_testimony, y_testimony)
    testimony_line.set_label('Testimonies')
    plt.fill_between(x_testimony, y_testimony)
    plt.xticks(rotation=45)
    for n, label in enumerate(ax.xaxis.get_ticklabels()):
        if n % every_nth != 0:
            label.set_visible(False)
    plt.legend(loc='upper left')
    plt.savefig(create_plot_file_name(figure_name))


def plot_hashtag_per_day_count(data_path: str, hashtag: str, figure_name: str):
    counter = stats.count_hashtag_per_day(
        data_path,
        hashtag,
        (2017, 10, 1),
        (2017, 11, 1))
    plot_days_counts(counter, figure_name)


def plot_hashtags_per_day_count(
        data_path: str,
        hashtags: List[str],
        figure_name: str,
        start_date: Tuple = (2017, 10, 1),
        end_date: Tuple = (2019, 11, 1),
        every_nth: int = 15,
        filter_function: Callable = None):
    fig, ax = plt.subplots()
    fig.set_size_inches(12, 8)
    plt.rcParams["font.family"] = "Noto Sans CJK JP"
    for hashtag in hashtags:
        hashtag_counter = stats.count_hashtag_per_day(
            data_path,
            hashtag,
            start_date,
            end_date,
            filter_function)
        lists = hashtag_counter.items()
        if any([item[1] > 0 for item in lists]):
            x, y = zip(*lists)
            plt.plot(x, y, label=hashtag)
    plt.xticks(rotation=45)
    for n, label in enumerate(ax.xaxis.get_ticklabels()):
        if n % every_nth != 0:
            label.set_visible(False)
    plt.legend(loc='upper right')
    plt.savefig(create_plot_file_name(figure_name))


def plot_hashtags_per_month_count(data_path: str, hashtags: List[str], figure_name: str):
    plt.rcParams["font.family"] = "Noto Sans CJK JP"
    plt.figure(figsize=[15, 10])
    for hashtag in hashtags:
        hashtag_counter = stats.count_hashtag_per_month(data_path, hashtag)
        lists = hashtag_counter.items()
        x, y = zip(*lists)
        plt.plot(x, y, label=hashtag)
    plt.xticks(rotation=45)
    plt.legend(loc='upper right')
    plt.savefig(create_plot_file_name(figure_name))


def plot_tweet_per_hour_and_weekday_heatmap(data_path: str, figure_name: str):
    hour_week_df = count_time_of_tweets(data_path)
    fig, ax = plt.subplots()
    sns.heatmap(hour_week_df)
    ax.set_frame_on(False)  # remove all spines
    plt.savefig(create_plot_file_name(figure_name))
    
    
def plot_languages(data_path: str, figure_name: str):
    lang_dict = stats.count_languages(data_path)
    lang_dict.pop("ja")
    plt.bar(list(lang_dict.keys()), lang_dict.values())
    plt.savefig(create_plot_file_name(figure_name))


def plot_tweet_counts_per_users(data_path: str, figure_name: str):
    tweet_per_user = stats.count_tweet_per_user(data_path)
    counter = stats.count_users_tweet_counts(tweet_per_user)
    scatter_from_count_dict(counter, "Number of Tweets", "Number of Users", figure_name)


def plot_public_metric(data_path: str, metric_name: str, figure_name: str):
    metric_count = stats.count_public_metrics(data_path, metric_name)
    scatter_from_count_dict(metric_count, "Retweets", "Number of tweets", figure_name)


def plot_all_public_metrics(data_path: str, figure_name: str, filter_function: Callable = None):
    fig, ax = plt.subplots()
    for metric_name in [
            ("retweet_count", "tomato"),
            ("reply_count", "mediumseagreen"),
            ("like_count", "gold"),
            ("quote_count", "cornflowerblue")]:
        metric_count = stats.count_public_metrics(data_path, metric_name[0], filter_function)
        lists = metric_count.items()
        x, y = zip(*lists)
        ax.scatter(x, y, s=5, c=metric_name[1], label=metric_name[0])
    plt.legend(loc='upper right')
    plt.xlabel("Metric count")
    plt.ylabel("Number of tweets")
    plt.savefig(create_plot_file_name(figure_name))


def plot_gender_counts(data_path: str, figure_name: str, filter_function: Callable = None):
    gender_count = stats.count_gendered_corpus(data_path, filter_function)
    pie_chart_from_count_dict(gender_count, figure_name)


def plot_quotes_in_corpus(data_path: str, figure_name: str, filter_function: Callable = None):
    plt.rcParams["font.family"] = "Noto Sans CJK JP"
    quote_count = stats.count_quote_corpus(data_path, filter_function)
    pie_chart_from_count_dict(quote_count, figure_name)


def plot_word_cloud(data_path: str, figure_name: str, filter_function: Callable = None):
    text = nlp.tokenize_all_tweet_texts(data_path, paths.JAPANESE_STOP_WORDS, "text", filter_function)
    word_cloud_from_tokenized_text(text, figure_name)


def plot_annotation_texts(data_path: str, figure_name: str):
    plt.rcParams["font.family"] = "Noto Sans CJK JP"
    annotation_texts_count = stats.count_annotation_texts(data_path, "emotion")
    pie_chart_from_count_dict(annotation_texts_count, figure_name)


def plot_tweet_types(data_path: str, figure_name: str):
    plt.rcParams["font.family"] = "Noto Sans CJK JP"
    type_counts = stats.count_type_frequency(data_path)
    pie_chart_from_count_dict(type_counts, figure_name)


def plot_hour_of_tweet(data_path: str, figure_name: str):
    total_hour_tweeted_count = stats.count_publication_time(data_path)
    testimony_hour_tweeted_count = stats.count_publication_time(data_path, is_testimony)
    x_full, y_full = zip(*sorted(total_hour_tweeted_count.items()))
    x_testimony, y_testimony = zip(*sorted(testimony_hour_tweeted_count.items()))
    fig, ax = plt.subplots()
    fig.set_size_inches(10, 8)
    full_line, = plt.plot(x_full, y_full)
    full_line.set_label('Entire Corpus')
    plt.fill_between(x_full, y_full)
    testimony_line, = plt.plot(x_testimony, y_testimony)
    testimony_line.set_label('Testimonies')
    plt.fill_between(x_testimony, y_testimony)
    plt.legend(loc='upper left')
    plt.savefig(create_plot_file_name(figure_name))


def plot_analysis_pie_charts(
        variable_dict_json_path: str,
        analysis_csv_path: str):
    variables_dict = stats.count_analysis_variable(
        variable_dict_json_path=variable_dict_json_path,
        analysis_csv_path=analysis_csv_path)
    for variable_name, variable_data in variables_dict.items():
        pie_chart_from_count_dict(
            count_dict=variable_data.get("count_dict"),
            figure_name=os.path.join(paths.VISUALS_DIR, f"{variable_name}_pie_chart.png"),
            title=variable_data.get("title"))


if __name__ == "__main__":
    dates_list = [
        ((2017, 10, 1), (2017, 11, 1)),
        ((2017, 11, 1), (2017, 12, 1)),
        ((2017, 12, 1), (2018, 1, 1)),
        ((2018, 1, 1), (2018, 2, 1)),
        ((2018, 2, 1), (2018, 3, 1)),
        ((2018, 3, 1), (2018, 4, 1)),
        ((2018, 4, 1), (2018, 5, 1)),
        ((2018, 5, 1), (2018, 6, 1)),
        ((2018, 6, 1), (2018, 7, 1)),
        ((2018, 7, 1), (2018, 8, 1)),
        ((2018, 8, 1), (2018, 9, 1)),
        ((2018, 9, 1), (2018, 10, 1)),
        ((2018, 10, 1), (2018, 11, 1)),
        ((2018, 11, 1), (2018, 12, 1)),
        ((2018, 12, 1), (2019, 1, 1)),
        ((2019, 1, 1), (2019, 2, 1)),
        ((2019, 2, 1), (2019, 3, 1)),
        ((2019, 3, 1), (2019, 4, 1)),
        ((2019, 4, 1), (2019, 5, 1)),
        ((2019, 5, 1), (2019, 6, 1)),
        ((2019, 6, 1), (2019, 7, 1)),
        ((2019, 7, 1), (2019, 8, 1)),
        ((2019, 8, 1), (2019, 9, 1)),
        ((2019, 9, 1), (2019, 10, 1)),
        ((2019, 10, 1), (2019, 11, 1)),
    ]
    for dates in tqdm(dates_list):
        plot_days_counts(
            data_path=paths.JAPAN_2017_2019_CLEAN,
            figure_name=f"tweet count per days from {dates[0]} to {dates[1]}",
            filter_function=is_not_retweet,
            start_date=dates[0],
            end_date=dates[1],
            every_nth=2)