import matplotlib.pyplot as plt
import os

from typing import List, Dict, Callable, Tuple
from wordcloud import WordCloud

from utils import paths
from research import stats, nlp
from utils.tweet_utils import is_testimony


def create_plot_file_name(file_name: str):
    return os.path.join(paths.VISUALS_DIR, file_name)


def pie_chart_from_count_dict(count_dict: Dict, figure_name: str):
    lists = count_dict.items()
    x, y = zip(*lists)
    fig1, ax1 = plt.subplots()
    ax1.pie(y, labels=x, autopct='%1.1f%%',
            shadow=True, startangle=90)
    ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    plt.savefig(figure_name)


def histogram_from_count_dict(count_dict: Dict, figure_name: str):
    plt.bar(list(count_dict.keys()), count_dict.values())
    plt.savefig(figure_name)


def word_cloud_from_tokenized_text(text: str, figure_name: str):
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
    plt.savefig(figure_name)


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
    plt.savefig(figure_name)


def scatter_from_count_dict(count_dict: Dict, x_label: str, y_label: str, figure_name: str):
    lists = count_dict.items()
    x, y = zip(*lists)
    fig, ax = plt.subplots()
    ax.scatter(x, y, s=5)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.savefig(figure_name)


def plot_month_counts(data_path: str, figure_name: str):
    fig, ax = plt.subplots()
    month_dict = stats.count_corpus_per_month(data_path)
    lists = month_dict.items()
    x, y = zip(*lists)
    plt.plot(x, y)
    plt.xticks(rotation=45)
    fig.set_size_inches(8, 10)
    plt.savefig(figure_name)
    
    
def plot_days_counts(data_path: str, figure_name: str, filter_function: Callable = None, every_nth: int = 15):
    month_dict = stats.count_corpus_per_day(
        data_path,
        (2017, 10, 1),
        (2017, 11, 1),
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
    plt.savefig(figure_name)


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
        end_date: Tuple = (2017, 11, 1),
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
    plt.savefig(figure_name)


def plot_hashtags_per_month_count(data_path: str, hashtags: List[str], figure_name: str):
    plt.rcParams["font.family"] = "Noto Sans CJK JP"
    for hashtag in hashtags:
        hashtag_counter = stats.count_hashtag_per_month(data_path, hashtag)
        lists = hashtag_counter.items()
        x, y = zip(*lists)
        plt.plot(x, y, label=hashtag)
    plt.xticks(rotation=45)
    plt.legend(loc='upper right')
    plt.savefig(figure_name)
    
    
def plot_languages(data_path: str, figure_name: str):
    lang_dict = stats.count_languages(data_path)
    lang_dict.pop("ja")
    plt.bar(list(lang_dict.keys()), lang_dict.values())
    plt.savefig(figure_name)


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
    plt.savefig(figure_name)


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
    plt.savefig(figure_name)


def plot_all_metrics():
    plot_days_count_testimonies(paths.FINAL_CORPUS_DIR, create_plot_file_name("comparison_tweets_per_day_count.png"), 5)
    plot_days_counts(paths.FINAL_CORPUS_DIR, create_plot_file_name("testimony_tweets_per_day_count.png"), is_testimony, 5)
    plot_days_counts(paths.ANNOTATED_CORPUS_DIR, create_plot_file_name("testimony_tweets_per_day_count.png"))
    plot_hashtags_per_day_count(
        data_path=paths.FINAL_CORPUS_DIR,
        hashtags=[
            "#timesup",
            "#wetoo",
            "#パワハラ",
            "#セクハラ",
            "#metoo",
            "#ｍｅｔｏｏ"
        ],
        figure_name=create_plot_file_name("total_hashtags_per_day.png"),
        start_date=(2017, 10, 1),
        end_date=(2017, 11, 1),
        every_nth=5)
    plot_hashtags_per_day_count(
        data_path=paths.FINAL_CORPUS_DIR,
        hashtags=[
            "#timesup",
            "#wetoo",
            "#パワハラ",
            "#セクハラ",
            "#metoo",
            "#ｍｅｔｏｏ"
        ],
        figure_name=create_plot_file_name("testimony_hashtags_per_day.png"),
        start_date=(2017, 10, 1),
        end_date=(2017, 11, 1),
        every_nth=5,
        filter_function=is_testimony)
    plot_hour_of_tweet(paths.FINAL_CORPUS_DIR, create_plot_file_name("comparison_hour_tweeted.png"))
    plot_gender_counts(paths.FINAL_CORPUS_DIR, create_plot_file_name("total_gender_counts.png"))
    plot_gender_counts(paths.FINAL_CORPUS_DIR, create_plot_file_name("testimony_gender_counts.png"), is_testimony)
    plot_all_public_metrics(paths.FINAL_CORPUS_DIR, create_plot_file_name("total_public_metrics.png"))
    plot_all_public_metrics(paths.FINAL_CORPUS_DIR, create_plot_file_name("testimony_public_metrics.png"), is_testimony)
    plot_hashtags_per_day_count(
        data_path=paths.CLEAN_DATA_DIR,
        hashtags=[
            "#timesup",
            "#wetoo",
            "#パワハラ",
            "#セクハラ",
            "#metoo",
            "#ｍｅｔｏｏ"
        ],
        figure_name=create_plot_file_name("full_data_hashtags_per_day.png"),
        start_date=(2017, 10, 1),
        end_date=(2018, 11, 1))
    plot_month_counts(paths.CLEAN_DATA_DIR, create_plot_file_name("full_data_tweet_count_per_month.png"))
    plot_quotes_in_corpus(paths.FINAL_CORPUS_DIR, create_plot_file_name("testimony_type_of_quotes.png"), is_testimony)
    plot_word_cloud(paths.FINAL_CORPUS_DIR, create_plot_file_name("testimony_word_cloud.png"), is_testimony)
    plot_annotation_texts(paths.FINAL_CORPUS_DIR, create_plot_file_name("testimony_lexical_field_emotion.png"))


if __name__ == "__main__":
    plot_all_metrics()
