from typing import List, Dict

import matplotlib.pyplot as plt

from wordcloud import WordCloud

from utils import paths
from research import stats, nlp


def pie_chart_from_count_dict(count_dict: Dict):
    lists = count_dict.items()
    x, y = zip(*lists)
    fig1, ax1 = plt.subplots()
    ax1.pie(y, labels=x, autopct='%1.1f%%',
            shadow=True, startangle=90)
    ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    plt.show()


def histogram_from_count_dict(count_dict: Dict):
    colors = []
    plt.bar(list(count_dict.keys()), count_dict.values())
    plt.show()


def word_cloud_from_tokenized_text(text: str):
    wordcloud = WordCloud(
        font_path=paths.JAPANESE_FONT_PATH,
        background_color="white",
        max_font_size=200).generate(text)
    plt.figure()
    plt.imshow(wordcloud, interpolation="bilinear")
    plt.axis("off")
    plt.show()


def days_histogram_from_day_dict(month_dict: Dict, every_nth: int = 15):
    lists = month_dict.items()
    x, y = zip(*lists)
    fig, ax = plt.subplots()
    plt.plot(x, y)
    plt.xticks(rotation=45)
    for n, label in enumerate(ax.xaxis.get_ticklabels()):
        if n % every_nth != 0:
            label.set_visible(False)
    plt.show()


def scatter_from_count_dict(count_dict: Dict, x_label: str, y_label: str):
    lists = count_dict.items()
    x, y = zip(*lists)
    fig, ax = plt.subplots()
    ax.scatter(x, y, s=5)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.show()


def plot_month_counts(data_path: str):
    month_dict = stats.count_corpus_per_month(data_path)
    lists = month_dict.items()
    x, y = zip(*lists)
    plt.plot(x, y)
    plt.xticks(rotation=45)
    plt.show()
    
    
def plot_days_counts(data_path: str):
    month_dict = stats.count_corpus_per_day(
        data_path,
        (2017, 10, 1),
        (2017, 11, 1))
    days_histogram_from_day_dict(month_dict, 15)


def plot_hashtag_per_day_count(data_path: str, hashtag: str):
    counter = stats.count_hashtag_per_day(
        data_path,
        hashtag,
        (2017, 10, 1),
        (2017, 11, 1))
    plot_days_counts(counter)


def plot_hashtags_per_day_count(data_path: str, hashtags: List[str]):
    fig, ax = plt.subplots()
    plt.rcParams["font.family"] = "Noto Sans CJK JP"
    for hashtag in hashtags:
        hashtag_counter = stats.count_hashtag_per_day(
            data_path,
            hashtag,
            (2017, 10, 1),
            (2017, 11, 1))
        lists = hashtag_counter.items()
        x, y = zip(*lists)
        plt.plot(x, y, label=hashtag)
    plt.xticks(rotation=45)
    every_nth = 15
    for n, label in enumerate(ax.xaxis.get_ticklabels()):
        if n % every_nth != 0:
            label.set_visible(False)
    plt.legend(loc='upper right')
    plt.show()


def plot_hashtags_per_month_count(data_path: str, hashtags: List[str]):
    plt.rcParams["font.family"] = "Noto Sans CJK JP"
    for hashtag in hashtags:
        hashtag_counter = stats.count_hashtag_per_month(data_path, hashtag)
        lists = hashtag_counter.items()
        x, y = zip(*lists)
        plt.plot(x, y, label=hashtag)
    plt.xticks(rotation=45)
    plt.legend(loc='upper right')
    plt.show()
    
    
def plot_languages(data_path: str):
    lang_dict = stats.count_languages(data_path)
    lang_dict.pop("ja")
    plt.bar(list(lang_dict.keys()), lang_dict.values())
    plt.show()


def plot_tweet_counts_per_users(data_path: str):
    tweet_per_user = stats.count_tweet_per_user(data_path)
    counter = stats.count_users_tweet_counts(tweet_per_user)
    scatter_from_count_dict(counter, "Number of Tweets", "Number of Users")


def plot_public_metric(data_path: str, metric_name: str):
    metric_count = stats.count_public_metrics(data_path, metric_name)
    scatter_from_count_dict(metric_count, "Retweets", "Number of tweets")


def plot_all_public_metrics(data_path: str):
    fig, ax = plt.subplots()
    for metric_name in [
            ("retweet_count", "tomato"),
            ("reply_count", "mediumseagreen"),
            ("like_count", "gold"),
            ("quote_count", "cornflowerblue")]:
        metric_count = stats.count_public_metrics(data_path, metric_name[0])
        lists = metric_count.items()
        x, y = zip(*lists)
        ax.scatter(x, y, s=5, c=metric_name[1], label=metric_name[0])
    plt.legend(loc='upper right')
    plt.xlabel("Metric count")
    plt.ylabel("Number of tweets")
    plt.show()


def plot_gender_counts(data_path: str):
    gender_count = stats.count_gendered_corpus(data_path)
    pie_chart_from_count_dict(gender_count)


def plot_quotes_in_corpus(data_path: str):
    plt.rcParams["font.family"] = "Noto Sans CJK JP"
    quote_count = stats.count_quote_corpus(data_path)
    pie_chart_from_count_dict(quote_count)


def plot_word_cloud(data_path: str):
    plt.rcParams["font.family"] = "Noto Sans CJK JP"
    text = nlp.tokenize_all_tweet_texts(data_path, paths.JAPANESE_STOP_WORDS, "text")
    word_cloud_from_tokenized_text(text)


def plot_annotation_texts(data_path: str):
    plt.rcParams["font.family"] = "Noto Sans CJK JP"
    annotation_texts_count = stats.count_annotation_texts(data_path, "emotion")
    pie_chart_from_count_dict(annotation_texts_count)


if __name__ == "__main__":
    plot_annotation_texts(paths.LEXICAL_FIELDS_CORPUS_DIR)


