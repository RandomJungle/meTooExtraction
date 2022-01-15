from typing import List

import matplotlib.pyplot as plt

import utils.paths as paths
from research.stats import count_corpus_per_month, count_corpus_per_day, count_languages, count_hashtag_per_day, \
    count_hashtag_per_month, count_tweet_per_user, count_users_tweet_counts, count_public_metrics


def plot_month_counts(data_path: str):
    month_dict = count_corpus_per_month(data_path)
    lists = month_dict.items()
    x, y = zip(*lists)
    plt.plot(x, y)
    plt.xticks(rotation=45)
    plt.show()
    
    
def plot_days_counts(data_path: str):
    month_dict = count_corpus_per_day(
        data_path,
        (2017, 10, 1),
        (2017, 11, 1))
    lists = month_dict.items()
    x, y = zip(*lists)
    fig, ax = plt.subplots()
    plt.plot(x, y)
    plt.xticks(rotation=45)
    every_nth = 15
    for n, label in enumerate(ax.xaxis.get_ticklabels()):
        if n % every_nth != 0:
            label.set_visible(False)
    plt.show()


def plot_hashtag_per_day_count(data_path: str, hashtag: str):
    counter = count_hashtag_per_day(
        data_path,
        hashtag,
        (2017, 10, 1),
        (2017, 11, 1))
    lists = counter.items()
    x, y = zip(*lists)
    fig, ax = plt.subplots()
    plt.plot(x, y)
    plt.xticks(rotation=45)
    every_nth = 15
    for n, label in enumerate(ax.xaxis.get_ticklabels()):
        if n % every_nth != 0:
            label.set_visible(False)
    plt.show()


def plot_hashtags_per_day_count(data_path: str, hashtags: List[str]):
    fig, ax = plt.subplots()
    plt.rcParams["font.family"] = "Noto Sans CJK JP"
    for hashtag in hashtags:
        hashtag_counter = count_hashtag_per_day(
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
        hashtag_counter = count_hashtag_per_month(data_path, hashtag)
        lists = hashtag_counter.items()
        x, y = zip(*lists)
        plt.plot(x, y, label=hashtag)
    plt.xticks(rotation=45)
    plt.legend(loc='upper right')
    plt.show()
    
    
def plot_languages(data_path: str):
    lang_dict = count_languages(data_path)
    lang_dict.pop("ja")
    plt.bar(list(lang_dict.keys()), lang_dict.values())
    plt.show()


def plot_tweet_counts_per_users(data_path: str):
    tweet_per_user = count_tweet_per_user(data_path)
    counter = count_users_tweet_counts(tweet_per_user)
    lists = counter.items()
    x, y = zip(*lists)
    fig, ax = plt.subplots()
    ax.scatter(x, y, s=5)
    plt.xlabel("Number of Tweets")
    plt.ylabel("Number of Users")
    plt.show()


def plot_public_metric(data_path: str, metric_name: str):
    metric_count = count_public_metrics(data_path, metric_name)
    lists = metric_count.items()
    x, y = zip(*lists)
    fig, ax = plt.subplots()
    ax.scatter(x, y, s=5)
    plt.xlabel("Retweets")
    plt.ylabel("Number of tweets")
    plt.show()


def plot_all_public_metrics(data_path: str):
    fig, ax = plt.subplots()
    for metric_name in [
            ("retweet_count", "tomato"),
            ("reply_count", "mediumseagreen"),
            ("like_count", "gold"),
            ("quote_count", "cornflowerblue")]:
        metric_count = count_public_metrics(data_path, metric_name[0])
        lists = metric_count.items()
        x, y = zip(*lists)
        ax.scatter(x, y, s=5, c=metric_name[1], label=metric_name[0])
    plt.legend(loc='upper right')
    plt.xlabel("Metric count")
    plt.ylabel("Number of tweets")
    plt.show()


if __name__ == "__main__":
    plot_all_public_metrics(paths.FINAL_CORPUS_DIR_PATH)


