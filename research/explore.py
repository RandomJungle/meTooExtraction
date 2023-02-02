from collections import Counter

from tqdm import tqdm

import utils.paths as paths
from research.stats import count_tweet_per_user, count_all_hashtags

from utils.file_utils import read_corpus_generator, select_tweets_from_ids
from utils.tweet_utils import get_language_of_tweet


def print_other_languages_tweets(data_path: str):
    """
    Print all tweet belonging to language: "und" (undetermined) in the corpus

    Args:
        data_path: path to dir of tweets

    Returns:
        None, print to console
    """
    for entry in read_corpus_generator(data_path):
        language = get_language_of_tweet(entry)
        if language == "und":
            text = entry.get('text')
            print(f"{language} : {text}" + '\n' + ('-' * 100) + '\n')


def print_tweets_with_hashtag(data_path: str, hashtag: str):
    """
    Print all tweets containing a certain hashtag

    Args:
        data_path: path to dir of tweets
        hashtag: hashtag key to search in tweets

    Returns:
        None, print tweet content
    """
    for tweet in read_corpus_generator(data_path):
        if hashtag in tweet.get('text'):
            print(tweet.get('text') + "\n\n" + ("-" * 50) + "\n\n")


def write_all_hashtags(data_path: str, output_path: str):
    """
    Write hashtag frequency count to a file

    Args:
        data_path: path to dir of tweets
        output_path: path to write output file

    Returns:
        None, write results to output file
    """
    with open(output_path, "w") as output_file:
        for key, value in count_all_hashtags(data_path).items():
            output_file.write(f"{value} tweets found with {key}\n")


def print_copycats(data_path: str, key="text"):
    """
    Print all copycats (duplicate tweets with same textual content) with their
    frequency of apparition

    Args:
        data_path: path to dir of tweets
        key: key to search tweet text

    Returns:
        None, print copycats to console
    """
    texts = []
    for tweet in read_corpus_generator(data_path):
        texts.append(tweet.get(key))
    for key, value in Counter(texts).items():
        if value > 1:
            print(f"\n{value} appearances for :\n{key}\n\n" + ("-" * 50))


def print_outliers_user_ids(data_path: str, threshold: int = 1000):
    """
    Print user ids with outlier number of tweets (defined by parameter threshold)

    Args:
        data_path: path to dir of tweets
        threshold: defined threshold above which a poster becomes an outlier

    Returns:
        None, print user id to console
    """
    counter = count_tweet_per_user(data_path)
    outlier_ids = []
    for key, value in counter.items():
        if value > threshold:
            outlier_ids.append(key)
            print(f"{key}: {value}")


def find_most_retweeted_tweets(data_path: str):
    count_dict = {}
    for tweet in tqdm(read_corpus_generator(data_path)):
        metric_count = tweet.get('public_metrics').get("retweet_count")
        count_dict.update({
            metric_count: count_dict.get(metric_count, []) + [tweet.get('id')]})
    count_dict = {key: value for key, value in sorted(
        count_dict.items(), key=lambda item: item[1])}
    return select_tweets_from_ids(data_path, list(count_dict.values())[0])


if __name__ == "__main__":
    most_retweeted = find_most_retweeted_tweets(paths.JAPAN_2017_2019)
    print(len(most_retweeted))
