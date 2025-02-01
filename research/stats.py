import datetime
import json
import os

from collections import OrderedDict, Counter
from typing import Dict, List, Callable, Tuple

import pandas as pd

from utils.converters import file_to_month
from utils.file_utils import read_corpus_generator, read_jsonl_generator, read_variable_dict, read_analysis_csv
from utils.tweet_utils import get_day_of_tweet, get_language_of_tweet, get_hour_of_tweet, get_weekday_of_tweet, \
    is_retweet


def build_days_dict(start_date, end_date):
    """
    Build a dict of days for use in other function that do statistics that include
    time information

    Args:
        start_date: Tuple of chosen start date in format (YYYY, mm, dd)
        end_date: Tuple of chosen end date in format (YYYY, mm, dd)

    Returns:
        Dict of dates between start and end date
    """
    dates = {}
    start_date = datetime.date(start_date[0], start_date[1], start_date[2])
    end_date = datetime.date(end_date[0], end_date[1], end_date[2])
    delta = datetime.timedelta(days=1)
    while start_date <= end_date:
        dates.update({f"{start_date}": 0})
        start_date += delta
    return dates


def count_corpus(data_path: str, filter_function=None):
    """
    Count the number of lines in all files in a listed directory

    Args:
        data_path: path to dir where counting will occur
        filter_function: Optional filter to remove some tweets

    Returns:
        Total number of lines for all files in dir
    """
    total = 0
    for tweet in read_corpus_generator(data_path):
        if (filter_function and filter_function(tweet)) or not filter_function:
            total += 1
    return total


def count_corpus_per_month(data_path: str):
    """
    Count number of tweets per months in a corpus and return them as a dict

    Args:
        data_path: path to dir of tweets

    Returns:
        Dict of month -> count
    """
    months = OrderedDict()
    for file_name in sorted(os.listdir(data_path)):
        key = file_to_month.get(file_name)
        months.update({key: sum(1 for l in open(os.path.join(data_path, file_name)))})
    return months


def count_corpus_per_day(
        data_path: str,
        start_date=(2017, 10, 1),
        end_date=(2019, 12, 31),
        filter_function=None):
    """
    Count number of tweets per day in a corpus and return them as a dict

    Args:
        data_path: path to dir of tweets
        start_date: date to start counting from
        end_date: date to end counting from
        filter_function: (Optional) function to filter certain tweet from the counting

    Returns:
        Dict of day -> count
    """
    days = build_days_dict(start_date, end_date)
    for tweet in read_corpus_generator(data_path):
        date = get_day_of_tweet(tweet)
        if date in days.keys():
            if (filter_function and filter_function(tweet)) or not filter_function:
                days.update({date: days.get(date, 0) + 1})
    return days


def count_time_of_tweets(data_path: str):
    """
    Count Matrix of hour of tweets x weekday

    Args:
        data_path: path to dir of tweets

    Returns:
        Dataframe pandas of hour of tweets x weekday
    """
    hours_per_weekday = {weekday: {hour: 0 for hour in range(0, 24)}
                         for weekday in range(1, 8)}
    for tweet in read_corpus_generator(data_path):
        weekday = get_weekday_of_tweet(tweet)
        hour = get_hour_of_tweet(tweet)
        hours_per_weekday[weekday].update({
            hour: hours_per_weekday.get(weekday).get(hour, 0) + 1})
    return pd.DataFrame(data=hours_per_weekday)


def count_languages(data_path: str):
    """
    Count number of languages present in corpus of tweets

    Args:
        data_path: path to dir of tweets

    Returns:
        Dict of languages count
    """
    languages = {}
    for entry in read_corpus_generator(data_path):
        language = get_language_of_tweet(entry)
        languages.update({language: languages.get(language, 0) + 1})
    return languages
            
            
def count_copycats(data_path: str):
    """
    Count all copycats (duplicate tweets with same textual content) in the corpus

    Args:
        data_path: path to dir of tweets

    Returns:
        Int number of copycats
    """
    texts = []
    for tweet in read_corpus_generator(data_path):
        texts.append(tweet.get('text'))
    return len(texts) - len(set(texts))


def count_all_hashtags(data_path: str):
    """
    Count all hashtags present in corpus and add them to a dict of frequency counts,
    but only if they appear more than once

    Args:
        data_path: path to dir of tweets

    Returns:
        Ordered dict of frequency counts for hashtags
    """
    hashtags_found = []
    for tweet in read_corpus_generator(data_path):
        entities = tweet.get("entities")
        if entities and isinstance(entities, dict):
            hashtags = entities.get("hashtags")
            if hashtags and isinstance(hashtags, list):
                for hashtag in hashtags:
                    hashtags_found.append(hashtag.get('tag').lower())
    counter = Counter(hashtags_found)
    return {key: value for key, value
            in sorted(counter.items(), key=lambda item: item[1]) if value > 9}


def count_hashtag_per_day(
        data_path: str,
        hashtag: str,
        start_date: Tuple = (2017, 10, 1),
        end_date: Tuple = (2019, 12, 31),
        filter_function: Callable = None):
    """
    Count one hashtag frequencies per day

    Args:
        data_path: path to dir of tweets
        hashtag: hashtag key to search in tweets
        start_date: (Optional, default to 01-10-2017)
            chosen start date in format (YYYY, mm, dd)
        end_date: (Optional, default to 31-10-2018)
            chosen end date in format (YYYY, mm, dd)
        filter_function: (Optional)

    Returns:
        Dict of days -> hashtag frequency that day
    """
    days = build_days_dict(start_date, end_date)
    for tweet in read_corpus_generator(data_path):
        if (filter_function and filter_function(tweet)) or not filter_function:
            if hashtag.lower() in tweet.get('text').lower():
                date = get_day_of_tweet(tweet)
                days.update({date: days.get(date, 0) + 1})
    return days


def count_hashtag_per_month(data_path: str, hashtag: str):
    """
    Count one hashtag frequencies per month

    Args:
        data_path: path to dir of tweets
        hashtag: hashtag key to search in tweets

    Returns:
        Dict of months -> hashtag frequency that month
    """
    months = OrderedDict()
    for file_name in sorted(os.listdir(data_path)):
        key = file_to_month.get(file_name)
        hashtag_count = 0
        for tweet in read_jsonl_generator(os.path.join(data_path, file_name)):
            if hashtag.lower() in tweet.get('text').lower():
                hashtag_count += 1
        months.update({key: hashtag_count})
    return months


def count_tweet_per_user(data_path: str):
    """
    Count number of tweets per user in corpus

    Args:
        data_path: path to dir of tweets

    Returns:
        Counter dict of author id -> tweet count
    """
    author_ids = []
    for tweet in read_corpus_generator(data_path):
        author_ids.append(tweet.get('author_id'))
    return Counter(author_ids)


def count_users_tweet_counts(tweet_per_user: Dict):
    """
    Count distribution of users with different tweet counts

    Args:
        tweet_per_user: Counter dict of author id -> tweet count

    Returns:
        Counter dict of number of users -> tweet count (e.g., 3 users have
        a tweet count of 100, 5 users have a tweet count of 36, etc.)
    """
    counts_list = []
    for value in tweet_per_user.values():
        counts_list.append(value)
    return Counter(counts_list)


def count_labels(data_path: str):
    """
    Count labels in corpus - if label not instantiated then a "not-testimony"
    label is applied, so this shouldn't be used on partially annotated data

    Args:
        data_path: path to dir of tweets

    Returns:
        Count dict of labels frequency in corpus
    """
    count_dict = {}
    for tweet in read_corpus_generator(data_path):
        label = tweet.get('labels', {}).get('category')
        label = label if label else "not_testimony"
        count_dict.update({label: count_dict.get(label, 0) + 1})
    return count_dict


def count_public_metrics(data_path: str, metric_name: str, filter_function: Callable = None):
    """
    Count the number associated with a public metric in all tweets

    Args:
        data_path: path to dir of tweets
        metric_name: metric name key to search for in tweets
        filter_function: (Optional) function to filter some tweets

    Returns:
        Count dict of Number of reactions -> count of tweet with that number
        (e.g. 50 retweets -> 56 tweets have 50 retweets, 89 retweets -> 5 tweets have
        89 retweets, etc.)
    """
    count_dict = {}
    for tweet in read_corpus_generator(data_path):
        if (filter_function and filter_function(tweet)) or not filter_function:
            metric = tweet.get('public_metrics').get(metric_name)
            count_dict.update({metric: count_dict.get(metric, 0) + 1})
    return count_dict


def count_gendered_corpus(data_path: str, filter_function: Callable = None):
    """
    Counts of number of tweets for each "gender" label

    Args:
        data_path: path to dir of tweets
        filter_function: (Optional) function to filter some tweets

    Returns:
        Dict of gender label -> count of that label
    """
    count_dict = {}
    for tweet in read_corpus_generator(data_path):
        if (filter_function and filter_function(tweet)) or not filter_function:
            if tweet.get('labels'):
                genders = tweet.get('labels').get('genders')
                for gender in genders:
                    count_dict.update({gender: count_dict.get(gender, 0) + 1})
    return count_dict


def count_quote_corpus(data_path: str, filter_function: Callable = None):
    """
    Counts of number of tweet per quote type

    Args:
        data_path: path to dir of tweets
        filter_function: (Optional) function to filter some tweets

    Returns:
        Dict of quote tye -> count of that quote type
    """
    count_dict = {}
    for tweet in read_corpus_generator(data_path):
        if (filter_function and filter_function(tweet)) or not filter_function:
            if tweet.get('labels'):
                quote_types = tweet.get('labels').get('quote_types')
                if quote_types:
                    for quote_type in quote_types:
                        count_dict.update({quote_type: count_dict.get(quote_type, 0) + 1})
                else:
                    count_dict.update({"no quote": count_dict.get("has_no_quote", 0) + 1})
    return count_dict


def count_annotations(data_path: str, label_keys: List[str] = None):
    """
    Count all annotations in corpus

    Args:
        data_path: path to dir of tweets
        label_keys: (Optional) list of label keys to look for, if present
            will discard labels not part of the list

    Returns:
        Dict of count label -> frequency count of label
    """
    count_dict = {}
    for tweet in read_corpus_generator(data_path):
        annotations = tweet.get("annotations")
        if label_keys:
            annotations = [annotation for annotation in annotations
                           if annotation.get('label') in label_keys]
        for annotation in annotations:
            label = annotation.get('label')
            count_dict.update({label: count_dict.get(label, 0) + 1})
    return count_dict


def count_annotation_texts(data_path: str, label_key: str):
    """


    Args:
        data_path:
        label_key:

    Returns:

    """
    count_dict = dict()
    for tweet in read_corpus_generator(data_path):
        annotations = [annot for annot in tweet.get("annotations")
                       if annot.get('label') == label_key]
        for annotation in annotations:
            text = annotation.get('text')
            count_dict.update({text: count_dict.get(text, 0) + 1})
    return count_dict


def count_tweets_per_query(data_path: str, query_json_path: str):
    """
    Count all tweets that match the queries provided in a json file

    Args:
        data_path: path to dir of tweets
        query_json_path: path to json file of queries

    Returns:
        Dict of query name -> count of tweets matching that query
    """
    with open(query_json_path, 'r') as query_json_file:
        queries_dict = json.loads(query_json_file.read())
    results = {}
    queries_keywords = {}
    for query_name, query in queries_dict.items():
        query_list = query.replace('(', '').replace(')', '').split(' ')
        keywords = [keyword for keyword in query_list if keyword.startswith('#')]
        negative_keywords = [keyword.replace('-', '') for keyword in query.split(' ') if keyword.startswith('-#')]
        queries_keywords.update({query_name: {'keywords': keywords, 'negative_keywords': negative_keywords}})
    for tweet in read_corpus_generator(data_path):
        tweet_text = tweet.get('text').lower().replace('＃', '#').replace('♯', '#')
        found = False
        for query_name, keywords_dict in queries_keywords.items():
            keywords = keywords_dict.get('keywords')
            negative_keywords = keywords_dict.get('negative_keywords')
            if any([keyword in tweet_text for keyword in keywords]) and not \
                    any([negative in tweet_text for negative in negative_keywords]):
                results.update({query_name: results.get(query_name, 0) + 1})
                found = True
        if not found:
            # discovered that some tweets use an alternate latin alphabet ?
            # print(tweet_text)
            pass
    return results


def count_publication_time(data_path: str, filter_function: Callable = None):
    """
    Count publication time of tweets in corpus

    Args:
        data_path: path to dir of tweets
        filter_function: (Optional) filter function to select tweets

    Returns:
        Count dict of hour -> number of tweets published at that hour
    """
    count_dict = {}
    for tweet in read_corpus_generator(data_path):
        if (filter_function and filter_function(tweet)) or not filter_function:
            hour = get_hour_of_tweet(tweet)
            count_dict.update({hour: count_dict.get(hour, 0) + 1})
    return count_dict


def count_analysis_variable(
        variable_dict_json_path: str,
        analysis_csv_path: str):
    """
    Count pre-defined analysis variables provided in json format

    Args:
        variable_dict_json_path: path to json file of variable
        analysis_csv_path: path to csv of analysis

    Returns:
        Variable dict object updated with frequency counts of variables
    """
    variables_dict = read_variable_dict(variable_dict_json_path)
    analysis_table = read_analysis_csv(analysis_csv_path)
    for variable_name, variable_data in variables_dict.items():
        variable_counts = {}
        for row in analysis_table:
            decoded_entry_name = variable_data.get('labels').get(row.get(variable_name))
            if not decoded_entry_name:
                missing_values_process = variable_data.get("missing_values")
                decoded_entry_name = missing_values_process if missing_values_process != "remove" else None
            if decoded_entry_name:
                variable_counts.update(
                    {decoded_entry_name: variable_counts.get(decoded_entry_name, 0) + 1})
        variable_data.update({"count_dict": variable_counts})
    return variables_dict


def count_word_frequency(data_path: str, keywords: List[str] = None):
    """
    Count word frequencies in corpus of tweets, from an optional list of
    keywords if present, else from all words in texts

    Args:
        data_path: path to dir of tweets
        keywords: (Optional) list of keywords to count frequency

    Returns:
        Sorted count dict of word -> frequency
    """
    count_dict = {}
    for tweet in read_corpus_generator(data_path):
        if not keywords:
            for word in tweet.get("text").split(" "):
                count_dict.update({word: count_dict.get(word, 0) + 1})
        else:
            for keyword in keywords:
                if keyword in tweet.get("text"):
                    count_dict.update({keyword: count_dict.get(keyword, 0) + 1})
    return dict(sorted(count_dict.items(), key=lambda item: item[1]))


def count_type_frequency(data_path: str):
    """
    Count frequency of different types of tweets [retweet, tweet, reply, quote]
    Tweets can potentially be part of more than one category

    Args:
        data_path: path to dir of tweets

    Returns:
        Frequency count dict of type -> count
    """
    count_dict = {}
    for tweet in read_corpus_generator(data_path):
        if tweet.get('referenced_tweets'):
            for reference in tweet.get('referenced_tweets'):
                if reference.get('type') == 'quoted':
                    tweet_type = 'quote'
                elif reference.get('type') == "replied_to":
                    tweet_type = 'reply'
                elif reference.get('type') == 'retweeted':
                    tweet_type = 'retweet'
                else:
                    tweet_type = reference.get('type')
                count_dict.update({tweet_type: count_dict.get(tweet_type, 0) + 1})
        else:
            count_dict.update({'tweet': count_dict.get('tweet', 0) + 1})
    return count_dict


if __name__ == "__main__":
    print(count_corpus(
        data_path='',
    ))
