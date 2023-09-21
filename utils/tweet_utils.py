from datetime import datetime, timezone, timedelta, tzinfo
from typing import Dict, List

import pytz


def get_day_of_tweet(tweet: Dict):
    return tweet.get('created_at')[0:10]


def get_hour_of_tweet(tweet: Dict):
    date_string = tweet.get("created_at")
    date = datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S.%fZ")
    hour = date.astimezone(pytz.timezone("Asia/Tokyo")).time().hour
    return hour


def get_weekday_of_tweet(tweet: Dict):
    date_string = tweet.get("created_at")
    date = datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S.%fZ")
    return date.isoweekday()


def get_language_of_tweet(tweet: Dict):
    return tweet.get('lang')


def get_timestamp_of_tweet(tweet: Dict):
    date_string = tweet.get("created_at")
    return convert_date_to_timestamp(date_string)


def convert_date_to_timestamp(date_string: str):
    date = datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S.%fZ")
    date = date.replace(tzinfo=timezone.utc)
    return date.timestamp()


def convert_timestamp_to_date(timestamp: float):
    return datetime.fromtimestamp(timestamp).strftime("%d-%m-%Y-%H:%M:%S")


def convert_timestamp_to_day(timestamp: float):
    return datetime.fromtimestamp(timestamp).strftime("%d-%m-%Y")


def is_conspiracy_tweet(tweet: Dict, conspiracy_keywords: List) -> bool:
    tweet_text = tweet.get('text')
    if any([keyword in tweet_text for keyword in conspiracy_keywords]):
        return True
    return False


def is_testimony(tweet: Dict) -> bool:
    return tweet.get('labels').get('category') == 'testimony'


def is_filtered_tweet(tweet: Dict, ids_list: List):
    if tweet.get('id') not in ids_list:
        return False
    else:
        return True


def is_retweet(tweet: Dict):
    if isinstance(tweet.get('referenced_tweets', 0), list):
        return any(['retweeted' in references.values()
                    for references in tweet.get('referenced_tweets', [])])
    return False


def is_not_retweet(tweet: Dict):
    if is_retweet(tweet):
        return False
    return True
