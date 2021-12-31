from datetime import datetime, timezone
from typing import Dict


def get_day_of_tweet(tweet: Dict):
    return tweet.get('created_at')[0:10]


def get_hour_of_tweet(tweet: Dict):
    return tweet.get('created_at')[10]


def get_language_of_tweet(tweet: Dict):
    return tweet.get('lang')


def get_timestamp_of_tweet(tweet: Dict):
    date_string = tweet.get("created_at")
    date = datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S.%fZ")
    date = date.replace(tzinfo=timezone.utc)
    return date.timestamp()


def convert_timestamp_to_date(timestamp: float):
    return datetime.fromtimestamp(timestamp).strftime("%d-%m-%Y-%H:%M:%S")


def convert_timestamp_to_day(timestamp: float):
    return datetime.fromtimestamp(timestamp).strftime("%d-%m-%Y")
