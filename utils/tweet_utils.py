from typing import Dict


def get_day_of_tweet(tweet: Dict):
    return tweet.get('created_at')[0:10]


def get_hour_of_tweet(tweet: Dict):
    return tweet.get('created_at')[10]


def get_language_of_tweet(tweet: Dict):
    return tweet.get('lang')

