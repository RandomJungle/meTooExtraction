import json
import re
from typing import Dict, List

from research import nlp
from utils import paths
from utils.file_utils import read_corpus_generator, read_corpus_list
from utils.tweet_utils import is_filtered_tweet


def annotate_corpus_pipeline(
        data_path: str,
        output_path: str,
        gender_json_path: str,
        filter_id_path: str,
        lexical_fields_path: str):
    with open(gender_json_path, 'r') as gender_json_file:
        gender_dict = json.loads(gender_json_file.read()).get('gender')
    with open(filter_id_path, 'r') as filter_id_file:
        ids_to_filter = json.loads(filter_id_file.read()).get('tweet_id_to_filter')
    tweets = read_corpus_list(data_path)
    for tweet in read_corpus_generator(data_path):
        if is_filtered_tweet(tweet, ids_to_filter):
            tweet.update({'labels': {'category': 'not_testimony'}})
            tweet.pop('label')
    gendered_tweets = add_gender_metadata(tweets, gender_dict)
    quote_tweets = add_quote_metadata(gendered_tweets)
    annotated_tweets = nlp.annotate_lexical_field(quote_tweets, lexical_fields_path)
    with open(output_path, 'w') as output_file:
        for tweet in annotated_tweets:
            output_file.write(json.dumps(tweet) + "\n")


def add_gender_metadata(tweets: List[Dict], gender_json_dict: Dict):
    annotated_tweets = []
    for tweet in tweets:
        tweet_text = tweet.get('text')
        gender_labels = []
        for key, value in gender_json_dict.items():
            if any([keyword in tweet_text for keyword in value]):
                gender_labels.append(key)
        if not gender_labels:
            gender_labels = ['neutral']
        if tweet.get('label'):
            tweet.update({'labels': {'genders': gender_labels, 'category': tweet.get('label')}})
            tweet.pop('label')
        else:
            tweet.update({'labels': {
                'genders': gender_labels,
                'category': tweet.get('labels').get('category', 'testimony')}})
        annotated_tweets.append(tweet)
    return annotated_tweets


def add_quote_metadata(tweets: List[Dict]):
    annotated_tweets = []
    for tweet in tweets:
        tweet_text = tweet.get('text')
        quote_match = re.search("(「[^」]*」|『[^』]*』)", tweet_text)
        if quote_match:
            quote_types = []
            if re.match("「[^」]*」", tweet_text):
                quote_types.append("「」")
            elif re.match("『[^』]*』", tweet_text):
                quote_types.append("『』")
            tweet.get('labels').update({
                'has_quote': 'true',
                'quote_texts': list(quote_match.groups()),
                'quote_types': quote_types})
        else:
            tweet.get('labels').update({'has_quote': 'false'})
        annotated_tweets.append(tweet)
    return annotated_tweets


if __name__ == "__main__":
    annotate_corpus_pipeline(
        data_path=paths.NO_TESTIMONY_CORPUS_DIR,
        output_path=paths.FINAL_CORPUS_JSONL,
        gender_json_path=paths.KEYWORDS_JSON_PATH,
        filter_id_path=paths.TWEET_IDS_TO_FILTER_JSON,
        lexical_fields_path=paths.LEXICAL_FIELDS_JSON
    )

