import json
import os
import re
import sys

sys.path.append('/home/juliette/projects/search-tweets-python')
from searchtweets import gen_request_parameters, load_credentials, ResultStream

from utils.file_utils import read_jsonl_generator
import utils.paths as paths


def create_request(query_string, start_date, end_date):
    # add param granularity="day" to get tweet count by day (other options : hour, minute)
    query = gen_request_parameters(
        query_string,
        start_time=start_date,
        end_time=end_date,
        tweet_fields="attachments,"
                     "author_id,"
                     "conversation_id,"
                     "created_at,"
                     "entities,"
                     "geo,"
                     "id,"
                     "in_reply_to_user_id,"
                     "lang,"
                     "public_metrics,"
                     "possibly_sensitive,"
                     "referenced_tweets,"
                     "reply_settings,"
                     "source,"
                     "text,"
                     "withheld",
        user_fields="created_at,"
                    "description,"
                    "entities,"
                    "id,"
                    "location,"
                    "name,"
                    "pinned_tweet_id,"
                    "profile_image_url,"
                    "protected,"
                    "public_metrics,"
                    "url,"
                    "username,"
                    "verified,"
                    "withheld",
        place_fields="contained_within,"
                     "country,"
                     "country_code,"
                     "full_name,"
                     "geo,"
                     "id,"
                     "name,"
                     "place_type",
        results_per_call=100
    )
    return query


def create_stream(query, search_args):
    rs = ResultStream(request_parameters=query,
                      max_requests=10000000,
                      max_tweets=10000000,
                      **search_args)
    return rs


def make_year_from_start_end(start, end):
    year = []
    start_date_match = re.match(
        r"(?P<start_year>\d{4})\D(?P<start_month>\d{2})", start)
    end_date_match = re.match(
        r"(?P<end_year>\d{4})\D(?P<end_month>\d{2})", end)
    start_year = int(start_date_match.group("start_year"))
    start_month = int(start_date_match.group("start_month"))
    end_year = int(end_date_match.group("end_year"))
    end_month = int(end_date_match.group("end_month"))
    while start_year <= end_year:
        while start_month < 12 and not (start_year == end_year and start_month == end_month):
            year.append((
                f"{start_year}-{str(start_month).zfill(2)}-01",
                f"{start_year}-{str(start_month + 1).zfill(2)}-01"))
            start_month += 1
        if start_year == end_year and start_month == end_month:
            break
        year.append((
            f"{start_year}-{str(start_month).zfill(2)}-01",
            f"{start_year + 1}-01-01"))
        start_year += 1
        start_month = 1
    return year


def write_year_of_tweets(output_path, query_dict):
    query_string = query_dict.get("query")
    year = make_year_from_start_end(
        start=query_dict.get('start'),
        end=query_dict.get('end'))
    search_args = load_credentials("~/.twitter_keys.yaml",
                                   yaml_key="search_tweets_api",
                                   env_overwrite=False)
    for month_tuple in year:
        print(f"Retrieving tweets from {month_tuple[0]} to {month_tuple[1]}")
        query = create_request(
            query_string,
            start_date=month_tuple[0],
            end_date=month_tuple[1]
        )
        stream_search = create_stream(query, search_args)
        file_path = os.path.join(output_path, f"{month_tuple[0]}---{month_tuple[1]}.jsonl")
        if os.path.exists(file_path):
            update_existing_file(file_path, stream_search)
        else:
            write_new_file(file_path, stream_search)


def update_existing_file(file_path: str, stream_search):
    done_ids = []
    for tweet in read_jsonl_generator(file_path):
        done_ids.append(tweet.get('id'))
    page_number = 0
    new_tweets_counter = 0
    with open(file_path, 'a') as jsonl_file:
        for page in stream_search.stream():
            items_number = page.get('meta').get('result_count')
            page_number += 1
            print(f"page n°{page_number} with {items_number} elements retrieved")
            try:
                newest_date = page.get('data')[0].get('created_at')
                oldest_date = page.get('data')[-1].get('created_at')
                print(f"from date {oldest_date} to date {newest_date}")
            except (IndexError, KeyError):
                print("created_at data missing")
            for tweet in page.get('data'):
                if tweet.get('id') not in done_ids:
                    jsonl_file.write(json.dumps(tweet) + "\n")
                    new_tweets_counter += 1
    print(f"{new_tweets_counter} tweets were added to existing file")


def write_new_file(file_path: str, stream_search):
    page_number = 0
    with open(file_path, 'w') as jsonl_file:
        for page in stream_search.stream():
            items_number = page.get('meta').get('result_count')
            page_number += 1
            print(f"page n°{page_number} with {items_number} elements retrieved")
            try:
                newest_date = page.get('data')[0].get('created_at')
                oldest_date = page.get('data')[-1].get('created_at')
                print(f"from date {oldest_date} to date {newest_date}")
            except (IndexError, KeyError):
                print("created_at data missing")
            for tweet in page.get('data'):
                jsonl_file.write(json.dumps(tweet) + "\n")


if __name__ == '__main__':

    with open(paths.QUERY_FILE_PATH, 'r') as query_file:
        query_json = json.loads(query_file.read())
    query_dict = query_json.get('query-japan-20-11-22')
    write_year_of_tweets(query_dict=query_dict, output_path=paths.JAPAN_LARGE_QUERY_RAW_DIR)
