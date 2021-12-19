import json
import os

from searchtweets import gen_request_parameters, load_credentials, ResultStream


def create_request(start_date, end_date):
    # add param granularity="day" to get tweet count by day (other options : hour, minute)
    query = gen_request_parameters(
        "(lang:ja OR place_country:JP) "
        "-is:retweet -is:nullcast "
        "(#metoo OR #wetoojapan OR #stopfeminicides OR #私は黙らない)",
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


def write_year_of_tweets(output_path):
    year = [
        ('2017-10-01', '2017-11-01'),
        ('2017-11-01', '2017-12-01'),
        ('2017-12-01', '2018-01-01'),
        ('2018-01-01', '2018-02-01'),
        ('2018-02-01', '2018-03-01'),
        ('2018-03-01', '2018-04-01'),
        ('2018-04-01', '2018-05-01'),
        ('2018-05-01', '2018-06-01'),
        ('2018-06-01', '2018-07-01'),
        ('2018-07-01', '2018-08-01'),
        ('2018-08-01', '2018-09-01'),
        ('2018-09-01', '2018-10-01'),
        ('2018-10-01', '2018-11-01'),
    ]
    search_args = load_credentials("~/.twitter_keys.yaml",
                                   yaml_key="search_tweets_api",
                                   env_overwrite=False)
    for month_tuple in year:
        print(f"Retrieving tweets from {month_tuple[0]} to {month_tuple[1]}")
        query = create_request(
            start_date=month_tuple[0],
            end_date=month_tuple[1]
        )
        stream_search = create_stream(query, search_args)
        file_path = os.path.join(output_path, f"{month_tuple[0]}---{month_tuple[1]}.jsonl")
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

    write_year_of_tweets("/data")
