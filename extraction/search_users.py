import requests
import json

from searchtweets import load_credentials
from typing import List

from utils import paths
from utils.file_utils import read_corpus_generator, read_jsonl_generator, convert_jsonl_user_info_to_csv


def chunks_generator(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def retrieve_user_ids(data_path: str):
    user_ids_list = []
    for tweet in read_corpus_generator(data_path):
        user_ids_list.append(tweet.get("author_id"))
    return list(set(user_ids_list))


def import_credentials():
    credentials = load_credentials("~/.twitter_keys.yaml",
                                   yaml_key="search_tweets_api",
                                   env_overwrite=False)
    return credentials


def create_url(user_ids: List):
    user_fields = [
        "id",
        "created_at",
        "name",
        "username",
        "protected",
        "verified",
        "withheld",
        "profile_image_url",
        "location",
        "url",
        "description",
        "entities",
        "pinned_tweet_id",
        "public_metrics"
    ]
    user_ids = "ids=" + ",".join(user_ids)
    user_fields = "user.fields=" + ",".join(user_fields)
    url = f"https://api.twitter.com/2/users?{user_ids}&{user_fields}"
    return url


def bearer_oauth(r):
    bearer_token = import_credentials().get('bearer_token')
    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2UserLookupPython"
    return r


def connect_to_endpoint(url):
    response = requests.request("GET", url, auth=bearer_oauth,)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Request returned an error: {} {}".format(
                response.status_code, response.text
            )
        )
    return response.json()


def query_users_info(data_path: str, output_jsonl_path: str, output_csv_path: str):
    jsons = []
    all_user_ids = retrieve_user_ids(data_path)
    for chunk in chunks_generator(all_user_ids, 100):
        url = create_url(chunk)
        json_response = connect_to_endpoint(url)
        jsons.extend(json_response.get('data'))
    with open(output_jsonl_path, 'a+') as output_file:
        for json_user_info in jsons:
            output_file.write(json.dumps(json_user_info, sort_keys=True) + "\n")
    convert_jsonl_user_info_to_csv(output_jsonl_path, output_csv_path)


if __name__ == "__main__":

    query_users_info(
        data_path=paths.FINAL_CORPUS_DIR,
        output_jsonl_path=paths.USER_INFO_JSONL,
        output_csv_path=paths.USER_INFO_CSV
    )
