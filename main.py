"""
useful api examples :
https://github.com/twitterdev/search-tweets-python/blob/master/examples/api_example.ipynb

article on looking into full archive :
https://developer.twitter.com/en/docs/tutorials/getting-historical-tweets-using-the-full-archive-search-endpoint

operators :
https://developer.twitter.com/en/docs/twitter-api/enterprise/powertrack-api/guides/operators
bio_location:
Matches tweets where the User object's location contains the specified keyword or phrase.
This operator performs a tokenized match, similar to the normal keyword rules on the message body.
lang:
Japanese: ja
place:
place_country:
profile_country:
profile_region:
profile_locality:
profile_subregion:
has:geo
has:profile_geo
is:reply
"""
from searchtweets import gen_request_parameters, load_credentials, collect_results, ResultStream


def create_request():
    query = gen_request_parameters(
        "lang:ja #metoo OR #wetoojapan OR #stopfeminicides OR #私は黙らない NOT is:retweet", results_per_call=100)
    return query


def query_tweets(query):
    rs = ResultStream(request_parameters=query,
                      max_results=500,
                      max_pages=1,
                      **search_args)
    return list(rs.stream())


if __name__ == '__main__':
    search_args = load_credentials("~/projects/meTooExtraction/.twitter_keys.yaml",
                                   yaml_key="search_tweets_v2",
                                   env_overwrite=False)
    query = create_request()
    print(query)
    tweets = query_tweets(query)
    [print(tweet['text'] + "\n\n" + ("-" * 80) + "\n\n", end='\n\n') for tweet in tweets[0]['data']]

