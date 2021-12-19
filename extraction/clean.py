import json
import os

from tqdm import tqdm

from utils.paths import TRANSLATED_DATA_PATH, CLEAN_DATA_PATH, RAW_DATA_PATH


def remove_doubles(input_dir, output_dir, language_filter=None):
    for file_name in os.listdir(input_dir):
        if file_name.endswith('.jsonl'):
            file_ids = []
            with open(os.path.join(input_dir, file_name), 'r') as input_file, \
                    open(os.path.join(output_dir, file_name), 'w') as output_file:
                for line in tqdm(input_file.readlines()):
                    tweet = json.loads(line)
                    tweet_id = tweet.get('id')
                    tweet_lang = tweet.get('lang')
                    if (tweet_id not in file_ids) and \
                            (not language_filter or tweet_lang == language_filter):
                        output_file.write(line)
                        file_ids.append(tweet_id)
                        
                        
if __name__ == "__main__":
    
    remove_doubles(
        RAW_DATA_PATH,
        CLEAN_DATA_PATH,
        language_filter="ja"
    )
