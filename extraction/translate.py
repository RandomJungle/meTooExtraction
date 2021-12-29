import json
import os
import random

import requests
import translators as ts

from googletrans import Translator
from tqdm import tqdm

import utils.paths as paths


def translate_file(
        input_file_path,
        output_file_path,
        source: str,
        destination: str):
    with open(input_file_path, 'r') as input_file, open(output_file_path, 'w') as output_file:
        for line in tqdm(input_file.readlines()):
            tweet = json.loads(line)
            text = tweet.get('text')
            try:
                translation = ts.baidu(text, from_language="jp", to_language="fra")
                tweet.update({f'{destination}_text': translation})
                print(translation)
            except (IndexError, TypeError, requests.exceptions.HTTPError):
                tweet_id = tweet.get('id')
                print(f"FAILED TRANSLATION on tweet n° {tweet_id}")
            output_file.write(json.dumps(tweet) + "\n")
            
            
def update_translated_file(
        input_file_path,
        output_file_path,
        source: str,
        destination: str):
    with open(output_file_path, 'r') as output_file:
        ids_translated = []
        for line in output_file.readlines():
            tweet = json.loads(line)
            if tweet.get(f'{destination}_text'):
                ids_translated.append(tweet.get('id'))
    with open(input_file_path, 'r') as input_file, open(output_file_path, 'a') as output_file:
        for line in tqdm(input_file.readlines()):
            tweet = json.loads(line)
            tweet_id = tweet.get('id')
            if tweet_id not in ids_translated:
                text = tweet.get('text')
                try:
                    translation = ts.baidu(text, from_language="jp", to_language="fra")
                    tweet.update({f'{destination}_text': translation})
                    print(translation)
                except (IndexError, TypeError, requests.exceptions.HTTPError):
                    print(f"FAILED TRANSLATION on tweet n° {tweet_id}")
                output_file.write(json.dumps(tweet) + "\n")

            
def translate_corpus(input_dir, output_dir, source: str = "ja", destination: str = "en"):
    already_translated = os.listdir(output_dir)
    for file_name in os.listdir(input_dir):
        if file_name in already_translated:
            print(f"rebooting translation on {file_name}")
            update_translated_file(
                os.path.join(input_dir, file_name),
                os.path.join(output_dir, file_name),
                source,
                destination
            )
            print(f"finished translating {file_name}")
        if file_name.endswith('.jsonl') and file_name not in already_translated:
            print(f"starting translation on {file_name}")
            translate_file(
                os.path.join(input_dir, file_name),
                os.path.join(output_dir, file_name),
                source,
                destination
            )
            print(f"finished translating {file_name}")
            
            
def control_language(output_dir, destination: str):
    translator = Translator()
    for file_name in tqdm(os.listdir(output_dir)):
        with open(os.path.join(output_dir, file_name), 'r') as translated_file:
            tweet = json.loads(random.choice(translated_file.readlines()))
            tweet_id = tweet.get('id')
            translation = tweet.get(f'{destination}_text')
            language = translator.detect(translation)
            if not translation:
                print(f"no translation found in {file_name}, on tweet n° {tweet_id}")
                break
            elif language.lang != destination:
                print(f"language of translation is not {destination} "
                      f"in {file_name}, on tweet n° {tweet_id}")


def manual_translate(input_dir, output_dir, destination: str):
    for file_name in tqdm(os.listdir(input_dir)):
        with open(os.path.join(input_dir, file_name), 'r') as input_file, \
                open(os.path.join(output_dir, file_name), 'w') as output_file:
            for line in input_file.readlines():
                tweet = json.loads(line)
                tweet_text = tweet.get('text')
                translation = tweet.get(f'{destination}_text')
                if not translation or translation == "":
                    print(f"no {destination} translation for :\n\n{tweet_text}\n\n"
                          f"Enter/Paste your content. Ctrl-D or Ctrl-Z ( windows ) to save it.\n-->")
                    contents = []
                    while True:
                        try:
                            line = input()
                        except EOFError:
                            break
                        if line in ['exit', 'q', 'quit', 'stop']:
                            break
                        contents.append(line)
                    translation = '\n'.join(contents)
                tweet.update({f'{destination}_text': translation})
                output_file.write(json.dumps(tweet) + '\n')


if __name__ == "__main__":
    translate_corpus(
        paths.FILTERED_DATA_PATH,
        paths.TEMP_DATA_PATH,
        source="ja",
        destination="fr"
    )
    control_language(
        paths.TEMP_DATA_PATH,
        "fr"
    )
