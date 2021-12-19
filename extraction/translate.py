import json
import os
import random

from tqdm import tqdm

from googletrans import Translator

from utils.paths import TRANSLATED_DATA_PATH, CLEAN_DATA_PATH


def translate_file(
        input_file_path,
        output_file_path,
        source: str = "ja",
        destination: str = "en"):
    translator = Translator()
    with open(input_file_path, 'r') as input_file, open(output_file_path, 'w') as output_file:
        for line in tqdm(input_file.readlines()):
            tweet = json.loads(line)
            text = tweet.get('text')
            try:
                translation = translator.translate(text, src=source, dest=destination)
                tweet.update({'english_text': translation.text})
            except IndexError:
                tweet_id = tweet.get('id')
                print(f"FAILED TRANSLATION on tweet n° {tweet_id}")
            output_file.write(json.dumps(tweet) + "\n")
            
            
def update_translated_file(
        input_file_path,
        output_file_path,
        source: str = "ja",
        destination: str = "en"):
    translator = Translator()
    with open(output_file_path, 'r') as output_file:
        ids_translated = []
        for line in output_file.readlines():
            tweet = json.loads(line)
            if tweet.get('english_text'):
                ids_translated.append(tweet.get('id'))
    with open(input_file_path, 'r') as input_file, open(output_file_path, 'a') as output_file:
        for line in tqdm(input_file.readlines()):
            tweet = json.loads(line)
            tweet_id = tweet.get('id')
            if tweet_id not in ids_translated:
                text = tweet.get('text')
                try:
                    translation = translator.translate(text, src=source, dest=destination)
                    tweet.update({'english_text': translation.text})
                except IndexError:
                    print(f"FAILED TRANSLATION on tweet n° {tweet_id}")
                output_file.write(json.dumps(tweet) + "\n")

            
def translate_corpus(input_dir, output_dir):
    already_translated = os.listdir(output_dir)
    for file_name in os.listdir(input_dir):
        if file_name in already_translated:
            print(f"rebooting translation on {file_name}")
            update_translated_file(
                os.path.join(input_dir, file_name),
                os.path.join(output_dir, file_name))
            print(f"finished translating {file_name}")
        if file_name.endswith('.jsonl') and file_name not in already_translated:
            print(f"starting translation on {file_name}")
            translate_file(
                os.path.join(input_dir, file_name),
                os.path.join(output_dir, file_name))
            print(f"finished translating {file_name}")
            
            
def control_language(output_dir):
    translator = Translator()
    for file_name in tqdm(os.listdir(output_dir)):
        with open(os.path.join(output_dir, file_name), 'r') as translated_file:
            tweet = json.loads(random.choice(translated_file.readlines()))
            tweet_id = tweet.get('id')
            translation = tweet.get('english_text')
            language = translator.detect(translation)
            if not translation:
                print(f"no translation found in {file_name}, on tweet n° {tweet_id}")
                break
            elif language.lang != "en":
                print(f"language of translation is not english in {file_name}, on tweet n° {tweet_id}")
                

if __name__ == "__main__":

    translate_corpus(
        CLEAN_DATA_PATH,
        TRANSLATED_DATA_PATH
    )
    control_language(
        TRANSLATED_DATA_PATH
    )
