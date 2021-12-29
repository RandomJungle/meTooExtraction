import datetime
import json
import os

import utils.paths as paths


def read_corpus_generator(data_path: str):
    for file_name in os.listdir(data_path):
        with open(os.path.join(data_path, file_name), 'r') as data_file:
            for entry in data_file.readlines():
                yield json.loads(entry)


def read_file_generator(file_path: str):
    with open(file_path, 'r') as data_file:
        for entry in data_file.readlines():
            yield json.loads(entry)


def build_days_dict():
    dates = {}
    start_date = datetime.date(2017, 10, 1)
    end_date = datetime.date(2018, 10, 31)
    delta = datetime.timedelta(days=1)
    while start_date <= end_date:
        dates.update({f"{start_date}": 0})
        start_date += delta
    return dates


def divide_corpus(data_path: str, output_path, n_folds: int):
    tweets = [tweet for tweet in read_corpus_generator(data_path)]
    size = int(len(tweets) / n_folds) + 1
    chunks = [tweets[x:x + size] for x in range(0, len(tweets), size)]
    for index, chunk in enumerate(chunks):
        with open(os.path.join(output_path, f"sample_{index + 1}_octobre.jsonl"), "w") as output_file:
            for tweet in chunk:
                output_file.write(json.dumps(tweet) + "\n")


if __name__ == "__main__":
    divide_corpus(paths.TRANSLATED_FR_DATA_PATH, paths.ANNOTATION_CHUNKS, 4)