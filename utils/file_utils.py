import datetime
import json
import os


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

