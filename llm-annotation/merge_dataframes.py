import json

import pandas as pd


if __name__ == '__main__':

    with open('/home/juliette/projects/meTooExtraction/data_translated_2.jsonl', 'r') as input_file:
        for index, line in enumerate(input_file.readlines()):
            try:
                json.loads(line)
            except json.decoder.JSONDecodeError:
                print(index)

    translated = pd.read_json(
        '/home/juliette/projects/meTooExtraction/data_translated_2.jsonl',
        lines=True
    )
    translated = translated[['id', 'english', 'french']]
    data = pd.read_csv('/home/juliette/Downloads/data_temp - data_temp.csv')
    new_data = data.merge(
        translated,
        on='id',
        how='left',
        validate='1:1'
    )
    new_data.to_csv(
        '/home/juliette/projects/meTooExtraction/translated.csv',
        index=False
    )