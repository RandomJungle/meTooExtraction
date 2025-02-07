import os
from typing import Optional

import deepl
import pandas as pd
from dotenv import load_dotenv, find_dotenv

from utils.file_utils import read_json_dataframe


def query_translation(
        dataframe: pd.DataFrame,
        text_column: Optional[str] = 'text',
        translation_column: Optional[str] = 'text_en') -> pd.DataFrame:

    translator = deepl.Translator(os.getenv('DEEPL_API_KEY'))

    dataframe[translation_column] = dataframe[text_column].apply(
        lambda x: translator.translate_text(
            text=x,
            target_lang='EN-GB'
        ).text
    )

    return dataframe


if __name__ == '__main__':

    load_dotenv(find_dotenv())

    df = read_json_dataframe(
        file_path=os.environ.get('LATEST_DATASET_PATH'),
        remove_duplicates=True
    )

    output_df = query_translation(df)
    output_df.to_json(
        os.path.join(
            os.getenv('OUTPUT_DATASETS_DIR'),
            f'tweets_2017_2019_translate_deepl.json'
        ),
        orient='table'
    )