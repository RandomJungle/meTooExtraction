import os
from typing import Optional

import numpy as np
import pandas as pd
from dotenv import load_dotenv, find_dotenv
from openai import OpenAI
from tqdm import tqdm

from utils.df_transform import chunk_dataframe
from utils.file_utils import read_json_dataframe


def query_embeddings(
        dataframe: pd.DataFrame,
        model_name: Optional[str] = 'text-embedding-3-large',
        num_chunks: Optional[int] = 1):

    data = dataframe[['id', 'text']]
    chunks = chunk_dataframe(data, num_chunks)
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY", ""))
    outputs = []

    for chunk in tqdm(chunks):
        chunk_response = client.embeddings.create(
            model=model_name,
            input=chunk['text'].tolist()
        )
        chunk[f'{model_name}_embeddings'] = [emb.embedding for emb in chunk_response.data]
        outputs.append(chunk)

    output_dataframe = pd.concat(outputs, ignore_index=True)
    merged = pd.merge(
        left=dataframe,
        right=output_dataframe[['id', f'{model_name}_embeddings']],
        on='id',
        how='left',
        validate='1:1'
    )
    return merged


def query_chat(
        dataframe: pd.DataFrame,
        temperature: float,
        num_chunks: int):

    client = OpenAI(api_key=os.environ.get('OPENAI_API_KEY'))
    data = dataframe[['id', 'text']]
    chunks = np.array_split(data, num_chunks)
    responses = []

    for chunk in chunks:
        completion = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "developer",
                    "content": "You are an annotator who needs to annotate texts in japanese"
                },
                {
                    "role": "user",
                    "content": (
                        "In the following table in array format, specified by the tags <t></t> "
                        "there is a column 'text' that contains tweets, "
                        "can you add a column to this table and return it in json format, "
                        "the content of the new column should be an annotation on the text "
                        "with 'true' if the text is a testimony and 'false' otherwise"
                        f"\n\ntable : <t>{chunk}</t>"
                    ),
                }
            ],
            temperature=temperature
        )
        responses.append(completion.choices[0].message.content)

    return responses


if __name__ == '__main__':

    load_dotenv(find_dotenv())

    df = read_json_dataframe(
        file_path=os.environ.get('USERS_DATA_PATH'),
        remove_duplicates=False
    )
    output_responses = query_embeddings(
        dataframe=df,
        num_chunks=45,
    )

