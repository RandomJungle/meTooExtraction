from typing import Optional, List

import pandas as pd
from dotenv import find_dotenv, load_dotenv
from mistralai import Mistral

import os

from tqdm import tqdm

from utils.df_transform import chunk_dataframe
from utils.file_utils import read_json_dataframe


def query_embeddings(
        dataframe: pd.DataFrame,
        model_name: Optional[str] = 'mistral-embed',
        num_chunks: Optional[int] = 1) -> pd.DataFrame:

    data = dataframe[['id', 'text']]
    if num_chunks <= 1:
        chunks = [data]
    else:
        chunks = chunk_dataframe(data, num_chunks)
    client = Mistral(api_key=os.getenv('MISTRAL_API_KEY', ''))
    outputs = []

    for chunk in tqdm(chunks):
        chunk_response = client.embeddings.create(
            model=model_name,
            inputs=chunk['text'].tolist()
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
        model_name: Optional[str] = 'mistral-embed',
        temperature: Optional[float] = 0.2,
        num_chunks: Optional[int] = 1) -> List[str]:

    data = dataframe[['id', 'text']]
    if num_chunks <= 1:
        chunks = [data]
    else:
        chunks = chunk_dataframe(data, num_chunks)
    responses = []

    client = Mistral(api_key=os.getenv('MISTRAL_API_KEY', ''))

    for chunk in tqdm(chunks):
        chunk_csv = chunk.to_csv(index=False)
        chunk_response = client.chat.complete(
            model=model_name,
            messages=[
                {
                    'content': (
                        'In the following table in csv format, specified by the tags <t></t> '
                        'there is a column "text" that contains tweets in japanese, '
                        'please annotate with 2 new column returned as json, '
                        'one with the english translation of the text, '
                        'and one with tags that represent the sentiment expressed by the text'
                        'from the following set of labels : '
                        '{frustration, anger, sadness, shock, respect, shame, joy, humour}'
                        f'\n\ntable : <t>{chunk_csv}</t>'
                    ),
                    'role': 'user',
                },
            ],
            stream=False,
            temperature=temperature,
            response_format={
                'type': 'json_object',
            }
        )
        content = chunk_response.choices[0].message.content
        responses.append(content)

    return responses


if __name__ == '__main__':

    load_dotenv(find_dotenv())

    df = read_json_dataframe(
        file_path=os.environ.get('USERS_DATA_PATH'),
        remove_duplicates=True
    )
    output_responses = query_embeddings(
        dataframe=df,
        num_chunks=45,
    )
