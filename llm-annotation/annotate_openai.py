import os
from typing import Optional, Dict

import pandas as pd
from dotenv import load_dotenv, find_dotenv
from openai import OpenAI
from tqdm import tqdm

from llm_utils import convert_content_to_json
from utils.df_transform import chunk_dataframe
from utils.file_utils import read_json_dataframe, read_prompt_file


def query_embeddings(
        dataframe: pd.DataFrame,
        model_name: Optional[str] = 'text-embedding-3-large',
        num_chunks: Optional[int] = 1):

    data = dataframe[['id', 'text']]
    chunks = chunk_dataframe(data, num_chunks)
    client = OpenAI(api_key=os.getenv('OPENAI_API_KEY', ''))
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
        prompt: Dict,
        model_name: Optional[str] = 'gpt-4o',
        temperature: Optional[float] = 0.2,
        num_chunks: Optional[int] = 1,
        stream: Optional[bool] = False) -> pd.DataFrame:

    data = dataframe[['id', 'text']]
    chunks = chunk_dataframe(data, num_chunks)
    responses = []

    client = OpenAI(
        api_key=os.environ.get('OPENAI_API_KEY')
    )
    for chunk in tqdm(chunks):
        chunk_json = chunk.to_json(orient='records')
        messages = [
            {
                'role': 'developer',
                'content': prompt.get('role')
            },
            {
                'role': 'user',
                'content': prompt.get('content') + f'<t>{chunk_json}</t>',
            }
        ]
        response = client.chat.completions.create(
            model=model_name,
            messages=messages,
            temperature=temperature,
            stream=stream
        )
        if stream:
            collected_messages = []
            for res_chunk in response:
                chunk_content = res_chunk.choices[0].delta.content
                collected_messages.append(chunk_content)
                print(chunk_content)
            content = ''.join([m for m in collected_messages if m is not None])
        else:
            content = response.choices[0].message.content
        mini_df = pd.DataFrame.from_records(
            convert_content_to_json(content)
        )
        responses.append(mini_df)

    output_dataframe = pd.concat(responses, ignore_index=True)
    merged = pd.merge(
        left=dataframe,
        right=output_dataframe[prompt.get('output_columns')],
        on='id',
        how='left',
        validate='1:1'
    )
    return merged


if __name__ == '__main__':

    load_dotenv(find_dotenv())

    task = 'translate'
    model = 'gpt-4o-2024-11-20'

    df = read_json_dataframe(
        file_path=os.environ.get('LATEST_DATASET_PATH'),
        remove_duplicates=False
    )
    prompt_translate = read_prompt_file(
        os.getenv('PROMPT_FILE_PATH'),
        task=task
    )
    output_df = query_chat(
        dataframe=df,
        prompt=prompt_translate,
        num_chunks=100,
        model_name=model,
        temperature=0.3,
        stream=True
    )
    output_df.to_json(
        os.path.join(
            os.getenv('OUTPUT_DATASETS_DIR'),
            f'tweets_2017_2019_{task}_{model}.json'
        ),
        orient='table'
    )

