import os
from typing import Optional, Dict

import anthropic
import pandas as pd
from dotenv import load_dotenv, find_dotenv
from tqdm import tqdm

from llm_utils import convert_content_to_json
from utils.df_transform import chunk_dataframe
from utils.file_utils import read_json_dataframe, read_prompt_file


def query_chat(
        dataframe: pd.DataFrame,
        prompt: Dict,
        model_name: Optional[str] = 'claude-3-5-sonnet-20241022',
        temperature: Optional[float] = 0.2,
        num_chunks: Optional[int] = 1,
        stream: Optional[bool] = False) -> pd.DataFrame:

    data = dataframe[['id', 'text']]
    chunks = chunk_dataframe(data, num_chunks)
    responses = []

    client = anthropic.Anthropic(
        api_key=os.getenv('ANTHROPIC_API_KEY', '')
    )
    for chunk in tqdm(chunks):
        chunk_json = chunk.to_json(orient='records')
        messages = [
            {
                'role': 'user',
                'content': [
                    {
                        'type': 'text',
                        'text': prompt.get('content') + f'<t>{chunk_json}</t>'
                    }
                ],
            }
        ]
        if not stream:
            response = client.messages.create(
                model=model_name,
                messages=messages,
                temperature=temperature,
                stream=stream,
                system=prompt.get('role'),
                max_tokens=2048
            )
            content = response.content
        else:
            with client.messages.stream(
                model=model_name,
                messages=messages,
                temperature=temperature,
                system=prompt.get('role'),
                max_tokens=2048
            ) as stream:
                collected_messages = []
                for res_chunk in stream.text_stream:
                    collected_messages.append(res_chunk)
                    print(res_chunk, end="", flush=True)
                content = ''.join([m for m in collected_messages if m is not None])

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
    )
    return merged


if __name__ == '__main__':

    load_dotenv(find_dotenv())

    task = 'translate'
    model = 'claude-3-sonnet-20240229'

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
        temperature=0.2,
        stream=True
    )
    output_df.to_json(
        os.path.join(
            os.getenv('OUTPUT_DATASETS_DIR'),
            f'tweets_2017_2019_{task}_{model}.json'
        ),
        orient='table'
    )

