from typing import Optional, List, Dict

import pandas as pd
from dotenv import find_dotenv, load_dotenv
from mistralai import Mistral

import os

from tqdm import tqdm

from llm_utils import convert_content_to_json
from utils.df_transform import chunk_dataframe
from utils.file_utils import read_json_dataframe, read_prompt_file, read_pdf_bibliography


def query_embeddings(
        dataframe: pd.DataFrame,
        model_name: Optional[str] = 'mistral-embed',
        num_chunks: Optional[int] = 1) -> pd.DataFrame:

    data = dataframe[['id', 'text']]
    chunks = chunk_dataframe(data, num_chunks)
    outputs = []

    client = Mistral(
        api_key=os.getenv('MISTRAL_API_KEY', '')
    )
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
        prompt: Dict,
        model_name: Optional[str] = 'mistral-large-latest',
        temperature: Optional[float] = 0.2,
        num_chunks: Optional[int] = 1,
        stream: Optional[bool] = False) -> pd.DataFrame:

    data = dataframe[['id', 'text']]
    chunks = chunk_dataframe(data, num_chunks)
    responses = []

    client = Mistral(
        api_key=os.getenv('MISTRAL_API_KEY', '')
    )

    for chunk in tqdm(chunks):
        chunk_json = chunk.to_json(orient='records')
        messages = [
            {
                'content': prompt.get('role'),
                'role': 'system',
            },
            {
                'content': prompt.get('content') + f'<t>{chunk_json}</t>',
                'role': 'user',
            },
        ]
        if not stream:
            response = client.chat.complete(
                model=model_name,
                messages=messages,
                temperature=temperature,
                response_format={
                    'type': 'json_object',
                }
            )
            content = response.choices[0].message.content
        else:
            response = client.chat.stream(
                model=model_name,
                messages=messages,
                temperature=temperature,
                response_format={
                    'type': 'json_object',
                }
            )
            collected_messages = []
            for res_chunk in response:
                chunk_content = res_chunk.data.choices[0].delta.content
                collected_messages.append(chunk_content)
                print(chunk_content)
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
        how='left'
    )
    return merged


def query_documents_summary(
        documents: Dict[str, str],
        prompt: Dict,
        output_path: str,
        model_name: Optional[str] = 'mistral-large-latest',
        temperature: Optional[float] = 0.2,
        stream: Optional[bool] = False) -> None:

    client = Mistral(
        api_key=os.getenv('MISTRAL_API_KEY', '')
    )

    for document_name, document in tqdm(documents.items()):

        messages = [
            {
                'content': prompt.get('role'),
                'role': 'system',
            },
            {
                'content': prompt.get('content') + f'<doc>{document}</doc>',
                'role': 'user',
            },
        ]
        if not stream:
            response = client.chat.complete(
                model=model_name,
                messages=messages,
                temperature=temperature
            )
            content = response.choices[0].message.content
        else:
            response = client.chat.stream(
                model=model_name,
                messages=messages,
                temperature=temperature
            )
            collected_messages = []
            for res_chunk in response:
                chunk_content = res_chunk.data.choices[0].delta.content
                collected_messages.append(chunk_content)
                print(chunk_content)
            content = ''.join([m for m in collected_messages if m is not None])

        with open(output_path, 'a') as output_file:
            output_file.write(
                f'## {document_name}\n\n{content}\n\n\n\n\n'
            )


def query_txt_summary(
        document: str,
        prompt: Dict,
        output_path: str,
        model_name: Optional[str] = 'mistral-large-latest',
        temperature: Optional[float] = 0.2,
        stream: Optional[bool] = False) -> None:

    client = Mistral(
        api_key=os.getenv('MISTRAL_API_KEY', '')
    )

    messages = [
        {
            'content': prompt.get('role'),
            'role': 'system',
        },
        {
            'content': prompt.get('content') + document,
            'role': 'user',
        },
    ]
    if not stream:
        response = client.chat.complete(
            model=model_name,
            messages=messages,
            temperature=temperature
        )
        content = response.choices[0].message.content
    else:
        response = client.chat.stream(
            model=model_name,
            messages=messages,
            temperature=temperature
        )
        collected_messages = []
        for res_chunk in response:
            chunk_content = res_chunk.data.choices[0].delta.content
            collected_messages.append(chunk_content)
            print(chunk_content)
        content = ''.join([m for m in collected_messages if m is not None])

    with open(output_path, 'w') as output_file:
        output_file.write(content)


if __name__ == '__main__':

    load_dotenv(find_dotenv())

    task = 'SOTA'
    model = 'mistral-large-latest'
    temp = 0.2
    version = '1.1'

    if task == 'SOTA':
        with open(
            os.path.join(
                os.getenv('BIBLIOGRAPHY_DIR_PATH'),
                'bibliography_mistral-large-latest_temp-0.2.txt'
            ), 'r'
        ) as txt_file:
            articles_summary = txt_file.read()
        prompt_dict = read_prompt_file(
            os.getenv('PROMPT_FILE_PATH'),
            task=task,
            version=version
        )
        query_txt_summary(
            document=articles_summary,
            prompt=prompt_dict,
            output_path=os.path.join(
                os.getenv('BIBLIOGRAPHY_DIR_PATH'),
                f'SOTA_v{version}_{model}_temp-{temp}.md'
            ),
            model_name='mistral-large-latest',
            temperature=temp,
            stream=True
        )

    if task == 'documents_summary':
        docs = read_pdf_bibliography(
            os.path.join(
                os.getenv('BIBLIOGRAPHY_DIR_PATH'),
                'bibliographie_japonaise'
            )
        )
        prompt_dict = read_prompt_file(
            os.getenv('PROMPT_FILE_PATH'),
            task=task,
            version=version
        )
        query_documents_summary(
            documents=docs,
            prompt=prompt_dict,
            output_path=os.path.join(
                os.getenv('BIBLIOGRAPHY_DIR_PATH'),
                f'bibliography_v{version}_{model}_temp-{temp}.md'
            ),
            model_name='mistral-large-latest',
            temperature=temp,
            stream=True
        )

    if task == 'translation':
        df = read_json_dataframe(
            file_path=os.environ.get('LATEST_DATASET_PATH'),
            remove_duplicates=True
        )
        prompt_dict = read_prompt_file(
            os.getenv('PROMPT_FILE_PATH'),
            task=task,
            version='1.1'
        )
        output_df = query_chat(
            dataframe=df,
            prompt=prompt_dict,
            num_chunks=100,
            model_name=model,
            stream=True
        )
        output_df.to_json(
            os.path.join(
                os.getenv('OUTPUT_DATASETS_DIR'),
                f'tweets_2017_2019_{task}_{model}.json'
            ),
            orient='table'
        )
