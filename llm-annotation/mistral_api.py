import os
import time

import numpy as np
import pandas as pd

from dotenv import find_dotenv, load_dotenv
from mistralai import Mistral
from tqdm import tqdm


def query_mistral(api_key, content: str, model: str, temperature: float) -> str:
    """
    Query mistral model with API key from environment variable

    Args:
        api_key: Mistral API key
        content: str of prompt
        model: name of the model to query
        temperature: temperature between 0.0 and 1.0

    Returns:
        Model's response content as str
    """
    client = Mistral(api_key=api_key)
    chat_response = client.chat.complete(
        model=model,
        messages=[
            {
                'role': 'user',
                'content': content
            },
        ],
        temperature=temperature
    )
    return chat_response.choices[0].message.content


def clean_response(response_text):
    response.replace('', '')


if __name__ == '__main__':

    # Parameters
    load_dotenv(find_dotenv())
    mistral_api_key = os.environ.get('MISTRAL_API_KEY')
    model_name = 'mistral-large-2407'
    temp = 0.0

    # Data
    data = pd.read_csv('/home/juliette/Downloads/data_temp - data_temp.csv')
    data_selected = data[['id', 'text']]
    data_chunks = np.array_split(data_selected, 6)

    responses = []

    for df_chunk in tqdm(data_chunks):

        csv_str = df_chunk.to_csv(index=False)
        prompt = f"""Here is a csv table with some texts in japanese, stored in the 'text' column. 
        translate the text to two new columns, one for the french version and one for the english version, 
        and return exclusively the result as a formated jsonl with one line per row of the csv :\n{csv_str}"""

        response = query_mistral(
            api_key=mistral_api_key,
            content=prompt,
            model=model_name,
            temperature=temp
        )
        responses.append(response)
        with open('/home/juliette/Desktop/data_translated_2.jsonl', 'w') as output_data:
            output_data.write('\n'.join(responses))
        time.sleep(10)
