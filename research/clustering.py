import os
from typing import Optional

import numpy as np
import pandas as pd
from dotenv import load_dotenv, find_dotenv
from sklearn.manifold import TSNE
from umap import UMAP

from utils.file_utils import read_json_dataframe


def tsne_dimensionality_reduction(
        dataframe: pd.DataFrame,
        embeddings_column: str,
        num_dim: Optional[int] = 2) -> pd.DataFrame:
    reducer = TSNE(
        n_components=num_dim
    )
    embeddings = dataframe[embeddings_column].to_list()
    reduced = reducer.fit_transform(np.array(embeddings))
    dataframe[f'tsne_{embeddings_column}_x'] = [dim[0] for dim in reduced]
    dataframe[f'tsne_{embeddings_column}_y'] = [dim[1] for dim in reduced]
    return dataframe


def umap_dimensionality_reduction(
        dataframe: pd.DataFrame,
        embeddings_column: str,
        num_dim: Optional[int] = 2) -> pd.DataFrame:
    reducer = UMAP(
        n_components=num_dim,
        unique=True
    )
    embeddings = dataframe[embeddings_column].to_list()
    reduced = reducer.fit_transform(np.array(embeddings))
    dataframe[f'umap_{embeddings_column}_x'] = [dim[0] for dim in reduced]
    dataframe[f'umap_{embeddings_column}_y'] = [dim[1] for dim in reduced]
    return dataframe


if __name__ == '__main__':

    load_dotenv(find_dotenv())

    df = read_json_dataframe(
        file_path=os.environ.get('USERS_DATA_PATH'),
        remove_duplicates=False
    )
    df = umap_dimensionality_reduction(
        dataframe=df,
        embeddings_column='mistral-embed_embeddings'
    )
