import os
from typing import Optional, Tuple

import numpy as np
import pandas as pd
from dotenv import load_dotenv, find_dotenv
from matplotlib import pyplot as plt
from scipy.cluster.hierarchy import dendrogram
from sklearn.cluster import AgglomerativeClustering
from sklearn.manifold import TSNE
from umap import UMAP

from research.plots import plot_tsne_scatter
from utils.file_utils import read_json_dataframe


def plot_dendrogram(model, **kwargs):

    counts = np.zeros(model.children_.shape[0])
    n_samples = len(model.labels_)
    for i, merge in enumerate(model.children_):
        current_count = 0
        for child_idx in merge:
            if child_idx < n_samples:
                current_count += 1  # leaf node
            else:
                current_count += counts[child_idx - n_samples]
        counts[i] = current_count

    linkage_matrix = np.column_stack(
        [model.children_, model.distances_, counts]
    ).astype(float)

    # Plot the corresponding dendrogram
    dendrogram(linkage_matrix, **kwargs)


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


def agglomerative_clustering(
        dataframe: pd.DataFrame,
        embeddings_column: str,
        n_clusters: Optional[int] = 10) -> Tuple[pd.DataFrame, AgglomerativeClustering]:
    model = AgglomerativeClustering(
        n_clusters=None,
        metric='euclidean',
        memory=None,
        connectivity=None,
        compute_full_tree='auto',
        linkage='ward',
        distance_threshold=(dist := 3),
        compute_distances=False
    )
    embeddings = dataframe[embeddings_column].to_list()
    clustering_labels = model.fit_predict(np.array(embeddings))
    clustering_labels = [str(label) for label in clustering_labels]
    print(f"Number of labels : {len(set(clustering_labels))} with distance threshold : {dist}")
    dataframe[f'clustering_agglo_{n_clusters}'] = clustering_labels
    return dataframe, model


if __name__ == '__main__':

    load_dotenv(find_dotenv())

    df = read_json_dataframe(
        file_path=os.environ.get('LATEST_DATASET_PATH'),
        remove_duplicates=False
    )
    df, clustering_model = agglomerative_clustering(
        dataframe=df,
        embeddings_column='mistral-embed_embeddings'
    )
    plt.title("Hierarchical Clustering Dendrogram")

    mistral_model = 'mistral-embed'
    openai_model = 'text-embedding-3-large'
    model = mistral_model
    dim_red = 'umap'
    # plot the top three levels of the dendrogram
    plot_dendrogram(clustering_model, truncate_mode="level", p=5)
    plt.xlabel("Number of points in node (or index of point if no parenthesis).")
    plt.show()
    plot_tsne_scatter(
        dataframe=df,
        embedding_column=f'{dim_red}_{model}_embeddings',
        color_column='clustering_agglo_10'
    )
