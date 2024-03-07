import unicodedata
from itertools import product

import pandas as pd


def generate_all_odds(df, impute_cols):
    """
    Gera um dataset com todas as combinacoes possiveis para as chaves passadas
        Args:
            df (pd.DataFrame): dataframe a ser submetido ao processo
            impute_cols (list): lista de colunas a ter todos os valores possiveis gerados

        Returns:
            imputed_df (pd.DataFrame): dataset contendo todas as combinacoes
            possiveis de chaves
    """
    base_sets = {}
    for col in impute_cols:
        base_sets[col] = df[col].unique()

    imputed_df = pd.DataFrame(
        product(*[base_sets[col] for col in impute_cols]), columns=impute_cols
    )

    return imputed_df


def strip_accents(text):
    """
    Função que devolve um texto sem acentos

    Args:
        text (str): texto com acento

    Returns:
        str: texto sem acento
    """
    text = unicodedata.normalize("NFD", text).encode("ascii", "ignore").decode("utf-8")

    return str(text)


def type_conversion(df, old_types="float64", new_type="float32"):
    """
    Função que converte todas as colunas de um tipo para outro

    Args:
      df(pd.Dataframe): Dataset de entrada
      old_type (datatype): tipo do dado de entrada
      new_type (datatype): tipo do dado de saida

    Returns:
      df(pd.Dataframe): Dataset com tipos modificados
    """
    cols = df.select_dtypes(old_types).columns
    df.loc[:, cols] = df.loc[:, cols].astype(new_type)
    return df
