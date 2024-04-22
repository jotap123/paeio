import pandas as pd

from paeio.parallelism import applyParallel
from paeio.path import get_date_from_names
from paeio import io


def load_last_file(glob, _format="parquet", **kwargs):
    """Le o ultimo arquivo de um historico de arquivos

    Args:
        glob (str): path
        ext (str): Tipo de extensao do arquivo. Default: .parquet
    """
    file_to_load = io.glob(glob)[-1]

    if _format == "parquet":
        df = io.read_parquet(file_to_load, **kwargs)
    elif _format == "csv":
        df = io.read_csv(file_to_load, **kwargs)
    else:
        df = io.read_excel(file_to_load, **kwargs)
    return df


def extract_file(
    x,
    file_format,
    verbose=0,
    file_func=lambda x: x,
    keep_origin_col=False,
    errors="raise",
    mode="read",
    **kwargs,
):

    if verbose > 0:
        if mode == "read":
            print(f"Reading {x['directory'].values[0]}")
        elif mode == "rename":
            print(f"Renaming {x['directory'].values[0]}")

    if mode == "read":
        dict = {"parquet": io.read_parquet, "excel": io.read_excel, "csv": io.read_csv}
    elif mode == "rename":
        dict = {form: io.rename_file for form in ["parquet", "excel", "csv"]}

    if file_format in list(dict.keys()):
        func = dict[file_format]
        try:
            df = func(x["directory"].values[0], **kwargs)

            if keep_origin_col:
                df["origin"] = x["directory"].values[0]

            df = file_func(df)

        except Exception as e:
            if errors == "raise":
                raise e
            elif errors == "ignore":
                return pd.DataFrame()

    else:
        try:
            df = file_func(x["directory"].values[0], **kwargs)

        except Exception as e:
            if errors == "raise":
                raise e
            elif errors == "ignore":
                return pd.DataFrame()

    return df


def datalake_walk(
    base_folder,
    file_format="parquet",
    min_date="2000-01-01",
    last_layer="folder",
    max_date="2099-12-31",
    verbose=0,
    n_jobs=-1,
    file_func=lambda x: x,
    date_sep="-",
    occ=-1,
    keep_origin_col=False,
    errors="raise",
    mode="read",
    **kwargs,
):
    """
    Função que permite navegar de forma recursiva dentro de um sistema de pastas
    dividido em anos, meses e dias e concatena os arquivos navegados.
    Ele permite implementação de lógica incremental a partir do parâmetro min_date.

    Args:
        base_folder (str): Diretório raiz que contem os anos, meses e dias
        min_date (str): Data mínima desejada
        file_format (str): Tipo de arquivo a ser lido na pasta.
            Pode ser parquet, csv ou excel.
        last_layer (str): Ultima camada de pastas. Pode ser year, month ou day.
        max_date (str): Data máxima desejada
        verbose (int): Nivel de verbosidade do codigo.
            Acima de zero, printa entradas nas pastas
        n_jobs (int): Numero de jobs maximo a ser usado na paralelização da leitura
        file_func (func): Função a ser aplicada no momento da leitura
        date_sep (str): Separador de data em pastas com arquivos com datas no nome
        occ (int): Ocorrencia do split que a data se encontra em caso de folder
        keep_origin_col (bool): Manter ou não o nome da coluna de diretorio de origem
        errors (str): Pode ser 'raise' (quebra codigo quando da erro)
            ou 'ignore' (segue com dataframe vazio)
        mode (str): Modo de leitura. Pode ser 'read' para ler o arquivo
            ou 'rename' para renomear o arquivo
        **kwargs: Argumentos a serem passados na read_any (ex: sep do read_csv)

    Returns:
        pd.DataFrame: DataFrame concatenado final com todos os arquivos que foram navegados

    """
    min_date = pd.to_datetime(min_date)
    max_date = pd.to_datetime(max_date)

    extension = file_format if file_format != "excel" else "xls"

    list_folders = [col for col in io.glob(base_folder) if f".{extension}" in col]

    start = -2
    date_format = "%Y"

    if last_layer == "day":
        start = -4
        date_format = "%Y/%m/%d"
    elif last_layer == "month":
        start = -3
        date_format = "%Y/%m"

    df_folders = pd.DataFrame(list_folders, columns=["directory"])
    df_folders["file_name"] = df_folders["directory"].str.split("/").str.get(-1)

    if last_layer != "folder":
        df_folders["date"] = pd.to_datetime(
            df_folders["directory"]
            .str.split("/")
            .str.slice(start=start, stop=-1)
            .str.join("/"),
            format=date_format,
        )
        df_folders = df_folders.loc[df_folders["date"].between(min_date, max_date)]

    else:
        try:
            df_folders["date"] = get_date_from_names(
                df_folders["directory"], sep=date_sep, occ=occ
            )
            df_folders = df_folders.loc[df_folders["date"].between(min_date, max_date)]
        except:
            pass

    df = None

    if len(df_folders) == 1:
        df = extract_file(
            df_folders,
            file_format=file_format,
            verbose=verbose,
            file_func=file_func,
            keep_origin_col=keep_origin_col,
            errors=errors,
            mode=mode,
            **kwargs,
        )
    elif len(df_folders) > 1:
        df = applyParallel(
            df_folders.groupby(["directory"]),
            extract_file,
            keep_origin_col=keep_origin_col,
            file_format=file_format,
            file_func=file_func,
            verbose=verbose,
            errors=errors,
            n_jobs=n_jobs,
            mode=mode,
            **kwargs,
        ).reset_index(drop=True)

    return df
