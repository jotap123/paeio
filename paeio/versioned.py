import pandas as pd

from paeio import io
from paeio.parallelism import applyParallel
from paeio.path import get_date_from_names


def load_last_file(glob, _format="parquet", **kwargs):
    """
    Reads the last file from a history.

    Args:
        glob (str): path
        _format (str): file format. Default: .parquet
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
    Function that allows recursively navigating within a folder system
    divided into years, months, and days and concatenates the navigated files.
    It allows for the implementation of incremental logic based on the min_date parameter.

    Args:
        base_folder (str): Root directory (preferably containing years, months, and days)
        min_date (str): Desired minimum date
        file_format (str): Type of file to be read in the folder. Parquet, CSV, or Excel.
        last_layer (str): Last layer of folders. Can be year, month, or day.
        max_date (str): Desired maximum date
        verbose (int): Code verbosity level.
            Above zero, prints entries in folders
        n_jobs (int): Maximum number of jobs to be used in parallelizing reading
        file_func (func): Function to be applied at the time of reading
        date_sep (str): Date separator in folders with files with dates in the name
        occ (int): Occurrence of the split where the date is located in case of folder
        keep_origin_col (bool): Whether or not to keep the original directory column name
        errors (str): Can be 'raise' (breaks code when error occurs)
            or 'ignore' (continues with empty dataframe)
        mode (str): Reading mode. Can be 'read' to read the file
            or 'rename' to rename the file
        **kwargs: Arguments to be passed in read_any (e.g., sep for read_csv)

    Returns:
        pd.DataFrame: Final concatenated DataFrame with all the files that were navigated

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
