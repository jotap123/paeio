import gc
import json
import logging
import os
import re
import tempfile
import pickle
import warnings
from functools import partial
from io import BytesIO, StringIO
from typing import Union
from urllib.request import urlretrieve

import numpy as np
import pandas as pd
from azure.core.exceptions import ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobClient, BlobServiceClient
from azure.storage.filedatalake import DataLakeFileClient, DataLakeServiceClient

# Set the logging level for all azure-* libraries
logger = logging.getLogger("azure")
logger.setLevel(logging.ERROR)
warnings.filterwarnings("ignore")

DEFAULT_CREDENTIAL_KWARGS = json.loads(os.getenv("DEFAULT_CREDENTIAL_KWARGS", "{}"))
DEFAULT_SERVICE_KWARGS = json.loads(os.getenv("DEFAULT_SERVICE_KWARGS", "{}"))
DEFAULT_CONN_KWARGS = json.loads(os.getenv("DEFAULT_CONN_KWARGS", "{}"))
DEFAULT_GLOB_CONN_KWARGS = json.loads(os.getenv("DEFAULT_GLOB_CONN_KWARGS", "{}"))
DEFAULT_BLOB_SERVICE = os.getenv("DEFAULT_BLOB_SERVICE", "gen2")
DEFAULT_UPLOAD_MODE = os.getenv("DEFAULT_UPLOAD_MODE", "full")
DEFAULT_NUM_THREADS = os.getenv("DEFAULT_NUM_THREADS", -1)


def create_blob_service(
    uri,
    conn_type=DEFAULT_BLOB_SERVICE,
    service_kwargs=DEFAULT_SERVICE_KWARGS,
    credential_kwargs=DEFAULT_CREDENTIAL_KWARGS,
):
    """
    Creates the connection service to a certain storage account
    Args:
        uri (str): Url with adress in a storage account
        conn_type (str): Type of connection to Azure. Can receive 'blob' or 'gen2'.
        service_kwargs (dict): dict with connection params to Azure services.
    Returns:
        object: DataLakeServiceClient (gen2)/BlobServiceClient (blob), depending on conn_type
    """

    credential = DefaultAzureCredential(**credential_kwargs)

    account_name = uri.split("//")[1].split(".")[0]

    if conn_type == "gen2":
        dlService = DataLakeServiceClient(
            account_url=f"https://{account_name}.dfs.core.windows.net",
            credential=credential,
            **service_kwargs,
        )

    elif conn_type == "blob":
        dlService = BlobServiceClient(
            account_url=f"https://{account_name}.blob.core.windows.net",
            credential=credential,
            **service_kwargs,
        )

    return dlService


def rename_file(uri_old, uri_new, conn_type=DEFAULT_BLOB_SERVICE):
    """
    Renames a file in an Azure Data Lake environment.
    Args:
        uri_old (str): old URL
        uri_new (str): new URL
        conn_type (str): Type of connection to Azure. Can receive 'blob' or 'gen2'.
    """

    service_client = create_blob_service(uri=uri_old, conn_type=conn_type)
    container_name = uri_old.split("/")[3]
    old_name = "/".join(uri_old.split("/")[4:])
    new_name = "/".join(uri_new.split("/")[4:])

    try:
        if conn_type == "gen2":
            file_system_client = service_client.get_file_system_client(
                file_system=container_name
            )
            file_client = file_system_client.get_file_client(file_path=old_name)

        elif conn_type == "blob":
            file_client = service_client.get_blob_client(
                container=container_name, blob=old_name
            )

        file_client.rename_file(new_name)

    finally:
        file_client.close()
        del file_client

        if conn_type == "gen2":
            file_system_client.close()
            del file_system_client

        service_client.close()
        del service_client

        gc.collect()


def upload_chunks(
    file_client: Union[DataLakeFileClient, BlobClient],
    data: Union[BytesIO, StringIO],
    **upload_kwargs,
):
    """
    Generic function to upload files in chunks

    Args:
        file_client (Union[DataLakeFileClient,BlobClient]): Azure client file.
        data (Union[BytesIO,StringIO]): Data stream to be uploaded
        **upload_kwargs: Args to be used with upload_data/upload_blob.
        See DataLakeFileClient.upload_data ou BlobFileClient/upload_blob documentation
        for details.

    """
    message = "You must pass chunk_size if using upload_mode=chunks"
    assert "chunk_size" in upload_kwargs, message

    if "chunk_size" in upload_kwargs:
        chunk_size = upload_kwargs.pop("chunk_size")

    upload_kwargs.pop("overwrite")

    if isinstance(file_client, DataLakeFileClient):
        file_client.create_file()

    elif isinstance(file_client, BlobClient):
        file_client.create_append_blob()

    while True:
        read_data = data.read(chunk_size)

        if not read_data:
            break

        if isinstance(file_client, DataLakeFileClient):
            if file_client.exists():
                filesize_previous = file_client.get_file_properties().size
            else:
                filesize_previous = 0

            file_client.append_data(
                data=read_data,
                offset=filesize_previous,
                length=len(read_data),
                **upload_kwargs,
            )
            file_client.flush_data(filesize_previous + len(read_data))

        elif isinstance(data, StringIO):
            read_data = "".join(read_data)

        elif isinstance(file_client, BlobClient):
            file_client.append_block(read_data, **upload_kwargs)


def upload_data(
    byte_stream: Union[BytesIO, StringIO],
    file_client: Union[DataLakeFileClient, BlobClient],
    upload_mode: str = DEFAULT_UPLOAD_MODE,
    **upload_kwargs,
):
    """
    Generic function to upload files.

    Args:
        byte_stream (Union[BytesIO,StringIO]): Data stream to be uploaded
        file_client (Union[DataLakeFileClient,BlobClient]): Azure client file.
        upload_mode (str): 'full' for direct upload or 'chunks' for uploading in parts.
            If 'chunks' is set. 'chunk_size' has to be declared in 'upload_kwargs'.
        **upload_kwargs: Args to be used with upload_data/upload_blob.
            See DataLakeFileClient.upload_data ou BlobFileClient/upload_blob documentation
        for details.
    """
    if isinstance(file_client, DataLakeFileClient):
        delete_func = file_client.delete_file
    elif isinstance(file_client, BlobClient):
        delete_func = file_client.delete_blob

    if upload_mode == "full":
        if isinstance(file_client, DataLakeFileClient):
            upload_func = file_client.upload_data

        elif isinstance(file_client, BlobClient):
            if "chunk_size" in upload_kwargs:
                upload_kwargs.pop("chunk_size")
            upload_func = file_client.upload_blob

    elif upload_mode == "chunks":
        upload_func = partial(upload_chunks, file_client=file_client)

    if file_client.exists():
        delete_func()

    byte_stream.seek(0)

    if upload_mode == "full":
        if isinstance(byte_stream, StringIO):
            byte_stream = "".join(byte_stream.readlines())

    upload_func(data=byte_stream, overwrite=True, **upload_kwargs)

    return file_client


def to_any(
    byte_stream,
    uri,
    conn_type=DEFAULT_BLOB_SERVICE,
    upload_mode=DEFAULT_UPLOAD_MODE,
    verbose=1,
    **upload_kwargs,
):
    """
    Generic function for writing data.

    Args:
        byte_stream (stream): Data stream to be uploaded
        uri (url): Target url to receive data stream
        upload_mode (str): 'full' for direct upload or 'chunks' for uploading in parts.
            If 'chunks' is set. 'chunk_size' has to be declared in 'upload_kwargs'.
        conn_type (str): Type of connection to Azure. Can receive 'blob' or 'gen2'.
        **upload_kwargs: Args to be used with upload_data/upload_blob.
            See DataLakeFileClient.upload_data ou BlobFileClient/upload_blob documentation
        for details.
    """

    service_client = create_blob_service(uri=uri, conn_type=conn_type)
    container_name = uri.split("/")[3]
    blob_name = "/".join(uri.split("/")[4:])

    if verbose > 0:
        logger.info(f"Writing {blob_name}")

    if conn_type == "gen2":
        file_system_client = service_client.get_file_system_client(
            file_system=container_name
        )
        file_client = file_system_client.get_file_client(file_path=blob_name)

    elif conn_type == "blob":
        file_client = service_client.get_blob_client(
            container=container_name, blob=blob_name
        )

    try:
        file_client = upload_data(
            byte_stream=byte_stream,
            file_client=file_client,
            upload_mode=upload_mode,
            **upload_kwargs,
        )

    finally:
        file_client.close()
        del file_client

        if conn_type == "gen2":
            file_system_client.close()
            del file_system_client

        service_client.close()
        del service_client

        gc.collect()


def read_any(uri, func, conn_type=DEFAULT_BLOB_SERVICE, **kwargs):
    """
    Generic function for reading data.

    Args:
        uri (url): Url where data will be streamt from
        func: Reading function
        conn_type (str): Type of connection to Azure. Can receive 'blob' or 'gen2'.
        **kwargs: Args to be passed to the Reading function

    Returns:
        Output of function 'func'
    """

    def close_connections(*connections):
        for conn in connections:
            if conn:
                conn.close()
                del conn

    service_client = create_blob_service(uri, conn_type=conn_type)
    container_name = uri.split("/")[3]
    blob_name = "/".join(uri.split("/")[4:])

    byte_stream = BytesIO()

    if conn_type == "gen2":
        file_system_client = service_client.get_file_system_client(
            file_system=container_name
        )
        file_client = file_system_client.get_file_client(blob_name)

    elif conn_type == "blob":
        file_client = service_client.get_blob_client(
            container=container_name, blob=blob_name
        )

    assert file_client.exists(), f"Could not find blob in {blob_name}"

    try:
        if conn_type == "gen2":
            byte_stream.write(file_client.download_file().readall())
        elif conn_type == "blob":
            byte_stream.write(file_client.download_blob().readall())

        byte_stream.seek(0)
        df = func(byte_stream, **kwargs)

    except Exception as e:
        raise Exception(f"Could not read blob in {blob_name}: {e}")

    finally:
        close_connections(byte_stream, file_client, file_system_client, service_client)
        gc.collect()

    return df


def to_parquet(
    df,
    uri,
    conn_type=DEFAULT_BLOB_SERVICE,
    upload_kwargs=DEFAULT_CONN_KWARGS,
    upload_mode=DEFAULT_UPLOAD_MODE,
    **kwargs,
):
    """
    Parquet writing function

    Args:
        df (pd.DataFrame): DataFrame to be uploaded in Azure
        uri (str): Target URL for data upload
        conn_type (str): Type of connection to Azure. Can receive 'blob' or 'gen2'.
        upload_kwargs (dict): Args to be used with upload_data/upload_blob.
            See DataLakeFileClient.upload_data ou BlobFileClient/upload_blob documentation
        for details.
        **kwargs: Args to be used with the parquet reading function.
            See df.to_parquet for details.
    """
    byte_stream = BytesIO()
    df.to_parquet(byte_stream, use_deprecated_int96_timestamps=True, **kwargs)
    to_any(
        byte_stream, uri, conn_type=conn_type, upload_mode=upload_mode, **upload_kwargs
    )


def to_excel(
    df,
    uri,
    conn_type=DEFAULT_BLOB_SERVICE,
    mode="pandas",
    upload_kwargs=DEFAULT_CONN_KWARGS,
    upload_mode=DEFAULT_UPLOAD_MODE,
    **kwargs,
):
    """
    Excel writing function.

    Args:
        df (pd.DataFrame): DataFrame to be uploaded in Azure
        uri (str): Target URL for data upload
        conn_type (str): Type of connection to Azure. Can receive 'blob' or 'gen2'.
        upload_kwargs (dict): Args to be used with upload_data/upload_blob.
            See DataLakeFileClient.upload_data ou BlobFileClient/upload_blob documentation
        for details.
        **kwargs: Argumentos a serem passados para a função de escrita em parquet.
            Consultar df.to_parquet para mais detalhes.
    """

    # Pyexcelerate still not supported
    if mode == "pyexcelerate":
        logger.warn("to_excel method is currently not supported with pyexcelerate mode")
        mode = "pandas"

    func_dict = {"pandas": pd.DataFrame.to_excel}

    byte_stream = BytesIO()
    func_dict[mode](df, byte_stream, **kwargs)
    to_any(
        byte_stream, uri, conn_type=conn_type, upload_mode=upload_mode, **upload_kwargs
    )


def to_csv(
    df,
    uri,
    conn_type="gen2",
    encoding="utf-8",
    upload_kwargs=DEFAULT_CONN_KWARGS,
    upload_mode=DEFAULT_UPLOAD_MODE,
    **kwargs,
):
    """
    Função de escrita para arquivos csv e consequente subida a Azure.

    Args:
        df (pd.DataFrame): DataFrame to be uploaded in Azure
        uri (str): Target URL for data upload
        conn_type (str): Type of connection to Azure. Can receive 'blob' or 'gen2'.
        upload_kwargs (dict): Args to be used with upload_data/upload_blob.
            See DataLakeFileClient.upload_data ou BlobFileClient/upload_blob documentation
        for details.
        **kwargs: Argumentos a serem passados para a função de escrita em parquet.
            Consultar df.to_parquet para mais detalhes.
    """
    # Csv writing currently not supported in blob conn_type
    if conn_type == "blob":
        logger.warn("to_csv method is currently not supported with blob conn_type")
        conn_type = "gen2"

    byte_stream = StringIO()
    df.to_csv(byte_stream, encoding=encoding, **kwargs)

    to_any(
        byte_stream,
        uri,
        encoding=encoding,
        conn_type=conn_type,
        upload_mode=upload_mode,
        **upload_kwargs,
    )


def build_re(glob_str):
    """
    Function that matches a pattern for path recognition.
    Args:
        glob_str (str): String with glob pattern.
    Returns:
        str: String translated to regex pattern.
    """

    opts = re.compile("([.]|[*][*]/|[*]|[?])|(.)")
    out = ""
    for pattern_match, literal_text in opts.findall(glob_str):
        if pattern_match == ".":
            out += "[.]"
        elif pattern_match == "**/":
            out += "(?:.*/)?"
        elif pattern_match == "*":
            out += "[^/]*"
        elif pattern_match == "?":
            out += "."
        elif literal_text:
            out += literal_text
    return out


def glob(uri, conn_kwargs=DEFAULT_GLOB_CONN_KWARGS, **kwargs):
    """
    Function that allows getting all files in a given root directory.

    Args:
        uri (str): Url with regex pattern.
            Supports:
                - '*' gets any string between the last character before it and the first after
                - (word1|word2) allows filtering str with either word1 or word2
        conn_kwargs (dict): connection args
        **kwargs (dict): get_paths args
    Returns:
        list: List with all files that match the given pattern and are inside root directory.
    """

    blob_service = create_blob_service(uri, conn_type="gen2")
    container_name = uri.split("/")[3]
    container_url = "/".join(uri.split("/")[:4])
    blob_name = "/".join(uri.split("/")[4:])
    container_client = blob_service.get_file_system_client(file_system=container_name)

    # Como a get_paths exige que a path exista, nós quebramos a url em duas variaveis:
    # - path: com a parte da url que exista uma pasta
    # - path_suffix: com a query desejada dentro dessa pasta

    lista = re.split("^(.*?[\*])", blob_name)

    if len(lista) == 1:
        path = lista[0]
        path_suffix = ""
    else:
        new_split = "/".join(lista[:-1]).split("/")
        path = "/".join(new_split[:-1])
        path_suffix = new_split[-1] + lista[-1]

    list_blobs = [
        container_url + "/" + unit.name
        for unit in container_client.get_paths(path=path, **kwargs, **conn_kwargs)
    ]

    result_list = []

    if len(list_blobs) == 0:
        print("No file match the specified criteria")
        return result_list

    if len(path_suffix) == 0:
        return list_blobs

    else:
        path_suffix = build_re(path_suffix)
        result_list = [
            i for i in np.array(list_blobs) if re.search(path_suffix, i) is not None
        ]

    return result_list


def read_parquet(uri, mode="pandas", conn_type=DEFAULT_BLOB_SERVICE, **kwargs):
    """
    Generic parquet reading file.
    Args:
        uri (str): Target URL file
        conn_type (str): Type of connection to Azure. Can receive 'blob' or 'gen2'.
        **kwargs: Reader functions extra args.
    Returns:
        pd.DataFrame
    """
    func = {"pandas": pd.read_parquet}
    func = func[mode]
    return read_any(uri, func, conn_type=conn_type, **kwargs)


def read_csv(uri, mode="pandas", conn_type=DEFAULT_BLOB_SERVICE, **kwargs):
    """
    Generic csv reading file.
    Args:
        uri (str): Target URL file
        conn_type (str): Type of connection to Azure. Can receive 'blob' or 'gen2'.
        **kwargs: Reader functions extra args.
    Returns:
        pd.DataFrame
    """
    func = {"pandas": pd.read_csv}
    func = func[mode]
    return read_any(uri, func, conn_type=conn_type, **kwargs)


def read_excel(uri, mode="pandas", conn_type=DEFAULT_BLOB_SERVICE, **kwargs):
    """
    Generic excel reading file.
    Args:
        uri (str): Target URL file
        conn_type (str): Type of connection to Azure. Can receive 'blob' or 'gen2'.
        **kwargs: Reader functions extra args.
    Returns:
        pd.DataFrame
    """

    # A partir de uma determinada versao, o xlrd parou de dar suporte a xlsx.
    # Usa-se por padrão a engine openpyxl. Se ela não for passada, agnt força a engine
    if (".xlsx" in uri) & ("engine" not in kwargs):
        kwargs["engine"] = "openpyxl"

    func = {"pandas": pd.read_excel}
    func = func[mode]
    return read_any(uri, func, conn_type=conn_type, **kwargs)


def read_url(uri, sas_token, _format, **kwargs):
    """Read from a container with SAS token"""
    with tempfile.NamedTemporaryFile() as tf:
        url_tok = uri + sas_token
        urlretrieve(url_tok, tf.name)
        df = read_any(uri=tf.name, _format=_format, **kwargs)
        return df


def file_exists(path):
    """Checks if an Azure Data Lake file exists"""
    last_dir = path.replace(path.split("/")[-1], "*")

    try:
        if path in glob(last_dir):
            return True
        else:
            return False
    except ResourceNotFoundError:
        return False


def save_pickle(model, path):
    """Save pickle files in an Azure Data Lake"""
    with tempfile.NamedTemporaryFile() as tfile:
        pickle.dump(model, open(tfile.name, 'wb'))
        model_file = open(tfile.name, 'rb')
        byte_stream = BytesIO()
        byte_stream.write(model_file.read())
        to_any(byte_stream, path)
