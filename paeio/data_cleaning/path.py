import re
import pandas as pd


def path_join(*args, sep='/'):
    """ Une partes de um path """
    return f"{sep}".join([*args])


def get_date_from_names(names_list, sep="_", occ=-1):
    """ Given a list with filenames, returns the dates contained within each filename
    """
    result_list = []
    for item in names_list:
        if sep == "_":
            match = re.search(r"\d{4}_\d{2}_\d{2}", item.split("/")[occ])
        elif sep == "-":
            match = re.search(r"\d{4}-\d{2}-\d{2}", item.split("/")[occ])
        elif sep == "/":
            match = re.search(r"\d{4}/\d{2}/\d{2}", item)
        elif sep == "":
            match = re.search(r"\d{4}\d{2}\d{2}", item.split("/")[occ])

        result_list.append(str(match.group()))

    if sep == "-":
        return pd.to_datetime(result_list, format="%Y-%m-%d")
    elif sep == "/":
        return pd.to_datetime(result_list, format="%Y/%m/%d")
    elif sep == "_":
        return pd.to_datetime(result_list, format="%Y_%m_%d")
    elif sep == "":
        return pd.to_datetime(result_list, format="%Y%m%d")
    else:
        raise "Date format not supported"
