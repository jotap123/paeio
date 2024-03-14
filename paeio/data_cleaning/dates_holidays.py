from datetime import datetime

import pandas as pd


def is_weekend(date, include_friday=False):
    """
    Retorna uma resposta se o dia especificado eh fim de semana

    Args:
        date (datetime.date): data a ser testada
        include_friday (bool): True para incluir sexta no teste logico

    Returns:
        bool: True se a data passada esta no final de semana
    """
    if include_friday:
        weekend = [4, 5, 6]
    else:
        weekend = [5, 6]

    return date.weekday() in weekend


def week_to_date_row(week, ref=pd.to_datetime("1989-12-31")):
    """
    Função que retorna a data do primeiro dia da semana
    Para nós, o primeiro dia é sempre domingo

    Args:
       week (str): Entidade com os números da semana
       ref (str): Data referencia de inicio

    Returns:
       str: Data acrescida dos números da semana
    """

    return ref + datetime.timedelta(days=week * 7)


def week_to_date_column(week_column, ref=pd.to_datetime("1989-12-31")):
    """
    Função que retorna série com a data do primeiro dia da semana.
    Para nós, o primeiro dia é sempre domingo

    Args:
       week_column (pd.Series): Coluna com os números da semana
       ref (str): Data referencia de inicio

    Returns:
       pd.Series: Coluna com o primeiro dia da semana
    """
    return pd.to_timedelta(week_column * 7, "d") + ref
