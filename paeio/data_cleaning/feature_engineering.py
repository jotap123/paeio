import numpy as np
import pandas as pd


def create_lag_features(df, group_columns, target_column, num_lags, start=1, step=1):
    """
        Args:
            df (pd.DataFrame): dataset
            group_columns (list): colunas a serem agrupadas
            target_column (str): coluna a ser aplicada a funcao
            num_lags (int): numero maximo de lag
            start (int): numero inicial de lag
            step (int): intervalo entre os numeros de lag

        Returns:
            df (pd.DataFrame): dataset com colunas de lag
    """
    for i in range(start, num_lags + 1, step):
        column_name = f"{target_column}_lag_{i}"
        df[column_name] = df.groupby(group_columns)[target_column].shift(i)

    return df


def create_ma_features(df, group_columns, target_column, num_periods):
    """
      Args:
          df (pd.DataFrame): dataset
          group_columns (list): colunas a serem agrupadas
          target_column (str): coluna a ser aplicada a funcao
          num_periods (int): numero minimo de janelas a serem usadas para a media movel

      Returns:
          df (pd.DataFrame): dataset com colunas de ma
  """

    column_name = f"{target_column}_ma_{num_periods}"

    df[column_name] = (
        df.groupby(group_columns)[target_column]
        .rolling(num_periods)
        .mean()
        .shift(1)
        .reset_index(np.arange(len(group_columns)).tolist(), drop=True)
    )

    return df


def month_encoder(month, ref=pd.to_datetime("1990-01-01")):
    """
    Função que retorna o número de meses que passaram-se de uma data de referência

    Args:
      month (pd.Series): Série com datas
      ref (str): Data referencia de inicio

    Returns:
      PeriodArray: Série com número de meses que se passaram desde a data de referência
    """
    return (month.to_period("M") - ref.to_period("M")).n


def new_date_encoder(date: pd.Series,
                     freq: str = 'M',
                     date_start: str = '1970-01-01'):

    """
    Função que retorna uma série com o número do mês/dia relativo
        a data de referência

    Args:
        date (pd.Series): Série com datas
        freq (str): Permite em meses ('M') e em dias ('D')
        date_start (str): Data referencia de inicio

    Returns:
        pd.Series: Série com número do mês/dia relativo a data de referência
    """

    date = pd.DatetimeIndex(date)

    if freq == 'M':
        date = pd.to_datetime(
            np.where(date.day == 1, date, date - pd.offsets.MonthBegin(n=1)),
            format='%Y-%m-%d'
        )

    date_start = pd.DatetimeIndex(np.full_like(date, date_start))

    return date.to_period(freq).astype('int') - date_start.to_period(freq).astype('int')


def week_encoder(date, ref=pd.to_datetime("1989-12-31")):
    """
    Função que retorna o número de semanas que passara-se
    desde uma data de referência (começando por domingo)

    Args:
      date (pd.Series): Série com datas
      ref (str): Data referencia de inicio

    Returns:
      pd.Series: Série com número de semanas passadas após a data referência
    """
    return np.floor((date - ref).dt.days / 7)
