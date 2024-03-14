import pandas as pd
from joblib import Parallel, delayed


def applyParallel(
    dfGrouped,
    func,
    n_jobs=-1,
    concat_results=True,
    backend="loky",
    plot_mode=False,
    verbose_par=0,
    *args,
    **kwargs,
):
    """
    Função que implementa o apply do pandas usando backend paralelizavel.

    Args:
        dfGrouped (pd.DataFrame or zip): Objeto groupby do pandas
            ou zip(objeto groupby + axes do matplotlib) para plot_mode=True
        func: Funcao definida pelo usuario
        n_jobs: Numero de jobs a serem utilizados no calculo da funcao
        concat_results (bool): Se o usuario deseja que os resultados sejam concatenados
            e retornados num novo dataframe
        backend (str): Backend paralelo. Pode ser 'loky', 'threading', 'multiprocessing'.
        plot_mode (bool): Se esta usando em modo de plot ou nao
        verbose (int): Parametro que controla verbosidade do Parallel
            (o quao ele vai printar mais)
        *args: Argumentos da funcao a ser aplicada
        **kwargs: Argumentos de palavra-chave da funcao a ser aplicada

    Returns:
        Se concat_results=True: pd.DataFrame: DataFrame com resultados calculados.
        Se False: dict: Dicionário com chave sendo o grupo e valor sendo o resultado
        da função do grupo especifico.
    """

    def temp_func(func, name, group, *args, **kwargs):
        return name, func(group, *args, **kwargs)

    if plot_mode:
        backend = "threading"
        series_par = dict(
            Parallel(n_jobs=n_jobs, backend=backend, verbose=verbose_par)(
                delayed(temp_func)(func, name, group, ax=ax, *args, **kwargs)
                for (name, group), ax in dfGrouped
            )
        )

    else:
        series_par = dict(
            Parallel(n_jobs=n_jobs, backend=backend, verbose=verbose_par)(
                delayed(temp_func)(func, name, group, *args, **kwargs)
                for name, group in dfGrouped
            )
        )

    if concat_results:
        return pd.concat(series_par, sort=True)
    else:
        return series_par
