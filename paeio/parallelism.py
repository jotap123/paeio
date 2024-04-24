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
    Function that implements pandas apply using a parallelizable backend.

    Args:
        dfGrouped (pd.DataFrame or zip): Pandas groupby object
            or zip(pandas groupby object + matplotlib axes) for plot_mode=True
        func: User-defined function
        n_jobs: Number of jobs to be used in function calculation
        concat_results (bool): Whether the user wants the results to be concatenated
            and returned in a new dataframe
        backend (str): Parallel backend. Can be 'loky', 'threading', 'multiprocessing'.
        plot_mode (bool): Whether it is being used in plot mode or not
        verbose (int): Parameter controlling verbosity of Parallel
            (how much it will print)
        *args: Function arguments to be applied
        **kwargs: Function keyword arguments to be applied

    Returns:
        If concat_results=True: pd.DataFrame: DataFrame with calculated results.
        If False: dict: Dictionary with key being the group and value being the result
        of the specific group's function.
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
