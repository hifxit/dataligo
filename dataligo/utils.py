def which_dataframe(df):
    df_type = str(type(df)).split("'")[1]
    if df_type.startswith('pandas'):
        return 'pandas'
    elif df_type.startswith('polars'):
        return 'polars'
    elif df_type.startswith('dask'):
        return 'dask'