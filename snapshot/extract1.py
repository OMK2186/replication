__author__ = 'ketankk'

import dask.dataframe as dd
import pandas as pd
df = pd.read_csv(" /File Path ")
ddf = dd.from_pandas(df, npartitions=10)


