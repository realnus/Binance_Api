import numpy as np
import pandas as pd

import dask.dataframe as dd
import dask.array as da
import dask.bag as db

df = dd.read_csv('2015-*-*.csv')