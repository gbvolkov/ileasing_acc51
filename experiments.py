from enum import Enum
from typing import Any
import pandas as pd
from pathlib import Path

sss= "_".join([str(x) for x in range(1, 11)])

df = pd.read_excel('./data/test.xlsx')

lastcolidx = df.columns[-1]
rowswithlastcolumn = df.dropna(subset=[df.columns[-3], df.columns[-2], df.columns[-1]], how='all')
df2 = rowswithlastcolumn

idx = df2.isna().sum(axis=1).idxmin()
idx2 = df2.isna().sum(axis=1).idxmax()


row = df.iloc[1:2]

df.to_csv('./data/test.csv', mode="w", header=True, index=False)
