import pandas as pd
from pathlib import Path
import re
from datetime import datetime
import locale
import shutil
import os
import numpy
import csv
#from dask import dataframe as dd

firstrow = 0
chunk = 100000
nrows = 48331728
#csv_file_delimeter = "!~!"
csv_file_delimeter = chr(188)
inname = "./acc51parsed_excel.csv"
outname = "./transformed.csv"


#df = dd.read_csv(inname, engine="python")
#df.to_csv(outname, single_file=True, encoding="utf-8", mode='w', header_first_partition_only=True, kwargs={"sep":csv_file_delimeter, "engine":"python"})

#df = pd.read_csv(inname, iterator=True, chunksize=1000) # gives TextFileReader, which is iteratable with chunks of 1000 rows.
#partial_desc = df.describe()

with open(inname,'r', encoding="utf-8") as f:
    reader = csv.reader(f)
    headers = next(reader)
    prevrow = headers
    try :
        for row in reader :
            #print(row)
            prevrow = row
    except Exception as err:
        print(err)
        print(prevrow)

while firstrow < nrows :
    if firstrow + chunk > nrows :
        chunk = nrows-firstrow

    df = pd.read_csv(inname, skiprows=firstrow, nrows=chunk)
    if (Path(outname).is_file()) :
        df.to_csv(outname, mode="a", header=False, index=False, sep=csv_file_delimeter)
    else:
        df.to_csv(outname, mode="w", index=False, sep=csv_file_delimeter)        
    #numpy.savetxt(
    #    f,
    #    df,
    #    delimiter=csv_file_delimeter,
    #    header=csv_file_delimeter.join(df.columns.values),
    #    fmt="%s",
    #    comments="",
    #    encoding="utf-8",
    #)

    firstrow=firstrow + chunk
#f.close()