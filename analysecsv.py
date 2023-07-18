import pandas as pd
import re
import os
from datetime import datetime
import csv
from dask import dataframe as dd
from dask.delayed import delayed
from argparse import (
    ArgumentParser,
    ArgumentDefaultsHelpFormatter,
    BooleanOptionalAction,
)
import sys


def transform_csv(outname, inname, csv_file_delimeter):
    with open(outname, "w+", encoding="utf-8", newline='') as fout:
        with open(inname, "r", encoding="utf-8") as fin:
            reader = csv.reader(fin)
            writer = csv.writer(fout, delimiter=csv_file_delimeter)
            headers = next(reader)
            ncols = len(headers)
            writer.writerow(headers)
            prevrow = headers
            try:
                for row in reader:
                    writer.writerow(row)
                    if len(row) != ncols:
                        print(row)
                    # if i%40309 == 0 :
                    # i = i+1
                    prevrow = row
            except Exception as err:
                print(err)
                print(prevrow)


def formatDate(dtStr):
    try:
        newstr = re.sub("[-/]", ".", dtStr)
        return datetime.strptime(newstr, "%d.%m.%Y")
    except Exception:
        return datetime.now()


def load_df(filename, skiprows, nrows, columns):
    print(f"\f{skiprows}-{skiprows+nrows}")
    return pd.read_csv(
        filename, skiprows=skiprows, nrows=nrows, dtype=str, names=columns
    )


def transformDF(df):
    df["Date"] = df.apply(
        lambda dt: datetime.strptime(re.sub("[-/]", ".", dt.Date), "%d.%m.%Y"), axis=1
    )
    return df


def main():
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        "-i", "--input", default="../DataSplit", help="Input folder"
    )
    parser.add_argument("-o", "--output", default="./result.csv", help="Resulting file")
    parser.add_argument("-uids", "--uids", default="../RawData", help="Folders with Clients UIDS")
    parser.add_argument(
        "--transform",
        default=False,
        action=BooleanOptionalAction,
        help="Weather to replace field separator (--no-transform opposite option)",
    )
    parser.add_argument(
        "-c", "--csvout", default="./transformed.csv", help="Transformed csv file name"
    )
    parser.add_argument(
        "-d",
        "--delimeter",
        default=chr(188),
        help="New transformed csv filed delimeter",
    )
    args = vars(parser.parse_args())

    inname = args["input"]
    outname = args["output"]
    uidsdir = args["uids"]
    bTrans = args["transform"]
    transcvname = args["csvout"]
    csv_file_delimeter = args["delimeter"]  # chr(188)

    sys.stdout.reconfigure(encoding="utf-8") # type: ignore
    print(
        "START:",
        datetime.now(),
        "\ninput:", inname,
        "\noutput:", outname,
        "\nuids:", uidsdir,
        "\ntransform:", bTrans,
        "\ntranscvname:", transcvname,
        "\ndelimeter:", csv_file_delimeter,
    )

    divisions = None
    if uidsdir is not None:
        divisions = []
        for root in os.scandir(uidsdir) :
            if root.is_dir() :
                parts = os.path.split(root.path)
                clientid = parts[1]
                divisions.append(clientid)

    if divisions is not None :
        print("Divisions: ", len(divisions), "\n")

    ddf = dd.read_csv(inname + '/*.csv', blocksize=None, dtype=str) # type: ignore
    print("\tset Index")
    ddf.set_index("CLIENTID", divisions = divisions)
    print("\tIndex set")
    ddf["Date"] = dd.to_datetime(ddf["Date"], format="%d.%m.%Y", exact=True) # type: ignore

    #print("\tgroup by")
    #dfDate = ddf.groupby("CLIENTID").aggregate(
    #    dtMin=pd.NamedAgg(column="Date", aggfunc="min"),
    #    dtMax=pd.NamedAgg(column="Date", aggfunc="max"),
    #)
    #print("\tgrouped")
    print("\tgroup by 2")
    dfDate = ddf.groupby(["CLIENTID", ddf.Date.dt.year, ddf.Date.dt.month])["Date"].aggregate("count")
    print("\tgrouped 2")
    dfDate.columns=["CLIENTID", "YEAR", "MONTH", "Entries"] #не рабоает!!!!


    dd.to_csv(dfDate, outname, single_file=True, encoding="utf-8") # type: ignore

    print("Result produced")
    if bTrans:
        print("Start transformation")
        transform_csv(transcvname, inname, csv_file_delimeter)
    print("FINISHED")

main()
