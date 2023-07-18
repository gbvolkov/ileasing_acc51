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
    except:
        return datetime.now()


def load_df(filename, skiprows, nrows, columns):
    print(f"\f{skiprows}-{skiprows+nrows}")
    df = pd.read_csv(
        filename, skiprows=skiprows, nrows=nrows, dtype=str, names=columns
    )  # , converters={'Date': formatDate})
    return df


def transformDF(df):
    df["Date"] = df.apply(
        lambda dt: datetime.strptime(re.sub("[-/]", ".", dt.Date), "%d.%m.%Y"), axis=1
    )
    return df


def main():
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        "-i", "--input", default="./acc51parsed_excel.csv", help="Input file"
    )
    parser.add_argument(
        "-n",
        "--nrows",
        default=48098106,
        type=int,
        help="Number of rows. If 0 it will be calculated",
    )
    parser.add_argument("-o", "--output", default="./result.csv", help="Input file")
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
    nrows = args["nrows"]
    uidsdir = args["uids"]
    chunk = 100000
    bTrans = args["transform"]
    transcvname = args["csvout"]
    csv_file_delimeter = args["delimeter"]  # chr(188)

    sys.stdout.reconfigure(encoding="utf-8")
    print(
        "START:",
        datetime.now(),
        "\ninput:", inname,
        "\noutput:", outname,
        "\nnrows:", nrows,
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
    if nrows == 0:
        with open(inname, "r", encoding="utf-8") as fin:
            reader = csv.reader(fin)
            for row in reader:
                nrows = nrows + 1

    # nrows = 49909689 #(full)
    # nrows = 1811583 #(pdf)
    # nrows = 48098106 #(excel)
    print("NRows: ", nrows, "\n")
    if divisions is not None :
        print("Divisions: ", len(divisions), "\n")

    metadf = pd.read_csv(inname, skiprows=0, nrows=0, 
                         dtype={"Date": "datetime64[ns]",
                                "Document" : str,
                                "Debet_Analitics" : str,
                                "Credit_Analitics" : str,
                                "Debet_Account" : str,
                                "Debet_Amount" : str,
                                "Credit_Account" : str,
                                "Credit_Amount" : str,
                                "Balance_D" : str,
                                "Balance" : str,
                                "Company_Name" : str,
                                "Start" : str,
                                "Finish" : str,
                                "OpenD" : str,
                                "OpenBalance" : str,
                                "file" : str,
                                "processdate" : str,
                                "CLIENTID" : str,
                                "SUBSET" : str,
                                "Result" : str})
    #ddf = dd.from_delayed(
    #    [
    #        delayed(load_df)(
    #            inname, firstrow, min(nrows - firstrow, chunk)-1, metadf.columns
    #        )
    #        for firstrow in range(1, nrows, chunk)
    #    ],
    #    meta=metadf,
    #    verify_meta=False,
    #)
    #ddf = dd.read_csv('./DataSplit.full/*.csv', blocksize=None, header=0, dtype=str, names=metadf.columns)
    ddf = dd.read_csv('./DataSplit/*.csv', blocksize=None, dtype=str)
    print("\tset Index")
    ddf.set_index("CLIENTID", divisions = divisions)
    print("\tIndex set")
    # ddf[['Date']] = ddf[['Date']].apply(lambda dt: datetime.strptime(re.sub('[-/]','.', dt.values[0]), "%d.%m.%Y"), axis=1, meta={'Date':'datetime64[ns]'})
    # ddf[['dtDate']] = ddf.map_partitions(transformDF, meta={'Date':'datetime64[ns]'})
    # ddf = ddf.assign(dtDate=lambda x: datetime.strptime(re.sub('[-/]','.', x.Date), "%d.%m.%Y"))
    
    #ddt = ddf.get_partition(0).compute().iloc[35:45]["Date"]

    ddf["Date"] = dd.to_datetime(ddf["Date"], format="%d.%m.%Y", exact=True)

    print("\tgroup by")
    dfDate = ddf.groupby("CLIENTID").aggregate(
        dtMin=pd.NamedAgg(column="Date", aggfunc="min"),
        dtMax=pd.NamedAgg(column="Date", aggfunc="max"),
    )
    print("\tgrouped")

    # dd.to_csv(dfDate, outname, single_file=True, encoding='utf-8')
    dd.to_csv(dfDate, outname, single_file=False, encoding="utf-8")

    print("Result produced")
    if bTrans:
        print("Start transformation")
        transform_csv(transcvname, inname, csv_file_delimeter)
    print("FINISHED")


main()
