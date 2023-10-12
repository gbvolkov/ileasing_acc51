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
    with open(outname, "w+", encoding="utf-8", newline="") as fout:
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


def dalyEntriesbyClient(ddf, uidsdir):
    divisions = None
    if uidsdir is not None:
        divisions = []
        for root in os.scandir(uidsdir):
            if root.is_dir():
                parts = os.path.split(root.path)
                clientid = parts[1]
                divisions.append(clientid)

    if divisions is not None:
        print("Divisions: ", len(divisions), "\n")

    print("\tset Index")
    ddf.set_index("clientID", divisions=divisions)
    print("\tIndex set")
    ddf["entryDate"] = dd.to_datetime(ddf["entryDate"], format="%d.%m.%Y", exact=True)  # type: ignore

    # print("\tgroup by")
    # dfDate = ddf.groupby("CLIENTID").aggregate(
    #    dtMin=pd.NamedAgg(column="Date", aggfunc="min"),
    #    dtMax=pd.NamedAgg(column="Date", aggfunc="max"),
    # )
    # print("\tgrouped")
    print("\tgroup by 2")
    dfDate = ddf.groupby(["clientID", ddf.entryDate.dt.year, ddf.entryDate.dt.month])[
        "entryDate"
    ].aggregate("count")
    print("\tgrouped 2")
    dfDate.columns = ["clientID", "YEAR", "MONTH", "Entries"]  # не работает!!!!
    return dfDate


def periodsGroups(ddf):
    ddf = (
        ddf.replace(r"\d", "X", regex=True)
        .replace(
            r"(\s+[Яя]нварь|[Фф]евраль|[Мм]арт|[Аа]прель|[Мм]ай|[Ии]юнь|[Ии]юль|[Аа]вгуст|[Сс]ентябрь|[Оо]ктябрь|[Нн]оябрь|[Дд]екабрь\s+)",
            "_MONTH_",
            regex=True,
        )
        .replace(
            r"(\s*г.\s*)",
            "",
            regex=True,
        )
        .replace(
            r"\s*_MONTH_\s*",
            "_MONTH_",
            regex=True
        )
    )
    return ddf.groupby(["stmtDate"])["stmtDate"].aggregate("count")


def main():
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument("-i", "--input", default="./data/DS1", help="Input folder")
    parser.add_argument(
        "-o", "--output", default="./data/result.csv", help="Resulting file"
    )
    parser.add_argument(
        "-uids", "--uids", default="../FullData", help="Folders with Clients UIDS"
    )
    parser.add_argument(
        "--transform",
        default=False,
        action=BooleanOptionalAction,
        help="Weather to replace field separator (--no-transform opposite option)",
    )
    parser.add_argument(
        "-c",
        "--csvout",
        default="./data/transformed.csv",
        help="Transformed csv file name",
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

    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore
    print(
        "START:",
        datetime.now(),
        "\ninput:",
        inname,
        "\noutput:",
        outname,
        "\nuids:",
        uidsdir,
        "\ntransform:",
        bTrans,
        "\ntranscvname:",
        transcvname,
        "\ndelimeter:",
        csv_file_delimeter,
    )

    ddf = dd.read_csv(inname + "/*.csv", blocksize=None, dtype=str)  # type: ignore

    dfRes = periodsGroups(ddf)
    # dfRes = dalyEntriesbyClient(ddf, uidsdir)

    dd.to_csv(dfRes, outname, single_file=True, encoding="utf-8")  # type: ignore

    print("Result produced")
    # if bTrans:
    #    print("Start transformation")
    #    transform_csv(transcvname, inname, csv_file_delimeter)
    # print("FINISHED")


main()
