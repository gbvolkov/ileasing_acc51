from daterange_utils import get_date_range, normalise_range_str
import pandas as pd
import re
import os
from datetime import datetime, timedelta
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


def regexTest():
    headarr = [
        "Карточка счета 51 за  Февраль 2023-Апрель 2023",
        "Карточка счета 51 за 01.10.2023 - 01.12.2023",
        "Карточка счета 51 за 2023",
        "Карточка счета 51 за 4 квартал 2023",
        "Карточка счета 51 за Ноябрь 2023",
        "Период: с 10.01.2023 по 10.12.2023",
        "Карточка счета 51 за период с 10.01.2023 по 10.12.2023",
        "Период: апрель 2023 -март 2023",
        "Карточка счета 51 за 1/1/2020 - 31/1/2023",
        "Карточка счета 51 с 01.12.2023 по 31.12.2023",
    ]

    headarr = [normalise_range_str(head) for head in headarr]
    dates = [get_date_range(head) for head in headarr]
    print(dates)
    # for head in headarr:
    #    head = re.sub(r"Карточка\s+счета\s+.*\s+за\s+", "", head)
    #    head = re.sub(r"Карточка\s+счета\s+.*\s+с\s+", "", head)
    #    head = re.sub(r"[Пп]ериод[:]?\s*", "", head)
    #    head = re.sub(r"\d", "X", head)
    #    head = re.sub(r"\s+по\s+", "-", head)
    #    head = re.sub(r"\s*-\s*", "-", head)
    #    head = re.sub(
    #        r"([\s]*(январ[ья]|[Фф]еврал[ья]|[Мм]арт[а]*|[Аа]прел[ья]|[Мм]а[йя]|[Ии]юн[ья]|[Ии]юл[ья]|[Аа]вгуст[а]*|[Сс]ентябр[ья]|[Оо]ктябр[ья]|[Нн]оябр[ья]|[Дд]екабр[ья])[\s]*)",
    #        "_MONTH_",
    #        head,
    #    )
    #    head = re.sub(r"(\s*г.\s*)", "", head)
    #    head = re.sub(r"\s*_MONTH_\s*", "_MONTH_", head)
    #    head = re.sub(r"/", ".", head)
    #    head = re.sub(r"с\s*", "", head)
    #    print(head)
    #
    return


def periodsGroups(ddf):
    ddf = (
        ddf.replace(r"Карточка\s+счета\s+.*\s+за\s+", "", regex=True)
        .replace(r"Карточка\s+счета\s+.*\s+с\s+", "", regex=True)
        .replace(r"[Пп]ериод[:]?\s*", "", regex=True)
        .replace(r"\d", "X", regex=True)
        .replace(r"\s+по\s+", "-", regex=True)
        .replace(r"\s*-\s*", "-", regex=True)
        .replace(
            r"([\s]*([Яя]нвар[ья]|[Фф]еврал[ья]|[Мм]арт[а]?|[Аа]прел[ья]|[Мм]а[йя]|[Ии]юн[ья]|[Ии]юл[ья]|[Аа]вгуст[а]?|[Сс]ентябр[ья]|[Оо]ктябр[ья]|[Нн]оябр[ья]|[Дд]екабр[ья])[\s]*)",
            "_MONTH_",
            regex=True,
        )
        .replace(r"(\s*г\.\s*)", "", regex=True)
        .replace(r"\s*_MONTH_\s*", "_MONTH_", regex=True)
        .replace(r"/", ".", regex=True)
        .replace(r"с\s+", "", regex=True)
    )
    return ddf.groupby(["stmtDate"])["stmtDate"].aggregate("count")


def main():
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument("-i", "--input", default="./data/DS1", help="Input folder")
    parser.add_argument(
        "-o", "--output", default="./data/res.csv", help="Resulting file"
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
    # regexTest()

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
