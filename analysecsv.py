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


QUARTS={
    1: (1,3)
    ,2: (4,6)
    ,3: (7,9)
    ,4: (10,12)
}
MONTHS={
    'январь': 1
    ,'февраль': 2
    ,'март': 3
    ,'апрель': 4
    ,'май': 5
    ,'июнь': 6
    ,'июль': 7
    ,'август': 8
    ,'сентябрь': 9
    ,'октябрь': 10
    ,'ноябрь': 11
    ,'декабрь': 12
}

import types
rt = types.SimpleNamespace()
rt.RE_MONTHRANGE=r"([Яя]нвар[ья]|[Фф]еврал[ья]|[Мм]арт[а]*|[Аа]прел[ья]|[Мм]а[йя]|[Ии]юн[ья]|[Ии]юл[ья]|[Аа]вгуст[а]*|[Сс]ентябр[ья]|[Оо]ктябр[ья]|[Нн]оябр[ья]|[Дд]екабр[ья])\s*(\d{4})\s*-\s*(январ[ья]|[Фф]еврал[ья]|[Мм]арт[а]*|[Аа]прел[ья]|[Мм]а[йя]|[Ии]юн[ья]|[Ии]юл[ья]|[Аа]вгуст[а]*|[Сс]ентябр[ья]|[Оо]ктябр[ья]|[Нн]оябр[ья]|[Дд]екабр[ья])\s*(\d{4})"
rt.RE_MONTH=r"([Яя]нвар[ья]|[Фф]еврал[ья]|[Мм]арт[а]*|[Аа]прел[ья]|[Мм]а[йя]|[Ии]юн[ья]|[Ии]юл[ья]|[Аа]вгуст[а]*|[Сс]ентябр[ья]|[Оо]ктябр[ья]|[Нн]оябр[ья]|[Дд]екабр[ья])\s*(\d{4})"
rt.RE_DATERANGE=r"(\d{1,2})[.-/](\d{1,2})[.-/](\d{4})\s*-\s*(\d{1,2})[.-/](\d{1,2})[.-/](\d{4})"
rt.RE_YEAR=r"(\d{4})"
rt.RE_QUART=r"(\d{1})\s*квартал\s*(\d{4})"

class REqual(str):
    "Override str.__eq__ to match a regex pattern."
    def __eq__(self, pattern):
        return re.fullmatch(pattern, self)

def get_last_day_of_month(year: int, mon: int)->datetime:
    nextmon = datetime(year, mon, 28) + timedelta(days=4)
    return nextmon - timedelta(days=nextmon.day)    

def get_date_range(headerstr: str) -> list[str]:
    dtrange = []
    match REqual(headerstr):
        case rt.RE_MONTHRANGE:
            dtrange = re.findall(rt.RE_MONTHRANGE, headerstr, re.MULTILINE)
            monfrom=MONTHS[dtrange[0][0].lower()]
            yearfrom=int(dtrange[0][1])
            monto=MONTHS[dtrange[0][2].lower()]
            yearto=int(dtrange[0][3])
            datefrom = datetime(yearfrom, monfrom, 1)
            nextmon = datetime(yearto, monto, 28) + timedelta(days=4)
            dateto = nextmon - timedelta(days=nextmon.day)
            dtrange = [datefrom.strftime("%d-%m-%Y"), dateto.strftime("%d-%m-%Y")]
            print(dtrange)
        case rt.RE_MONTH:
            dtrange = re.findall(rt.RE_MONTH, headerstr, re.MULTILINE)
            monfrom=MONTHS[dtrange[0][0].lower()]
            yearfrom=int(dtrange[0][1])
            datefrom = datetime(yearfrom, monfrom, 1)
            nextmon = datetime(yearfrom, monfrom, 28) + timedelta(days=4)
            dateto = nextmon - timedelta(days=nextmon.day)
            dtrange = [datefrom.strftime("%d-%m-%Y"), dateto.strftime("%d-%m-%Y")]
            print(dtrange)
        case rt.RE_DATERANGE:
            dtrange = re.findall(rt.RE_DATERANGE, headerstr, re.MULTILINE)
            dayfrom=int(dtrange[0][0])
            monfrom=int(dtrange[0][1])
            yearfrom=int(dtrange[0][2])
            dayto=int(dtrange[0][3])
            monto=int(dtrange[0][4])
            yearto=int(dtrange[0][5])
            datefrom = datetime(yearfrom, monfrom, dayfrom)
            dateto = datetime(yearto, monto, dayto)
            dtrange = [datefrom.strftime("%d-%m-%Y"), dateto.strftime("%d-%m-%Y")]
            print(dtrange)
        case rt.RE_YEAR:
            dtrange = re.findall(rt.RE_YEAR, headerstr, re.MULTILINE)
            year=int(dtrange[0][0])
            datefrom = datetime(year, 1, 1)
            dateto = datetime(year, 12, 31)
            dtrange = [datefrom.strftime("%d-%m-%Y"), dateto.strftime("%d-%m-%Y")]
            print(dtrange)
        case rt.RE_QUART:
            dtrange = re.findall(rt.RE_QUART, headerstr, re.MULTILINE)
            quart=int(dtrange[0][0])
            year=int(dtrange[0][1])
            monfrom = QUARTS[quart][0]
            monto = QUARTS[quart][1]
            datefrom = datetime(year, monfrom, 1)
            nextmon = datetime(year, monto, 28) + timedelta(days=4)
            dateto = nextmon - timedelta(days=nextmon.day)
            dtrange = [datefrom.strftime("%d-%m-%Y"), dateto.strftime("%d-%m-%Y")]
            print(dtrange)
        case _:
            dtrange = [headerstr]
    return dtrange


def normalise_range_str(rangestr: str) -> str:
    rangestr = re.sub(r"Карточка\s+счета\s+.*\s+за\s+", "", rangestr)
    rangestr = re.sub(r"Карточка\s+счета\s+.*\s+с\s+", "", rangestr)
    rangestr = re.sub(r"[Пп]ериод[:]?\s*", "", rangestr)
    #rangestr = re.sub(r"\d", "X", rangestr)
    rangestr = re.sub(r"\s+по\s+", "-", rangestr)
    rangestr = re.sub(r"\s*-\s*", "-", rangestr)
    #rangestr = re.sub(
    #    r"([\s]*([Яя]нвар[ья]|[Фф]еврал[ья]|[Мм]арт[а]*|[Аа]прел[ья]|[Мм]а[йя]|[Ии]юн[ья]|[Ии]юл[ья]|[Аа]вгуст[а]*|[Сс]ентябр[ья]|[Оо]ктябр[ья]|[Нн]оябр[ья]|[Дд]екабр[ья])[\s]*)",
    #    "_MONTH_",
    #    rangestr,
    #)
    rangestr = re.sub(r"(\s*г.\s*)", "", rangestr)
    rangestr = re.sub(r"/", ".", rangestr)
    rangestr = re.sub(r"с\s*", "", rangestr)
    return rangestr


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
    #for head in headarr:
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
        .replace(r"(\s*г.\s*)", "", regex=True)
        .replace(r"\s*_MONTH_\s*", "_MONTH_", regex=True)
        .replace(r"/", ".", regex=True)
        .replace(r"с\s*", "", regex=True)
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
    regexTest()

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
