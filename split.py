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
from os.path import exists as file_exists


def formatDate(dtStr):
    try:
        newstr = re.sub("[-/]", ".", dtStr)
        return datetime.strptime(newstr, "%d.%m.%Y")
    except Exception:
        return datetime.now()


def split_csv(outname, inname):
    fout = None
    with open(inname, "r", encoding="utf-8") as fin:
        reader = csv.reader(fin)
        header = next(reader)
        clididx = header.index("clientID")
        dtidx = header.index("entryDate")

        curclient = ""
        writer = None
        bWriteHeader = False
        for row in reader:
            row[dtidx] = datetime.strftime(formatDate(row[dtidx]), "%d.%m.%Y")
            clientid = row[clididx]
            if curclient != clientid:
                curclient = clientid
                if fout is not None:
                    fout.close()
                outfilename = outname + "/" + curclient + ".csv"
                bWriteHeader = not file_exists(outfilename)
                fout = open(outfilename, "a+", encoding="utf-8", newline="")
                writer = csv.writer(fout)
            if writer is not None:
                if bWriteHeader:
                    bWriteHeader = False
                    writer.writerow(header)
                writer.writerow(row)


def main():
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        "-i", "--input", default="./data/parsed_51_excel.csv", help="Input file"
    )
    parser.add_argument(
        "-o",
        "--output",
        default="./data/DataSplit",
        help="Output folder. No ending delimeter",
    )
    args = vars(parser.parse_args())

    inname = args["input"]
    outname = args["output"]

    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore
    print(
        "START:",
        datetime.now(),
        "\ninput:",
        inname,
        "\noutput:",
        outname,
    )

    print("Start transformation")
    split_csv(outname, inname)
    print("FINISHED")


main()
