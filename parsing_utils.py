from datetime import datetime
import os
from pathlib import Path
from io import TextIOWrapper

import pandas as pd

from utils import print_exception


def getFileExtList(isExcel, isPDF) -> list[str]:
    FILEEXT = []
    if isExcel:
        FILEEXT += [".xls", ".xlsx", ".xlsm"]
    if isPDF:
        FILEEXT += [".pdf"]
    return FILEEXT


def getFilesList(log: str, start: int, end: int, doctype: str = "выписка") -> list[str]:
    # sourcery skip: min-max-identity
    df = pd.read_csv(
        log,
        on_bad_lines="skip",
        names=[
            "status",
            "clientid",
            "filename",
            "sheets",
            "doctype",
            "filetype",
            "error",
        ],
        delimiter="|",
    )
    if end < 0:
        end = len(df)
    if start < 0:
        start = 0
    df = df.iloc[start:end]
    filelist = df["filename"][
        (df["status"] == "PROCESSED") & (df["doctype"] == doctype)
    ]
    return pd.Series(filelist).to_list()  # type: ignore


def processOther(
    inname: str, clientid: str, logf: TextIOWrapper
) -> tuple[pd.DataFrame, int, bool]:
    return (pd.DataFrame(), 0, True)


def runParsing(process_func, clientid, outname, inname, doneFolder, logf) -> int:
    filename = os.path.basename(inname)
    print(f"{datetime.now()}:START: {clientid}: {filename}")

    df, pages, berror = process_func(inname, clientid, logf)
    if not berror:
        if not df.empty:
            df.to_csv(
                outname, mode="a+", header=not Path(outname).is_file(), index=False
            )
            logstr = f"{datetime.now()}:PROCESSED: {clientid}:{filename}:{pages}:{str(df.shape[0])}:{outname}\n"
            # move2Folder(inname, doneFolder)
        else:
            berror = True
            logstr = (
                f"{datetime.now()}:EMPTY: {clientid}:{filename}:{pages}:0:{outname}\n"
            )
        logf.write(logstr)
    print(f"{datetime.now()}:DONE: {clientid}: {filename}")
    return not berror


def process_data_from_preanalysis(
    process_func,
    preanalysislog,
    logname,
    outbasename,
    bSplit,
    maxFiles,
    doneFolder,
    FILEEXT,
    start,
    end,
):
    cnt = 0
    outname = outbasename + ".csv"
    DIRPATH = "../Data"
    fileslist = getFilesList(preanalysislog, start, end)
    with open(logname, "w", encoding="utf-8", buffering=1) as logf:
        print(
            f"START:{datetime.now()}\ninput:{DIRPATH}\nlog:{logname}\noutput:{outname}\nsplit:{bSplit}\nmaxinput:{maxFiles}\ndone:{doneFolder}\nextensions:{FILEEXT}\nrange:{start}-{end}"
        )

        for fname in filter(
            lambda file: any(ext for ext in FILEEXT if (file.lower().endswith(ext))),
            fileslist,
        ):
            if Path(fname).is_file():
                parts = os.path.split(os.path.dirname(fname))
                clientid = parts[1]
                inname = fname
                try:
                    pages = 0
                    if bSplit and cnt % maxFiles == 0:
                        outname = outbasename + str(cnt) + ".csv"
                    try:
                        cnt += runParsing(
                            process_func, clientid, outname, inname, doneFolder, logf
                        )
                    except Exception as err:
                        print_exception(err, inname, clientid, f"{pages}", logf)
                except Exception as err:
                    print_exception(err, inname, clientid, "-999", logf)

    # df = pd.DataFrame(np.unique(DATATYPES))
    # df.to_csv("./data/datatypes.csv", mode="w", index = False)
