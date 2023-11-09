from datetime import datetime
import os
from pathlib import Path
from io import TextIOWrapper
import re

import pandas as pd
import numpy as np
from const import REGEX_ACCOUNT, REGEX_AMOUNT, REGEX_BIC, REGEX_INN

from utils import print_exception


def get_header_values(header: str, signature: str) -> dict:
    hdr = re.sub(
        r"[\n\.\,\(\)\/\-\|]",
        "",
        re.sub(r"№", "n", re.sub(r"\s+", "", re.sub(r"ё", "е", header))),
    ).lower()
    eoh = hdr.find(signature[:8].replace("|", ""))
    if eoh != -1:
        header = header[: int(len(header) * eoh / len(hdr))]
    account = re.search(REGEX_ACCOUNT, header)
    account = account.group() if account else ""
    bic = re.search(REGEX_BIC, header)
    bic = bic.group() if bic else ""
    inn = re.search(REGEX_INN, header)
    inn = inn.group() if inn else ""
    amount = re.search(REGEX_AMOUNT, header)
    amount = amount.group() if amount else ""
    return {
        "header": header,
        "bic": bic,
        "account": account,
        "inn": inn,
        "amount": amount,
    }


# sets to_ignore to True, if entrDate is empty
def cleanup_and_enreach_processed_data(
    df: pd.DataFrame,
    inname: str,
    clientid: str,
    params: dict,
    sheet: str,
    function_name: str,
) -> pd.DataFrame:
    # cleanup
    df = df[df.entryDate.notna()]
    df.entryDate = df.entryDate.astype(str).replace(r"[\s\n]", "", regex=True)
    # df = df[df.entryDate.astype(str).str.contains(r"^(?:\d{1,2}[\,\.\-\/]\d{1,2}[\,\.\-\/]\d{2,4}|\d{2,4}[\,\.\-\/]\d{1,2}[\,\.\-\/]\d{1,2}[\w ]*)", regex=True)]
    df.loc[
        df.entryDate.astype(str).str.contains(
            r"^(?:\d{1,2}[\,\.\-\/]\d{1,2}[\,\.\-\/]\d{2,4}|\d{2,4}[\,\.\-\/]\d{1,2}[\,\.\-\/]\d{1,2}[\w ]*)",
            regex=True,
            na=False,
        )
        == False,
        "result",
    ] = 1

    # enreach
    df["__hdrclientTaxCode"] = params["inn"]
    df["__hdrclientBIC"] = params["bic"]
    df["__hdrclientAcc"] = params["account"]
    df["__hdropenBalance"] = params["amount"]
    df["__header"] = params["header"]

    df["clientID"] = clientid
    df["filename"] = f"{inname}_{sheet}"
    df["function"] = function_name
    df["processdate"] = datetime.now()

    return df


# removes rows, containing 1, 2, 3, 4, 5, ... (assuming that is just rows with columns numbers, which should be ignored)
def cleanup_raw_data(df: pd.DataFrame) -> pd.DataFrame:
    ncols = len(df.columns)
    row2del = "_".join([str(x) for x in range(1, ncols + 1)])

    df["__rowval"] = (
        pd.Series(
            df.fillna("").astype(str).replace(r"\s+", "", regex=True).values.tolist()
        )
        .str.join("_")
        .values
    )
    df = df[df.__rowval != row2del]

    return df.drop("__rowval", axis=1)


def set_data_columns(df) -> pd.DataFrame:
    header1 = df.iloc[0]
    header1 = header1.mask(header1 == "").fillna(method="ffill").fillna("").astype(str)
    header1 = (
        header1.str.lower()
        .replace("\n", "")
        .replace(r"\s+", "", regex=True)
        .replace(r"ё", "е", regex=True)
        .fillna("")
        .astype(str)
    )
    datastart = 1

    if len(df.axes[0]) > 1 and df.iloc[1].mask(df.iloc[1] == "").isnull().iloc[0]:
        header2 = df.iloc[1].fillna("").astype(str)
        header2 = (
            header2.str.lower()
            .replace("\n", "")
            .replace(r"\s+", "", regex=True)
            .replace(r"ё", "е", regex=True)
            .replace(r"\d+\.?\d*", "", regex=True)
            .fillna("")
            .astype(str)
        )
        header = pd.concat([header1, header2], axis=1).apply(
            lambda x: ".".join([y for y in x if y]), axis=1
        )
        datastart = 2
    else:
        header = header1
    df = df[datastart:]
    df.columns = (
        header.str.lower()
        .replace(r"[\n\.\,\(\)\/\-]", "", regex=True)
        .replace(r"№", "n", regex=True)
        .replace(r"\s+", "", regex=True)
        .replace(r"ё", "е", regex=True)
        .mask(header1 == "")
        .fillna("column")
    )
    cols = pd.Series(df.columns)
    for dup in cols[cols.duplicated()].unique():
        cols[cols[cols == dup].index.values.tolist()] = [
            dup + "." + str(i) if i != 0 else dup for i in range(sum(cols == dup))
        ]
    df.columns = cols
    ncols = len(df.columns)
    return df[df[list(df.columns)].isnull().sum(axis=1) < ncols * 0.8].dropna(
        axis=0, how="all"
    )


"""
Берём первые пятьдесят строк
Сливаем каждую строку со следующей
В результирующем датасете ищем первую строку с минимальным количеством нулов
"""
def clean_data(row):
    return (
        row.replace("\n", "")
        .replace(r"\s+", "", regex=True)
        .replace(r"\d+\.?\d*", "", regex=True)
        .fillna("")
        .astype(str)
    )

def find_header_row(df: pd.DataFrame) -> tuple[int, int, list[int]]:
    cols_full = df.replace(r'^\s*$', np.nan, regex=True).count()
    df_head = df.iloc[:50].fillna("").astype(str)
    df_head = df_head.apply(clean_data)
    maxnotna = df_head.mask(df_head == "").notna().sum(axis=1).max()
    min_count = int((50.0 * maxnotna / 100) + 1)

    results = []
    for idx in range(len(df_head.index) - 1):
        header1 = df_head.iloc[idx]
        if header1.mask(header1 == "").notna().sum() >= min_count:
            cols_prev = df.iloc[:idx].replace(r'^\s*$', np.nan, regex=True).count()
            cols = cols_full-cols_prev
            header2 = df_head.iloc[idx + 1]
            header = pd.concat([header1, header2, cols], axis=1).apply(
                lambda x: ".".join(
                    [
                        y
                        for y in x[:2]
                        if y
                        or not x.iloc[0]
                        and not x.iloc[1]
                        and x.iloc[2] > 0
                    ]
                ),
                axis=1,
            )
            cnas = header.mask(header == "").isna().sum()
            rowidx = df_head.iloc[idx : idx + 1].index[0]
            results.append({"_idx": rowidx, "_cnas": cnas, "_header": header})

    result = pd.DataFrame(results)
    min_cnas_idx = result._cnas.idxmin()
    result = result.loc[min_cnas_idx]
    header = result._header
    ncols = header.mask(header == "").notna().sum()
    return (
        result._idx,
        ncols,
        header.mask(header == "").dropna().index.to_list(),
    )


def remove_nan_columns(df: pd.DataFrame, threshold: float) -> pd.DataFrame:
    return df.loc[:, df.isnull().mean() < threshold]

def find_last_row_index(df: pd.DataFrame, firstrowidx: int, nheadercols: int) -> int:
    for idx in range(len(df.index) - 1, df.index.get_loc(firstrowidx), -1):
        cnavalues = df.iloc[idx].isnull().sum()
        if (nheadercols - cnavalues) * 100 / nheadercols > 47 and not all(df.iloc[idx][nheadercols - 3 :].isnull()):
            return df.iloc[idx : idx + 1].index[0] # type: ignore
    return firstrowidx + 1

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    return df.dropna(axis=1, how="all").dropna(axis=0, how="all")

def get_table_range(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    lastrow = len(df.index) - 1
    firstrowidx, nheadercols, headercols = find_header_row(df)

    if nheadercols > 0:
        df = remove_nan_columns(df, 0.9)
        lastrowidx = find_last_row_index(df, firstrowidx, nheadercols)
        header = clean_dataframe(df.loc[: firstrowidx - 1])
        footer = clean_dataframe(df.loc[lastrowidx + 1 :])
        null_cols = df.loc[firstrowidx:lastrowidx, headercols].isna().all()
        headercols = [headercols[colno] for colno, ifna in enumerate(null_cols) if not ifna]
        df = df.loc[firstrowidx:, headercols]
        data = clean_dataframe(df)
    else:
        header = pd.DataFrame()
        data = pd.DataFrame()
        footer = pd.DataFrame()
    return header, data, footer


def get_file_ext_list(isExcel, isPDF) -> list[str]:
    FILEEXT = []
    if isExcel:
        FILEEXT += [".xls", ".xlsx", ".xlsm"]
    if isPDF:
        FILEEXT += [".pdf"]
    return FILEEXT


def getFilesList(log: str, start: int, end: int, doctype: list[str] = None) -> list[str]: # type: ignore
    if doctype is None:
        doctype = ["выписка"]
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
        (df["status"] == "PROCESSED") & (df["doctype"].isin(doctype))
    ]
    return pd.Series(filelist).to_list()  # type: ignore


def process_other(
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
    doctype = None
):
    cnt = 0
    outname = outbasename + ".csv"
    DIRPATH = "../Data"
    fileslist = getFilesList(preanalysislog, start, end, doctype) # type: ignore
    with open(logname, "w", encoding="utf-8", buffering=1) as logf:
        print(
            f"START:{datetime.now()}\ninput:{preanalysislog}\nlog:{logname}\noutput:{outname}\nsplit:{bSplit}\nmaxinput:{maxFiles}\ndone:{doneFolder}\nextensions:{FILEEXT}\nrange:{start}-{end}\ndocTypes={doctype}"
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
