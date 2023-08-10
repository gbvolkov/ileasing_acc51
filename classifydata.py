from enum import Enum
from typing import Any
import pandas as pd
from pathlib import Path
import re
from datetime import datetime
import locale
import shutil
import os
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter, BooleanOptionalAction
import sys
from io import TextIOWrapper
import numpy as np
import csv
import re
from BankStatement_NO_process import NoneHDR_process, DATATYPES

from excelutils import getExcelSheetKind
from pdfutils import getHeadLinesPDF, getPDFData

#using this (type: ignore) since camelot does not have stubs
from const import COLUMNS, DOCTYPES, HDRSIGNATURES, REGEX_ACCOUNT, REGEX_AMOUNT, REGEX_BIC, REGEX_INN
from BankStatement_1_process import BankStatement_1_process
from BankStatement_2_process import BankStatement_2_process
from BankStatement_3_process import BankStatement_3_process
from BankStatement_4_process import BankStatement_4_process
from BankStatement_5_process import BankStatement_5_process
from BankStatement_6_process import BankStatement_6_process
from BankStatement_7_process import BankStatement_7_process
from BankStatement_8_process import BankStatement_8_process
from BankStatement_9_process import BankStatement_9_process
from BankStatement_10_process import BankStatement_10_process
from BankStatement_11_process import BankStatement_11_process
from BankStatement_12_process import BankStatement_12_process
from BankStatement_13_process import BankStatement_13_process
from BankStatement_14_process import BankStatement_14_process
from BankStatement_15_process import BankStatement_15_process
from BankStatement_16_process import BankStatement_16_process
from BankStatement_17_process import BankStatement_17_process
from BankStatement_18_process import BankStatement_18_process
from BankStatement_19_process import BankStatement_19_process
from BankStatement_20_process import BankStatement_20_process
from BankStatement_21_process import BankStatement_21_process
from BankStatement_22_process import BankStatement_22_process
from BankStatement_23_process import BankStatement_23_process
from BankStatement_24_process import BankStatement_24_process
from BankStatement_25_process import BankStatement_25_process
from BankStatement_26_process import BankStatement_26_process
from BankStatement_27_process import BankStatement_27_process
from BankStatement_28_process import BankStatement_28_process
from BankStatement_29_process import BankStatement_29_process
from BankStatement_30_process import BankStatement_30_process
from BankStatement_31_process import BankStatement_31_process
from BankStatement_32_process import BankStatement_32_process
from BankStatement_33_process import BankStatement_33_process
from BankStatement_34_process import BankStatement_34_process
from BankStatement_35_process import BankStatement_35_process
from BankStatement_36_process import BankStatement_36_process
from BankStatement_37_process import BankStatement_37_process
from BankStatement_38_process import BankStatement_38_process
from BankStatement_39_process import BankStatement_39_process
from BankStatement_40_process import BankStatement_40_process
from BankStatement_41_process import BankStatement_41_process
from BankStatement_42_process import BankStatement_42_process
from BankStatement_43_process import BankStatement_43_process
from BankStatement_44_process import BankStatement_44_process
from BankStatement_45_process import BankStatement_45_process
from BankStatement_46_process import BankStatement_46_process
from BankStatement_47_process import BankStatement_47_process
from BankStatement_48_process import BankStatement_48_process
from BankStatement_49_process import BankStatement_49_process
from BankStatement_50_process import BankStatement_50_process
from BankStatement_51_process import BankStatement_51_process
from BankStatement_52_process import BankStatement_52_process
from BankStatement_53_process import BankStatement_53_process
from BankStatement_54_process import BankStatement_54_process
from BankStatement_55_process import BankStatement_55_process
from BankStatement_56_process import BankStatement_56_process
from BankStatement_57_process import BankStatement_57_process
from BankStatement_58_process import BankStatement_58_process
from BankStatement_59_process import BankStatement_59_process
from BankStatement_60_process import BankStatement_60_process

"""
Берём первые пятдесят строк
Сливаем каждую строку со следующей
В результирующем датасете ищем первую строку с минимальным количеством нулов
"""
def findHeaderRow(df: pd.DataFrame) -> tuple[int, int, list[int]]:
    df = df.iloc[:50].fillna("").astype(str)
    result = pd.DataFrame(columns=["_idx", "_cnas", "_header"])
    axis=0
    # Delete rows containing either 60% or more than 60% NaN Values
    perc = 50.0 
    df = df.replace('\n', '').replace(r'\s+', '', regex=True).replace(r'\d+\.?\d*', '', regex=True).fillna("").astype(str)
    maxnotna = df.mask(df == '').notna().sum(axis=1).max()
    min_count =  int((perc*maxnotna/100) + 1)

    for idx in range(len(df.index)-1):
        
        header1 = df.iloc[idx].replace('\n', '').replace(r'\s+', '', regex=True).replace(r'\d+\.?\d*', '', regex=True).fillna("").astype(str)
        if header1.mask(header1 == '').notna().sum() >= min_count:
            header2 = df.iloc[idx+1].fillna("").astype(str)
            header = pd.concat([header1, header2], axis=1).apply(
                lambda x: '.'.join([y for y in x if y]), axis=1)
            cnas = header.mask(header == '').isna().sum()
            rowidx = df.iloc[idx:idx+1].index[0]
            result = pd.concat([result, pd.DataFrame([{"_idx": rowidx, "_cnas": cnas, "_header": header}])])
    result = result[result._idx==result[result._cnas==result._cnas.min()]._idx.min()]
    if result._header.size > 0:
        header = result._header[0]
        ncols = header.mask(header=='').notna().sum()
    else:
        header = pd.DataFrame()
        ncols = 0
    return result[result._cnas==result._cnas.min()]._idx.min(), ncols, header.mask(header=='').dropna().index.to_list()


def getTableRange(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    lastrow = len(df.index)-1
    ncols = len(df.columns)
    footer = pd.DataFrame()

    firstrowidx, nheadercols, headercols = findHeaderRow(df)

    if nheadercols > 0:
        #Удаляем из хвоста все столбцы, где больше 90% значений NaN
        partialColumns = (df.isnull().sum() > lastrow * 0.9)
        for idx in range(len(partialColumns.index)-1, 0, -1):
            if not partialColumns.iloc[idx]:
                break
            ncols-=1
        dfFilled = df.iloc[:,:ncols]

        lastrpowidx = firstrowidx + 1 # type: ignore

        for idx in range(len(df.index)-1, 0, -1):
            cnavalues = dfFilled.iloc[idx].isnull().sum()
            if (ncols-cnavalues)*100/nheadercols > 47 and not all(dfFilled.iloc[idx][ncols-3:].isnull()):
                lastrow = idx
                lastrpowidx = df.iloc[lastrow:lastrow+1].index[0]
                break
        header = df.loc[:firstrowidx-1].dropna(axis=1,how='all').dropna(axis=0,how='all') # type: ignore
        footer = df.loc[lastrpowidx+1 : ].dropna(axis=1,how='all').dropna(axis=0,how='all') # type: ignore
        #df = df.loc[firstrowidx : lastrpowidx, headercols]
        df = df.loc[firstrowidx : , headercols]

        data = df.dropna(axis=1,how='all').dropna(axis=0,how='all')
    else:
        header = pd.DataFrame()
        data = pd.DataFrame()
        footer = pd.DataFrame()
    return (
		header,
		data,
		footer,
	)

def setDataColumns(df) -> pd.DataFrame:
    header1 = df.iloc[0]
    header1 = header1.mask(header1 == '').fillna(method='ffill').fillna("").astype(str)
    header1 = header1.str.lower().replace('\n', '').replace(r'\s+', '', regex=True).replace(r'ё', 'е', regex=True).fillna("").astype(str)
    datastart = 1

    if len(df.axes[0]) > 1 and df.iloc[1].mask(df.iloc[1]=='').isnull().iloc[0]:
        header2 = df.iloc[1].fillna("").astype(str)
        header2 = header2.str.lower().replace('\n', '').replace(r'\s+', '', regex=True).replace(r'ё', 'е', regex=True).replace(r'\d+\.?\d*', '', regex=True).fillna("").astype(str)
        header = pd.concat([header1, header2], axis=1).apply(
            lambda x: '.'.join([y for y in x if y]), axis=1)
        datastart = 2
    else:
        header = header1
    df = df[datastart:]
    df.columns = header.str.lower().replace(r'[\n\.\,\(\)\/\-]', '', regex=True).replace(r'№', 'n', regex=True).replace(r'\s+', '', regex=True).replace(r'ё', 'е', regex=True).mask(header1 == '').fillna("column")
    cols=pd.Series(df.columns)
    for dup in cols[cols.duplicated()].unique(): 
        cols[cols[cols == dup].index.values.tolist()] = [dup + '.' + str(i) if i != 0 else dup for i in range(sum(cols == dup))]
    df.columns=cols
    ncols = len(df.columns)
    return df[df[list(df.columns)].isnull().sum(axis=1) < ncols * 0.8].dropna(
        axis=0, how='all'
    )

#removes rows, containing 1, 2, 3, 4, 5, ... (assuming that is just rows with columns numbers, which should be ignored)
def cleanupRawData(df: pd.DataFrame) -> pd.DataFrame:
    ncols = len(df.columns)
    row2del = "_".join([str(x) for x in range(1, ncols+1)])

    df["__rowval"] = pd.Series(df.fillna("").astype(str).replace(r'\s+', '', regex=True).values.tolist()).str.join('_').values
    df = df[df.__rowval != row2del]

    return df.drop("__rowval", axis=1)

import warnings
warnings.filterwarnings("ignore", 'This pattern has match groups')

#sets to_ignore to True, if entrDate is empty
def cleanupAndEnreachProcessedData(df: pd.DataFrame, inname: str, clientid: str, params: dict, sheet: str, function_name: str) -> pd.DataFrame:

    #cleanup
    df = df[df.entryDate.notna()]
    df.entryDate = df.entryDate.astype(str).replace(r'[\s\n]', '', regex=True)
    #df = df[df.entryDate.astype(str).str.contains(r"^(?:\d{1,2}[\,\.\-\/]\d{1,2}[\,\.\-\/]\d{2,4}|\d{2,4}[\,\.\-\/]\d{1,2}[\,\.\-\/]\d{1,2}[\w ]*)", regex=True)]
    df.loc[df.entryDate.astype(str).str.contains(r"^(?:\d{1,2}[\,\.\-\/]\d{1,2}[\,\.\-\/]\d{2,4}|\d{2,4}[\,\.\-\/]\d{1,2}[\,\.\-\/]\d{1,2}[\w ]*)", regex=True, na=False) == False, "result"] = 1

    #enreach
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

def getHeaderValues(header: str, signature: str) -> dict:
    hdr = re.sub(r"[\n\.\,\(\)\/\-\|]", '',  re.sub(r'№', 'n', re.sub(r'\s+', '', re.sub(r'ё', 'е', header)))).lower()
    eoh = hdr.find(signature[:8].replace('|', ''))
    if eoh != -1:
        header = header[:int(len(header)*eoh/len(hdr))]
    account = re.search(REGEX_ACCOUNT, header)
    account = account.group() if account else ""
    bic = re.search(REGEX_BIC, header)
    bic = bic.group() if bic else ""
    inn = re.search(REGEX_INN, header)
    inn = inn.group() if inn else ""
    amount = re.search(REGEX_AMOUNT, header)
    amount = amount.group() if amount else ""
    return {"header": header, "bic": bic, "account": account, "inn": inn, "amount": amount}

def processData(df: pd.DataFrame, headerstr: str, inname: str, clientid: str, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    # sourcery skip: extract-method
    #outdata = pd.DataFrame(columns=["file", "clientid", "sheet", "function", "signature", "header"])
    header, data, footer = getTableRange(df)
    if data.empty:
        return pd.DataFrame(
            [["EMPTY", clientid, inname, sheet, "", "", "", ""]],
            columns=[
                "status",
                "clientid",
                "file",
                "sheet",
                "function",
                "params",
                "signature",
                "header",
            ],
        )
    data = setDataColumns(data)
    data = cleanupRawData(data)
    signature = "|".join(data.columns).replace('\n', ' ')
    if not headerstr and len(header != 0):
        headerstr = "|".join(header[:].apply(lambda x: '|'.join(x.dropna().astype(str)), axis=1))
    params = getHeaderValues(headerstr, signature)

    funcs = list(filter(lambda item: item is not None, [sig.get(signature) for sig in HDRSIGNATURES]))
    func = funcs[0] if funcs else NoneHDR_process
    return pd.DataFrame(
        [["PROCESSED", clientid, inname, sheet, func.__name__, str({k:v for k, v in params.items() if k in ('bic', 'account', 'inn', 'amount')}), signature, headerstr]], # type: ignore
        columns=[
            "status",
            "clientid",
            "file",
            "sheet",
            "function",
            "params",
            "signature",
            "header",
        ],
    ) 

def processExcel(inname: str, clientid: str, logf: TextIOWrapper) -> tuple[pd.DataFrame, int, bool]:
    berror = False
    df = pd.DataFrame()
    result = pd.DataFrame()

    sheets = pd.read_excel(inname, header=None, sheet_name=None)
    if len(sheets) > 1 :
        print(f"{datetime.now()}:{inname}:WARNING:{len(sheets)} sheets found")
    for sheet in sheets:
        try:
            df = sheets[sheet].dropna(axis=1,how='all')
            if not df.empty:
                kind, header = getExcelSheetKind(df)
                if kind == "выписка":
                    outdata = processData(df, "|".join(header), inname, clientid, str(sheet), logf)
                    if not outdata.empty:
                        result = pd.concat([result, outdata])
                else: 
                    logstr = f"{datetime.now()}:PASSED:{clientid}:{os.path.basename(inname)}:{sheet}:0:{kind}\n"
                    logf.write(logstr)
        except Exception as err:
            berror = True   
            print(f"{datetime.now()}:{inname}_{sheet}:ERROR:{err}")
            logstr = f"{datetime.now()}:ERROR:{clientid}:{os.path.basename(inname)}:{sheet}:0:{type(err).__name__} {str(err)}\n"
            logf.write(logstr)
    return (result, len(sheets), berror)

def processPDF(inname: str, clientid: str, logf: TextIOWrapper) -> tuple[pd.DataFrame, int, bool]:
    berror = False
    result = pd.DataFrame()

    try:
        df = getPDFData(inname, 1)
        if not df.empty:
            header = getHeadLinesPDF(inname, 30)
            outdata = processData(df, "|".join(header), inname, clientid, "pdf", logf)
            if not outdata.empty:
                result = pd.concat([result, outdata])
    except Exception as err:
        berror = True   
        print(f"{datetime.now()}:{inname}:ERROR:{err}")
        logstr = f"{datetime.now()}:ERROR:{clientid}:{os.path.basename(inname)}:pdf:0:{type(err).__name__} {str(err)}\n"
        logf.write(logstr)
    return (result, 1, berror)

def processOther(inname: str, clientid: str, logf: TextIOWrapper) -> tuple[pd.DataFrame, int, bool]:
    return (pd.DataFrame(), 0, True)

def process(inname: str, clientid: str, logf: TextIOWrapper) -> tuple[pd.DataFrame, int, bool]:
    processFunc = processOther

    if inname.lower().endswith('.xls') or inname.lower().endswith('.xlsx'):
        processFunc = processExcel
    elif inname.lower().endswith('.pdf'):
        processFunc = processPDF

    df, pages, berror = processFunc(inname, clientid, logf)
    return (df, pages, berror)


def runParsing(clientid, outname, inname, doneFolder, logf) -> int:
    filename = os.path.basename(inname)
    print(f"{datetime.now()}:START: {clientid}: {filename}")


    df, pages, berror = process(inname, clientid, logf)
    if not berror:
        if not df.empty:
            df.to_csv(outname, mode="a+", header=not Path(outname).is_file(), index=False)
            logstr = f"{datetime.now()}:PROCESSED: {clientid}:{filename}:{pages}:{str(df.shape[0])}:{outname}\n"
            #move2Folder(inname, doneFolder)
        else: 
            berror = True
            logstr = f"{datetime.now()}:EMPTY: {clientid}:{filename}:{pages}:0:{outname}\n"
        logf.write(logstr)
    print(f"{datetime.now()}:DONE: {clientid}: {filename}")
    return not berror

def getFilesList(log: str, start: int, end: int) -> list[str]:
    # sourcery skip: min-max-identity
    df = pd.read_csv(log, on_bad_lines='skip', names=['status', 'clientid', 'filename', 'sheets', 'doctype', 'filetype', 'error'], delimiter='|')
    if end < 0:
        end = len(df)
    if start < 0:
        start = 0
    df = df.iloc[start:end]
    filelist = df['filename'][(df['status']=='PROCESSED') & (df['doctype'] == 'выписка')]
    return pd.Series(filelist).to_list() # type: ignore

def main():
    sys.stdout.reconfigure(encoding="utf-8") # type: ignore

    preanalysislog, logname, outbasename, bSplit, maxFiles, doneFolder, FILEEXT, start, end = getParameters()
    cnt = 0
    outname = outbasename + ".csv"
    DIRPATH = '../Data'
    fileslist = getFilesList(preanalysislog, start, end)
    with open(logname, "w", encoding='utf-8', buffering=1) as logf:

        sys.stdout.reconfigure(encoding="utf-8", line_buffering=True) # type: ignore
        print(f"START:{datetime.now()}\ninput:{DIRPATH}\nlog:{logname}\noutput:{outname}\nsplit:{bSplit}\nmaxinput:{maxFiles}\ndone:{doneFolder}\nextensions:{FILEEXT}\nrange:{start}-{end}")

  
        #for file in fileslist:
        for fname in filter(lambda file: any(ext for ext in FILEEXT if (file.lower().endswith(ext))), fileslist):
            if Path(fname).is_file():
                parts = os.path.split(os.path.dirname(fname))
                clientid = parts[1]
                inname = fname
                try:
                    pages = 0
                    if bSplit and cnt % maxFiles == 0:
                        outname = outbasename + str(cnt) + ".csv"
                    try :
                        cnt += runParsing(clientid, outname, inname, doneFolder, logf)
                    except Exception as err:
                        logf.write(f"{datetime.now()}:FILE_ERROR:{clientid}:{os.path.basename(inname)}:{pages}::{type(err).__name__} {str(err)}\n")
                        print(f"{datetime.now()}:{inname}:ERROR:{err}")
                except Exception as err:
                    print(f"{datetime.now()}:{clientid}:!!!CRITICAL ERROR!!! {err}")
                    logf.write(f"{datetime.now()}:CRITICAL ERROR:{clientid}:ND:ND:ERROR\n")

    #with open('datatypes.csv', 'w', encoding='utf-8') as f:
    df = pd.DataFrame(np.unique(DATATYPES))
    df.to_csv("./data/datatypes.csv", mode="w", index = False)

def move2Folder(fname: str, doneFolder: str):
    outdir = f"{doneFolder}/{os.path.split(os.path.dirname(fname))[1]}"
    outname = f"{outdir}/{os.path.basename(fname)}"
    if not os.path.isdir(outdir):
        os.mkdir(outdir)
    shutil.move(fname, outname)

def getFileExtList(isExcel, isPDF) -> list[str]:
    FILEEXT = []
    if isExcel:
        FILEEXT += ['.xls', '.xlsx', '.xlsm']
    if isPDF:
        FILEEXT += ['.pdf']
    return FILEEXT

def getArguments():
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument("-d", "--data", default="./data/test_preanalysis.csv", help="Data folder")
    parser.add_argument("-r", "--done", default="./data/Done", help="Done folder")
    parser.add_argument("-l", "--logfile", default="./data/test_classify_log.txt", help="Log file")
    parser.add_argument("-o", "--output", default="./data/test_classify", help="Resulting file name (no extension)")
    parser.add_argument("--split", default=True, action=BooleanOptionalAction, help="Weather splitting resulting file required (--no-spilt opposite option)")
    parser.add_argument("-m", "--maxinput", default=500, type=int, help="Maximum files sored in one resulting file")
    parser.add_argument("--pdf", default=True, action=BooleanOptionalAction, help="Weather to include pdf (--no-pdf opposite option)")
    parser.add_argument("--excel", default=True, action=BooleanOptionalAction, help="Weather to include excel files (--no-excel opposite option)")
    parser.add_argument("-s", "--start", default=-1, type=int, help="Starting position in data file")
    parser.add_argument("-e", "--end", default=-1, type=int, help="Ending position in data file (not included)")
    return vars(parser.parse_args())

def getParameters():
    args = getArguments()

    preanalysislog = args["data"]
    logname = args["logfile"]
    outbasename = args["output"]
    bSplit = args["split"]
    maxFiles = args["maxinput"]
    doneFolder = args["done"] + "/"
    FILEEXT = getFileExtList(args["excel"], args["pdf"])
    start = args["start"]
    end = args["end"]
    return preanalysislog,logname,outbasename,bSplit,maxFiles,doneFolder,FILEEXT, start, end

if __name__ == "__main__":
    DATATYPES = []
    main()
