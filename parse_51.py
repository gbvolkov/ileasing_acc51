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
import PyPDF2
import sys
from io import TextIOWrapper

#using this (type: ignore) since camelot does not have stubs
import camelot # type: ignore
from pdfminer.high_level import extract_pages
from pdfminer.layout import LTTextBoxHorizontal, LTTextLineHorizontal

STATE = Enum('State', ['stINIT', 'stPERIOD', 'stHEADER', 'stBALANCE'])

PERIOD_STR = "Карточка счета "
BALANCE_STR = "Сальдо на начало"
CHECK_STR = ["Обороты за период и сальдо на конец", "Обороты за период"]
SEARCH_STR = "Отбор:"
HEADER_STR = "Период"
DATE_REGEX = r"\d{1,2}[.\-/]\d{1,2}[.\-/]\d{4}"

#Получаем даты из строки с периодом выписки
def getPeriod(periodstr) -> list[str]:
    dates: list[str] = []
    locale.setlocale(locale.LC_ALL, 'ru_RU')
    if periods := [x.strip() for x in re.findall(DATE_REGEX, periodstr)]:
        dates.extend(iter(periods))
    else:
        periods = [x.strip() for x in re.findall(r"\s\w+\s\d+ г.", periodstr)]
        try:
            if periods:
                dates.extend(iter(periods))
        except Exception:
            dates.clear()

    if len(dates) == 1:
        dates.append(dates[0])
    elif not dates:
        if (periodstr.startswith("Карточка счета 51 ")):
            dates.extend(
                (
                    periodstr[len("Карточка счета 51 ") : -1],
                    periodstr[len("Карточка счета 51 ") : -1],
                )
            )
        else:
            dates.extend((periodstr, periodstr))
    return dates


def getResult(data) -> pd.Series:
    row = data.iloc[[-1][0]]
    row = row.astype(str).str.replace(' ', '')
    row = row.astype(str).str.replace(',', '.')
    row = pd.to_numeric(row, errors='coerce')
    row.dropna(inplace=True)
    return row


# Получаем две строки. В первой заполняем все merged слобцы значением из предыдущего (fliina('ffil)). Во второй все NaN заполняем ".".
# Потом соединяем первую и вторую строки (concatstr или что-то вроде)
# После этого смотрим на дубликаты и оставляем только первое вхождение
# И ещё костыль - оставляем последний столбец
def getHeader(dfhead) -> tuple[list[str], list[str]]:  # sourcery skip: raise-specific-error
    dfhead.iloc[:1] = dfhead.iloc[:1].fillna(
        method='ffill', axis=1
    )
    dfhead.iloc[1:2] = dfhead.iloc[1:2].fillna("")
    head = dfhead.iloc[:2].apply(
        lambda x: '.'.join([y for y in x if y]), axis=0
    )
    head = head.drop_duplicates()
    columns = [
        head[head == 'Дебет.Счет'].index,
        head[head == 'Кредит.Счет'].index,
        head[head.str.contains('Текущ', na=False)].index,
    ]
    if (
        columns[0].empty
        or columns[1].empty
        or columns[2].empty
    ):
        raise Exception(
            'Incorrect structure. Incomplete data'
        )
    cols2del = head[
        head.str.contains('Пока', na=False)
    ].index

    return (columns, cols2del)

#Из первых строк файла получаем имя компании, начало и конец периода, баланс на начало периода, магический столбец 'Д' и список значимых столбцов (для объединённых ячеек excel)
def getDefinition(data) -> dict[str, Any]:  # sourcery skip: raise-specific-error
    state = STATE.stINIT.value
    idx = 0
    companyName = "ND"
    periods = ["ND"] * 2
    columns: list[str] = []
    cols2del: list[str] = []
    while (state <= STATE.stHEADER.value and idx < data.shape[0] and idx < 20):
        row = data.iloc[[idx][0]]

        if isinstance(row[0], str) and len(row[0]) > 0:
            match state:
                case STATE.stINIT.value:
                    companyName = row[0]
                    state = state + 1
                case STATE.stPERIOD.value:
                    if PERIOD_STR in row[0]:
                        periods = getPeriod(row[0])
                        state = state + 1
                case STATE.stHEADER.value:
                    if HEADER_STR in row[0] or "Дата" in row[0]:
                        # Получаем две строки. В первой заполняем все merged слобцы значением из предыдущего (fliina('ffil)). Во второй все NaN заполняем ".".
                        # Потом соединяем первую и вторую строки (concatstr или что-то вроде)
                        # После этого смотрим на дубликаты и оставляем только первое вхождение
                        # И ещё костыль - оставляем последний столбец
                        dfhead = data.iloc[idx : idx + 2]
                        (columns, cols2del) = getHeader(dfhead)
                        state = state + 1
        idx += 1
    if (state <= STATE.stHEADER.value) :
        raise Exception('Incorrect header structure')
    return {'Company_Name':companyName, 'Start':periods[0], 'Finish':periods[1], 'columns': columns, 'cols2del': cols2del}


#Совершенно костыльная процедура
def addMissedColumns(data, columns, cols2del) -> pd.DataFrame:
    # sourcery skip: raise-specific-error
    maxidx = data.columns[-1]
    df = data.reindex(columns=[*data.columns.tolist(), *list(range(maxidx+1, maxidx+11-len(data.columns)))],  fill_value = 0.0)
    try:
        col1 = df.columns.get_loc(columns[0][0])
        col2 = df.columns.get_loc(columns[1][0])
        if (col2-col1 <= 1):
            df.insert(col1+1, columns[0][0]+1, 0.0)
        try:
            if not columns[2].empty :
                col3 = df.columns.get_loc(columns[2][0])
                if (col3-col2 <= 1):
                    df.insert(col2+1, columns[1][0]+1, 0.0)
            else :
                df.insert(8, columns[1][0]+1, 'Д')
                df.insert(9, columns[1][0], 0.0)
        except Exception:
            #df.insert(df.shape[1], columns[2][0]-1, 'Д')
            #df.insert(df.shape[1], columns[2][0], 0.0)
            df.insert(8, columns[2][0]-1, 'Д')
            df.insert(9, columns[2][0], 0.0)
    except Exception  as e:
        raise Exception('Incorrect data structure') from e
    df = df.drop(list(cols2del), axis=1)
    return df.iloc[:, 0:10]


#Returns (trancated df, openbalance, controlDebet, controlCredit, controlBalance)
def getControlValues(df) -> tuple[pd.DataFrame, float, float, float, float]:
    #Берём контрольные данные из строки "Обороты за период и сальдо на конец"
    firstrow = df.loc[df.iloc[:,0].str.contains(BALANCE_STR, na=False)]
    lastrow = df.loc[df.iloc[:,0].str.contains(CHECK_STR[0], na=False) | df.iloc[:,0].str.contains(CHECK_STR[1], na=False)]

    openbalance = 0.0
    try :
        openvalues = getResult(firstrow)
        if not openvalues.empty :
            openbalance = openvalues.iloc[0].round(2)
    except Exception :
        openbalance = 0.0

    controlDebet = 0.0
    controlCredit = 0.0
    controlBalance = 0.0
    try :
        checkvalues = getResult(lastrow)
        if not checkvalues.empty :
            try :
                controlDebet = checkvalues.iloc[0].round(2)
            except Exception :
                controlDebet = 0
            try :
                controlCredit = checkvalues.iloc[1].round(2)
            except Exception :
                controlCredit = 0
            try :
                controlBalance  = checkvalues.iloc[2].round(2)
            except Exception :
                controlBalance = 0
    except Exception :
        controlDebet = 0.0
        controlCredit = 0.0
        controlBalance = 0.0

    lowidx = 1 if firstrow.empty else firstrow.index[0]+1
    highidx = df.shape[0] + 1 if lastrow.empty else lastrow.index[0]
    return (df.iloc[lowidx:highidx,:], openbalance, controlDebet, controlCredit, controlBalance)


COLUMNS = ["Date", "Document"
                    , "Debet_Analitics", "Credit_Analitics"
                    , "Debet_Account", "Debet_Amount"
                    , "Credit_Account", "Credit_Amount"
                    , "Balance_D", "Balance"]

def getControlledValues(df, inname) -> tuple[float, float, float, float, float]:
    closeBalance = 0.0
    openBalance = 0.0
    totalDebet = 0.0
    totalCredit = 0.0
    balanceCheck = 0.0

    try:
        df['Debet_Amount'] = pd.to_numeric(df['Debet_Amount'], errors='coerce')
        df['Credit_Amount'] = pd.to_numeric(df['Credit_Amount'], errors='coerce')
        df['Balance'] = pd.to_numeric(df['Balance'], errors='coerce')
        totalDebet = df['Debet_Amount'].sum().round(2)
        totalCredit = df['Credit_Amount'].sum().round(2)

        if not df.empty :
            openBalance = df.iloc[[0][0]]['OpenBalance'].round(2)
            closeBalance = df.iloc[[-1][0]]['Balance'].round(2)
        balanceCheck = round(openBalance + totalDebet - totalCredit, 2)
    except Exception:
        print(f"{datetime.now()}:{inname}:ERROR.:Checksum cannot be calculated!")
    return(closeBalance, openBalance, totalDebet, totalCredit, balanceCheck)

def checkControlledValues(closeBalance, openBalance, balanceCheck, totalDebet, totalCredit,
                                controlBalance, controlDebet, controlCredit, inname) -> int:
    status = 0
    if totalDebet != controlDebet:
        print(f"{datetime.now()}:{inname}:WARNING. CONTROL CHECK FAILED:DEBIT:{totalDebet}!={controlDebet}")
        status += 1
    if totalCredit != controlCredit:
        print(f"{datetime.now()}:{inname}:WARNING. CONTROL CHECK FAILED:CREDIT:{totalCredit}!={controlCredit}")
        status += 2
    if closeBalance != controlBalance:
        print(f"{datetime.now()}:{inname}:WARNING. CONTROL CHECK FAILED:CLOSE BALANCE:{closeBalance}!={controlBalance}")
        status += 4
    if balanceCheck != controlBalance:
        print(f"{datetime.now()}:{inname}:WARNING. CONTROL CHECK FAILED:BALANCE CHECK:{balanceCheck}!={controlBalance}")
        status += 8
    if totalDebet == 0.0 and totalCredit == 0.0:
        print(f"{datetime.now()}:{inname}:WARNING. Zero turnovers")
        status += 16
    return status

def normalizeNumericData(df): 
    df['Debet_Amount'].fillna(0.0, inplace=True)
    df['Credit_Amount'].fillna(0.0, inplace=True)
    df['Balance'].fillna(0.0, inplace=True)

    df['Debet_Amount'] = df['Debet_Amount'].astype(str).str.replace(' ', '')
    df['Debet_Amount'] = df['Debet_Amount'].astype(str).str.replace(',', '.')
    df['Credit_Amount'] = df['Credit_Amount'].astype(str).str.replace(' ', '')
    df['Credit_Amount'] = df['Credit_Amount'].astype(str).str.replace(',', '.')
    df['Balance'] = df['Balance'].astype(str).str.replace(' ', '')
    df['Balance'] = df['Balance'].astype(str).str.replace(',', '.')

def publishgDataFrame(df, xlsname, clientid, searchstr, definition, initialBalance, controlDebet, controlCredit, controlBalance) -> pd.DataFrame:
    
    if not df.empty :
        df = df.iloc[:,0:10]
        df.columns = COLUMNS
    else :
        df = pd.DataFrame(columns = COLUMNS)

    normalizeNumericData(df)

    #Добавляем столбцы из заголовка
    df['Company_Name'] = definition['Company_Name']
    df['Start'] = definition['Start']
    df['Finish'] = definition['Finish']
    df['OpenD'] = 'Д' #definition['OpenD']
    df['OpenBalance'] = initialBalance #definition['OpenBalance'].round(2)
    df['file'] = xlsname
    df['processdate'] = datetime.now()

    #Убираем строки с промежуточным результатом (типа Сальдо на сентябрь etc)
    df['Date'] = df['Date'].fillna("NODATE")
    df['Date'] = df['Date'].apply(lambda x: f"{x.strftime('%d.%m.%Y')}" if isinstance(x,datetime) else f"{x}")
    df = df[df.Date.astype(str).str.match(DATE_REGEX).fillna(False)]

    #Проверяем коррекность данных: сверка оборотов и остатков по счёту
    (closeBalance, openBalance, totalDebet, totalCredit, balanceCheck) = getControlledValues(df, xlsname)

    status = checkControlledValues(closeBalance, openBalance, balanceCheck, totalDebet, totalCredit,
                                controlBalance, controlDebet, controlCredit, xlsname)

    df.insert(df.shape[1], 'CLIENTID', clientid)
    df.insert(df.shape[1], 'SUBSET', searchstr)
    df.insert(df.shape[1], 'Result', status)

    return df

def getDataFrameFromExcel(df, clientid, xlsname) -> pd.DataFrame:
    
    #Из первых строк файла получаем имя компании, начало и конец периода, баланс на начало периода, 
    #   магический столбец 'Д' и список значимых столбцов (для объединённых ячеек excel)
    #try:
    definition = getDefinition(df)

    #Пытаемся найти отбор по счёту
    searchrow = df.loc[df[0] == SEARCH_STR]
    searchstr = ""
    try :
        if not searchrow.empty :
            searchstr = searchrow[2].values[0]
    except Exception as err :
        searchstr = ""

    df, openbalance, controlDebet, controlCredit, controlBalance = getControlValues(df)

    if not df.empty :
        df = df.dropna(axis=1,how='all')
        df = addMissedColumns(df, definition['columns'], definition['cols2del'])

    df = publishgDataFrame(df, xlsname, clientid, searchstr, definition, openbalance, controlDebet, controlCredit, controlBalance)
    return df

def get_headlines_pdf(pdfname: str, nlines: int = 3) -> list[str]:
    """
    Extracts the headlines from a PDF file using pdfminer library.

    :param pdfname: Name of the PDF file.
    :type pdfname: str
    :param nlines: Number of headlines to extract. Default is 3.
    :type nlines: int
    :return: List of extracted headlines.
    :rtype: list[str]
    """
    result: list[str] = []
    for page_layout in extract_pages(pdfname, maxpages=1):
        for element in page_layout:
            if isinstance(element, LTTextBoxHorizontal):
                txt = element.get_text()
                lines = [x.strip() for x in txt.split("\n")]
                result.extend(line for line in lines if len(line) > 0)
    return result

def get_headlines_pdf2(pdfname: str, nlines: int = 3) -> list[str]:
    """
    Extracts the headlines from a PDF file using PyPDF2 library.

    :param pdfname: Name of the PDF file.
    :type pdfname: str
    :param nlines: Number of headlines to extract. Default is 3.
    :type nlines: int
    :return: List of extracted headlines.
    :rtype: list[str]
    """
    result: list[str] = []
    with open(pdfname,'rb') as f:
        pdf_reader = PyPDF2.PdfFileReader(f)
        txt = pdf_reader.pages[0].extract_text()
        lines = [x.strip() for x in txt.split("\n")]
        result.extend([line for line in lines if len(line) > 0])
    return result

def pdfPagesCount(pdfname)->int:
    with open(pdfname,'rb') as f:
        pdfReader = PyPDF2.PdfFileReader(f)
        return pdfReader.getNumPages()

def getDataFrameFromPDF(inname) -> pd.DataFrame:
    df = pd.DataFrame()
    npages = pdfPagesCount(inname)
    spage = 1
    cchunk = 100
    if npages >= 500 :
        print(f"{datetime.now()}:{inname}:WARINING: huge pdf:{npages} pages")
    while spage <= npages :
        lpage = min(spage + cchunk - 1, npages)
        tables = camelot.read_pdf(inname, pages=f'{spage}-{lpage}', line_scale = 100, shift_text=['l', 't'], backend="poppler", layout_kwargs = {"char_margin": 0.1, "line_margin": 0.1, "boxes_flow": None})
        for tbl in tables :
            df = pd.concat([df, tbl.df])
        if npages >= 500 :
            print(f"{datetime.now()}:{inname}:PROCESSED: {spage}-{lpage} of {npages} pages")
        spage = lpage + 1
    return df.reset_index(drop=True)

def normalizeDColumntForPDF(df) -> pd.DataFrame:
    df['Contains_D'] = list(map(lambda x: str(x).startswith('Д\n'), df[8].astype(str)))
    if df['Contains_D'].any() :
        df[[8,9]] = df[8].str.split("\n", n = 1, expand = True)
    else :
        df[9] = df[8]
        df[8] = ''
    return df.drop(axis=1, columns=['Contains_D'])

def processPDF(inname: str, clientid: str, logf: TextIOWrapper) -> tuple[pd.DataFrame, int, bool]:
    berror = False
    df = pd.DataFrame()
    headers = get_headlines_pdf2(inname, 4)
    npages = 0

    if len(headers)>=2 and headers[1].startswith("Карточка счета 51"):
        df = prepareDataFromPDF(headers, inname, clientid)
    else:
        berror = True
    return (df, npages, berror)

def prepareDataFromPDF(headers: list[str], inname: str, clientid: str) -> pd.DataFrame:
    companyName = headers[0]
    periods = getPeriod(headers[1])

    definition = {'Company_Name':companyName, 'Start':periods[0], 'Finish':periods[1], 'columns': [], 'cols2del': []}
    df = getDataFrameFromPDF(inname)
    df = normalizeDColumntForPDF(df)

    (
        df,
        openbalance,    
        controlDebet,
        controlCredit,
        controlBalance,
    ) = getControlValues(df)
    df = publishgDataFrame(
        df,
        inname,
        clientid,
        "",
        definition,
        openbalance,
        controlDebet,
        controlCredit,
        controlBalance,
    )
    return df

def processExcel(inname: str, clientid: str, logf: TextIOWrapper) -> tuple[pd.DataFrame, int, bool]:
    berror = False
    df = pd.DataFrame()
    sheets = pd.read_excel(inname, header=None, sheet_name=None)
    if len(sheets) > 1 :
        print(f"{datetime.now()}:{inname}:WARNING:{len(sheets)} sheets found")
    for sheet in sheets:
        try:
            df = pd.concat(
                [
                    df,
                    getDataFrameFromExcel(
                        sheets[sheet], clientid, f"{inname}_{sheet}"
                    ),
                ]
            )
        except Exception as err :
            berror = True   
            print(f"{datetime.now()}:{inname}_{sheet}:ERROR:{err}")
            logstr = f"{datetime.now()}:ERROR:{clientid}:{os.path.basename(inname)}:{sheet}:0:{type(err).__name__} {str(err)}\n"
            logf.write(logstr)
    return (df, len(sheets), berror)

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
            shutil.move(inname, doneFolder + clientid + '_' + filename)
        else: 
            berror = True
            logstr = f"{datetime.now()}:EMPTY: {clientid}:{filename}:{pages}:0:{outname}\n"
        logf.write(logstr)
    print(f"{datetime.now()}:DONE: {clientid}: {filename}")
    return not berror

def main():
    DIRPATH, logname, outbasename, bSplit, maxFiles, doneFolder, FILEEXT = getParameters()
    cnt = 0
    outname = outbasename + ".csv"

    with open(logname, "w", encoding='utf-8', buffering=1) as logf:

        sys.stdout.reconfigure(encoding="utf-8", line_buffering=True) # type: ignore
        print(f"START:{datetime.now()}\ninput:{DIRPATH}\nlog:{logname}\noutput:{outname}\nsplit:{bSplit}\nmaxinput:{maxFiles}\ndone:{doneFolder}\nextensions:{FILEEXT}")

        for root, dirs, files in os.walk(DIRPATH):
            for name in filter(lambda file: any(ext for ext in FILEEXT if (file.lower().endswith(ext))), files):
                parts = os.path.split(root)
                clientid = parts[1]
                inname = os.path.join(root, name)
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

def getFileExtList(isExcel, isPDF) -> list[str]:
    FILEEXT = []
    if isExcel:
        FILEEXT += ['.xls', '.xlsx']
    if isPDF:
        FILEEXT += ['.pdf']
    return FILEEXT

def getArguments():
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument("-d", "--data", default="../Data", help="Data folder")
    parser.add_argument("-r", "--done", default="../Done", help="Done folder")
    parser.add_argument("-l", "--logfile", default="./acc51log.txt", help="Log file")
    parser.add_argument("-o", "--output", default="./parsed", help="Resulting file name (no extension)")
    parser.add_argument("--split", default=True, action=BooleanOptionalAction, help="Weather splitting resulting file required (--no-spilt opposite option)")
    parser.add_argument("-m", "--maxinput", default=500, type=int, help="Maximum files sored in one resulting file")
    parser.add_argument("--pdf", default=True, action=BooleanOptionalAction, help="Weather to include pdf (--no-pdf opposite option)")
    parser.add_argument("--excel", default=True, action=BooleanOptionalAction, help="Weather to include excel files (--no-excel opposite option)")
    return vars(parser.parse_args())

def getParameters():
    args = getArguments()

    DIRPATH = args["data"]
    logname = args["logfile"]
    outbasename = args["output"]
    bSplit = args["split"]
    maxFiles = args["maxinput"]
    doneFolder = args["done"] + "/"
    FILEEXT = getFileExtList(args["excel"], args["pdf"])
    return DIRPATH,logname,outbasename,bSplit,maxFiles,doneFolder,FILEEXT

if __name__ == "__main__":
    main()
