from enum import Enum
import pandas as pd
from pathlib import Path
import re
from datetime import datetime
import locale
import shutil
import os
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter, BooleanOptionalAction

import camelot
from pdfminer.high_level import extract_pages
from pdfminer.layout import LTTextBoxHorizontal, LTTextLineHorizontal

STATE = Enum('State', ['stINIT', 'stPERIOD', 'stHEADER', 'stBALANCE'])

PERIOD_STR = "Карточка счета "
BALANCE_STR = "Сальдо на начало"
CHECK_STR = ["Обороты за период и сальдо на конец", "Обороты за период"]
SEARCH_STR = "Отбор:"
HEADER_STR = "Период"
DATE_REGEX = "\d{1,2}[.\-/]\d{1,2}[.\-/]\d{4}"

#Получаем даты из строки с периодом выписки
def getPeriod(periodstr):
    dates = []
    locale.setlocale(locale.LC_ALL, 'ru_RU')
    periods = [x.strip() for x in re.findall(DATE_REGEX, periodstr)]
    if len(periods) > 0:
        for period in periods:
            #dtStr = period.replace('/', '.').replace('-','.')
            #dt = datetime.strptime(dtStr, "%d.%m.%Y")
            dates.append(period)
    else:
        periods = [x.strip() for x in re.findall("\s\w+\s\d+ г.", periodstr)]
        try: 
            if len(periods) > 0:
                for period in periods:
                    #dt = datetime.strptime(period, ' %B %Y г.')
                    dates.append(period)
        except:
            dates.clear()

    if len(dates) == 1:
        dates.append(dates[0])
    elif len(dates) == 0:
        if (periodstr.startswith("Карточка счета 51 ")):
            dates.append(periodstr[len("Карточка счета 51 "):-1])
            dates.append(periodstr[len("Карточка счета 51 "):-1])
        else:
            dates.append(periodstr)
            dates.append(periodstr)
    return dates


def getResult(data):
    row = data.iloc[[-1][0]]
    row = row.astype(str).str.replace(' ', '')
    row = row.astype(str).str.replace(',', '.')
    row = pd.to_numeric(row, errors='coerce')
    row.dropna(inplace=True)
    return row

#Из первых строк файла получаем имя компании, начало и конец периода, баланс на начало периода, магический столбец 'Д' и спосок значимых столбцов (для объединённых ячеек excel)
def getDefinition(data):
    state = STATE.stINIT.value
    idx = 0
    companyName = "ND"
    periods = ["ND"] * 2
    columns = []
    while(state <= STATE.stHEADER.value and idx < data.shape[0] and idx < 20):
        row = data.iloc[[idx][0]]

        if isinstance(row[0], str) and len(row[0]) > 0:
            match state:
                case STATE.stINIT.value:
                    companyName = row[0]
                    state = state+1
                case STATE.stPERIOD.value:
                    if PERIOD_STR in row[0]:
                        periods = getPeriod(row[0])
                        state = state+1
                case STATE.stHEADER.value:
                    if HEADER_STR in row[0] or "Дата" in row[0]:
                        #Получаем две строки. В первой заполняем все merged слобцы значением из предыдущего (fliina('ffil)). Во второй все NaN заполняем ".". 
                        #Потом соединяем первую и вторую строки (concatstr или что-то вроде)
                        #После этого смотрим на дубликаты и оставляем только первое вхождение
                        #И ещё костыль - оставляем последний столбец
                        dfhead = data.iloc[idx:idx+2]
                        dfhead.iloc[0:1]=dfhead.iloc[0:1].fillna(method='ffill', axis=1)
                        dfhead.iloc[1:2]=dfhead.iloc[1:2].fillna("")
                        head = dfhead.iloc[0:2].apply(lambda x: '.'.join([y for y in x if y]), axis=0)
                        head = head.drop_duplicates()
                        columns = []
                        columns.append(head[head=='Дебет.Счет'].index)
                        columns.append(head[head=='Кредит.Счет'].index)
                        #columns.append(head[head=='Текущее сальдо'].index)
                        columns.append(head[head.str.contains('Текущ', na=False)].index)
                        if columns[0].empty or columns[1].empty or columns[2].empty :
                            raise Exception('Incorrect structure. Incomplete data')
                        cols2del = head[head.str.contains('Пока', na=False)].index
                        state=state+1
                #case STATE.stBALANCE.value:
                #    if BALANCE_STR in row[0]:
                #        firstrowidx=idx+1
                #        state = state+1
                        #openD = row[columns[len(columns)-2]]
                        #openBalance=row[columns[len(columns)-1]]
        idx = idx+1
    if (state <= STATE.stHEADER.value) :
        raise Exception('Incorrect header structure')
    return {'Company_Name':companyName, 'Start':periods[0], 'Finish':periods[1], 'columns': columns, 'cols2del': cols2del}


#Совершенно костыльная процедура
def addMissedColumns(data, columns, cols2del):
    maxidx = data.columns[-1]
    df = data.reindex(columns=[*data.columns.tolist(), *list(range(maxidx+1, maxidx+11-len(data.columns)))],  fill_value = 0.0)
    try :
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
    except Exception :
        raise Exception('Incorrect data structure')
    df = df.drop([col for col in cols2del], axis=1)
    return df.iloc[:, 0:10]


#Returns (trancated df, openbalance, controlDebet, controlCredit, controlBalance)
def getControlValues(df) :
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

    lowidx = 1
    highidx = df.shape[0] + 1
    if not firstrow.empty :
        lowidx = firstrow.index[0]+1
    if not lastrow.empty :
        highidx = lastrow.index[0]

    return (df.iloc[lowidx:highidx,:], openbalance, controlDebet, controlCredit, controlBalance)


COLUMNS = ["Date", "Document"
                    , "Debet_Analitics", "Credit_Analitics"
                    , "Debet_Account", "Debet_Amount"
                    , "Credit_Account", "Credit_Amount"
                    , "Balance_D", "Balance"]

def publishgDataFrame(df, xlsname, clientid, searchstr, definition, openbalance, controlDebet, controlCredit, controlBalance) :
    
    if not df.empty :
        df.columns = COLUMNS
    else :
        df = pd.DataFrame(columns = COLUMNS)
        
    df['Debet_Amount'].fillna(0.0, inplace=True)
    df['Credit_Amount'].fillna(0.0, inplace=True)
    df['Balance'].fillna(0.0, inplace=True)

    df['Debet_Amount'] = df['Debet_Amount'].astype(str).str.replace(' ', '')
    df['Debet_Amount'] = df['Debet_Amount'].astype(str).str.replace(',', '.')
    df['Credit_Amount'] = df['Credit_Amount'].astype(str).str.replace(' ', '')
    df['Credit_Amount'] = df['Credit_Amount'].astype(str).str.replace(',', '.')
    df['Balance'] = df['Balance'].astype(str).str.replace(' ', '')
    df['Balance'] = df['Balance'].astype(str).str.replace(',', '.')

    #Добавляем столбцы из заголовка
    df['Company_Name'] = definition['Company_Name']
    df['Start'] = definition['Start']
    df['Finish'] = definition['Finish']
    df['OpenD'] = 'Д' #definition['OpenD']
    df['OpenBalance'] = openbalance #definition['OpenBalance'].round(2)
    df['file'] = xlsname
    df['processdate'] = datetime.now()

    #Убираем строки с промежуточным результатом (типа Сальдо на сентябрь etc)
    df['Date'] = df['Date'].fillna("NODATE")
    df['Date'] = df['Date'].apply(lambda x: f"{x.strftime('%d-%d-%Y')}" if isinstance(x,datetime) else f"{x}")
    df = df[df.Date.astype(str).str.match(DATE_REGEX).fillna(False)]

    #Проверяем коррекность данных: сверка оборотов и остатков по счёту
    try:
        df['Debet_Amount'] = pd.to_numeric(df['Debet_Amount'], errors='coerce')
        df['Credit_Amount'] = pd.to_numeric(df['Credit_Amount'], errors='coerce')
        df['Balance'] = pd.to_numeric(df['Balance'], errors='coerce')
        totalDebet = df['Debet_Amount'].sum().round(2)
        totalCredit = df['Credit_Amount'].sum().round(2)
        
        if not df.empty :
            openBalance = df.iloc[[0][0]]['OpenBalance'].round(2)
            closeBalance = df.iloc[[-1][0]]['Balance'].round(2)
        else :
            openBalance = 0
            closeBalance = 0
        balanceCheck = (openBalance + totalDebet - totalCredit).round(2)
    except Exception:
        print(xlsname, ':ERROR.:', 'Checksum cannot be calculated!')
        totalDebet = 0.0
        totalCredit = 0.0

    status = 0
    if totalDebet != controlDebet :
        print(xlsname, ':WARNING. CONTROL CHECK FAILED:', 'DEBIT:', totalDebet, "!=", controlDebet)
        status = status+1
    if totalCredit != controlCredit :
        print(xlsname, ':WARNING. CONTROL CHECK FAILED:', 'CREDIT:', totalCredit, "!=", controlCredit)
        status = status+2
    if closeBalance != controlBalance :
        print(xlsname, ':WARNING. CONTROL CHECK FAILED:', 'CLOSE BALANCE:', closeBalance, "!=", controlBalance)
        status = status+4
    if  balanceCheck != controlBalance:
        print(xlsname, ':WARNING. CONTROL CHECK FAILED:', 'BALANCE CHECK', balanceCheck, "!=", controlBalance)
        status = status+8
    if totalDebet == 0.0 and totalCredit == 0.0:
        print(xlsname, ':WARNING. Zero turnovers')
        status = status+16

    df.insert(df.shape[1], 'CLIENTID', clientid)
    df.insert(df.shape[1], 'SUBSET', searchstr)
    df.insert(df.shape[1], 'Result', status)

    return df

def getDataFrameFromExcel(df, clientid, xlsname):
    
    #Из первых строк файла получаем имя компании, начало и конец периода, баланс на начало периода, 
    #   магический столбец 'Д' и список значимых столбцов (для объединённых ячеек excel)
    #try:
    definition = getDefinition(df)

    #Пытаемся найти отбор по счёту
    searchrow = df.loc[df[0] == SEARCH_STR]
    searchstr = ""
    if not searchrow.empty :
        searchstr = searchrow[2]

    df, openbalance, controlDebet, controlCredit, controlBalance = getControlValues(df)

    if not df.empty :
        df = df.dropna(axis=1,how='all')
        df = addMissedColumns(df, definition['columns'], definition['cols2del'])

    df = publishgDataFrame(df, xlsname, clientid, searchstr, definition, openbalance, controlDebet, controlCredit, controlBalance)
    return df

def getHeadLinesPDF(pdfname: str, nlines: int = 3) :
    result = []
    for page_layout in extract_pages(pdfname, maxpages=1) :
        for element in page_layout :
            if isinstance(element, LTTextBoxHorizontal) :
                txt = element.get_text()
                lines = [x.strip() for x in txt.split("\n")]
                for line in lines :
                    if len(line) > 0 :
                        result.append(line)
                        if len(result) >= nlines :
                            return result
    return result


DOCTYPES = ["выписка", "оборотно-сальдовая ведомость", "обороты счета", "обороты счёта", "анализ счета", "анализ счёта", "карточка счёта 51", "карточка счета 51"]

def processPDF(pdfname, clientid) :
    kinds = []
    berror = False
    try :
        headers = getHeadLinesPDF(pdfname, 10)
        suitable = [kind for kind in DOCTYPES if len([row for row in headers if kind in row.lower()])>0]
        if len(suitable) > 0 :
            kinds.append(suitable[0])
        print(pdfname, "::KIND:", kinds)

        #for header in filter(lambda row: any([kind for kind in DOCTYPES if kind in row.lower()]), headers) :
        #    print(pdfname, ":", header)
    except Exception as err :
        berror = True   
        print(pdfname, '_', 'ND', ':ERROR:', err)
        logstr = "ERROR:" + clientid + ":" + os.path.basename(pdfname) + ":ND:0:" + type(err).__name__ + " " + str(err) + "\n"
        logf.write(logstr)
    if len(kinds) > 0 :
        return (kinds[0], berror)
    else :
        return ("UNDEFINED", berror)

def getHeadLinesEXCEL(data, nlines: int = 3):
    idx = 0
    result = []
    while(idx < data.shape[0] and idx < nlines):
        row = data.iloc[[idx][0]]
        result.append(row.dropna(how='all'))
        idx = idx+1
    return result

def processExcel(xlsname, clientid) :
    berror = False
    sheets = pd.read_excel(xlsname, header=None, sheet_name=None)
    kinds = []
    if len(sheets) > 1 :
        print(xlsname, ':WARNING:', len(sheets), " sheets found")
    #if len(sheets) > 1 :
    for sheet in sheets :
        try :
            headers = getHeadLinesEXCEL(sheets[sheet], 10)
            suitable = [kind for kind in DOCTYPES if len([row for row in headers if any(row.astype(str).str.contains(kind, case=False).dropna(how='all'))])>0]
            if len(suitable) > 0 :
                kinds.append(suitable[0])
            print(xlsname, ":", sheet, ":KIND:", kinds)
            break
            #for header in filter(lambda row: any([kind for kind in KINDS if any(row.str.contains(kind, case=False).dropna(how='all'))]), headers) :
            #    print(xlsname, ":", header.values)
        except Exception as err :
            berror = True   
            print(xlsname, '_', sheet, ':ERROR:', err)
            logstr = "ERROR:" + clientid + ":" + os.path.basename(xlsname) + ":" + sheet + ":0:" + type(err).__name__ + " " + str(err) + "\n"
            logf.write(logstr)
    if len(kinds) > 0 :
        return (kinds[0], berror)
    else :
        return ("UNDEFINED", berror)

parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
parser.add_argument("-d", "--data", default="../Acc51", help="Data folder")
parser.add_argument("-r", "--done", default="../Done", help="Done folder")
parser.add_argument("-l", "--logfile", default="./preanalys51log_new.txt", help="Log file")
parser.add_argument("-o", "--output", default="./preanalys51_new", help="Resulting file name (no extension)")
parser.add_argument("--split", default=True, action=BooleanOptionalAction, help="Weather splitting resulting file required (--no-spilt opposite option)")
parser.add_argument("-m", "--maxinput", default=500, type=int, help="Maximum files sored in one resulting file")
args = vars(parser.parse_args())


DIRPATH = args["data"] # + "/*/xls*"
logname = args["logfile"]
outbasename = args["output"]
bSplit = args["split"]
maxFiles = args["maxinput"]
doneFolder = args["done"] + "/"

logf = open(logname, "w", encoding='utf-8')
cnt = 0
#outname = "parsed.csv"
outname = outbasename + ".csv"

FILEEXT = ['.xls', '.xlsx', '.xml', '.pdf']

for root, dirs, files in os.walk(DIRPATH) :
    for name in filter(lambda file: any([ext for ext in FILEEXT if (file.lower().endswith(ext))]), files) :
        try :
            try :
                fileext = Path(name).suffix
                parts = os.path.split(root)
                clientid = parts[1]
                inname = root + os.sep + name  

                if name.lower().endswith('.xls') or name.lower().endswith('.xlsx') :
                    #sheets = pd.read_excel(inname, header=None, sheet_name=None)
                    if bSplit and cnt % maxFiles == 0 :
                        outname = outbasename + str(cnt) + ".csv"
                    kind, berror = processExcel(inname, clientid)
                elif name.lower().endswith('.pdf') :
                    kind, berror = processPDF(inname, clientid)

                if len(kind) > 0 :
                    cnt = cnt + 1

                    try :
                        logstr = "PROCESSED:" + clientid + ":" + os.path.basename(inname) + ":ALL:" + kind + ":" + fileext + ":" + outname + "\n"
                        logf.write(logstr)
                    except Exception as err:
                        logstr = "PROCESSED:" + clientid + ":ND:ND:ND:ND:ERROR " + err + "\n"
                        logf.write(logstr)

                #if not berror :
                #    shutil.move(inname, doneFolder + clientid + '_' + os.path.basename(inname))
            except Exception as err :
                print(inname, ':ERROR:', err)
                logstr = "FILE_ERROR:" + clientid + ":" + os.path.basename(inname) + "::::" + type(err).__name__ + " " + str(err) + "\n"
                logf.write(logstr)
        except Exception as err :
            print('!!!CRITICAL ERROR!!!', err)
            logstr = "CRITICAL ERROR:" + clientid + ":ND:ND:ERROR\n"
            logf.write(logstr)

logf.close()