from enum import Enum
import pandas as pd
from pathlib import Path
import re
import math
from datetime import datetime
import locale
import shutil
import os
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter, BooleanOptionalAction

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
    periods = re.findall(DATE_REGEX, periodstr)
    if len(periods) > 0:
        for period in periods:
            #dtStr = period.replace('/', '.').replace('-','.')
            #dt = datetime.strptime(dtStr, "%d.%m.%Y")
            dates.append(period)
    else:
        periods = re.findall("\s\w+\s\d+ г.", periodstr) 
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

#Получаем список значимых столбцов
def getColumns(row):
    columns = [0,1,2,3,4,5,7,8,10,11]
    cols = []
    idx = 0
    for value in row:
        if not (isinstance(value, float) and math.isnan(value)):
            cols.append(idx)
        idx = idx+1
    print(cols)
    return cols


def getResult(data):
    #row = data.iloc[[-1][0]]
    row = data.iloc[[-1][0]]
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
    return {'Company_Name':companyName, 'Start':periods[0], 'Finish':periods[1], 'columns': columns} #, 'OpenD':openD, 'OpenBalance':openBalance, 'FirstRowIDX': firstrowidx, 'columns': columns}


#Совершенно костыльная процедура
def addMissedColumns(data, columns):
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
    return df.iloc[:, 0:10]

def getDataFrameFromExcel(df, clientid, xlsname):
    
    #Из первых строк файла получаем имя компании, начало и конец периода, баланс на начало периода, 
    #   магический столбец 'Д' и список значимых столбцов (для объединённых ячеек excel)
    #try:
    definition = getDefinition(df)

    #Берём контрольные данные из строки "Обороты за период и сальдо на конец"
    firstrow = df.loc[df[0].str.contains(BALANCE_STR, na=False)]
    lastrow = df.loc[df[0].str.contains(CHECK_STR[0], na=False) | df[0].str.contains(CHECK_STR[1], na=False)]
    #Пытаемся найти отбор по счёту
    searchrow = df.loc[df[0] == SEARCH_STR]
    searchstr = ""
    if not searchrow.empty :
        searchstr = searchrow[2]

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
    #lastrow = df.loc[df[0] == CHECK_STR]
    #firstrow = df.loc[df[0] == BALANCE_STR]

    #df = pd.read_excel(xlsname, header=None, skiprows=firstrow.index[0]+1, nrows=lastrow.index[0]-firstrow.index[0]-1)#definition['FirstRowIDX'])#, usecols=readablecols)
    lowidx = 1
    highidx = df.shape[0] + 1
    if not firstrow.empty :
        lowidx = firstrow.index[0]+1
    if not lastrow.empty :
        highidx = lastrow.index[0]

    df = df.iloc[lowidx:highidx,:]
    if not df.empty :
        df = df.dropna(axis=1,how='all')
        df = addMissedColumns(df, definition['columns'])
        df.columns = ["Date", "Document"
                    , "Debet_Analitics", "Credit_Analitics"
                    , "Debet_Account", "Debet_Amount"
                    , "Credit_Account", "Credit_Amount"
                    , "Balance_D", "Balance"]
    else :
        df = pd.DataFrame(columns = ["Date", "Document"
                , "Debet_Analitics", "Credit_Analitics"
                , "Debet_Account", "Debet_Amount"
                , "Credit_Account", "Credit_Amount"
                , "Balance_D", "Balance"])
        
    df['Debet_Amount'].fillna(0.0, inplace=True)
    df['Credit_Amount'].fillna(0.0, inplace=True)
    df['Balance'].fillna(0.0, inplace=True)
    #df.columns = header[0:len(readablecols)]

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

    df['Debet_Amount'] = df['Debet_Amount'].astype(str).str.replace(' ', '')
    df['Debet_Amount'] = df['Debet_Amount'].astype(str).str.replace(',', '.')
    df['Credit_Amount'] = df['Credit_Amount'].astype(str).str.replace(' ', '')
    df['Credit_Amount'] = df['Credit_Amount'].astype(str).str.replace(',', '.')
    df['Balance'] = df['Balance'].astype(str).str.replace(' ', '')
    df['Balance'] = df['Balance'].astype(str).str.replace(',', '.')
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

DIRPATH = './Data/*.xls*'


#if (Path('parsed.csv').is_file()):
#    Path.unlink('parsed.csv')
#Записываем результат

#for root, dirs, files in os.walk('./Data'):
#    for name in filter(lambda file: file.endswith('.xls') or file.endswith('.xlsx'), files):
#        parts = os.path.split(root)
#        clientid = parts[1]
#        filepath = root + os.sep + name
#        print(filepath)

#for xlsname in glob.glob(DIRPATH, recursive=True):
    #print('START: ' + xlsname)

parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
parser.add_argument("-d", "--data", default="./Data", help="Data folder")
parser.add_argument("-r", "--done", default="./Done", help="Done folder")
parser.add_argument("-l", "--logfile", default="./acc51log.txt", help="Log file")
parser.add_argument("-o", "--output", default="./parsed", help="Resulting file name (no extension)")
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

for root, dirs, files in os.walk(DIRPATH) :
    for name in filter(lambda file: file.lower().endswith('.xls') or file.lower().endswith('.xlsx'), files) :
        try :
            try :
                parts = os.path.split(root)
                clientid = parts[1]
                xlsname = root + os.sep + name  
                sheets = pd.read_excel(xlsname, header=None, sheet_name=None)
                if bSplit and cnt % maxFiles == 0 :
                    outname = outbasename + str(cnt) + ".csv"
                berror = False
                if len(sheets) > 1 :
                    print(xlsname, ':WARNING:', len(sheets), " sheets found")
                for sheet in sheets :
                    try :
                        df = sheets[sheet]
                        df = getDataFrameFromExcel(df, clientid, xlsname + "_" + sheet)
                        if not df.empty :
                            cnt = cnt + 1
                            if (Path(outname).is_file()):
                                df.to_csv(outname, mode="a", header=False, index=False)
                            else:
                                df.to_csv(outname, mode="w", index=False)

                            try :
                                logstr = "PROCESSED:" + clientid + ":" + os.path.basename(xlsname) + ":" + sheet + ":" + str(df.shape[0]) + ":" + outname + "\n"
                                logf.write(logstr)
                            except Exception as err:
                                logstr = "PROCESSED:" + clientid + ":ND:ND:ND:ERROR " + err + "\n"
                                logf.write(logstr)
                    except Exception as err :
                        berror = True   
                        print(xlsname, '_', sheet, ':ERROR:', err)
                        logstr = "ERROR:" + clientid + ":" + os.path.basename(xlsname) + ":" + sheet + ":0:" + type(err).__name__ + " " + str(err) + "\n"
                        logf.write(logstr)
                if not berror :
                    shutil.move(xlsname, doneFolder + clientid + '_' + os.path.basename(xlsname))
            except Exception as err :
                print(xlsname, ':ERROR:', err)
                logstr = "FILE_ERROR:" + clientid + ":" + os.path.basename(xlsname) + "::0:" + type(err).__name__ + " " + str(err) + "\n"
                logf.write(logstr)
        except Exception as err :
            print('!!!CRITICAL ERROR!!!', err)
            logstr = "CRITICAL ERROR:" + clientid + ":ND:ND:ERROR\n"
            logf.write(logstr)

logf.close()