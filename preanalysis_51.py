from enum import Enum
import pandas as pd
from pathlib import Path
from datetime import datetime
import os
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter, BooleanOptionalAction
import sys
import PyPDF2

from pdfminer.high_level import extract_pages
from pdfminer.layout import LTTextBoxHorizontal, LTTextLineHorizontal

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

def getHeadLinesPDF2(pdfname: str, nlines: int = 3):
    result: list[str] = []
    with open(pdfname,'rb') as f:
        pdfReader = PyPDF2.PdfFileReader(f)
        txt = pdfReader.pages[0].extract_text()
        lines = [x.strip() for x in txt.split("\n")]
        result.extend(line for line in lines if len(line) > 0)
    return result

DOCTYPES = ["выписка", "оборотно-сальдовая ведомость", "обороты счета", "обороты счёта", "анализ счета", "анализ счёта", "карточка счёта 51", "карточка счета 51"]
def processPDF(pdfname, clientid, logf):
    kinds = []
    berror = False
    try:
        headers = getHeadLinesPDF2(pdfname, 30)
        if suitable := [
            kind
            for kind in DOCTYPES
            if len([row for row in headers if kind in row.lower()]) > 0
        ]:
            kinds.append(suitable[0])
        print(pdfname, "::KIND:", kinds)

            #for header in filter(lambda row: any([kind for kind in DOCTYPES if kind in row.lower()]), headers) :
            #    print(pdfname, ":", header)
    except Exception as err :
        berror = True   
        print(pdfname, '_', 'ND', ':ERROR:', err)
        fileext = Path(pdfname).suffix
        logstr = "ERROR|" + clientid + "|" + os.path.basename(pdfname) + "|ND|UNDEFINED|" + fileext + "|" + type(err).__name__ + " " + str(err) + "\n"
        logf.write(logstr)
    return (kinds[0], berror) if kinds else ("UNDEFINED", berror)

def getHeadLinesEXCEL(data, nlines: int = 3):
    idx = 0
    result = []
    while (idx < data.shape[0] and idx < nlines):
        row = data.iloc[[idx][0]]
        result.append(row.dropna(how='all'))
        idx += 1
    return result

def processExcel(xlsname, clientid, logf):
    berror = False
    sheets = pd.read_excel(xlsname, header=None, sheet_name=None)
    kinds = []
    if len(sheets) > 1:
        print(xlsname, ':WARNING:', len(sheets), " sheets found")
    #if len(sheets) > 1 :
    for sheet in sheets:
        try:
            headers = getHeadLinesEXCEL(sheets[sheet], 10)
            if suitable := [
                kind
                for kind in DOCTYPES
                if len(
                    [
                        row
                        for row in headers
                        if any(
                            row.astype(str)
                            .str.contains(kind, case=False)
                            .dropna(how='all')
                        )
                    ]
                )
                > 0
            ]:
                kinds.append(suitable[0])
            print(xlsname, ":", sheet, ":KIND:", kinds)
            #break
        except Exception as err :
            berror = True   
            print(xlsname, '_', sheet, ':ERROR:', err)
            fileext = Path(xlsname).suffix
            logstr = "ERROR|" + clientid + "|" + os.path.basename(xlsname) + "|" + sheet + "|UNDEFINED|" + fileext + "|" + type(err).__name__ + " " + str(err) + "\n"
            logf.write(logstr)
    return (kinds[0], berror) if kinds else ("UNDEFINED", berror)


def process(inname, clientid, logf):
    kind = ""
    berror = False
    if inname.lower().endswith('.xls') or inname.lower().endswith('.xlsx') :
        #sheets = pd.read_excel(inname, header=None, sheet_name=None)
        kind, berror = processExcel(inname, clientid, logf)
    elif inname.lower().endswith('.pdf') :
        kind, berror = processPDF(inname, clientid, logf)
    return (kind, berror)

def main():
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument("-d", "--data", default="../Data", help="Data folder")
    parser.add_argument("-l", "--logfile", default="./data/preanalys_test_2.txt", help="Log file")
    args = vars(parser.parse_args())

    DIRPATH = args["data"] # + "/*/xls*"
    logname = args["logfile"]

    with open(logname, "w", encoding='utf-8') as logf:
        cnt = 0

        FILEEXT = ['.xls', '.xlsx', '.pdf']
        sys.stdout.reconfigure(encoding="utf-8") # type: ignore

        for root, dirs, files in os.walk(DIRPATH):
            for name in filter(lambda file: any(ext for ext in FILEEXT if (file.lower().endswith(ext))), files):
                logf.flush()
                sys.stdout.flush()
                inname = root + os.sep + name  
                parts = os.path.split(root)
                clientid = parts[1]
                try :
                    fileext = Path(name).suffix
                    try :
                        kind, berror = process(inname, clientid, logf)
                        #kind = ""
                        #if name.lower().endswith('.xls') or name.lower().endswith('.xlsx') :
                        #    #sheets = pd.read_excel(inname, header=None, sheet_name=None)
                        #    kind, berror = processExcel(inname, clientid, logf)
                        #elif name.lower().endswith('.pdf') :
                        #    kind, berror = processPDF(inname, clientid, logf)

                        if len(kind) > 0 :
                            cnt = cnt + 1
                            try :
                                logstr = "PROCESSED|" + clientid + "|" + inname + "|ALL|" + kind + "|" + fileext + "|" + "OUT" + "\n"
                                logf.write(logstr)
                            except Exception as err:
                                logstr = "PROCESSED|" + clientid + "|ND|ND|UNDEFINED|" + fileext + "|ERROR " + err + "\n"
                                logf.write(logstr)
                        else : 
                            logstr = "FILE_ERROR|" + clientid + "|" + inname + "||UNDEFINED|" + fileext + "|" + "TYPE CANNOT BE DEFINED" + "\n"
                            logf.write(logstr)
                    except Exception as err :
                        print(inname, ':ERROR:', err)
                        logstr = "FILE_ERROR|" + clientid + "|" + inname + "||UNDEFINED|" + fileext + "|" + type(err).__name__ + " " + str(err) + "\n"
                        logf.write(logstr)

                except Exception as err :
                    print('!!!CRITICAL ERROR!!!', err)
                    logstr = "CRITICAL ERROR|" + clientid + "|ND|ND|ERROR\n"
                    logf.write(logstr)

main()