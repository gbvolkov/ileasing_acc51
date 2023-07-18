from enum import Enum
import pandas as pd
from pathlib import Path
import os
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter, BooleanOptionalAction
import sys
import PyPDF2
from pdfminer.high_level import extract_pages
from pdfminer.layout import LTTextBoxHorizontal, LTTextLineHorizontal

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
    for page_layout in extract_pages(pdfname, maxpages=1) :
        for element in page_layout :
            if isinstance(element, LTTextBoxHorizontal) :
                txt = element.get_text()
                lines = [x.strip() for x in txt.split("\n")]
                for line in lines :
                    if len(line) > 0 :
                        result.append(line)
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

DOCTYPES = ["выписка", "оборотно-сальдовая ведомость", "обороты счета", "обороты счёта", "анализ счета", "анализ счёта", "карточка счёта 51", "карточка счета 51"]
def process_pdf(pdfname, clientid, logf):
    """
    Processes a PDF file and determines its type.

    :param pdfname: Name of the PDF file.
    :type pdfname: str
    :param clientid: Client ID.
    :type clientid: str
    :param logf: Log file.
    :type logf: file
    :return: Tuple containing the determined type and a flag indicating if there was an error.
    :rtype: tuple[str, bool]
    """
    kinds = []
    berror = False
    try:
        headers = get_headlines_pdf2(pdfname, 30)
        if suitable := [
            kind
            for kind in DOCTYPES
            if len([row for row in headers if kind in row.lower()]) > 0
        ]:
            kinds.append(suitable[0])
        print(pdfname, "::KIND:", kinds)
    except Exception as err :
        berror = True   
        print(pdfname, '_', 'ND', ':ERROR:', err)
        fileext = Path(pdfname).suffix
        logstr = f"ERROR:{clientid}:{os.path.basename(pdfname)}:ND:UNDEFINED:{fileext}:{ type(err).__name__} {str(err)}\n"
        logf.write(logstr)
    return (kinds[0], berror) if kinds else ("UNDEFINED", berror)

def get_headlines_excel(data, nlines: int = 3):
    """
    Extracts the headlines from an Excel file.

    :param data: Excel data.
    :type data: pd.DataFrame
    :param nlines: Number of headlines to extract. Default is 3.
    :type nlines: int
    :return: List of extracted headlines.
    :rtype: list[pd.Series]
    """
    idx = 0
    result = []
    while (idx < data.shape[0] and idx < nlines):
        row = data.iloc[[idx][0]]
        result.append(row.dropna(how='all'))
        idx += 1
    return result

def process_excel(xlsname, clientid, logf):
    """
    Processes an Excel file and determines its type.

    :param xlsname: Name of the Excel file.
    :type xlsname: str
    :param clientid: Client ID.
    :type clientid: str
    :param logf: Log file.
    :type logf: file
    :return: Tuple containing the determined type and a flag indicating if there was an error.
    :rtype: tuple[str, bool]
    """
    berror = False
    sheets = pd.read_excel(xlsname, header=None, sheet_name=None)
    kinds = []
    if len(sheets) > 1:
        print(xlsname, ':WARNING:', len(sheets), " sheets found")
    for sheet in sheets:
        try:
            headers = get_headlines_excel(sheets[sheet], 10)
            if headers:
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
        except Exception as err :
            berror = True   
            print(xlsname, '_', sheet, ':ERROR:', err)
            fileext = Path(xlsname).suffix
            logstr = f"ERROR:{clientid}:{os.path.basename(xlsname)}:{sheet}:UNDEFINED:{fileext}:{ type(err).__name__} {str(err)}\n"
            logf.write(logstr)
    return (kinds[0], berror) if kinds else ("UNDEFINED", berror)

def process(inname, clientid, logf):
    """
    Processes a file and determines its type.

    :param inname: Name of the file.
    :type inname: str
    :param clientid: Client ID.
    :type clientid: str
    :param logf: Log file.
    :type logf: file
    :return: Tuple containing the determined type and a flag indicating if there was an error.
    :rtype: tuple[str, bool]
    """
    kind = ""
    berror = False
    if inname.lower().endswith('.xls') or inname.lower().endswith('.xlsx') :
        kind, berror = process_excel(inname, clientid, logf)
    elif inname.lower().endswith('.pdf') :
        kind, berror = process_pdf(inname, clientid, logf)
    return (kind, berror)

def main():
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument("-d", "--data", default="../Data", help="Data folder")
    parser.add_argument("-l", "--logfile", default="./preanalys_test_2.txt", help="Log file")
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
                inname = os.path.join(root, name)
                parts = os.path.split(root)
                clientid = parts[1]
                try :
                    fileext = Path(name).suffix
                    try :
                        kind, berror = process(inname, clientid, logf)
                        if len(kind) > 0 :
                            cnt = cnt + 1
                            try :
                                logstr = f"PROCESSED:{clientid}:{os.path.basename(inname)}:ALL:{kind}:{fileext}:OUT\n"
                                logf.write(logstr)
                            except Exception as err:
                                logstr = f"PROCESSED:{clientid}:ND:ND:UNDEFINED:{fileext}:ERROR {err}\n"
                                logf.write(logstr)
                        else : 
                            logstr = f"FILE_ERROR:{clientid}:{os.path.basename(inname)}::UNDEFINED:{fileext}:TYPE CANNOT BE DEFINED\n"
                            logf.write(logstr)
                    except Exception as err :
                        print(inname, ':ERROR:', err)
                        logstr = f"FILE_ERROR:{clientid}:{os.path.basename(inname)}::UNDEFINED:{fileext}:{type(err).__name__} {str(err)}\n"
                        logf.write(logstr)

                except Exception as err :
                    print('!!!CRITICAL ERROR!!!', err)
                    logstr = f"CRITICAL ERROR:{clientid}:ND:ND:UNDEFINED:ND:ERROR {type(err).__name__} {str(err)}\n"
                    logf.write(logstr)

if __name__ == "__main__":
    main()