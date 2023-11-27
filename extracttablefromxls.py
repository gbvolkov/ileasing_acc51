import pandas as pd
from datetime import datetime
import os
from argparse import (
    ArgumentParser,
    ArgumentDefaultsHelpFormatter,
    BooleanOptionalAction,
)
import sys
from io import TextIOWrapper
from BankStatement_NO_process import NoneHDR_process, DATATYPES
from const import get_active_types, set_active_types

from excelutils import get_excel_sheet_kind
from parsing_utils import (
    cleanup_raw_data,
    cleanup_and_enreach_processed_data,
    get_file_ext_list,
    get_header_values,
    get_table_range,
    process_data_from_preanalysis,
    process_other,
    set_data_columns,
)
from pdfutils import get_head_lines_pdf, get_pdf_data

from process_map import HDRSIGNATURES
from utils import move_to_folder, print_exception, split_list


import warnings

warnings.filterwarnings("ignore", "This pattern has match groups")


def process_data(
    df: pd.DataFrame,
    headerstr: str,
    inname: str,
    clientid: str,
    sheet: str,
    logf: TextIOWrapper,
) -> pd.DataFrame:
    outdata = pd.DataFrame()
    header, data, footer = get_table_range(df)
    if not data.empty:
        data = set_data_columns(data)
        data = cleanup_raw_data(data)
        signature = "|".join(data.columns).replace("\n", " ")
        if not header.empty:
            headerstr = "|".join(
                header[:].apply(lambda x: "|".join(x.dropna().astype(str)), axis=1)
            )
        params = get_header_values(headerstr, signature)

        #funcs = [sig.get(signature) for sig in HDRSIGNATURES if sig.get(signature) is not None]
        #func = funcs[0] if funcs else NoneHDR_process
        func = next((sig.get(signature) for sig in HDRSIGNATURES if sig.get(signature)), NoneHDR_process)
        outdata = func(header, data, footer, inname, clientid, params, sheet, logf)  # type: ignore
        if not outdata.empty:
            outdata = cleanup_and_enreach_processed_data(outdata, inname, clientid, params, sheet, func.__name__)  # type: ignore
    return outdata



def process_sheet(sheet_data: pd.DataFrame, inname: str, clientid: str, sheet_name: str, logf: TextIOWrapper) -> tuple[pd.DataFrame, bool]:
    processed_data = pd.DataFrame()
    try:
        df = sheet_data.dropna(axis=1, how="all")
        if df.empty:
            return (processed_data, False)

        kind, header = get_excel_sheet_kind(df)
        if kind in get_active_types():
            return (process_data(df, "|".join(header), inname, clientid, sheet_name, logf), False)
        logstr = f"{datetime.now()}:PASSED:{clientid}:{os.path.basename(inname)}:{sheet_name}:0:{kind}\n"
        logf.write(logstr)
        return (processed_data, False)
    except Exception as err:
        print_exception(err, inname, clientid, sheet_name, logf)
        return(processed_data, True)


def process_excel(
    inname: str, clientid: str, logf: TextIOWrapper
) -> tuple[pd.DataFrame, int, bool]:
    with pd.ExcelFile(inname) as xls:
        sheets_dict = pd.read_excel(xls, sheet_name=None, header=None)
        
        if len(sheets_dict) > 1:
            print(f"{datetime.now()}:{inname}:WARNING:{len(sheets_dict)} sheets found")
        datas, errors = zip(*[process_sheet(sheet, inname, clientid, str(sheet_name), logf) for sheet_name, sheet in sheets_dict.items()])
    
    return (pd.concat(datas), len(sheets_dict), any(errors))


def process_pdf(
    inname: str, clientid: str, logf: TextIOWrapper
) -> tuple[pd.DataFrame, int, bool]:
    berror = False
    result = pd.DataFrame()

    try:
        df = get_pdf_data(inname)
        if not df.empty:
            header = get_head_lines_pdf(inname, 30)
            outdata = process_data(df, "|".join(header), inname, clientid, "pdf", logf)
            if not outdata.empty:
                result = pd.concat([result, outdata])
    except Exception as err:
        berror = True
        print_exception(err, inname, clientid, "pdf", logf)
    return (result, 1, berror)


def process(
    inname: str, clientid: str, logf: TextIOWrapper
) -> tuple[pd.DataFrame, int, bool]:
    processFunc = process_other

    if inname.lower().endswith(".xls") or inname.lower().endswith(".xlsx"):
        processFunc = process_excel
    elif inname.lower().endswith(".pdf"):
        processFunc = process_pdf

    df, pages, berror = processFunc(inname, clientid, logf)
    return (df, pages, berror)


def main():
    sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)  # type: ignore
    (
        preanalysislog,
        logname,
        outbasename,
        bSplit,
        maxFiles,
        doneFolder,
        FILEEXT,
        start,
        end,
        types,
    ) = get_parameters()
    set_active_types(types)
    process_data_from_preanalysis(
        process,
        preanalysislog,
        logname,
        outbasename,
        bSplit,
        maxFiles,
        doneFolder,
        FILEEXT,
        start,
        end,
        get_active_types(),
    )


def get_arguments():
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        "-d", "--data", default="./data/test_preanalysis.csv", help="Data folder"
    )
    parser.add_argument("-r", "--done", default="./data/Done", help="Done folder")
    parser.add_argument(
        "-l", "--logfile", default="./data/test_parsing_log.log.txt", help="Log file"
    )
    parser.add_argument(
        "-o",
        "--output",
        default="./data/test_parsed_statements",
        help="Resulting file name (no extension)",
    )
    parser.add_argument(
        "--split",
        default=True,
        action=BooleanOptionalAction,
        help="Weather splitting resulting file required (--no-spilt opposite option)",
    )
    parser.add_argument(
        "-m",
        "--maxinput",
        default=500,
        type=int,
        help="Maximum files sored in one resulting file",
    )
    parser.add_argument(
        "--pdf",
        default=False,
        action=BooleanOptionalAction,
        help="Weather to include pdf (--no-pdf opposite option)",
    )
    parser.add_argument(
        "--excel",
        default=True,
        action=BooleanOptionalAction,
        help="Weather to include excel files (--no-excel opposite option)",
    )
    parser.add_argument(
        "-s", "--start", default=-1, type=int, help="Starting position in data file"
    )
    parser.add_argument(
        "-e",
        "--end",
        default=-1,
        type=int,
        help="Ending position in data file (not included)",
    )
    parser.add_argument(
        "-t",
        "--types",
        default=["карточка счета 51", "выписка"], 
        type=split_list,
        help="List of document types to process",
    )
    return vars(parser.parse_args())


def get_parameters():
    args = get_arguments()

    preanalysislog = args["data"]
    logname = args["logfile"]
    outbasename = args["output"]
    bSplit = args["split"]
    maxFiles = args["maxinput"]
    doneFolder = args["done"] + "/"
    FILEEXT = get_file_ext_list(args["excel"], args["pdf"])
    start = args["start"]
    end = args["end"]
    types = args["types"]
    return (
        preanalysislog,
        logname,
        outbasename,
        bSplit,
        maxFiles,
        doneFolder,
        FILEEXT,
        start,
        end,
        types,
    )


if __name__ == "__main__":
    DATATYPES = []
    main()
