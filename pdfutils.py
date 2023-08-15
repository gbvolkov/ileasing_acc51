import sys
from datetime import datetime
import pandas as pd

from pdfminer.layout import LTTextBoxHorizontal, LTTextLineHorizontal
from pdfminer.high_level import extract_pages
import PyPDF2
import camelot  # type: ignore

from const import DOCTYPES


def pdf_pages_count(pdfname) -> int:
    with open(pdfname, "rb") as f:
        # pdfReader = PyPDF2.PdfFileReader(f)
        pdfReader = PyPDF2.PdfReader(f)
        return pdfReader.getNumPages()


def pdf_get_y_value(elem):
    return (elem.y0, -1 * elem.x0)


def get_head_lines_pdf(pdfname: str, nlines: int = 3):
    result = []
    for page_layout in extract_pages(pdfname, maxpages=1):
        for element in sorted(list(filter(lambda elem: isinstance(elem, LTTextBoxHorizontal), page_layout)), key=pdf_get_y_value, reverse=True):  # type: ignore
            if isinstance(element, LTTextBoxHorizontal):
                txt = element.get_text()
                lines = [x.strip() for x in txt.split("\n")]
                for line in lines:
                    if len(line) > 0:
                        result.append(line)
                        if len(result) >= nlines:
                            return result
    return result


def get_pdf_data(inname: str, maxpages=0) -> pd.DataFrame:
    npages = pdf_pages_count(inname)
    if maxpages > 0:
        npages = min(npages, maxpages)
    spage = 1
    cchunk = 100

    df = pd.DataFrame()

    if npages >= 500:
        print(datetime.now(), ":", inname, ":WARINING: huge pdf:", npages, " pages")
        sys.stdout.flush()
    while spage <= npages:
        lpage = min(spage + cchunk - 1, npages)
        tables = camelot.read_pdf(
            inname,
            pages=f"{spage}-{lpage}",
            line_scale=100,
            shift_text=["l", "t"],
            backend="poppler",
            layout_kwargs={"char_margin": 0.1, "line_margin": 0.1, "boxes_flow": None},
        )
        for tbl in tables:
            df = pd.concat([df, tbl.df])
        if npages >= 500:
            print(
                datetime.now(),
                ":",
                inname,
                f":PROCESSED: {spage}-{lpage} of ",
                npages,
                " pages",
            )
            sys.stdout.flush()
        spage = lpage + 1
    # df = pd.concat([headerData, df])
    return df.reset_index(drop=True).dropna(axis=1, how="all")
