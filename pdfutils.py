import sys
from datetime import datetime
import pandas as pd

from pdfminer.layout import LTTextBoxHorizontal, LTTextLineHorizontal
from pdfminer.high_level import extract_pages
import PyPDF2
import camelot  # type: ignore

from const import DOCTYPES
from itertools import chain, islice


def pdf_pages_count(pdfname) -> int:
    with open(pdfname, "rb") as f:
        pdf_reader = PyPDF2.PdfReader(f)
        return pdf_reader.getNumPages()


def pdf_get_y_value(elem):
    return (elem.y0, -1 * elem.x0)


def get_head_lines_pdf(pdfname: str, nlines: int = 3):
    text_boxes = chain.from_iterable(
        sorted(
            (elem for elem in page_layout if isinstance(elem, LTTextBoxHorizontal)),
            key=pdf_get_y_value,
            reverse=True
        )
        for page_layout in extract_pages(pdfname, maxpages=1)
    )
    lines = (line.strip() for box in text_boxes for line in box.get_text().split("\n") if line.strip())
    return list(islice(lines, nlines))

def print_status(message: str, inname: str, npages: int, spage: int = None, lpage: int = None): # type: ignore
    current_time = datetime.now()
    page_info = f":PROCESSED: {spage}-{lpage} of " if spage and lpage else ""
    print(f"{current_time} : {inname} :{message}{page_info}{npages} pages", flush=True)


def get_pdf_data(inname: str, maxpages=0) -> pd.DataFrame:
    npages = min(pdf_pages_count(inname), maxpages or pdf_pages_count(inname))
    cchunk = 100
    tables_list = []

    if npages >= 500:
        print_status("WARNING: huge pdf:", inname, npages)

    for spage in range(1, npages + 1, cchunk):
        lpage = min(spage + cchunk - 1, npages)
        tables = camelot.read_pdf(
            inname,
            pages=f"{spage}-{lpage}",
            line_scale=100,
            shift_text=["l", "t"],
            backend="poppler",
            layout_kwargs={"char_margin": 0.1, "line_margin": 0.1, "boxes_flow": None},
        )
        tables_list.extend(t.df for t in tables)
        if npages >= 500:
            print_status("", inname, npages, spage, lpage)

    df = pd.concat(tables_list, ignore_index=True)
    return df.dropna(axis=1, how="all")

