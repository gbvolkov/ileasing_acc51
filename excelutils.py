import pandas as pd
from const import DOCTYPES



def get_head_lines_excel(data, nlines: int = 3):
    result = []
    for idx in range(min(nlines, data.shape[0])):
        row = data.iloc[idx].dropna(how="all").tolist()
        result.append(row)
    return [str(row) for row in result]


def get_excel_sheet_kind(df) -> tuple[str, list[str]]:
    headers = get_head_lines_excel(df, 30)
    suitable_kind = next(
        (kind for kind in DOCTYPES if any(kind in str(row).lower() for row in headers)),
        "UNDEFINED"
    )
    return (suitable_kind, headers)
