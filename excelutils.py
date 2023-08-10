
from const import DOCTYPES


def getHeadLinesEXCEL(data, nlines: int = 3):
    idx = 0
    result = []
    while (idx < data.shape[0] and idx < nlines):
        row = data.iloc[[idx][0]].dropna(how='all').tolist()
        #result.append(row.dropna(how='all'))
        result = result + row
        idx += 1
    return [str(row) for row in result]

def getExcelSheetKind(df) -> tuple[str, list[str]]:
    kinds = []
    headers = getHeadLinesEXCEL(df, 30)
    if suitable := [
        kind
        for kind in DOCTYPES
        if len([row for row in headers if kind in str(row).lower()]) > 0
    ]:
        kinds.append(suitable[0])
    return (kinds[0] if kinds else "UNDEFINED", headers)
