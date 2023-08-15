from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS


# датадокта|номердокта|корреспондентбанк|корреспондентсчет|корреспондентнаименование|видопер|оборотыподебету|оборотыпокредиту|назначениеплатежа
# COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_34_process(
    header: pd.DataFrame,
    data: pd.DataFrame,
    footer: pd.DataFrame,
    inname: str,
    clientid: str,
    params: dict,
    sheet: str,
    logf: TextIOWrapper,
) -> pd.DataFrame:
    df = pd.DataFrame(columns=COLUMNS)

    df["entryDate"] = data["датадокта"]
    # df["cpBIC"] = data["Корреспондент.БИК"]
    df["cpBank"] = data["корреспондентбанк"]
    df["cpAcc"] = data["корреспондентсчет"]
    df["cpName"] = data["корреспондентнаименование"]
    df["Debet"] = data["оборотыподебету"]
    df["Credit"] = data["оборотыпокредиту"]
    df["Comment"] = data["назначениеплатежа"]

    # acc = header[header.iloc[:,0].fillna("").str.startswith('Выписка по счету')].dropna(axis=1,how='all')
    # if acc.size > 0:
    #    df["clientAcc"] = acc.iloc[:,0].values[0]
    # df["clientName"] = header.iloc[2,0]
    if len(header.axes[0]) >= 1:
        set_header_fields(header, df)
    if len(footer.axes[0]) >= 1:
        set_footer_fields(footer, df)

    return df

def set_footer_fields(footer, df):
    turnovers = footer[footer.iloc[:, 0] == "Итого:"].dropna(axis=1, how="all")
    if turnovers.size > 1:
        df["totalDebet"] = turnovers.iloc[:, 1].values[0]
    if turnovers.size > 2:
        df["totalCredit"] = turnovers.iloc[:, 2].values[0]
    if len(footer.axes[0] >= 2):
        df["closingBalance"] = footer.iloc[1, 0]

def set_header_fields(header, df):
    df["clientBank"] = header.iloc[0, 0]
    if len(header.axes[0] >= 5):
        df["openBalance"] = header.iloc[4, 0]
