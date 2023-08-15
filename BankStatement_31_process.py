from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS


# документ|датаоперации|корреспондентнаименование|корреспондентинн|корреспондентсчет|корреспондентбик|вхостаток|оборотдт|обороткт|назначениеплатежа
# COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_31_process(
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

    df["entryDate"] = data["датаоперации"]
    df["cpBIC"] = data["корреспондентбик"]
    # df["cpBank"] = data["Банк корр."]
    df["cpAcc"] = data["корреспондентсчет"]
    df["cpTaxCode"] = data["корреспондентинн"]
    df["cpName"] = data["корреспондентнаименование"]
    df["Debet"] = data["оборотдт"]
    df["Credit"] = data["обороткт"]
    df["Comment"] = data["назначениеплатежа"]

    if len(header.axes[0]) >= 3:
        set_header_fields(header, df)
    if len(footer.axes[0]) >= 1:
        set_footer_fields(footer, df)

    return df


def set_footer_fields(footer, df):
    cbalance = footer[footer.iloc[:, 1] == "Исходящий остаток:"].dropna(
        axis=1, how="all"
    )
    if cbalance.size > 1:
        df["closingBalance"] = cbalance.iloc[:, 1].values[0]
    turnovers = footer[footer.iloc[:, 0] == "ИТОГО:"].dropna(axis=1, how="all")
    if turnovers.size > 1:
        df["totalDebet"] = turnovers.iloc[:, 1].values[0]
    if turnovers.size > 2:
        df["totalCredit"] = turnovers.iloc[:, 2].values[0]


def set_header_fields(header, df):
    acc = header[
        header.iloc[:, 0].fillna("").str.startswith("Выписка по счету")
    ].dropna(axis=1, how="all")
    if acc.size > 0:
        df["clientAcc"] = acc.iloc[:, 0].values[0]
    df["clientName"] = header.iloc[2, 0]
    df["clientBank"] = header.iloc[1, 0]

    obalance = header[header.iloc[:, 1] == "Входящий остаток:"].dropna(
        axis=1, how="all"
    )
    if obalance.size > 1:
        df["openBalance"] = obalance.iloc[:, 1].values[0]
