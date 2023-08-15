from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS


# nдокумента|дата|бик|nсчета|дебоборот|кредоборот|иннинаименованиеполучателя|назначениеплатежа
# COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_16_process(
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

    df["entryDate"] = data["дата"]
    df["cpBIC"] = data["бик"]
    # df["cpBank"] = data["Банк контрагента"]
    df["cpAcc"] = data["nсчета"]
    # df["cpTaxCode"] = data["Корреспондент.ИНН"]
    df["cpName"] = data["иннинаименованиеполучателя"]
    df["Debet"] = data["дебоборот"]
    df["Credit"] = data["кредоборот"]
    df["Comment"] = data["назначениеплатежа"]

    if len(header.axes[0]) >= 2:
        set_header_fields(header, df)

    return df

def set_header_fields(header, df):
    df["clientAcc"] = header.iloc[1, 0]
    df["clientName"] = header.iloc[0, 0]
    df["clientBank"] = header.iloc[1, 0]
