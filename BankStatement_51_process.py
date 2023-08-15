from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS


# дата|n|во|контрагентинн|контрагентбикбанка|контрагентсчет|контрагентнаименование|оборотыrurдебет|оборотыrurкредит|назначение
# COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_51_process(
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
    df["cpBIC"] = data["контрагентбикбанка"]
    # df["cpBank"] = data["наименованиебанка"]
    df["cpAcc"] = data["контрагентсчет"]
    df["cpTaxCode"] = data["контрагентинн"]
    df["cpName"] = data["контрагентнаименование"]

    df["Debet"] = data["оборотыrurдебет"]
    df["Credit"] = data["оборотыrurкредит"]
    df["Comment"] = data["назначение"]

    return df
