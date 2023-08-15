from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS


# датаоперации|датадокумента|во|nдокта|коррсчет|бик|наименованиебанка|счет|иннинаименованиекорреспондента|дебет|кредит|назначениеплатежа
# COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_48_process(
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
    df["cpBIC"] = data["бик"]
    df["cpBank"] = data["наименованиебанка"]
    df["cpAcc"] = data["счет"]
    # df["cpTaxCode"] = data["иннкорр"]
    df["cpName"] = data["иннинаименованиекорреспондента"]

    df["Debet"] = data["дебет"]
    df["Credit"] = data["кредит"]
    df["Comment"] = data["назначениеплатежа"]

    return df
