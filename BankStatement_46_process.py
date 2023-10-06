from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS


# no|датаоперации|nдокумента|шифрдокумента|бикбанкакорреспондента|наименованиекорреспондента|nсчетакорреспондента|дебет|кредит|суммавнацпокрытии|назначениеплатежа
# no|датаоперации|nдокумента|nсчетакорреспондента|дебет|суммавнацпокрытии|кредит|суммавнацпокрытии.1|назначениеплатежа
# COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_46_process(
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
    if "бикбанкакорреспондента" in data.columns:
        df["cpBIC"] = data["бикбанкакорреспондента"]
    # df["cpBank"] = data["контрагент.банк(бик,наименование)"]
    df["cpAcc"] = data["nсчетакорреспондента"]
    # df["cpTaxCode"] = data["иннконтрагента"]
    if "наименованиекорреспондента" in data.columns:
        df["cpName"] = data["наименованиекорреспондента"]

    df["Debet"] = data["дебет"]
    df["Credit"] = data["кредит"]
    df["Comment"] = data["назначениеплатежа"]

    return df
