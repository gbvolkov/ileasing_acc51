from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#номердокумента|датадокумента|датаоперации|счет|контрагент|иннконтрагента|бикбанкаконтрагента|коррсчетбанкаконтрагента|наименованиебанкаконтрагента|счетконтрагента|списание|зачисление|назначениеплатежа
#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_6_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:

    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["датадокумента"]
    df["cpBIC"] = data["бикбанкаконтрагента"]
    df["cpBank"] = data["наименованиебанкаконтрагента"]
    df["cpAcc"] = data["счетконтрагента"]
    df["cpTaxCode"] = data["иннконтрагента"]
    df["cpName"] = data["контрагент"]
    df["Debet"] = data["списание"]
    df["Credit"] = data["зачисление"]
    df["Comment"] = data["назначениеплатежа"]

    #header: За период,c 22.06.2020 по 22.06.2021,,,,
    df["clientAcc"] = data["счет"]

    df["clientID"] = clientid
    df["filename"] = f"{inname}_{sheet}"
    df['processdate'] = datetime.now()

    return df
