from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#"Номер документа|Дата документа|Дата операции|Счёт|Контрагент|ИНН контрагента|БИК банка контрагента|Корр.счёт банка контрагента|Наименование банка контрагента|Счёт контрагента|Списание|Зачисление|Назначение платежа
#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_6_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:

    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["Дата документа"]
    df["cpBIC"] = data["БИК банка контрагента"]
    df["cpBank"] = data["Наименование банка контрагента"]
    df["cpAcc"] = data["Счёт контрагента"]
    df["cpTaxCode"] = data["ИНН контрагента"]
    df["cpName"] = data["Контрагент"]
    df["Debet"] = data["Списание"]
    df["Credit"] = data["Зачисление"]
    df["Comment"] = data["Назначение платежа"]

    #header: За период,c 22.06.2020 по 22.06.2021,,,,
    df["clientAcc"] = data["Счёт"]

    df["clientID"] = clientid
    df["filename"] = f"{inname}_{sheet}"

    return df
