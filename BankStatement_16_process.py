from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#№ документа|Дата|БИК|№ Счёта|Деб. оборот|Кред. оборот|ИНН и наименование получателя|Назначение платежа
#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_16_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["Дата"]
    df["cpBIC"] = data["БИК"]
    #df["cpBank"] = data["Банк контрагента"]
    df["cpAcc"] = data["№ Счёта"]
    #df["cpTaxCode"] = data["Корреспондент.ИНН"]
    df["cpName"] = data["ИНН и наименование получателя"]
    df["Debet"] = data['Деб. оборот']
    df["Credit"] = data['Кред. оборот']
    df["Comment"] = data["Назначение платежа"]

    if len(header.axes[0]) >= 2:
        df["clientAcc"] = header.iloc[1,0]
        df["clientName"] = header.iloc[0,0]
        df["clientBank"] = header.iloc[0,0]

    df["clientID"] = clientid
    df["filename"] = f"{inname}_{sheet}"
    df['processdate'] = datetime.now()
    
    return df
