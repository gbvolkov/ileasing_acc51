from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#Дата документа|Номер документа|Поступление|Списание|Счёт организации|Организация|ИНН организации|Счёт контрагента|Контрагент|ИНН контрагента|Назначение платежа|Код дебитора|Тип документа
#Дата документа|Номер документа|Счёт организации|Организация|ИНН организации|Счёт контрагента|Контрагент|ИНН контрагента|Назначение платежа|Поступление|Списание|Остаток входящий|Остаток исходящий|Код дебитора|Тип документа
#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientTaxCode", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_33_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["Дата документа"]
    #df["cpBIC"] = data["Корреспондент.БИК"]
    #df["cpBank"] = data["Банк корр."]
    df["cpAcc"] = data["Счёт контрагента"]
    df["cpTaxCode"] = data["ИНН контрагента"]
    df["cpName"] = data["Контрагент"]
    df["Debet"] = data['Списание']
    df["Credit"] = data['Поступление']
    df["Comment"] = data["Назначение платежа"]

    df["clientAcc"] = data["Счёт организации"]
    df["clientName"] = data["Организация"]
    df["clientTaxCode"] = data["ИНН организации"]
    #df["clientBank"] = header.iloc[1,0]

    df["clientID"] = clientid
    df["filename"] = f"{inname}_{sheet}"
    
    return df
