from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#Тип|Дата|Номер|Вид операции|Сумма|Валюта|Основание платежа|БИК Банка получателя|Счет Получателя|Наименование Получателя
#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_14_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    df = pd.DataFrame(columns = COLUMNS)

    if data['Тип'].iloc[0] == 'Номер счета':
        header = data.iloc[:2]
        data = data.iloc[2:]

    df["entryDate"] = data["Дата"]
    df["cpBIC"] = data["БИК Банка получателя"]
    #df["cpBank"] = data["Банк контрагента"]
    df["cpAcc"] = data["Счет Получателя"]
    #df["cpTaxCode"] = data["Корреспондент.ИНН"]
    df["cpName"] = data["Наименование Получателя"]
    
    #>0 credit, < 0 debet
    df["Debet"] = data.apply(lambda row: row['Сумма'] if str(row['Сумма']).startswith('-') else '0.00', axis=1)
    df["Credit"] = data.apply(lambda row: '0.00' if str(row['Сумма']).startswith('-') else row['Сумма'], axis=1)

    df["Comment"] = data["Основание платежа"]

    #header: За период,c 22.06.2020 по 22.06.2021,,,,

    df["clientAcc"] = header.iloc[1,0]
    df["openBalance"] = header.iloc[1,3]
    df["closingBalance"] = header.iloc[1,4]
    df["totalDebet"] = header.iloc[1,5]
    df["totalCredit"] = header.iloc[1,6]
    
    df["clientID"] = clientid
    df["filename"] = f"{inname}_{sheet}"
    df['processdate'] = datetime.now()
    
    return df
