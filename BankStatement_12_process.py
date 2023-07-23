from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#Дата|РО|Док.|КБ|Внеш.счет|Счет|Дебет|Кредит|Назначение|Контрагент|Контр. ИНН
#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_12_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["Дата"]
    df["cpBIC"] = data["КБ"]
    #df["cpBank"] = data["Банк контрагента"]
    df["cpAcc"] = data["Внеш.счет"]
    df["cpTaxCode"] = data["Контр. ИНН"]
    df["cpName"] = data["Контрагент"]
    df["Debet"] = data["Дебет"]
    df["Credit"] = data["Кредит"]
    df["Comment"] = data["Назначение"]

    #header: За период,c 22.06.2020 по 22.06.2021,,,,
    df["clientBank"] = header.iloc[0,0]
    df["clientAcc"] = header.iloc[1,0]
    df["clientName"] = header.iloc[2,0]
    #df["clientBIC"] = data["Клиент.БИК"]
    #df["clientBank"] = header.iloc[0,0]
    
    turnovers = str(footer.iloc[0,0]).split(":")
    if len(turnovers) > 1:
        df["totalDebet"] = turnovers[1]
    if len(turnovers) > 2:
        df["totalCredit"] = turnovers[2]
    balances = str(footer.iloc[1,0]).split(":")
    if len(balances) > 2:
        df["closingBalance"] = balances[2]

    df["clientID"] = clientid
    df["filename"] = f"{inname}_{sheet}"
    df['processdate'] = datetime.now()
    
    return df
