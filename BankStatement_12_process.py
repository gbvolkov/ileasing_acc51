from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#Дата|РО|Док.|КБ|Внеш.счет|Счет|Дебет|Кредит|Назначение|Контрагент|Контр. ИНН
#дата|ро|док.|кб|внеш.счет|счет|дебет|кредит|назначение|контрагент|контр.инн
#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_12_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["дата"]
    df["cpBIC"] = data["кб"]
    #df["cpBank"] = data["Банк контрагента"]
    df["cpAcc"] = data["внеш.счет"]
    df["cpTaxCode"] = data["контр.инн"]
    df["cpName"] = data["контрагент"]
    df["Debet"] = data["дебет"]
    df["Credit"] = data["кредит"]
    df["Comment"] = data["назначение"]

    #header: За период,c 22.06.2020 по 22.06.2021,,,,
    if len(header.axes[0]) >= 3:
        df["clientBank"] = header.iloc[0,0]
        df["clientAcc"] = header.iloc[1,0]
        df["clientName"] = header.iloc[2,0]
        #df["clientBIC"] = data["Клиент.БИК"]
        #df["clientBank"] = header.iloc[0,0]
        
    if len(header.axes[0]) >= 2:
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
