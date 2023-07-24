from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#Дата операции|№ док.|Вид операции|Контрагент|ИНН контрагента|БИК банка контрагента|Лицевой счет|Дебет|Кредит|Назначение
#датаоперации|№док.|видоперации|контрагент|иннконтрагента|бикбанкаконтрагента|лицевойсчет|дебет|кредит|назначение
#датаоперации|№док.|видоперации|контрагент|иннконтрагента|бикбанкаконтрагента|лицевойсчет|дебет|кредит|назначение|суммавнац.покрытии|курс

#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_9_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["датаоперации"]
    df["cpBIC"] = data["бикбанкаконтрагента"]
    #df["cpBank"] = data["Контрагент.Банк"]
    df["cpAcc"] = data["лицевойсчет"]
    df["cpTaxCode"] = data["иннконтрагента"]
    df["cpName"] = data["контрагент"]
    df["Debet"] = data["дебет"]
    df["Credit"] = data["кредит"]
    df["Comment"] = data["назначение"]

    #header: За период,c 22.06.2020 по 22.06.2021,,,,
    if len(header.axes[0]) >= 5:
        df["clientName"] = header.iloc[4:,0]
        #df["clientBIC"] = data["Клиент.БИК"]
        df["clientBank"] = header.iloc[0:,0]
        df["clientAcc"] = header.iloc[3,0]
        
        obalance = header[header.iloc[:,0].fillna("").str.startswith('ВХОДЯЩИЙ ОСТАТОК')].dropna(axis=1,how='all')
        if obalance.size > 1:
            df["openBalance"] = obalance.iloc[:,1].values[0]
    if len(footer.axes[0]) >= 1:
        cbalance = footer[footer.iloc[:,0] == 'ИСХОДЯЩИЙ ОСТАТОК'].dropna(axis=1,how='all')
        if cbalance.size > 1:
            df["closingBalance"] = cbalance.iloc[:,1].values[0]

    df["clientID"] = clientid
    df["filename"] = f"{inname}_{sheet}"
    df['processdate'] = datetime.now()
    
    return df
