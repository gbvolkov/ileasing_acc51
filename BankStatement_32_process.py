from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#Дата|№ док.|ВО|БИК банка контрагента|Банк контрагента|ИНН контрагента|Контрагент|Счет контрагента|Дебет|Кредит|Назначение платежа
#дата|№док.|во|бикбанкаконтрагента|банкконтрагента|иннконтрагента|контрагент|счетконтрагента|дебет|кредит|назначениеплатежа
#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_32_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, params: dict, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["дата"]
    df["cpBIC"] = data["бикбанкаконтрагента"]
    df["cpBank"] = data["банкконтрагента"]
    df["cpAcc"] = data["счетконтрагента"]
    df["cpTaxCode"] = data["иннконтрагента"]
    df["cpName"] = data["контрагент"]
    df["Debet"] = data['дебет']
    df["Credit"] = data['кредит']
    df["Comment"] = data["назначениеплатежа"]

    if len(header.axes[0]) >= 1:
        acc = header[header.iloc[:,0] == 'Счет'].dropna(axis=1,how='all')
        if acc.size > 1:
            df["clientAcc"] = acc.iloc[:,1].values[0]
        cname = header[header.iloc[:,0] == 'Владелец счета'].dropna(axis=1,how='all')
        if cname.size > 1:
            df["clientName"] = cname.iloc[:,1].values[0]
        df["clientBank"] = header.iloc[0,0]

        obalance = header[header.iloc[:,0].fillna("").str.startswith('ВХОДЯЩИЙ ОСТАТОК')].dropna(axis=1,how='all')
        if obalance.size > 2:
            df["openBalance"] = obalance.iloc[:,2].values[0]

    
    return df
