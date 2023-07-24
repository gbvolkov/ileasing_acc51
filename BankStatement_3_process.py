from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#Дата операции|Номер документа|Дебет|Кредит|Контрагент.Наименование |Контрагент.ИНН |Контрагент.КПП |Контрагент.Счет |Контрагент.БИК |Контрагент.Наименование банка |Назначение платежа|Тип документа
#датаоперации|номердокумента|дебет|кредит|контрагент.наименование|контрагент.инн|контрагент.кпп|контрагент.бик|контрагент.наименованиебанка|назначениеплатежа|коддебитора|типдокумента
#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_3_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:

    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["датаоперации"]
    df["cpBIC"] = data["контрагент.бик"]
    df["cpBank"] = data["контрагент.наименованиебанка"]
    if "контрагент.счет" in data.columns:
        df["cpAcc"] = data["контрагент.счет"]
    df["Debet"] = data["дебет"]
    df["Credit"] = data["кредит"]
    df["cpTaxCode"] = data["контрагент.инн"]
    df["cpName"] = data["контрагент.наименование"]
    df["Comment"] = data["назначениеплатежа"]

    #header: За период,с 01.05.2021 по 31.05.2021,,,,
    if len(header.axes[0]) >= 1:
        acc = header[header.iloc[:,0] == 'Выписка по счету'].dropna(axis=1,how='all')
        if acc.size > 1:
            df["clientAcc"] = acc.iloc[:,1].values[0]
        cname = header[header.iloc[:,0] == 'Владелец счета'].dropna(axis=1,how='all')
        if cname.size > 1:
            df["clientName"] = cname.iloc[:,1].values[0]
        cBIC = header[header.iloc[:,0] == 'БИК'].dropna(axis=1,how='all')
        if cBIC.size > 1:
            df["clientBIC"] = cBIC.iloc[:,1].values[0]

        obalance = header[header.iloc[:,0] == 'Остаток входящий'].dropna(axis=1,how='all')
        if obalance.size > 2:
            df["openBalance"] = obalance.iloc[:,1].values[0]
        if obalance.size > 5:
            df["totalDebet"] = obalance.iloc[:,4].values[0]
        cbalance = header[header.iloc[:,0] == 'Остаток исходящий'].dropna(axis=1,how='all')
        if cbalance.size > 2:
            df["closingBalance"] = cbalance.iloc[:,1].values[0]
        if cbalance.size > 5:
            df["totalCredit"] = cbalance.iloc[:,4].values[0]

    df["clientID"] = clientid
    df["filename"] = f"{inname}_{sheet}"
    df['processdate'] = datetime.now()

    return df
