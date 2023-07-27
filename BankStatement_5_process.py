from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#дата|номердокумента|дебет|кредит|контрагентнаименование|контрагентинн|контрагенткпп|контрагентбик|контрагентнаименованиебанка|назначениеплатежа|типдокумента
#дата|номердокумента|дебет|кредит|контрагентнаименование|контрагентинн|контрагенткпп|контрагентбик|контрагентнаименованиебанка|назначениеплатежа|коддебитора|типдокумента
#дата|номердокумента|дебет|кредит|контрагентнаименование|контрагентинн|контрагенткпп|контрагентсчет|контрагентбик|контрагентнаименованиебанка|назначениеплатежа|коддебитора|типдокумента
#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_5_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, params: dict, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:

    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["дата"]
    df["cpBIC"] = data["контрагентбик"]
    df["cpBank"] = data["контрагентнаименованиебанка"]
    if "контрагент.счет" in data.columns:
        df["cpAcc"] = data["контрагентсчет"]
    #df["cpAcc"] = data["Реквизиты корреспондента.Счет"]
    df["cpTaxCode"] = data["контрагентинн"]
    df["cpName"] = data["контрагентнаименование"]
    df["Debet"] = data["дебет"]
    df["Credit"] = data["кредит"]
    df["Comment"] = data["назначениеплатежа"]

    #header: За период,c 22.06.2020 по 22.06.2021,,,,
    if len(header.axes[0]) >= 1:
        acc = header[header.iloc[:,0] == 'Выписка по счету'].dropna(axis=1,how='all')
        if acc.size > 1:
            df["clientAcc"] = acc.iloc[:,1].values[0]
        bank = header[header.iloc[:,0] == 'БИК'].dropna(axis=1,how='all')
        if bank.size > 1:
            df["clientBIC"] = bank.iloc[:,1].values[0]
        cname = header[header.iloc[:,0] == 'Владелец счета'].dropna(axis=1,how='all')
        if cname.size > 1:
            df["clientName"] = cname.iloc[:,1].values[0]

        obalance = header[header.iloc[:,0] == 'Остаток входящий'].dropna(axis=1,how='all')
        if obalance.size > 1:
            df["openBalance"] = obalance.iloc[:,1].values[0]
        if obalance.size > 4:
            df["totalDebet"] = obalance.iloc[:,4].values[0]
        cbalance = header[header.iloc[:,0] == 'Остаток исходящий'].dropna(axis=1,how='all')
        if cbalance.size > 1:
            df["closingBalance"] = cbalance.iloc[:,1].values[0]
        if cbalance.size > 4:
            df["totalCredit"] = cbalance.iloc[:,4].values[0]

    return df
