from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#датаоперации|номердокумента|корреспондентнаименованиеинн|корреспондентномерсчета|корреспондентнаименованиебанкабик|дебет|кредит|назначениеплатежа
#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_50_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, params: dict, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:

    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["датаоперации"]
    #df["cpBIC"] = data["контрагентбик"]
    df["cpBank"] = data["корреспондентнаименованиебанкабик"]
    df["cpAcc"] = data["корреспондентномерсчета"]
    df["Debet"] = data["дебет"]
    df["Credit"] = data["кредит"]
    #df["cpTaxCode"] = data["контрагентинн"]
    df["cpName"] = data["корреспондентнаименованиеинн"]
    df["Comment"] = data["назначениеплатежа"]

    #header: За период,с 01.05.2021 по 31.05.2021,,,,
    if len(header.axes[0]) >= 5:
        df["clientName"] = header.iloc[0,0]
        df["clientAcc"] = header.iloc[1,0]

        df["openBalance"] = header.iloc[4,0]
    if len(footer.axes[0]) >= 1:
        cbalance = footer[footer.iloc[:,0].str.startswith('Исходящий остаток')].dropna(axis=1,how='all')
        if cbalance.size >= 1:
            df["closingBalance"] = cbalance.iloc[:,1].values[0]
        turnovers = footer[footer.iloc[:,0].str.startswith('Итого')].dropna(axis=1,how='all')
        if turnovers.size > 1:
            df["totalDebet"] = turnovers.iloc[:,1].values[0]
        if turnovers.size > 2:
            df["totalCredit"] = turnovers.iloc[:,2].values[0]

    return df
