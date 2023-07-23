from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#Дата док-та|Номер док-та|Корреспондент.Банк|Корреспондент.Счет|Корреспондент.Наименование|Вид опер.|Обороты.подебету|Обороты.покредиту|Назначение платежа
#Дата док-та|Номер док-та|Корреспондент.Банк|Корреспондент.Счет|Корреспондент.Наименование|Вид опер.|Обороты.по дебету|Обороты.по кредиту|Назначение платежа
#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_34_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    df = pd.DataFrame(columns = COLUMNS)

    if "Обороты.по дебету" in data.columns:
        data.rename(columns={"Обороты.по дебету": "Обороты.подебету", "Обороты.по кредиту": "Обороты.покредиту"})
    df["entryDate"] = data["Дата док-та"]
    #df["cpBIC"] = data["Корреспондент.БИК"]
    df["cpBank"] = data["Корреспондент.Банк"]
    df["cpAcc"] = data["Корреспондент.Счет"]
    df["cpName"] = data["Корреспондент.Наименование"]
    df["Debet"] = data['Обороты.подебету']
    df["Credit"] = data['Обороты.покредиту']
    df["Comment"] = data["Назначение платежа"]

    #acc = header[header.iloc[:,0].fillna("").str.startswith('Выписка по счету')].dropna(axis=1,how='all')
    #if acc.size > 0:
    #    df["clientAcc"] = acc.iloc[:,0].values[0]
    #df["clientName"] = header.iloc[2,0]
    if len(header.axes[0]) >= 1 :
        df["clientBank"] = header.iloc[0,0]
    if len(header.axes[0] >= 5) :
        df["openBalance"] = header.iloc[4,0]
    if len(header.axes[0] >= 2) :
        df["closingBalance"] = footer.iloc[1,0]
    turnovers = footer[footer.iloc[:,0] == 'Итого:'].dropna(axis=1,how='all')
    if turnovers.size > 1:
        df["totalDebet"] = turnovers.iloc[:,1].values[0]
    if turnovers.size > 2:
        df["totalCredit"] = turnovers.iloc[:,2].values[0]

    df["clientID"] = clientid
    df["filename"] = f"{inname}_{sheet}"
    
    return df
