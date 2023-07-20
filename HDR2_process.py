from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#Дата|Вид (шифр) операции (ВО)|Номер документа Банка|Номер документа|БИК банка корреспондента|Корреспондирующий счет|Сумма по дебету|Сумма по кредиту
def HDR2_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:

    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["Дата"]
    df["cpBIC"] = data["БИК банка корреспондента"]
    df["cpAcc"] = data["Корреспондирующий счет"]
    df["Debet"] = data["Сумма по дебету"]
    df["Credit"] = data["Сумма по кредиту"]

    #header: За период с 1 декабря 2019 г. по 30 ноября 2020 г.,,,,,
    acc = header[header.iloc[:,0] == 'СЧЕТ'].dropna(axis=1,how='all')
    if acc.size > 1:
        df["clientAcc"] = acc.iloc[:,1].values[0]
    cname = header[header.iloc[:,0] == 'НАЗВАНИЕ'].dropna(axis=1,how='all')
    if cname.size > 1:
        df["clientName"] = cname.iloc[:,1].values[0]
    obalance = header[header.iloc[:,0] == 'Входящий остаток'].dropna(axis=1,how='all')
    if obalance.size > 2:
        df["openBalance"] = obalance.iloc[:,2].values[0]
    df["clientBank"] = header.iloc[1,0]

    turnovers = footer[footer.iloc[:,0] == 'Итого обороты:'].dropna(axis=1,how='all')
    if turnovers.size > 1:
        df["totalDebet"] = turnovers.iloc[:,1].values[0]
    if turnovers.size > 2:
        df["totalCredit"] = turnovers.iloc[:,2].values[0]
    closingbalace = footer[footer.iloc[:,0] == 'Исходящий остаток'].dropna(axis=1,how='all')
    if closingbalace.size > 2:
        df["closingBalance"] = closingbalace.iloc[:,2].values[0]

    df["clientID"] = clientid
    df["filename"] = f"{inname}_{sheet}"

    return df
