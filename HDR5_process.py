from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#Дата|Номер документа|Дебет|Кредит|Контрагент.Наименование|Контрагент.ИНН|Контрагент.КПП|Контрагент.БИК|Контрагент.Наименование банка|Назначение платежа|Тип документа
#Дата|Номер документа|Дебет|Кредит|Контрагент.Наименование|Контрагент.ИНН|Контрагент.КПП|Контрагент.БИК|Контрагент.Наименование банка|Назначение платежа|Код дебитора|Тип документа
#Дата|Номер документа|Дебет|Кредит|Контрагент.Наименование|Контрагент.ИНН|Контрагент.КПП|Контрагент.Счет|Контрагент.БИК|Контрагент.Наименование банка|Назначение платежа|Код дебитора|Тип документа
#Дата|Номер документа|Дебет|Кредит|Контрагент.Наименование|Контрагент.ИНН|Контрагент.КПП|Контрагент.Счёт|Контрагент.БИК|Контрагент.Наименование банка|Назначение платежа|Код дебитора|Тип документа

#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def HDR5_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:

    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["Дата"]
    df["cpBIC"] = data["Контрагент.БИК"]
    df["cpBank"] = data["Контрагент.Наименование банка"]
    if "Контрагент.Счет" in data.columns :
        df["cpAcc"] = data["Контрагент.Счет"]
    elif  "Контрагент.Счёт" in data.columns :
        df["cpAcc"] = data["Контрагент.Счёт"]
    #df["cpAcc"] = data["Реквизиты корреспондента.Счет"]
    df["cpTaxCode"] = data["Контрагент.ИНН"]
    df["cpName"] = data["Контрагент.Наименование"]
    df["Debet"] = data["Дебет"]
    df["Credit"] = data["Кредит"]
    df["Comment"] = data["Назначение платежа"]

    #header: За период,c 22.06.2020 по 22.06.2021,,,,
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

    df["clientID"] = clientid
    df["filename"] = f"{inname}_{sheet}"

    return df
