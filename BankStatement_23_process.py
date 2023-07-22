from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#Номер строки|Дата проводки|Вид операции|Номер документа|Счет плательщика/получателя|Реквизиты плательщика/получателя денежных средств.Наименование/ФИО|Реквизиты плательщика/получателя денежных средств.ИНН/КИО|Реквизиты плательщика/получателя денежных средств.КПП|Сумма Дебет|Сумма Кредит|Назначение платежа
#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_23_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["Дата проводки"]
    #df["cpBIC"] = data["БИК банка корр."]
    #df["cpBank"] = data["Банк контрагента"]
    df["cpAcc"] = data["Счет плательщика/получателя"]
    df["cpTaxCode"] = data["Реквизиты плательщика/получателя денежных средств.ИНН/КИО"]
    df["cpName"] = data["Реквизиты плательщика/получателя денежных средств.Наименование/ФИО"]
    df["Debet"] = data['Сумма Дебет']
    df["Credit"] = data['Сумма Кредит']
    df["Comment"] = data["Назначение платежа"]

    acc = header[header.iloc[:,0] =='Счет: '].dropna(axis=1,how='all')
    if acc.size > 1:
        df["clientAcc"] = acc.iloc[:,1].values[0]
    clname = header[header.iloc[:,0] == 'Наименование:'].dropna(axis=1,how='all')
    if clname.size > 1:
        df["clientName"] = clname.iloc[:,1].values[0]
    df["clientBank"] = header.iloc[1,0]

    obalance = header[header.iloc[:,0] == 'Входящий остаток'].dropna(axis=1,how='all')
    if obalance.size > 2:
        df["openBalance"] = obalance.iloc[:,2].values[0]
    cbalance = footer[footer.iloc[:,1] == 'Исходящий остаток'].dropna(axis=1,how='all')
    if cbalance.size > 2:
        df["closingBalance"] = cbalance.iloc[:,2].values[0]
    turnovers = footer[footer.iloc[:,1] == 'Итого обороты:'].dropna(axis=1,how='all')
    if turnovers.size > 1:
        df["totalDebet"] = turnovers.iloc[:,1].values[0]
    if turnovers.size > 2:
        df["totalCredit"] = turnovers.iloc[:,2].values[0]

    df["clientID"] = clientid
    df["filename"] = f"{inname}_{sheet}"
    
    return df
