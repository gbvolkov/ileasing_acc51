from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#Дата опер.|КО|Номер докум.|Дата докум.|Дебет|Кредит|Рублевое покрытие|Контрагент.ИНН|Контрагент.КПП|Контрагент.Наименование|Контрагент.Счет|Контрагент.БИК|Контрагент.Коррсчет|Контрагент.Банк|Клиент.ИНН|Клиент.Наименование|Клиент.Счет|Клиент.КПП|Клиент.БИК|Клиент.Коррсчет|Клиент.Банк|Код|Назначение платежа|Очер. платежа|Бюджетный платеж.Статус сост.|Бюджетный платеж.КБК|Бюджетный платеж.ОКТМО|Бюджетный платеж.Основание|Бюджетный платеж.Налог. период|Бюджетный платеж.Номер док.|ID опер.
#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def HDR8_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["Дата докум."]
    df["cpBIC"] = data["Контрагент.БИК"]
    df["cpBank"] = data["Контрагент.Банк"]
    df["cpAcc"] = data["Контрагент.Счет"]
    df["cpTaxCode"] = data["Контрагент.ИНН"]
    df["cpName"] = data["Контрагент.Наименование"]
    df["Debet"] = data["Дебет"]
    df["Credit"] = data["Кредит"]
    df["Comment"] = data["Назначение платежа"]

    #header: За период,c 22.06.2020 по 22.06.2021,,,,
    df["clientName"] = data["Клиент.Наименование"]
    df["clientBIC"] = data["Клиент.БИК"]
    df["clientBank"] = data["Клиент.Банк"]
    df["clientAcc"] = data["Клиент.Счет"]
    
    obalance = header[header.iloc[:,0] == 'Входящий остаток'].dropna(axis=1,how='all')
    if obalance.size > 2:
        df["openBalance"] = obalance.iloc[:,2].values[0]

    if footer.axes[0].size > 2 :
        turnovers = footer.iloc[2]
        if turnovers.size > 1: 
            df["totalDebet"] = turnovers.iloc[1]
       
        if turnovers.size > 2:
            df["totalCredit"] = turnovers.iloc[2]
        if turnovers.size > 3: 
            df["closingBalance"] = turnovers.iloc[3]
    
    df["clientID"] = clientid
    df["filename"] = f"{inname}_{sheet}"
    
    return df
