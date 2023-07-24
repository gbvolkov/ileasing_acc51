from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#Дата проводки|№ документа|Клиент.ИНН|Клиент.Наименование|Клиент.Счет|Корреспондент.БИК|Корреспондент.Банк|Корреспондент.Счет|Корреспондент.ИНН|Корреспондент.Наименование|В О|Назначение платежа|Обороты.Дебет|Обороты.Кредит|Референс проводки
#датапроводки|№документа|клиент.инн|клиент.наименование|клиент.счет|корреспондент.бик|корреспондент.банк|корреспондент.счет|корреспондент.инн|корреспондент.наименование|во|назначениеплатежа|обороты.дебет|обороты.кредит|референспроводки
#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_35_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["датапроводки"]
    df["cpBIC"] = data["корреспондент.бик"]
    df["cpBank"] = data["корреспондент.банк"]
    df["cpAcc"] = data["корреспондент.счет"]
    df["cpTaxCode"] = data["корреспондент.инн"]
    df["cpName"] = data["корреспондент.наименование"]
    df["Debet"] = data['обороты.дебет']
    df["Credit"] = data['обороты.кредит']
    df["Comment"] = data["назначениеплатежа"]

    #df["clientBIC"] = header.iloc[1,0]
    #df["clientBank"] = header.iloc[1,0]
    df["clientAcc"] = data["клиент.счет"]
    df["clientName"] = data["клиент.наименование"]
    df["clientTaxCode"] = data["клиент.инн"]

    if len(header.axes[0]) >= 1:
        obalance = header[header.iloc[:,0].fillna("").str.startswith('Входящий остаток')].dropna(axis=1,how='all')
        if obalance.size > 1:
            df["openBalance"] = obalance.iloc[:,1].values[0]
    if len(footer.axes[0]) >= 1:
        cbalance = footer[footer.iloc[:,0].str.startswith('Исходящий остаток')].dropna(axis=1,how='all')
        if cbalance.size > 1:
            df["closingBalance"] = cbalance.iloc[:,1].values[0]
        turnovers = footer[footer.iloc[:,0] == 'Итого'].dropna(axis=1,how='all')
        if turnovers.size > 1:
            df["totalDebet"] = turnovers.iloc[:,1].values[0]
        if turnovers.size > 2:
            df["totalCredit"] = turnovers.iloc[:,2].values[0]

    df["clientID"] = clientid
    df["filename"] = f"{inname}_{sheet}"
    df['processdate'] = datetime.now()
    
    return df
