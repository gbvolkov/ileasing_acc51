from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#Дата опер.|КО|Номер докум.|Дата докум.|Дебет|Кредит|Рублевое покрытие|Контрагент.ИНН|Контрагент.КПП|Контрагент.Наименование|Контрагент.Счет|Контрагент.БИК|Контрагент.Коррсчет|Контрагент.Банк|Клиент.ИНН|Клиент.Наименование|Клиент.Счет|Клиент.КПП|Клиент.БИК|Клиент.Коррсчет|Клиент.Банк|Код|Назначение платежа|Очер. платежа|Бюджетный платеж.Статус сост.|Бюджетный платеж.КБК|Бюджетный платеж.ОКТМО|Бюджетный платеж.Основание|Бюджетный платеж.Налог. период|Бюджетный платеж.Номер док.|ID опер.
#датаопер.|ко|номердокум.|датадокум.|дебет|кредит|рублевоепокрытие|контрагент.инн|контрагент.кпп|контрагент.наименование|контрагент.счет|контрагент.бик|контрагент.коррсчет|контрагент.банк|клиент.инн|клиент.наименование|клиент.счет|клиент.кпп|клиент.бин|клиент.коррсчет|клиент.банк|код|назначениеплатежа|очер.платежа|бюджетныйплатеж.статуссост.|бюджетныйплатеж.кбк|бюджетныйплатеж.октмо|бюджетныйплатеж.основание|бюджетныйплатеж.налог.период|бюджетныйплатеж.номердок.|idопер.
#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_8_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["датадокум."]
    df["cpBIC"] = data["контрагент.бик"]
    df["cpBank"] = data["контрагент.банк"]
    df["cpAcc"] = data["контрагент.счет"]
    df["cpTaxCode"] = data["контрагент.инн"]
    df["cpName"] = data["контрагент.наименование"]
    df["Debet"] = data["дебет"]
    df["Credit"] = data["кредит"]
    df["Comment"] = data["назначениеплатежа"]

    #header: За период,c 22.06.2020 по 22.06.2021,,,,
    df["clientName"] = data["клиент.наименование"]
    df["clientBIC"] = data["клиент.бик"]
    df["clientBank"] = data["клиент.банк"]
    df["clientAcc"] = data["клиент.счет"]
    df["clientTaxCode"] = data["клиент.инн"]
    
    if len(header.axes[0]) >= 1:
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
    df['processdate'] = datetime.now()
    
    return df
