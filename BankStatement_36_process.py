from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#Номер счета|Идентификатор транзакции|Тип операции (пополнение/списание)|Категория операции|Статус|Дата создания операции|Дата авторизации|Дата транзакции|Идентификатор оригинальной операции|Сумма операции в валюте операции|Валюта операции|Сумма в валюте счета|Контрагент|ИНН контрагента|КПП контрагента|Счет контрагента|БИК банка контрагента|Корр. счет банка контрагента|Наименование банка контрагента|Назначение платежа|Номер платежа|Очередность|Код (УИН)|Номер карты|MCC|Место совершения (Город)|Место совершения (Страна)|Адрес организации|Банк|Статус составителя расчетного документа|КБК-код бюджетной классификации|Код ОКТМО|Основание налогового платежа|Налоговый период/Код таможенного органа|Номер налогового документа|Дата налогового документа|Тип налогового платежа
#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_36_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    
    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["Дата транзакции"]
    df["cpBIC"] = data["БИК банка контрагента"]
    df["cpBank"] = data["Наименование банка контрагента"]
    df["cpAcc"] = data["Счет контрагента"]
    df["cpTaxCode"] = data["ИНН контрагента"]
    df["cpName"] = data["Контрагент"]

    df["Debet"] = data.apply(lambda row: row['Сумма в валюте счета'] if row["Тип операции (пополнение/списание)"] == "Debit" else "", axis=1)
    df["Credit"] = data.apply(lambda row: row['Сумма в валюте счета'] if row["Тип операции (пополнение/списание)"] == "Credit" else "", axis=1)
    df["Comment"] = data["Назначение платежа"]

    #df["clientBIC"] = header.iloc[1,0]
    #df["clientBank"] = header.iloc[1,0]
    df["clientAcc"] = data["Номер счета"]
    cname = header[header.iloc[:,0] == 'Владелец счета'].dropna(axis=1,how='all')
    if cname.size > 1:
        df["clientName"] = cname.iloc[:,1].values[0]

    df["clientID"] = clientid
    df["filename"] = f"{inname}_{sheet}"
    
    return df
