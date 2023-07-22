from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#№ п.п|№ док.|Дата операции|БИК/SWIFT банка плат.|Наименование Банка плательщика|Наименование плательщика|ИНН плательщика|№ счета плательщика|БИК/SWIFT банка получ.|Наименование банка получателя|Наименование получателя|ИНН получателя|№ счета получателя|Сальдо входящее|Дебет|Кредит|Сальдо исходящее|Назначение платежа
#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_20_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["Дата операции"]

    df["cpBIC"] = data.apply(lambda row: row['БИК/SWIFT банка плат.'] if pd.isna(row['Дебет']) else row['БИК/SWIFT банка получ.'], axis=1)
    df["cpBank"] = data.apply(lambda row: row['Наименование Банка плательщика'] if pd.isna(row['Дебет']) else row['Наименование банка получателя'], axis=1)
    df["cpAcc"] = data.apply(lambda row: row['№ счета плательщика'] if pd.isna(row['Дебет']) else row['№ счета получателя'], axis=1)
    df["cpTaxCode"] = data.apply(lambda row: row['ИНН плательщика'] if pd.isna(row['Дебет']) else row['ИНН получателя'], axis=1)
    df["cpName"] = data.apply(lambda row: row['Наименование плательщика'] if pd.isna(row['Дебет']) else row['Наименование получателя'], axis=1)
    df["Debet"] = data['Дебет']
    df["Credit"] = data['Кредит']
    df["Comment"] = data["Назначение платежа"]


    df["clientBIC"] = data.apply(lambda row: row['БИК/SWIFT банка плат.'] if pd.isna(row['Кредит']) else row['БИК/SWIFT банка получ.'], axis=1)
    df["clientBank"] = data.apply(lambda row: row['Наименование Банка плательщика'] if pd.isna(row['Кредит']) else row['Наименование банка получателя'], axis=1)
    df["clientAcc"] = data.apply(lambda row: row['№ счета плательщика'] if pd.isna(row['Кредит']) else row['№ счета получателя'], axis=1)
    df["clientName"] = data.apply(lambda row: row['Наименование плательщика'] if pd.isna(row['Дебет']) else row['Наименование получателя'], axis=1)

    df["openBalance"] = data["Сальдо входящее"].iloc[0]
    df["closingBalance"] = data["Сальдо исходящее"].iloc[len(data)-1]

    df["clientID"] = clientid
    df["filename"] = f"{inname}_{sheet}"
    
    return df
