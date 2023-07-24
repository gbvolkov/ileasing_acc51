from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#№ п.п|№ док.|Дата операции|БИК/SWIFT банка плат.|Наименование Банка плательщика|Наименование плательщика|ИНН плательщика|№ счета плательщика|БИК/SWIFT банка получ.|Наименование банка получателя|Наименование получателя|ИНН получателя|№ счета получателя|Сальдо входящее|Дебет|Кредит|Сальдо исходящее|Назначение платежа
#№п.п|№док.|датаоперации|бик/swiftбанкаплат.|наименованиебанкаплательщика|наименованиеплательщика|иннплательщика|№счетаплательщика|бик/swiftбанкаполуч.|наименованиебанкаполучателя|наименованиеполучателя|иннполучателя|№счетаполучателя|сальдовходящее|дебет|кредит|сальдоисходящее|назначениеплатежа
#|№п.п|№док.|датаоперации|бик/swiftбанкаплат.|наименованиебанкаплательщика|наименованиеплательщика|иннплательщика|№счетаплательщика|бик/swiftбанкаполуч.|наименованиебанкаполучателя|наименованиеполучателя|иннполучателя|№счетаполучателя|сальдовходящее|дебет|кредит|сальдоисходящее|назначениеплатежа
#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_20_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["датаоперации"]

    df["cpBIC"] = data.apply(lambda row: row['бик/swiftбанкаплат.'] if pd.isna(row['дебет']) else row['бик/swiftбанкаполуч.'], axis=1)
    df["cpBank"] = data.apply(lambda row: row['наименованиебанкаплательщика'] if pd.isna(row['дебет']) else row['наименованиебанкаполучателя'], axis=1)
    df["cpAcc"] = data.apply(lambda row: row['№счетаплательщика'] if pd.isna(row['дебет']) else row['№счетаполучателя'], axis=1)
    df["cpTaxCode"] = data.apply(lambda row: row['иннплательщика'] if pd.isna(row['дебет']) else row['иннполучателя'], axis=1)
    df["cpName"] = data.apply(lambda row: row['наименованиеплательщика'] if pd.isna(row['дебет']) else row['наименованиеполучателя'], axis=1)
    df["Debet"] = data['дебет']
    df["Credit"] = data['кредит']
    df["Comment"] = data["назначениеплатежа"]


    df["clientBIC"] = data.apply(lambda row: row['бик/swiftбанкаплат.'] if pd.isna(row['кредит']) else row['бик/swiftбанкаполуч.'], axis=1)
    df["clientBank"] = data.apply(lambda row: row['наименованиебанкаплательщика'] if pd.isna(row['кредит']) else row['наименованиебанкаполучателя'], axis=1)
    df["clientAcc"] = data.apply(lambda row: row['№счетаплательщика'] if pd.isna(row['кредит']) else row['№счетаполучателя'], axis=1)
    df["clientName"] = data.apply(lambda row: row['наименованиеплательщика'] if pd.isna(row['кредит']) else row['наименованиеполучателя'], axis=1)
    df["clientTaxCode"] = data.apply(lambda row: row['иннплательщика'] if pd.isna(row['кредит']) else row['иннполучателя'], axis=1)

    df["openBalance"] = data["сальдовходящее"].iloc[0]
    df["closingBalance"] = data["сальдоисходящее"].iloc[len(data)-1]

    df["clientID"] = clientid
    df["filename"] = f"{inname}_{sheet}"
    df['processdate'] = datetime.now()
    
    return df
