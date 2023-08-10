from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#датадок|во|nдокум|бикбанкаконтраг|корреспондирующийсчет|оборотподебету|оборотпокредиту|инннаименованиеконтрагента|счетконтрагента|назначениеплатежа
#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientTaxCode", "clientName", "stmtDate", "stmtFrom", "stmtTo", 
#           "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "__header", "__hdrclientBIC", "__hdrclientAcc", "__hdrclientTaxCode",
#           "__hdropenBalance",
#           "toIgnore", "filename", 'processdate']

def BankStatement_57_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, params: dict, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    
    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["датадок"]
    df["cpBIC"] = data["бикбанкаконтраг"]
    #df["cpBank"] = data["банккорр"]
    df["cpAcc"] = data["счетконтрагента"]
    #df["clientAcc"] = data.apply(lambda row: row['счетполучателя'] if pd.isna(row.mask(row=='')['дебет']) else row['счетплательщика'], axis=1)
    #df["cpTaxCode"] = data.apply(lambda row: row['иннплательщика'] if row['оборотыrurдебет'].startswith('0.00') else row['иннполучателя'], axis=1)

    #df["clientTaxCode"] = data.apply(lambda row: row['иннполучателя'] if row['оборотыrurдебет'].startswith('0.00') else row['иннплательщика'], axis=1)

    df["cpName"] = data["инннаименованиеконтрагента"]

    df["Debet"] =  data["оборотподебету"]
    df["Credit"] = data["оборотпокредиту"]
    df["Comment"] = data["назначениеплатежа"]

    return df
