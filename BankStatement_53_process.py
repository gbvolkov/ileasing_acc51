from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#дата|n|иннплательщика|иннполучателя|корреспондентбик|корреспондентсчет|корреспондентнаименование|во|содержание|оборотыrurдебет|оборотыrurкредит
#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientTaxCode", "clientName", "stmtDate", "stmtFrom", "stmtTo", 
#           "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "__header", "__hdrclientBIC", "__hdrclientAcc", "__hdrclientTaxCode",
#           "__hdropenBalance",
#           "toIgnore", "filename", 'processdate']

def BankStatement_53_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, params: dict, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    
    data["оборотыrurдебет"] = data["оборотыrurдебет"].fillna("0.00")
    data["оборотыrurкредит"] = data["оборотыrurкредит"].fillna("0.00")

    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["дата"]
    df["cpBIC"] = data["корреспондентбик"]
    #df["cpBank"] = data["банккоррбикинаименование"]
    df["cpAcc"] = data["корреспондентсчет"]
    
    df["cpTaxCode"] = data.apply(lambda row: row['иннплательщика'] if row['оборотыrurдебет'].startswith('0.00') else row['иннполучателя'], axis=1)

    df["clientTaxCode"] = data.apply(lambda row: row['иннполучателя'] if row['оборотыrurдебет'].startswith('0.00') else row['иннплательщика'], axis=1)

    df["cpName"] = data["корреспондентнаименование"]

    df["Debet"] =  data["оборотыrurдебет"]
    df["Credit"] = data["оборотыrurкредит"]
    df["Comment"] = data["содержание"]

    return df
