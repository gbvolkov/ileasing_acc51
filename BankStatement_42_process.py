from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#n|дата|счеткорреспондент|оборотдебет|обороткредит|примечание
#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_42_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, params: dict, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    
    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["дата"]
    #df["cpBIC"] = data["бик"]
    #df["cpBank"] = data["реквизитыкорреспондента.банк"]
    df["cpAcc"] = data["счеткорреспондент"]
    #df["cpTaxCode"] = data["реквизитыкорреспондента.иннконтрагента"]
    #df["cpName"] = data["реквизитыкорреспондента.наименование"]

    df["Debet"] = data["оборотдебет"]
    df["Credit"] = data["обороткредит"]
    df["Comment"] = data["примечание"]

    
    return df
