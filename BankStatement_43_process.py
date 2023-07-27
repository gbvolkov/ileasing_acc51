from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#дата|номер|дебет|кредит|контрагентнаименованиеиннкппсчет|контрагентбанкбикнаименование|назначениеплатежа|коддебитор|документ
#дата|номер|дебет|кредит|контрагентнаименованиеиннкппсчет|контрагентбанкбикнаименование|назначениеплатежа|коддебитора|документ
#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_43_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, params: dict, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    
    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["дата"]
    #df["cpBIC"] = data["бик"]
    df["cpBank"] = data["контрагентбанкбикнаименование"]
    #df["cpAcc"] = data["счет-корреспондент"]
    #df["cpTaxCode"] = data["реквизитыкорреспондента.иннконтрагента"]
    df["cpName"] = data["контрагентнаименованиеиннкппсчет"]

    df["Debet"] = data["дебет"]
    df["Credit"] = data["кредит"]
    df["Comment"] = data["назначениеплатежа"]

    
    return df
