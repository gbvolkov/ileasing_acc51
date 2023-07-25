from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#номердокумента|ко|датаоперации|дебет|кредит|реквизитыконтрагентабик|реквизитыконтрагентанаименование|основаниеоперации
#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_37_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    
    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["датаоперации"]
    df["cpBIC"] = data["реквизитыконтрагентабик"]
    #df["cpBank"] = data["наименованиебанкаконтрагента"]
    #df["cpAcc"] = data["счетконтрагента"]
    #df["cpTaxCode"] = data["иннконтрагента"]
    df["cpName"] = data["реквизитыконтрагентанаименование"]

    df["Debet"] = data["дебет"]
    df["Credit"] = data["кредит"]
    df["Comment"] = data["основаниеоперации"]

    df["clientID"] = clientid
    df["filename"] = f"{inname}_{sheet}"
    df['processdate'] = datetime.now()
    
    return df
