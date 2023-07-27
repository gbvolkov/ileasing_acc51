from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#nдок|датадокумента|датаоперации|реквизитыкорреспондентанаименование|реквизитыкорреспондентасчет|реквизитыкорреспондентаиннконтрагента|реквизитыкорреспондентабанк|дебетсуммасуммавнп|кредитсуммасуммавнп|курсцбнадатуоперации|основаниеоперацииназначениеплатежа
#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_40_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, params: dict, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    
    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["датаоперации"]
    #df["cpBIC"] = data["реквизитыкорреспондента/counterpartydetails.бик/bic"]
    df["cpBank"] = data["реквизитыкорреспондентабанк"]
    df["cpAcc"] = data["реквизитыкорреспондентасчет"]
    df["cpTaxCode"] = data["реквизитыкорреспондентаиннконтрагента"]
    df["cpName"] = data["реквизитыкорреспондентанаименование"]

    df["Debet"] = data["дебетсуммасуммавнп"]
    df["Credit"] = data["кредитсуммасуммавнп"]
    df["Comment"] = data["основаниеоперацииназначениеплатежа"]

    
    return df
