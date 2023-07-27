from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#nпп|датадокументаdocumentdate|датавалютирvaluedate|видоперoptype|реквизитыкорреспондентаcounterpartydetailsбикbic|реквизитыкорреспондентаcounterpartydetailsсчетaccount|реквизитыкорреспондентаcounterpartydetailsиннinn|реквизитыкорреспондентаcounterpartydetailsкппkpp|реквизитыкорреспондентаcounterpartydetailsнаименованиеname|дебетdebit|кредитcredit|основаниеоперацииназначениеплатежаpaymentdetails
#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_38_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, params: dict, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    
    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["датадокументаdocumentdate"]
    df["cpBIC"] = data["реквизитыкорреспондентаcounterpartydetailsбикbic"]
    #df["cpBank"] = data["наименованиебанкаконтрагента"]
    df["cpAcc"] = data["реквизитыкорреспондентаcounterpartydetailsсчетaccount"]
    df["cpTaxCode"] = data["реквизитыкорреспондентаcounterpartydetailsиннinn"]
    df["cpName"] = data["реквизитыкорреспондентаcounterpartydetailsнаименованиеname"]

    df["Debet"] = data["дебетdebit"]
    df["Credit"] = data["кредитcredit"]
    df["Comment"] = data["основаниеоперацииназначениеплатежаpaymentdetails"]

    
    return df
