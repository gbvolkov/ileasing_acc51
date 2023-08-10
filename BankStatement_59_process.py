from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#nпп|датадокументаdocumentdate|датавалютирvaluedate|видоперoptype|реквизитыкорреспондентаcounterpartydetailsнаименованиеname|реквизитыкорреспондентаcounterpartydetailsиннinn|реквизитыкорреспондентаcounterpartydetailsкппkpp|реквизитыкорреспондентаcounterpartydetailsсчетaccount|реквизитыкорреспондентаcounterpartydetailsбикbik|реквизитыкорреспондентаcounterpartydetailsбанкbank|списаноdebit|зачисленоcredit|основаниеоперацииназначениеплатежаpaymentdetails
#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientTaxCode", "clientName", "stmtDate", "stmtFrom", "stmtTo", 
#           "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "__header", "__hdrclientBIC", "__hdrclientAcc", "__hdrclientTaxCode",
#           "__hdropenBalance",
#           "toIgnore", "filename", 'processdate']

def BankStatement_59_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, params: dict, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    
    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["датадокументаdocumentdate"]
    df["cpBIC"] = data["реквизитыкорреспондентаcounterpartydetailsбикbik"]
    df["cpBank"] = data["реквизитыкорреспондентаcounterpartydetailsбанкbank"]
    df["cpAcc"] = data["реквизитыкорреспондентаcounterpartydetailsсчетaccount"]
    #df["clientAcc"] = data.apply(lambda row: row['счетполучателя'] if pd.isna(row.mask(row=='')['дебет']) else row['счетплательщика'], axis=1)
    #df["cpTaxCode"] = data.apply(lambda row: row['иннплательщика'] if row['оборотыrurдебет'].startswith('0.00') else row['иннполучателя'], axis=1)

    df["clientTaxCode"] = data["реквизитыкорреспондентаcounterpartydetailsиннinn"]
    df["cpName"] = data["реквизитыкорреспондентаcounterpartydetailsнаименованиеname"]

    df["Debet"] =  data["списаноdebit"]
    df["Credit"] = data["зачисленоcredit"]
    df["Comment"] = data["основаниеоперацииназначениеплатежаpaymentdetails"]

    return df
