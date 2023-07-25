from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#датадокумента|номердокумента|поступление|списание|счеторганизации|организация|иннорганизации|счетконтрагента|контрагент|иннконтрагента|назначениеплатежа|коддебитора|типдокумента
#датадокумента|номердокумента|счеторганизации|организация|иннорганизации|счетконтрагента|контрагент|иннконтрагента|назначениеплатежа|поступление|списание|остатоквходящий|остатокисходящий|коддебитора|типдокумента
#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientTaxCode", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_33_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["датадокумента"]
    #df["cpBIC"] = data["Корреспондент.БИК"]
    #df["cpBank"] = data["Банк корр."]
    df["cpAcc"] = data["счетконтрагента"]
    df["cpTaxCode"] = data["иннконтрагента"]
    df["cpName"] = data["контрагент"]
    df["Debet"] = data['списание']
    df["Credit"] = data['поступление']
    df["Comment"] = data["назначениеплатежа"]

    df["clientAcc"] = data["счеторганизации"]
    df["clientName"] = data["организация"]
    df["clientTaxCode"] = data["иннорганизации"]
    #df["clientBank"] = header.iloc[1,0]

    df["clientID"] = clientid
    df["filename"] = f"{inname}_{sheet}"
    df['processdate'] = datetime.now()
    
    return df
