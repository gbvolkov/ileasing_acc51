from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#датаоперации|номердокумента|суммаподебету|суммапокредиту|контрагентсчетинннаименование|контрагентбанкбикнаименование|назначениеплатежа|коддебитора|типдокумента
#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_39_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    
    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["датаоперации"]
    #df["cpBIC"] = data["реквизитыкорреспондента/counterpartydetails.бик/bic"]
    df["cpBank"] = data["контрагентбанкбикнаименование"]
    #df["cpAcc"] = data["реквизитыкорреспондента/counterpartydetails.счет/account"]
    #df["cpTaxCode"] = data["реквизитыкорреспондента/counterpartydetails.инн/inn"]
    df["cpName"] = data["контрагентсчетинннаименование"]

    df["Debet"] = data["суммаподебету"]
    df["Credit"] = data["суммапокредиту"]
    df["Comment"] = data["назначениеплатежа"]

    df["clientID"] = clientid
    df["filename"] = f"{inname}_{sheet}"
    df['processdate'] = datetime.now()
    
    return df
