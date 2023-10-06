from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

# датадок|во|nдок|коррсчет|коррсчет.1|бик|счет|дебет|кредит|инн|названиеконтрагента|основаниедокумента
# COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientTaxCode", "clientName", "stmtDate", "stmtFrom", "stmtTo",
#           "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "__header", "__hdrclientBIC", "__hdrclientAcc", "__hdrclientTaxCode",
#           "__hdropenBalance",
#           "toIgnore", "filename", 'processdate']


def BankStatement_61_process(
    header: pd.DataFrame,
    data: pd.DataFrame,
    footer: pd.DataFrame,
    inname: str,
    clientid: str,
    params: dict,
    sheet: str,
    logf: TextIOWrapper,
) -> pd.DataFrame:
    df = pd.DataFrame(columns=COLUMNS)

    df["entryDate"] = data["датадок"]
    df["cpBIC"] = data["бик"]
    df["cpAcc"] = data["счет"]
    df["cpTaxCode"] = data["инн"]
    df["cpName"] = data["названиеконтрагента"]

    df["Debet"] = data["дебет"]
    df["Credit"] = data["кредит"]
    df["Comment"] = data["основаниедокумента"]

    return df
