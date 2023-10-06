from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

# nоп|датаоп|датапп|nпп|контрагентбик|контрагентрсчет|контрагенторганизация|контрагентинн|бюджетополучательлс|бюджетополучательнаименование|номердатабо|номердатадо|номердатасоглашениядоговорагпх|расходноеобязательство|датадок|nдок|видоп|квср|кфср|кцср|квр|косгу|допфк|допэк|допкр|ки|основание|ндс|назначениеплатежа|идентификаторплатежа|безправарасходования|оборотыполсдебет|оборотыполскредит|оборотыполсвтомчислевосстановлено|кассовыепрогнозы|оборотыпобанкудебет|оборотыпобанкукредит
# COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientTaxCode", "clientName", "stmtDate", "stmtFrom", "stmtTo",
#           "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "__header", "__hdrclientBIC", "__hdrclientAcc", "__hdrclientTaxCode",
#           "__hdropenBalance",
#           "toIgnore", "filename", 'processdate']


def BankStatement_62_process(
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

    df["entryDate"] = data["датаоп"]
    df["cpBIC"] = data["контрагентбик"]
    df["cpAcc"] = data["контрагентрсчет"]
    df["cpTaxCode"] = data["контрагентинн"]
    df["cpName"] = data["контрагенторганизация"]

    df["Debet"] = data["оборотыполсдебет"]
    df["Credit"] = data["оборотыполскредит"]
    df["Comment"] = data["основание"]

    return df
