from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#Дата документа|Дата операции|№|БИК|Счет|Контрагент|ИНН контрагента|БИК банка контрагента|Корр.счет банка контрагента|Наименование банка контрагента|Счет контрагента|Списание|Зачисление|Назначение платежа|Код"
def BankStatement_1_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:

    df = pd.DataFrame(columns = COLUMNS)

    df["clientBIC"] = data["БИК"]
    df["clientAcc"] = data["Счет"]

    df["entryDate"] = data["Дата документа"]
    df["cpBIC"] = data["БИК банка контрагента"]
    df["cpAcc"] = data["Счет контрагента"]
    df["cpTaxCode"] = data["ИНН контрагента"]
    df["cpName"] = data["Контрагент"]
    df["Debet"] = data["Списание"]
    df["Credit"] = data["Зачисление"]
    df["Comment"] = data["Назначение платежа"]

    df["clientID"] = clientid
    df["filename"] = f"{inname}_{sheet}"
    df['processdate'] = datetime.now()

    return df
