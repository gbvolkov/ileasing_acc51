from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#Дата документа|Дата операции|№|БИК|Счет|Контрагент|ИНН контрагента|БИК банка контрагента|Корр.счет банка контрагента|Наименование банка контрагента|Счет контрагента|Списание|Зачисление|Назначение платежа|Код"
#датадокумента|датаоперации|№|бик|счет|контрагент|иннконтрагента|бикбанкаконтрагента|корр.счетбанкаконтрагента|наименованиебанкаконтрагента|счетконтрагента|списание|зачисление|назначениеплатежа|код
def BankStatement_1_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:

    df = pd.DataFrame(columns = COLUMNS)

    df["clientBIC"] = data["бик"]
    df["clientAcc"] = data["счет"]

    df["entryDate"] = data["датадокумента"]
    df["cpBIC"] = data["бикбанкаконтрагента"]
    df["cpAcc"] = data["счетконтрагента"]
    df["cpTaxCode"] = data["иннконтрагента"]
    df["cpName"] = data["контрагент"]
    df["Debet"] = data["списание"]
    df["Credit"] = data["зачисление"]
    df["Comment"] = data["назначениеплатежа"]

    df["clientID"] = clientid
    df["filename"] = f"{inname}_{sheet}"
    df['processdate'] = datetime.now()

    return df
