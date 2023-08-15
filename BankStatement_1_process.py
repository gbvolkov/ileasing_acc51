from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS


# датадокумента|датаоперации|n|бик|счет|контрагент|иннконтрагента|бикбанкаконтрагента|коррсчетбанкаконтрагента|наименованиебанкаконтрагента|счетконтрагента|списание|зачисление|назначениеплатежа|код
# датадокумента|датаоперации|n|бик|счет|контрагент|иннконтрагента|бикбанкаконтрагента|коррсчетбанкаконтрагента|наименованиебанкаконтрагента|счетконтрагента|списание|зачисление|назначениеплатежа|код|показательстатуса101|коддоходабюджетнойклассификации104|кодоктмо105|показательоснованияплатежа106|показательналоговогопериодакодтаможенногооргана107|показательномерадокумента108|показательдатыдокумента109|показательтипаплатежа110
def BankStatement_1_process(
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

    df["entryDate"] = data["датадокумента"]
    df["cpBIC"] = data["бикбанкаконтрагента"]
    df["cpAcc"] = data["счетконтрагента"]
    df["cpTaxCode"] = data["иннконтрагента"]
    df["cpName"] = data["контрагент"]
    df["Debet"] = data["списание"]
    df["Credit"] = data["зачисление"]
    df["Comment"] = data["назначениеплатежа"]

    df["clientBIC"] = data["бик"]
    df["clientAcc"] = data["счет"]

    return df
