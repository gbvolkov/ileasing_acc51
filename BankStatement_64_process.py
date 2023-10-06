from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS
from utils import convert_to_float

# nдокумента|датасовершенияоперации|бикбанкаплательщиканаименованиебанкаплательщика|наименованиеплательщика|иннплательщика|номерсчетаплательщикакорсчетбанкаплательщика|бикбанкаполучателянаименованиебанкаполучателя|наименованиеполучателя|иннполучателя|номерсчетаполучателякорсчетбанкаполучателя|дебетовыйоборотввалютесчета|дебетовыйоборотврублевомэквиваленте|кредитовыйоборотввалютесчета|кредитовыйоборотврублевомэквиваленте|назначениеплатежа
# COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientTaxCode", "clientName", "stmtDate", "stmtFrom", "stmtTo",
#           "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "__header", "__hdrclientBIC", "__hdrclientAcc", "__hdrclientTaxCode",
#           "__hdropenBalance",
#           "toIgnore", "filename", 'processdate']
def BankStatement_64_process(
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

    df["entryDate"] = data["датасовершенияоперации"]
    df["cpBank"] = data.apply(lambda row: row['бикбанкаплательщиканаименованиебанкаплательщика'] if convert_to_float(str(row['дебетовыйоборотввалютесчета']))==0 else row['бикбанкаполучателянаименованиебанкаполучателя'], axis=1)
    df["cpAcc"] = data.apply(lambda row: row['номерсчетаплательщикакорсчетбанкаплательщика'] if convert_to_float(str(row['дебетовыйоборотввалютесчета']))==0 else row['номерсчетаполучателякорсчетбанкаполучателя'], axis=1)
    df["cpTaxCode"] = data.apply(lambda row: row['иннплательщика'] if convert_to_float(str(row['дебетовыйоборотввалютесчета']))==0 else row['иннполучателя'], axis=1)
    df["cpName"] = data.apply(lambda row: row['наименованиеплательщика'] if convert_to_float(str(row['дебетовыйоборотввалютесчета']))==0 else row['наименованиеполучателя'], axis=1)

    df["clientBank"] = data.apply(lambda row: row['бикбанкаплательщиканаименованиебанкаплательщика'] if convert_to_float(str(row['кредитовыйоборотввалютесчета']))==0 else row['бикбанкаполучателянаименованиебанкаполучателя'], axis=1)
    df["clientAcc"] = data.apply(lambda row: row['номерсчетаплательщикакорсчетбанкаплательщика'] if convert_to_float(str(row['кредитовыйоборотввалютесчета']))==0 else row['номерсчетаполучателякорсчетбанкаполучателя'], axis=1)
    df["clientTaxCode"] = data.apply(lambda row: row['иннплательщика'] if convert_to_float(str(row['кредитовыйоборотввалютесчета']))==0 else row['иннполучателя'], axis=1)
    df["clientName"] = data.apply(lambda row: row['наименованиеплательщика'] if convert_to_float(str(row['кредитовыйоборотввалютесчета']))==0 else row['наименованиеполучателя'], axis=1)


    df["Debet"] = data["дебетовыйоборотввалютесчета"]
    df["Credit"] = data["кредитовыйоборотввалютесчета"]
    df["Comment"] = data["назначениеплатежа"]

    footer = footer.dropna(axis=1, how="all")
    if len(footer.axes[0]) >= 1:
        set_footer_fields(footer, df)

    return df

def set_footer_fields(footer, df):

    #df["openBalance"] = footer.iloc[2, 0]
    #df["closingBalance"] = footer.iloc[2, 3]
    df["totalDebet"] = footer.iloc[0, 1]
    df["totalCredit"] = footer.iloc[0, 3]

