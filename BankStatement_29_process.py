from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS


# nпп|датасовершенияоперацииддммгг|реквизитыдокументанаоснованиикоторогобыласовершенаоперацияпосчетуспециальномубанковскомусчетувидшифр|реквизитыдокументанаоснованиикоторогобыласовершенаоперацияпосчетуспециальномубанковскомусчетуномер|реквизитыдокументанаоснованиикоторогобыласовершенаоперацияпосчетуспециальномубанковскомусчетудата|реквизитыбанкаплательщикаполучателяденежныхсредствномеркорреспондентскогосчета|реквизитыбанкаплательщикаполучателяденежныхсредствнаименование|реквизитыбанкаплательщикаполучателяденежныхсредствбик|реквизитыплательщикаполучателяденежныхсредствнаименованиефио|реквизитыплательщикаполучателяденежныхсредствиннкио|реквизитыплательщикаполучателяденежныхсредствкпп|реквизитыплательщикаполучателяденежныхсредствномерсчета|суммаоперациипосчетуподебету|суммаоперациипосчетупокредиту|назначениеплатежа
# #COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_29_process(
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

    # if "Реквизиты плательщика/получателя денежных средств.номер счета " in data.columns:
    #    data.rename(columns={"Реквизиты плательщика/получателя денежных средств.номер счета ": "Реквизиты плательщика/получателя денежных средств.номерсчета",
    #                         "Сумма операции по счету .по дебету|Сумма операции по счету .по кредиту": "Сумма операции по счету .подебету",
    #                         "Сумма операции по счету .по кредиту|Сумма операции по счету .по кредиту": "Сумма операции по счету .покредиту"})

    df["entryDate"] = data["датасовершенияоперацииддммгг"]
    df["cpBIC"] = data["реквизитыбанкаплательщикаполучателяденежныхсредствбик"]
    df["cpBank"] = data[
        "реквизитыбанкаплательщикаполучателяденежныхсредствнаименование"
    ]
    df["cpAcc"] = data["реквизитыплательщикаполучателяденежныхсредствномерсчета"]
    df["cpTaxCode"] = data["реквизитыплательщикаполучателяденежныхсредствиннкио"]
    df["cpName"] = data["реквизитыплательщикаполучателяденежныхсредствнаименованиефио"]
    df["Debet"] = data["суммаоперациипосчетуподебету"]
    df["Credit"] = data["суммаоперациипосчетупокредиту"]
    df["Comment"] = data["назначениеплатежа"]

    if len(header.axes[0]) >= 14:
        set_header_fields(header, df)

    if len(footer.axes[0]) >= 4:
        set_footer_fields(footer, df)

    return df

def set_footer_fields(footer, df):
    df["openBalance"] = footer.iloc[3, 0]
    df["closingBalance"] = footer.iloc[3, 6]
    df["totalDebet"] = footer.iloc[3, 2]
    df["totalCredit"] = footer.iloc[3, 3]

def set_header_fields(header, df):
    acc = header[header.iloc[:, 0] == "№"].dropna(axis=1, how="all")
    if acc.size > 1:
        df["clientAcc"] = (
                acc.iloc[:, 1:].astype(int).astype(str).iloc[0, :].str.cat(sep="")
            )

    df["clientName"] = header.iloc[13, 0]
    bic = header[header.iloc[:, 0] == "БИК"].dropna(axis=1, how="all")
    if bic.size > 1:
        df["clientBIC"] = (
                bic.iloc[:, 1:].astype(int).astype(str).iloc[0, :].str.cat(sep="")
            )
    df["clientBank"] = header.iloc[7, 0]
