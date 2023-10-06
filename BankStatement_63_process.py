from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS


# nпп|датасовершенияоперацииддммгггг|датасовершенияоперацииддммгггг.1|реквизитыдокументанаоснованиикоторогобыласовершенаоперацияпосчетуспециальномубанковскомусчетувидшифр|реквизитыдокументанаоснованиикоторогобыласовершенаоперацияпосчетуспециальномубанковскомусчетуномер|реквизитыдокументанаоснованиикоторогобыласовершенаоперацияпосчетуспециальномубанковскомусчетудата|реквизитыбанкаплательщикаполучателяденежныхсредствномеркорреспондентскогосчета|реквизитыбанкаплательщикаполучателяденежныхсредств|реквизитыбанкаплательщикаполучателяденежныхсредствнаименование|реквизитыбанкаплательщикаполучателяденежныхсредствбик|реквизитыбанкаплательщикаполучателяденежныхсредств.1|реквизитыплательщикаполучателяденежныхсредствнаименованиефио|реквизитыплательщикаполучателяденежныхсредств|реквизитыплательщикаполучателяденежныхсредствиннкио|реквизитыплательщикаполучателяденежныхсредствкпп|реквизитыплательщикаполучателяденежныхсредствномерсчетаспециальногобанковскогосчета|суммаоперациипосчетуспециальномубанковскомусчетуподебету|суммаоперациипосчетуспециальномубанковскомусчету|суммаоперациипосчетуспециальномубанковскомусчетупокредиту|назначениеплатежа|назначениеплатежа.1|назначениеплатежа.2# #COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_63_process(
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

    df["entryDate"] = data["датасовершенияоперацииддммгггг"]
    df["cpBIC"] = data["реквизитыбанкаплательщикаполучателяденежныхсредствбик"]
    df["cpBank"] = data[
        "реквизитыбанкаплательщикаполучателяденежныхсредствнаименование"
    ]
    df["cpAcc"] = data["реквизитыплательщикаполучателяденежныхсредствномерсчетаспециальногобанковскогосчета"]
    df["cpTaxCode"] = data["реквизитыплательщикаполучателяденежныхсредствиннкио"]
    df["cpName"] = data["реквизитыплательщикаполучателяденежныхсредствнаименованиефио"]
    df["Debet"] = data["суммаоперациипосчетуспециальномубанковскомусчетуподебету"]
    df["Credit"] = data["суммаоперациипосчетуспециальномубанковскомусчетупокредиту"]
    df["Comment"] = data["назначениеплатежа"]

    if len(header.axes[0]) >= 14:
        set_header_fields(header, df)

    footer = footer.iloc[1:4].dropna(axis=1, how="all")
    if len(footer.axes[0]) >= 3:
        set_footer_fields(footer, df)

    return df

def set_footer_fields(footer, df):

    df["openBalance"] = footer.iloc[2, 0]
    df["closingBalance"] = footer.iloc[2, 3]
    df["totalDebet"] = footer.iloc[2, 1]
    df["totalCredit"] = footer.iloc[2, 2]

def set_header_fields(header, df):
    acc = header[header.iloc[:, 0] == "№"].dropna(axis=1, how="all")
    if acc.size > 1:
        df["clientAcc"] = (
                acc.iloc[:, 1:].astype(int).astype(str).iloc[0, :].str.cat(sep="")
            )

    df["clientName"] = header.iloc[16, 0]
    bic = header[header.iloc[:, 0] == "БИК"].dropna(axis=1, how="all")
    if bic.size > 1:
        df["clientBIC"] = (
                bic.iloc[:, 1:].astype(int).astype(str).iloc[0, :].str.cat(sep="")
            )
    df["clientBank"] = header.iloc[7, 0]
