from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#nпп|датасовершенияоперацииддммгг|реквизитыдокументанаоснованиикоторогобыласовершенаоперацияпосчетуспециальномубанковскомусчетувидшифр|реквизитыдокументанаоснованиикоторогобыласовершенаоперацияпосчетуспециальномубанковскомусчетуномер|реквизитыдокументанаоснованиикоторогобыласовершенаоперацияпосчетуспециальномубанковскомусчетудата|реквизитыбанкаплательщикаполучателяденежныхсредствномеркорреспондентскогосчета|реквизитыбанкаплательщикаполучателяденежныхсредствнаименование|реквизитыбанкаплательщикаполучателяденежныхсредствбик|реквизитыплательщикаполучателяденежныхсредствнаименованиефио|реквизитыплательщикаполучателяденежныхсредствиннкио|реквизитыплательщикаполучателяденежныхсредствкпп|реквизитыплательщикаполучателяденежныхсредствномерсчетаспециальногобанковскогосчета|суммаоперациипосчетуспециальномубанковскомусчетуподебету|суммаоперациипосчетуспециальномубанковскомусчетупокредиту|назначениеплатежа
#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_18_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["датасовершенияоперацииддммгг"]
    df["cpBIC"] = data["реквизитыбанкаплательщикаполучателяденежныхсредствбик"]
    df["cpBank"] = data["реквизитыбанкаплательщикаполучателяденежныхсредствнаименование"]
    df["cpAcc"] = data["реквизитыплательщикаполучателяденежныхсредствномерсчетаспециальногобанковскогосчета"]
    df["cpTaxCode"] = data["реквизитыплательщикаполучателяденежныхсредствиннкио"]
    df["cpName"] = data["реквизитыплательщикаполучателяденежныхсредствнаименованиефио"]
    df["Debet"] = data['суммаоперациипосчетуспециальномубанковскомусчетуподебету']
    df["Credit"] = data['суммаоперациипосчетуспециальномубанковскомусчетупокредиту']
    df["Comment"] = data["назначениеплатежа"]

    if len(header.axes[0]) >= 15:
        acc = header[header.iloc[:,0] == '№'].dropna(axis=1,how='all')
        if acc.size > 1:
            df["clientAcc"] = acc.iloc[:,1:].astype(int).astype(str).iloc[0,:].str.cat(sep = "")

        df["clientName"] = header.iloc[14,0]
        bic = header[header.iloc[:,0] == 'БИК'].dropna(axis=1,how='all')
        if bic.size > 1:
            df["clientBIC"] = bic.iloc[:,1:].astype(int).astype(str).iloc[0,:].str.cat(sep = "")
        df["clientBank"] = header.iloc[8,0]

    if len(footer.axes[0]) >= 4:
        df["openBalance"] = footer.iloc[3,0]
        df["closingBalance"] = footer.iloc[3,3]
        df["totalDebet"] = footer.iloc[3,1]
        df["totalCredit"] = footer.iloc[3,2]

    df["clientID"] = clientid
    df["filename"] = f"{inname}_{sheet}"
    df['processdate'] = datetime.now()
    
    return df
