from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#nдок|датадокумента|датаоперации|реквизитыкорреспондентанаименование|реквизитыкорреспондентасчет|реквизитыкорреспондентаиннконтрагента|реквизитыкорреспондентабанк|дебетсуммасуммавнп|кредитсуммасуммавнп|курсцбнадатуоперации|основаниеоперацииназначениеплатежа
#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_4_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, params: dict, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:

    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["датадокумента"]
    df["cpBIC"] = data["реквизитыкорреспондентабанк"]
    #df["cpBank"] = data["Контрагент.Наименование банка "]
    df["cpAcc"] = data["реквизитыкорреспондентасчет"]
    df["cpTaxCode"] = data["реквизитыкорреспондентаиннконтрагента"]
    df["cpName"] = data["реквизитыкорреспондентанаименование"]
    df["Debet"] = data["дебетсуммасуммавнп"]
    df["Credit"] = data["кредитсуммасуммавнп"]
    df["Comment"] = data["основаниеоперацииназначениеплатежа"]

    #header: Выписка по счету клиента \nза период с 01.06.2020 по 01.09.2020
    if len(header.axes[0]) >= 1:
        header0 = header[0].str.split(':', expand=True)
        bank = header0[header0.iloc[:,0] == 'Банк'].dropna(axis=1,how='all')
        if bank.size > 1:
            df["clientBank"] = bank.iloc[:,1].values[0]

        acc = header0[header0.iloc[:,0] == 'Номер счета'].dropna(axis=1,how='all')
        if acc.size > 1:
            df["clientAcc"] = acc.iloc[:,1].values[0]
        cname = header0[header0.iloc[:,0] == 'Клиент'].dropna(axis=1,how='all')
        if cname.size > 1:
            df["clientName"] = cname.iloc[:,1].values[0]
        obalance = header.iloc[header.axes[0].size-1].dropna(how='all')
        if obalance.size > 0:
            df["openBalance"] = obalance.values[0]

    if len(footer.axes[0]) >= 1:
        cbalance = footer[footer.iloc[:,0] == 'Исходящий остаток на конец дня (периода)'].dropna(axis=1,how='all')
        if cbalance.size > 1:
            df["closingBalance"] = cbalance.iloc[:,1].values[0]
        turnoverns = footer[footer.iloc[:,0] == 'Обороты'].dropna(axis=1,how='all')
        if turnoverns.size > 0:
            df["totalDebet"] = turnoverns.iloc[:,1].values[0]
        if turnoverns.size > 1:
            df["totalCredit"] = turnoverns.iloc[:,2].values[0]

    return df
