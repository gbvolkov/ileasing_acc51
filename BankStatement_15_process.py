from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#№ П/П|Дата операции / Posting date|Дата валютир. / Value|Вид опер. / Op. type|Номер документа / Document number|Реквизиты корреспондента /Counter party details.Наименование / Name|Реквизиты корреспондента /Counter party details.Счет / Account|Реквизиты корреспондента /Counter party details.Банк / Bank|Дебет / Debit|Кредит / Credit|Основание операции (назначение платежа) / Payment details
#№п/п|датаоперации/postingdate|датавалютир./value|видопер./op.type|номердокумента/documentnumber|реквизитыкорреспондента/counterpartydetails.наименование/name|реквизитыкорреспондента/counterpartydetails.счет/account|реквизитыкорреспондента/counterpartydetails.банк/bank|дебет/debit|кредит/credit|основаниеоперации(назначениеплатежа)/paymentdetails
#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_15_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["датаоперации/postingdate"]
    df["cpBIC"] = data["реквизитыкорреспондента/counterpartydetails.банк/bank"]
    #df["cpBank"] = data["Банк контрагента"]
    df["cpAcc"] = data["реквизитыкорреспондента/counterpartydetails.счет/account"]
    #df["cpTaxCode"] = data["Корреспондент.ИНН"]
    df["cpName"] = data["реквизитыкорреспондента/counterpartydetails.наименование/name"]
    df["Debet"] = data['дебет/debit']
    df["Credit"] = data['кредит/credit']
    df["Comment"] = data["основаниеоперации(назначениеплатежа)/paymentdetails"]

    #header: За период,c 22.06.2020 по 22.06.2021,,,,

    if len(header.axes[0]) >= 1:
        acc = header[header.iloc[:,0] == 'Номер счета / Account number:'].dropna(axis=1,how='all')
        if acc.size > 1:
            df["clientAcc"] = acc.iloc[:,1].values[0]
        client = header[header.iloc[:,0] == 'Клиент / Client:'].dropna(axis=1,how='all')
        if client.size > 1:
            df["clientName"] = client.iloc[:,1].values[0]
        bank = header[header.iloc[:,0] == 'Банк / Bank:'].dropna(axis=1,how='all')
        if bank.size > 1:
            df["clientBank"] = bank.iloc[:,1].values[0]

        obalance = header[header.iloc[:,0] == 'Входящий остаток на начало дня (периода) / Opening Balance:'].dropna(axis=1,how='all')
        if obalance.size > 1:
            df["openBalance"] = obalance.iloc[:,1].values[0]
    if len(footer.axes[0]) >= 1:
        cbalance = footer[footer.iloc[:,0] == 'Исходящий остаток на конец дня (периода) / Closing balance'].dropna(axis=1,how='all')
        if cbalance.size > 1:
            df["closingBalance"] = cbalance.iloc[:,1].values[0]
        turnovers = footer[footer.iloc[:,0] == 'Обороты / Turnover'].dropna(axis=1,how='all')
        if turnovers.size > 1:
            df["totalDebet"] = turnovers.iloc[:,1].values[0]
        if turnovers.size > 2:
            df["totalCredit"] = turnovers.iloc[:,2].values[0]

    df["clientID"] = clientid
    df["filename"] = f"{inname}_{sheet}"
    df['processdate'] = datetime.now()
    
    return df
