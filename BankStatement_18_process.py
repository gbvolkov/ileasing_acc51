from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#№ п/п|Дата совершения операции (дд.мм.гг)|Реквизиты документа, на основании которого была совершена операция по счету (специальному банковскому счету).вид (шифр)|Реквизиты документа, на основании которого была совершена операция по счету (специальному банковскому счету).номер|Реквизиты документа, на основании которого была совершена операция по счету (специальному банковскому счету).дата|Реквизиты банка плательщика/получателя денежных средств.номер корреспондентского счета|Реквизиты банка плательщика/получателя денежных средств.наименование|Реквизиты банка плательщика/получателя денежных средств.БИК|Реквизиты плательщика/получателя денежных средств.наименование/Ф.И.О.|Реквизиты плательщика/получателя денежных средств.ИНН/КИО|Реквизиты плательщика/получателя денежных средств.КПП|Реквизиты плательщика/получателя денежных средств.номер счета (специального банковского счета)|Сумма операции по счету (специальному банковскому счету).по дебету|Сумма операции по счету (специальному банковскому счету).по кредиту|Назначение платежа
#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_18_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["Дата совершения операции (дд.мм.гг)"]
    df["cpBIC"] = data["Реквизиты банка плательщика/получателя денежных средств.БИК"]
    df["cpBank"] = data["Реквизиты банка плательщика/получателя денежных средств.наименование"]
    df["cpAcc"] = data["Реквизиты плательщика/получателя денежных средств.номер счета (специального банковского счета)"]
    df["cpTaxCode"] = data["Реквизиты плательщика/получателя денежных средств.ИНН/КИО"]
    df["cpName"] = data["Реквизиты плательщика/получателя денежных средств.наименование/Ф.И.О."]
    df["Debet"] = data['Сумма операции по счету (специальному банковскому счету).по дебету']
    df["Credit"] = data['Сумма операции по счету (специальному банковскому счету).по кредиту']
    df["Comment"] = data["Назначение платежа"]

    acc = header[header.iloc[:,0] == '№'].dropna(axis=1,how='all')
    if acc.size > 1:
        df["clientAcc"] = acc.iloc[:,1:].astype(int).astype(str).iloc[0,:].str.cat(sep = "")

    df["clientName"] = header.iloc[14,0]
    bic = header[header.iloc[:,0] == 'БИК'].dropna(axis=1,how='all')
    if bic.size > 1:
        df["clientBIC"] = bic.iloc[:,1:].astype(int).astype(str).iloc[0,:].str.cat(sep = "")
    df["clientBank"] = header.iloc[8,0]

    df["openBalance"] = footer.iloc[3,0]
    df["closingBalance"] = footer.iloc[3,6]
    df["totalDebet"] = footer.iloc[3,2]
    df["totalCredit"] = footer.iloc[3,3]

    df["clientID"] = clientid
    df["filename"] = f"{inname}_{sheet}"
    
    return df
