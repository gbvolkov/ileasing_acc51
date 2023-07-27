from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#номерсчета|идентификатортранзакции|типоперациипополнениесписание|категорияоперации|статус|датасозданияоперации|датаавторизации|дататранзакции|идентификатороригинальнойоперации|суммаоперацииввалютеоперации|валютаоперации|суммаввалютесчета|контрагент|иннконтрагента|кппконтрагента|счетконтрагента|бикбанкаконтрагента|коррсчетбанкаконтрагента|наименованиебанкаконтрагента|назначениеплатежа|номерплатежа|очередность|кодуин|номеркарты|mcc|местосовершениягород|местосовершениястрана|адресорганизации|банк|статуссоставителярасчетногодокумента|кбккодбюджетнойклассификации|кодоктмо|основаниеналоговогоплатежа|налоговыйпериодкодтаможенногооргана|номерналоговогодокумента|датаналоговогодокумента|типналоговогоплатежа
#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_36_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, params: dict, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    
    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["дататранзакции"]
    df["cpBIC"] = data["бикбанкаконтрагента"]
    df["cpBank"] = data["наименованиебанкаконтрагента"]
    df["cpAcc"] = data["счетконтрагента"]
    df["cpTaxCode"] = data["иннконтрагента"]
    df["cpName"] = data["контрагент"]

    df["Debet"] = data.apply(lambda row: row['суммаввалютесчета'] if row["типоперациипополнениесписание"] == "Debit" else "", axis=1)
    df["Credit"] = data.apply(lambda row: row['суммаввалютесчета'] if row["типоперациипополнениесписание"] == "Credit" else "", axis=1)
    df["Comment"] = data["назначениеплатежа"]

    #df["clientBIC"] = header.iloc[1,0]
    #df["clientBank"] = header.iloc[1,0]
    df["clientAcc"] = data["номерсчета"]
    if len(header.axes[0]) >= 1:
        cname = header[header.iloc[:,0] == 'Владелец счета'].dropna(axis=1,how='all')
        if cname.size > 1:
            df["clientName"] = cname.iloc[:,1].values[0]

    
    return df
