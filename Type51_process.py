import pandas as pd
from datetime import datetime
from io import TextIOWrapper
import os
import numpy as np

from const import COLUMNS

def getParametersValue(data: pd.DataFrame, col: str | int, lable: str, idx: int) -> pd.Series:
    value = pd.Series()
    rowval = data[data[col].astype(str).str.startswith(lable)].replace(r"\s+", "", regex=True).replace("", np.nan)
    if not rowval.empty:
        value = rowval.iloc[idx].dropna(how="all")
    return value

# период|документ|аналитикадт|аналитикакт|дебетсчет|кредитсчет|текущеесальдо
# COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientTaxCode", "clientName", "stmtDate", "stmtFrom", "stmtTo",
#           "codeIntDt", "codeIntCr",
#           "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "__header", "__hdrclientBIC", "__hdrclientAcc", "__hdrclientTaxCode",
#           "__hdropenBalance",
#           "toIgnore", "filename", 'processdate']

def Type51HDR_process(
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
    df["entryDate"] = data["период"]
    if all(col in data.columns for col in ["аналитикакт", "аналитикадт"]):
        df["clientAcc"] = data.apply(
            lambda row: row["аналитикадт"]
            if row["дебетсчет"] == "51"
            else row["аналитикакт"],
            axis=1,
        )    

        df["cpAcc"] = data.apply(
            lambda row: row["аналитикадт"]
            if row["кредитсчет"] == "51"
            else row["аналитикакт"],
            axis=1,
        )
    
    if "дебет" in data.columns:
        df["Debet"] = data["дебет"]
    if "кредит" in data.columns:
        df["Credit"] = data["кредит"]
    df["Comment"] = data["документ"]

    #для карточки 51 счёта. Счёт внутреннего учёта по Дебету и Кредиту
    if "дебетсчет" in data.columns:
        df["codeIntDt"] = data["дебетсчет"]
    if "кредитсчет" in data.columns:
        df["codeIntCr"] = data["кредитсчет"]

    openBalance = getParametersValue(data, "период", "Сальдо на начало", 0)
    closingBalance = getParametersValue(data, "период", "Обороты за период и сальдо на конец", -1)

    if openBalance.size > 1:
        df["openBalance"] = openBalance.iloc[openBalance.size-1]
    if closingBalance.size > 2:
        df["closingBalance"] = closingBalance.iloc[closingBalance.size-1]
        df["totalDebet"] = closingBalance.iloc[1]
        df["totalCredit"] = closingBalance.iloc[2]

    return df

#датапроводки|документ|документ.1|документ.2|документ.3|датадокта|аналитикадебет|аналитикакредит|оборотыдебет|обороты|оборотыкредит|обороты.1|текущеесальдодебет|текущеесальдокредит
# COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientTaxCode", "clientName", "stmtDate", "stmtFrom", "stmtTo",
#           "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "__header", "__hdrclientBIC", "__hdrclientAcc", "__hdrclientTaxCode",
#           "__hdropenBalance",
#           "toIgnore", "filename", 'processdate']

def Type51HDR_1_process(
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
    df["entryDate"] = data["датапроводки"]
    df["clientAcc"] = data.apply(
        lambda row: row["аналитикадебет"]
        if row["оборотыдебет"] == "51"
        else row["аналитикакредит"],
        axis=1,
    )    
    df["cpAcc"] = data.apply(
        lambda row: row["аналитикадебет"]
        if row["оборотыкредит"] == "51"
        else row["аналитикакредит"],
        axis=1,
    )    
    df["Debet"] = data["обороты"]
    df["Credit"] = data["обороты.1"]
    df["Comment"] = data["документ"]

    if "оборотыдебет" in data.columns:
        df["codeIntDt"] = data["оборотыдебет"]
    if "оборотыкредит" in data.columns:
        df["codeIntCr"] = data["оборотыкредит"]


    openBalance = getParametersValue(header, 0, "Начальное сальдо", 0)
    closingBalance = getParametersValue(header, 0, "Конечное сальдо", -1)
    turnovers = getParametersValue(header, 0, "Обороты", -1)

    if openBalance.size > 1:
        df["openBalance"] = openBalance.iloc[1]
    if closingBalance.size > 1:
        df["closingBalance"] = closingBalance.iloc[1]
    if turnovers.size > 2:
        df["totalDebet"] = turnovers.iloc[1]
        df["totalCredit"] = turnovers.iloc[2]
    
    return df

#дата|документ|операция|операция.1|дебетсчет|дебет|кредитсчет|кредит|текущеесальдо
# COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientTaxCode", "clientName", "stmtDate", "stmtFrom", "stmtTo",
#           "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "__header", "__hdrclientBIC", "__hdrclientAcc", "__hdrclientTaxCode",
#           "__hdropenBalance",
#           "toIgnore", "filename", 'processdate']

def Type51HDR_2_process(
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
    df["entryDate"] = data["дата"]
    df["Debet"] = data["дебет"]
    df["Credit"] = data["кредит"]
    df["Comment"] = data["документ"] + '|' + data["операция"] + "|" + data["операция.1"]

    if "дебетсчет" in data.columns:
        df["codeIntDt"] = data["дебетсчет"]
    if "кредитсчет" in data.columns:
        df["codeIntCr"] = data["кредитсчет"]


    openBalance = getParametersValue(data, "дата", "Сальдо на начало", 0)
    closingBalance = getParametersValue(data, "дата", "Обороты и сальдо на конец", -1)
    
    if openBalance.size > 1:
        df["openBalance"] = openBalance.iloc[openBalance.size-1]
    if closingBalance.size > 1:
        df["closingBalance"] = closingBalance.iloc[closingBalance.size-1]
        #df["totalDebet"] = closingBalance.iloc[:, 1].values[0]
        #df["totalCredit"] = closingBalance.iloc[:, 2].values[0]
    return df

# дата|документ|документ.1|операция|дебетсчет|дебет|дебетсумма|кредитсчет|кредит|кредитсумма|текущеесальдо|текущеесальдо.1
# COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientTaxCode", "clientName", "stmtDate", "stmtFrom", "stmtTo",
#           "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "__header", "__hdrclientBIC", "__hdrclientAcc", "__hdrclientTaxCode",
#           "__hdropenBalance",
#           "toIgnore", "filename", 'processdate']

def Type51HDR_3_process(
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
    df["entryDate"] = data["дата"]
    df["Debet"] = data["дебетсумма"]
    df["Credit"] = data["кредитсумма"]
    df["Comment"] = data["документ"]
    #для карточки 51 счёта. Счёт внутреннего учёта по Дебету и Кредиту
    if "дебетсчет" in data.columns:
        df["codeIntDt"] = data["дебетсчет"]
    if "кредитсчет" in data.columns:
        df["codeIntCr"] = data["кредитсчет"]

    openBalance = getParametersValue(data, "дата", "Сальдо на начало", 0)
    closingBalance = getParametersValue(data, "дата", "Сальдо на конец", -1)
    turnovers = getParametersValue(data, "дата", "Обороты за период", -1)

    if openBalance.size > 1:
        df["openBalance"] = openBalance.iloc[1]
    if closingBalance.size > 1:
        df["closingBalance"] = closingBalance.iloc[1]
    if turnovers.size > 2:
        df["totalDebet"] = turnovers.iloc[1]
        df["totalCredit"] = turnovers.iloc[2]

    return df

# дата|дебет|учетподебету|кредит|учетпокредиту|сумма|описание|текущийостатокдебет|текущийостатоккредит
# COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientTaxCode", "clientName", "stmtDate", "stmtFrom", "stmtTo",
#           "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "__header", "__hdrclientBIC", "__hdrclientAcc", "__hdrclientTaxCode",
#           "__hdropenBalance",
#           "toIgnore", "filename", 'processdate']

def Type51HDR_4_process(
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
    df["entryDate"] = data["дата"]
    df["Debet"] = data.apply(
        lambda row: row["сумма"]
        if str(row["дебет"]).startswith("51")
        else 0.00,
        axis=1,
    )
    df["Credit"] = data.apply(
        lambda row: row["сумма"]
        if str(row["кредит"]).startswith("51")
        else 0.00,
        axis=1,
    )

    df["clientAcc"] = data.apply(
        lambda row: row["учетподебету"]
        if str(row["дебет"]).startswith("51")
        else "",
        axis=1,
    )    
    df["cpAcc"] = data.apply(
        lambda row: row["учетпокредиту"]
        if str(row["кредит"]).startswith("51")
        else "",
        axis=1,
    )    
    df["Comment"] = data["описание"]

    if "дебет" in data.columns:
        df["codeIntDt"] = data["дебет"]
    if "кредит" in data.columns:
        df["codeIntCr"] = data["кредит"]


    openBalance = getParametersValue(data, "дата", "Входящий остаток", 0)
    closingBalance = getParametersValue(data, "дата", "Исходящий остаток", -1)
    turnovers = getParametersValue(data, "дата", "Обороты за период", -1)

    if openBalance.size > 1:
        df["openBalance"] = openBalance.iloc[1]
    if closingBalance.size > 1:
        df["closingBalance"] = closingBalance.iloc[1]
    if turnovers.size > 2:
        df["totalDebet"] = turnovers.iloc[1]
        df["totalCredit"] = turnovers.iloc[2]

    return df

