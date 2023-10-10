import pandas as pd
from datetime import datetime
from io import TextIOWrapper
import os
import numpy as np

from const import COLUMNS

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

    openBalance = data[data["период"].astype(str).str.startswith("Сальдо на начало")].replace(r"\s+", "", regex=True).replace("", np.nan).dropna(axis=1, how="all")
    if openBalance.size > 1:
        df["openBalance"] = openBalance.iloc[:, openBalance.size-1].values[0]
    closingBalance = data[data["период"].astype(str).str.startswith("Обороты за период и сальдо на конец")].replace(r"\s+", "", regex=True).replace("", np.nan).dropna(axis=1, how="all")
    if closingBalance.size > 2:
        df["closingBalance"] = closingBalance.iloc[:, closingBalance.size-1].values[0]
        df["totalDebet"] = closingBalance.iloc[:, 1].values[0]
        df["totalCredit"] = closingBalance.iloc[:, 2].values[0]

    #datatype = "|".join(data.columns).replace("\n", " ")
    #DATATYPES.append(datatype)
    #print(f"Datatype: {datatype} Type 51 not yet implemented.")

    #logstr = f'{datetime.now()}:NOT IMPLEMENTED:{clientid}:{os.path.basename(inname)}:{sheet}:0:"{datatype}"\n'
    #logf.write(logstr)

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


    openBalance = header[header.iloc[:, 0].astype(str).str.startswith("Начальное сальдо")].replace(r"\s+", "", regex=True).replace("", np.nan).dropna(axis=1, how="all")
    if openBalance.size > 1:
        df["openBalance"] = openBalance.iloc[:, 1].values[0]
    closingBalance = header[header.iloc[:, 0].astype(str).str.startswith("Конечное сальдо")].replace(r"\s+", "", regex=True).replace("", np.nan).dropna(axis=1, how="all")
    if closingBalance.size > 1:
        df["closingBalance"] = closingBalance.iloc[:, 1].values[0]
    turnovers = header[header.iloc[:, 0].astype(str).str.startswith("Обороты")].replace(r"\s+", "", regex=True).replace("", np.nan).dropna(axis=1, how="all")
    if turnovers.size > 2:
        df["totalDebet"] = turnovers.iloc[:, 1].values[0]
        df["totalCredit"] = turnovers.iloc[:, 2].values[0]

    #datatype = "|".join(data.columns).replace("\n", " ")
    #DATATYPES.append(datatype)
    #print(f"Datatype: {datatype} Type 51 not yet implemented.")

    #logstr = f'{datetime.now()}:NOT IMPLEMENTED:{clientid}:{os.path.basename(inname)}:{sheet}:0:"{datatype}"\n'
    #logf.write(logstr)

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

    openBalance = data[data["дата"].astype(str).str.startswith("Сальдо на начало")].replace(r"\s+", "", regex=True).replace("", np.nan).dropna(axis=1, how="all")
    if openBalance.size > 1:
        df["openBalance"] = openBalance.iloc[:, openBalance.size-1].values[0]
    closingBalance = data[data["дата"].astype(str).str.startswith("Обороты и сальдо на конец")].replace(r"\s+", "", regex=True).replace("", np.nan).dropna(axis=1, how="all")
    if closingBalance.size > 1:
        df["closingBalance"] = closingBalance.iloc[:, closingBalance.size-1].values[0]
        #df["totalDebet"] = closingBalance.iloc[:, 1].values[0]
        #df["totalCredit"] = closingBalance.iloc[:, 2].values[0]

    #datatype = "|".join(data.columns).replace("\n", " ")
    #DATATYPES.append(datatype)
    #print(f"Datatype: {datatype} Type 51 not yet implemented.")

    #logstr = f'{datetime.now()}:NOT IMPLEMENTED:{clientid}:{os.path.basename(inname)}:{sheet}:0:"{datatype}"\n'
    #logf.write(logstr)

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

    openBalance = data[data["дата"].astype(str).str.startswith("Сальдо на начало")].replace(r"\s+", "", regex=True).replace("", np.nan).dropna(axis=1, how="all")
    if openBalance.size > 1:
        df["openBalance"] = openBalance.iloc[:, 1].values[0]
    closingBalance = data[data["дата"].astype(str).str.startswith("Сальдо на конец")].replace(r"\s+", "", regex=True).replace("", np.nan).dropna(axis=1, how="all")
    if closingBalance.size > 1:
        df["closingBalance"] = closingBalance.iloc[:, 1].values[0]
    turnovers = data[data["дата"].astype(str).str.startswith("Обороты за период")].replace(r"\s+", "", regex=True).replace("", np.nan).dropna(axis=1, how="all")
    if turnovers.size > 2:
        df["totalDebet"] = turnovers.iloc[:, 1].values[0]
        df["totalCredit"] = turnovers.iloc[:, 2].values[0]


    #datatype = "|".join(data.columns).replace("\n", " ")
    #DATATYPES.append(datatype)
    #print(f"Datatype: {datatype} Type 51 not yet implemented.")

    #logstr = f'{datetime.now()}:NOT IMPLEMENTED:{clientid}:{os.path.basename(inname)}:{sheet}:0:"{datatype}"\n'
    #logf.write(logstr)

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


    openBalance = data[data["дата"].astype(str).str.startswith("Входящий остаток")].replace(r"\s+", "", regex=True).replace("", np.nan).dropna(axis=1, how="all")
    if openBalance.size > 1:
        df["openBalance"] = openBalance.iloc[:, 1].values[0]
    closingBalance = data[data["дата"].astype(str).str.startswith("Исходящий остаток")].replace(r"\s+", "", regex=True).replace("", np.nan).dropna(axis=1, how="all")
    if closingBalance.size > 1:
        df["closingBalance"] = closingBalance.iloc[:, 1].values[0]
    turnovers = data[data["дата"].astype(str).str.startswith("Обороты за период")].replace(r"\s+", "", regex=True).replace("", np.nan).dropna(axis=1, how="all")
    if turnovers.size > 2:
        df["totalDebet"] = turnovers.iloc[:, 1].values[0]
        df["totalCredit"] = turnovers.iloc[:, 2].values[0]


    #datatype = "|".join(data.columns).replace("\n", " ")
    #DATATYPES.append(datatype)
    #print(f"Datatype: {datatype} Type 51 not yet implemented.")

    #logstr = f'{datetime.now()}:NOT IMPLEMENTED:{clientid}:{os.path.basename(inname)}:{sheet}:0:"{datatype}"\n'
    #logf.write(logstr)

    return df

