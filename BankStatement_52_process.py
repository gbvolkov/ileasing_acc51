from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS


# датапроводки|счетдебет|счеткредит|сумма|nдок|вид|во|банккоррбикинаименование|назначениеплатежа
# COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_52_process(
    header: pd.DataFrame,
    data: pd.DataFrame,
    footer: pd.DataFrame,
    inname: str,
    clientid: str,
    params: dict,
    sheet: str,
    logf: TextIOWrapper,
) -> pd.DataFrame:
    clientacc = params["account"]
    data["счетдебет"] = data["счетдебет"].str.replace(r"^\n\s*", "")
    data["счеткредит"] = data["счеткредит"].str.replace(r"^\n\s*", "")

    df = pd.DataFrame(columns=COLUMNS)

    df["entryDate"] = data["датапроводки"]
    # df["cpBIC"] = data["контрагентбикбанка"]
    df["cpBank"] = data["банккоррбикинаименование"]

    df["cpAcc"] = data.apply(
        lambda row: row["счетдебет"]
        if str(row["счеткредит"]).startswith(clientacc)
        else row["счеткредит"],
        axis=1,
    )
    df["clientAcc"] = data.apply(
        lambda row: row["счеткредит"]
        if str(row["счеткредит"]).startswith(clientacc)
        else row["счетдебет"],
        axis=1,
    )

    # df["cpTaxCode"] = data["контрагентинн"]
    # df["cpName"] = data["контрагентнаименование"]

    df["Debet"] = data.apply(
        lambda row: row["сумма"] if str(row["счетдебет"]).startswith(clientacc) else "",
        axis=1,
    )
    df["Credit"] = data.apply(
        lambda row: row["сумма"]
        if str(row["счеткредит"]).startswith(clientacc)
        else "",
        axis=1,
    )
    df["Comment"] = data["назначениеплатежа"]

    return df
