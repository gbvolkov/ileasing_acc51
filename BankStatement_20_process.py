from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS


# nпп|nдок|датаоперации|бикswiftбанкаплат|наименованиебанкаплательщика|наименованиеплательщика|иннплательщика|nсчетаплательщика|бикswiftбанкаполуч|наименованиебанкаполучателя|наименованиеполучателя|иннполучателя|nсчетаполучателя|сальдовходящее|дебет|кредит|сальдоисходящее|назначениеплатежа
# column|nпп|nдок|датаоперации|бикswiftбанкаплат|наименованиебанкаплательщика|наименованиеплательщика|иннплательщика|nсчетаплательщика|бикswiftбанкаполуч|наименованиебанкаполучателя|наименованиеполучателя|иннполучателя|nсчетаполучателя|сальдовходящее|дебет|кредит|сальдоисходящее|назначениеплатежа
# COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_20_process(
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

    df["entryDate"] = data["датаоперации"]

    df["cpBIC"] = data.apply(
        lambda row: row["бикswiftбанкаплат"]
        if pd.isna(row["дебет"])
        else row["бикswiftбанкаполуч"],
        axis=1,
    )
    df["cpBank"] = data.apply(
        lambda row: row["наименованиебанкаплательщика"]
        if pd.isna(row["дебет"])
        else row["наименованиебанкаполучателя"],
        axis=1,
    )
    df["cpAcc"] = data.apply(
        lambda row: row["nсчетаплательщика"]
        if pd.isna(row["дебет"])
        else row["nсчетаполучателя"],
        axis=1,
    )
    df["cpTaxCode"] = data.apply(
        lambda row: row["иннплательщика"]
        if pd.isna(row["дебет"])
        else row["иннполучателя"],
        axis=1,
    )
    df["cpName"] = data.apply(
        lambda row: row["наименованиеплательщика"]
        if pd.isna(row["дебет"])
        else row["наименованиеполучателя"],
        axis=1,
    )
    df["Debet"] = data["дебет"]
    df["Credit"] = data["кредит"]
    df["Comment"] = data["назначениеплатежа"]

    df["clientBIC"] = data.apply(
        lambda row: row["бикswiftбанкаплат"]
        if pd.isna(row["кредит"])
        else row["бикswiftбанкаполуч"],
        axis=1,
    )
    df["clientBank"] = data.apply(
        lambda row: row["наименованиебанкаплательщика"]
        if pd.isna(row["кредит"])
        else row["наименованиебанкаполучателя"],
        axis=1,
    )
    df["clientAcc"] = data.apply(
        lambda row: row["nсчетаплательщика"]
        if pd.isna(row["кредит"])
        else row["nсчетаполучателя"],
        axis=1,
    )
    df["clientName"] = data.apply(
        lambda row: row["наименованиеплательщика"]
        if pd.isna(row["кредит"])
        else row["наименованиеполучателя"],
        axis=1,
    )
    df["clientTaxCode"] = data.apply(
        lambda row: row["иннплательщика"]
        if pd.isna(row["кредит"])
        else row["иннполучателя"],
        axis=1,
    )

    df["openBalance"] = data["сальдовходящее"].iloc[0]
    df["closingBalance"] = data["сальдоисходящее"].iloc[len(data) - 1]

    return df
