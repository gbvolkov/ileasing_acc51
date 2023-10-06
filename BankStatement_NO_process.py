import pandas as pd
from datetime import datetime
from io import TextIOWrapper
import os

from const import COLUMNS

DATATYPES: list[str] = []

def NoneHDR_process(
    header: pd.DataFrame,
    data: pd.DataFrame,
    footer: pd.DataFrame,
    inname: str,
    clientid: str,
    params: dict,
    sheet: str,
    logf: TextIOWrapper,
) -> pd.DataFrame:
    datatype = "|".join(data.columns).replace("\n", " ")
    DATATYPES.append(datatype)
    print(f"Datatype: {datatype} NOT FOUND")

    logstr = f'{datetime.now()}:NOT IMPLEMENTED:{clientid}:{os.path.basename(inname)}:{sheet}:0:"{datatype}"\n'
    logf.write(logstr)

    return pd.DataFrame(columns=COLUMNS)


def IgnoreHDR_process(
    header: pd.DataFrame,
    data: pd.DataFrame,
    footer: pd.DataFrame,
    inname: str,
    clientid: str,
    params: dict,
    sheet: str,
    logf: TextIOWrapper,
) -> pd.DataFrame:
    return pd.DataFrame(columns=COLUMNS)


def TestHDR_process(
    header: pd.DataFrame,
    data: pd.DataFrame,
    footer: pd.DataFrame,
    inname: str,
    clientid: str,
    params: dict,
    sheet: str,
    logf: TextIOWrapper,
) -> pd.DataFrame:
    NoneHDR_process(header, data, footer, inname, clientid, params, sheet, logf)

    nameparts = os.path.split(inname)
    fname = os.path.splitext(nameparts[1])[0]

    headerfname = os.path.join(nameparts[0], f"{fname}_{sheet}_header.csv")  # type: ignore
    datafname = os.path.join(nameparts[0], f"{fname}_{sheet}_data.csv")  # type: ignore
    footerfname = os.path.join(nameparts[0], f"{fname}_{sheet}_footer.csv")  # type: ignore

    header.to_csv(headerfname, mode="w", header=True, index=False)
    data.to_csv(datafname, mode="w", header=True, index=False)
    footer.to_csv(footerfname, mode="w", header=True, index=False)

    return pd.DataFrame(columns=COLUMNS)
