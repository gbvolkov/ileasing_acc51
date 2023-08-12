
from datetime import datetime
from io import TextIOWrapper
import os
import traceback


def print_exception(err: Exception, inname: str, clientid: str, sheet: str, logf: TextIOWrapper):
    print(f"{datetime.now()}:{inname}_{sheet}:ERROR:{err}")
    traceback.print_exc()
    logstr = f"{datetime.now()}:ERROR:{clientid}:{os.path.basename(inname)}:{sheet}::{type(err).__name__} {str(err)}\n"
    logf.write(logstr)

