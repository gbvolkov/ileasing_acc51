from datetime import datetime
from io import TextIOWrapper
import os
import shutil
import traceback

def convert_to_float(amount: str) -> float:
    return float(amount.replace(" ", "").replace(",", ".").replace("-","."))

def print_exception(
    err: Exception, inname: str, clientid: str, sheet: str, logf: TextIOWrapper
):
    print(f"{datetime.now()}:{inname}_{sheet}:ERROR:{err}")
    traceback.print_exc()
    logstr = f"{datetime.now()}:ERROR:{clientid}:{os.path.basename(inname)}:{sheet}::{type(err).__name__} {str(err)}\n"
    logf.write(logstr)


def move_to_folder(fname: str, doneFolder: str):
    outdir = f"{doneFolder}/{os.path.split(os.path.dirname(fname))[1]}"
    outname = f"{outdir}/{os.path.basename(fname)}"
    if not os.path.isdir(outdir):
        os.mkdir(outdir)
    shutil.move(fname, outname)

def split_list(arg):
    return arg.split(",")
