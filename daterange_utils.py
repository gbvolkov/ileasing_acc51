from datetime import datetime, timedelta
import re
import types


QUARTS={
    1: (1,3)
    ,2: (4,6)
    ,3: (7,9)
    ,4: (10,12)
}
MONTHS={
    'январь': 1
    ,'февраль': 2
    ,'март': 3
    ,'апрель': 4
    ,'май': 5
    ,'июнь': 6
    ,'июль': 7
    ,'август': 8
    ,'сентябрь': 9
    ,'октябрь': 10
    ,'ноябрь': 11
    ,'декабрь': 12
}

rt = types.SimpleNamespace()
rt.RE_MONTHRANGE=r"([Яя]нвар[ья]|[Фф]еврал[ья]|[Мм]арт[а]*|[Аа]прел[ья]|[Мм]а[йя]|[Ии]юн[ья]|[Ии]юл[ья]|[Аа]вгуст[а]*|[Сс]ентябр[ья]|[Оо]ктябр[ья]|[Нн]оябр[ья]|[Дд]екабр[ья])\s*(\d{4})\s*-\s*(январ[ья]|[Фф]еврал[ья]|[Мм]арт[а]*|[Аа]прел[ья]|[Мм]а[йя]|[Ии]юн[ья]|[Ии]юл[ья]|[Аа]вгуст[а]*|[Сс]ентябр[ья]|[Оо]ктябр[ья]|[Нн]оябр[ья]|[Дд]екабр[ья])\s*(\d{4})"
rt.RE_MONTH=r"([Яя]нвар[ья]|[Фф]еврал[ья]|[Мм]арт[а]*|[Аа]прел[ья]|[Мм]а[йя]|[Ии]юн[ья]|[Ии]юл[ья]|[Аа]вгуст[а]*|[Сс]ентябр[ья]|[Оо]ктябр[ья]|[Нн]оябр[ья]|[Дд]екабр[ья])\s*(\d{4})"
rt.RE_DATERANGE=r"(\d{1,2})[.-/](\d{1,2})[.-/](\d{4})\s*-\s*(\d{1,2})[.-/](\d{1,2})[.-/](\d{4})"
rt.RE_YEAR=r"(\d{4})"
rt.RE_QUART=r"(\d{1})\s*квартал\s*(\d{4})"

class REqual(str):
    "Override str.__eq__ to match a regex pattern."
    def __eq__(self, pattern):
        return re.fullmatch(pattern, self)

def get_last_day_of_month(year: int, mon: int)->datetime:
    nextmon = datetime(year, mon, 28) + timedelta(days=4)
    return nextmon - timedelta(days=nextmon.day)    

def get_date_range(headerstr: str) -> list[str]:
    dtrange = ['00-00-0000', '00-00-0000']
    try:
        match REqual(headerstr):
            case rt.RE_MONTHRANGE:
                dtrange = re.findall(rt.RE_MONTHRANGE, headerstr, re.MULTILINE)
                monfrom=MONTHS[dtrange[0][0].lower()]
                yearfrom=int(dtrange[0][1])
                monto=MONTHS[dtrange[0][2].lower()]
                yearto=int(dtrange[0][3])
                datefrom = datetime(yearfrom, monfrom, 1)
                nextmon = datetime(yearto, monto, 28) + timedelta(days=4)
                dateto = nextmon - timedelta(days=nextmon.day)
                dtrange = [datefrom.strftime("%d-%m-%Y"), dateto.strftime("%d-%m-%Y")]
            case rt.RE_MONTH:
                dtrange = re.findall(rt.RE_MONTH, headerstr, re.MULTILINE)
                monfrom=MONTHS[dtrange[0][0].lower()]
                yearfrom=int(dtrange[0][1])
                datefrom = datetime(yearfrom, monfrom, 1)
                nextmon = datetime(yearfrom, monfrom, 28) + timedelta(days=4)
                dateto = nextmon - timedelta(days=nextmon.day)
                dtrange = [datefrom.strftime("%d-%m-%Y"), dateto.strftime("%d-%m-%Y")]
            case rt.RE_DATERANGE:
                dtrange = re.findall(rt.RE_DATERANGE, headerstr, re.MULTILINE)
                dayfrom=int(dtrange[0][0])
                monfrom=int(dtrange[0][1])
                yearfrom=int(dtrange[0][2])
                dayto=int(dtrange[0][3])
                monto=int(dtrange[0][4])
                yearto=int(dtrange[0][5])
                datefrom = datetime(yearfrom, monfrom, dayfrom)
                dateto = datetime(yearto, monto, dayto)
                dtrange = [datefrom.strftime("%d-%m-%Y"), dateto.strftime("%d-%m-%Y")]
            case rt.RE_YEAR:
                dtrange = re.findall(rt.RE_YEAR, headerstr, re.MULTILINE)
                year=int(dtrange[0][0])
                datefrom = datetime(year, 1, 1)
                dateto = datetime(year, 12, 31)
                dtrange = [datefrom.strftime("%d-%m-%Y"), dateto.strftime("%d-%m-%Y")]
            case rt.RE_QUART:
                dtrange = re.findall(rt.RE_QUART, headerstr, re.MULTILINE)
                quart=int(dtrange[0][0])
                year=int(dtrange[0][1])
                monfrom = QUARTS[quart][0]
                monto = QUARTS[quart][1]
                datefrom = datetime(year, monfrom, 1)
                nextmon = datetime(year, monto, 28) + timedelta(days=4)
                dateto = nextmon - timedelta(days=nextmon.day)
                dtrange = [datefrom.strftime("%d-%m-%Y"), dateto.strftime("%d-%m-%Y")]
            case _:
                dtrange = [headerstr, headerstr]
    except Exception:
        return ['00-00-0000', '00-00-0000']
    return dtrange


def normalise_range_str(rangestr: str) -> str:
    rangestr = re.sub(r"Карточка\s+счета\s+.*\s+за\s+", "", rangestr)
    rangestr = re.sub(r"Карточка\s+счета\s+.*\s+с\s+", "", rangestr)
    rangestr = re.sub(r"[Пп]ериод[:]?\s*", "", rangestr)
    #rangestr = re.sub(r"\d", "X", rangestr)
    rangestr = re.sub(r"\s+по\s+", "-", rangestr)
    rangestr = re.sub(r"\s*-\s*", "-", rangestr)
    #rangestr = re.sub(
    #    r"([\s]*([Яя]нвар[ья]|[Фф]еврал[ья]|[Мм]арт[а]*|[Аа]прел[ья]|[Мм]а[йя]|[Ии]юн[ья]|[Ии]юл[ья]|[Аа]вгуст[а]*|[Сс]ентябр[ья]|[Оо]ктябр[ья]|[Нн]оябр[ья]|[Дд]екабр[ья])[\s]*)",
    #    "_MONTH_",
    #    rangestr,
    #)
    rangestr = re.sub(r"(\s*г\.\s*)", "", rangestr)
    rangestr = re.sub(r"/", ".", rangestr)
    rangestr = re.sub(r"с\s*", "", rangestr)
    return rangestr
