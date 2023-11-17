from datetime import datetime, timedelta
import re
import types


QUARTS={
    1: (1,3)
    ,2: (4,6)
    ,3: (7,9)
    ,4: (10,12)
}
HALFYEAR={
    1: (1,6)
    ,2: (7,12)
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
rt.RE_MONTHRANGE=r"([Яя]нвар[ья]|[Фф]еврал[ья]|[Мм]арт[а]*|[Аа]прел[ья]|[Мм]а[йя]|[Ии]юн[ья]|[Ии]юл[ья]|[Аа]вгуст[а]*|[Сс]ентябр[ья]|[Оо]ктябр[ья]|[Нн]оябр[ья]|[Дд]екабр[ья])\s*(\d{4})\s*-\s*([Яя]нвар[ья]|[Фф]еврал[ья]|[Мм]арт[а]*|[Аа]прел[ья]|[Мм]а[йя]|[Ии]юн[ья]|[Ии]юл[ья]|[Аа]вгуст[а]*|[Сс]ентябр[ья]|[Оо]ктябр[ья]|[Нн]оябр[ья]|[Дд]екабр[ья])\s*(\d{4})"
rt.RE_MONTH=r"([Яя]нвар[ья]|[Фф]еврал[ья]|[Мм]арт[а]*|[Аа]прел[ья]|[Мм]а[йя]|[Ии]юн[ья]|[Ии]юл[ья]|[Аа]вгуст[а]*|[Сс]ентябр[ья]|[Оо]ктябр[ья]|[Нн]оябр[ья]|[Дд]екабр[ья])\s*(\d{4})"
rt.RE_DATERANGE=r"(\d{1,2})[.-/](\d{1,2})[.-/](\d{4})\s*-\s*(\d{1,2})[.-/](\d{1,2})[.-/](\d{4})"
rt.RE_HALFYEAR=r"(\d{1})\s*полугодие\s*(\d{4})"
rt.RE_QUART=r"(\d{1})\s*квартал\s*(\d{4})"
rt.RE_YEAR=r"^(\d{4})"

class REqual(str):
    "Override str.__eq__ to match a regex pattern."
    def __eq__(self, pattern):
        return re.fullmatch(pattern, self)

#def get_last_day_of_month(year: int, mon: int)->datetime:
#    nextmon = datetime(year, mon, 28) + timedelta(days=4)
#    return nextmon - timedelta(days=nextmon.day)    

def last_day_of_month(year, month):
    next_month = datetime(year, month, 28) + timedelta(days=4)
    return next_month - timedelta(days=next_month.day)

def format_date_range(start_date, end_date):
    return [start_date.strftime("%d-%m-%Y"), end_date.strftime("%d-%m-%Y")]

def parse_date_from_match(pattern, string, parse_func):
    match = re.findall(pattern, string, re.MULTILINE)
    return parse_func(match[0]) if match else None

def get_date_range(headerstr: str) -> list[str]:
    def parse_monthrange(groups):
        monfrom = MONTHS[groups[0].lower()]
        yearfrom = int(groups[1])
        monto = MONTHS[groups[2].lower()]
        yearto = int(groups[3])
        return datetime(yearfrom, monfrom, 1), last_day_of_month(yearto, monto)

    def parse_month(groups):
        monfrom = MONTHS[groups[0].lower()]
        yearfrom = int(groups[1])
        return datetime(yearfrom, monfrom, 1), last_day_of_month(yearfrom, monfrom)

    def parse_daterange(groups):
        return datetime(int(groups[2]), int(groups[1]), int(groups[0])), datetime(int(groups[5]), int(groups[4]), int(groups[3]))

    def parse_year(groups):
        year = int(groups)
        return datetime(year, 1, 1), datetime(year, 12, 31)

    def parse_quart(groups):
        quart = int(groups[0])
        year = int(groups[1])
        monfrom = QUARTS[quart][0]
        monto = QUARTS[quart][1]
        return datetime(year, monfrom, 1), last_day_of_month(year, monto)

    def parse_halfyear(groups):
        half = int(groups[0])
        year = int(groups[1])
        monfrom = HALFYEAR[half][0]
        monto = HALFYEAR[half][1]
        return datetime(year, monfrom, 1), last_day_of_month(year, monto)

    patterns = {
        rt.RE_MONTHRANGE: parse_monthrange,
        rt.RE_MONTH: parse_month,
        rt.RE_DATERANGE: parse_daterange,
        rt.RE_YEAR: parse_year,
        rt.RE_QUART: parse_quart,
        rt.RE_HALFYEAR: parse_halfyear,
    }

    try:
        for pattern, parse_func in patterns.items():
            result = parse_date_from_match(pattern, headerstr, parse_func)
            if result:
                return format_date_range(*result)
        return [headerstr, headerstr]
    except Exception:
        return ['00-00-0000', '00-00-0000']


def normalise_range_str(rangestr: str) -> str:
    # Remove leading phrases and format ranges
    rangestr = re.sub(r"Карточка\s+счета\s+.*\s+(за|с)\s+", "", rangestr)
    rangestr = re.sub(r"[Пп]ериод[:]?\s*", "", rangestr)
    rangestr = re.sub(r"\s+по\s+|\s*-\s*", "-", rangestr)
    rangestr = re.sub(r"\s*г\.\s*|с\s+", "", rangestr)
    # Replace slashes with dots
    rangestr = re.sub(r"/", ".", rangestr)
    return rangestr