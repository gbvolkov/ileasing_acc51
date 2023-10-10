COLUMNS = [
    "clientID",
    "clientBIC",
    "clientBank",
    "clientAcc",
    "clientTaxCode",
    "clientName",
    "stmtDate",
    "stmtFrom",
    "stmtTo",
    "codeIntDt", #для карточки 51 счёта. Счёт внутреннего учёта по Дебету
    "codeIntCr", #для карточки 51 счёта. Счёт внутреннего учёта по Кредиту
    "openBalance",
    "totalDebet",
    "totalCredit",
    "closingBalance",
    "entryDate",
    "cpBIC",
    "cpBank",
    "cpAcc",
    "cpTaxCode",
    "cpName",
    "Debet",
    "Credit",
    "Comment",
    "__header",
    "__hdrclientBIC",
    "__hdrclientAcc",
    "__hdrclientTaxCode",
    "__hdropenBalance",
    "result",
    "filename",
    "function",
    "processdate",
]
DOCTYPES = [
    "выписка",
    "оборотно-сальдовая ведомость",
    "обороты счета",
    "обороты счёта",
    "анализ счета",
    "анализ счёта",
    "карточка счёта 51",
    "карточка счета 51",
    "список кредитовых операций",
    "список дебетовых операций",
    "список дебитовых операций"
]
REGEX_ACCOUNT = r"(?:\b\d{5}\s*\d{3}\s*\d\s*\d{5}\s*\d{6})"
REGEX_INN = r"(?:\b\d{10}\b)|(?:\b\d{12}\b)"
REGEX_BIC = r"\b\d{9}\b"
REGEX_AMOUNT = r"((?<![\.\-\,0-9])(?:(?:\d{1,3}){1}(?:\s*\d{3})*\s*\d{1,3})[.\-,]\d{0,2}(?![\.\-\,0-9]))"

ACTIVE_DOCTYPES = []

def set_active_types(types: list[str]):
    global ACTIVE_DOCTYPES
    ACTIVE_DOCTYPES = types
def get_active_types():
    return ACTIVE_DOCTYPES
