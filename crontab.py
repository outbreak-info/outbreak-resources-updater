import datetime


def match_range(condition: str, value: int) -> bool:
    boundarys = [int(c) for c in condition.split('-')]
    if len(boundarys) != 2:
        raise ValueError("Too many -")
    mi, ma = sorted(boundarys)
    return mi <= value <= ma


def match_every(condition: str, value: int) -> bool:
    every = int(condition.split('/')[1])  # */3/5/7 == */3, very lax.
    return value % every == 0


def match_expression(expression: str, value: int) -> bool:
    conditions = expression.split(',')
    for condition in conditions:
        if condition == '*':
            return True
        elif '-' in condition and match_range(condition, value):
            return True
        elif condition[:2] == '*/' and match_every(condition, value):
            return True
        else:
            return value == int(expression)


def match(entry: str, time: datetime.datetime) -> bool:
    if time.fold == 1:
        return False  # only trigger on the first one
    alias_mapping = {
        '@yearly': '0 0 1 1 *',
        '@annually': '0 0 1 1 *',
        '@monthly': '0 0 1 * *',
        '@weekly': '0 0 * * 0',  # on Sundays
        '@daily': '0 0 * * *',
        '@hourly': '0 * * * *',
    }

    month_mapping = {
        'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
        'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12,
    }

    dow_mapping = {
        'mon': 1, 'tue': 2, 'wed': 3, 'thu': 4, 'fri': 5, 'sat': 6, 'sun': 7,
    }
    # like Vixie cron, strictly three letters abbrev.
    if entry[0] == '@':
        entry = alias_mapping[entry.lower()]
    mm, hh, dom, mt, dow = entry.split()
    for name, number in month_mapping.items():
        dom = dom.replace(name, str(number))
    for name, number in dow_mapping.items():
        dow = dow.replace(name, str(number))
    dow = dow.replace('0', '7')  # because we are using ISO weekday
    # logic for dom/dow
    if dom == '*' or dow == '*':
        day_flag = match_expression(dow, time.isoweekday()) and \
            match_expression(dom, time.month)
    else:
        day_flag = match_expression(dow, time.isoweekday()) or \
            match_expression(dom, time.month)
    return \
        match_expression(mm, time.minute) and \
        match_expression(hh, time.hour) and \
        match_expression(mt, time.month) and \
        day_flag


def next_event(entry, t=None) -> datetime.datetime:
    if t is None:
        t = datetime.datetime.now()
        t = t.replace(microsecond=0)
    minute = datetime.timedelta(minutes=1)
    while not match(entry, t):
        t += minute
    return t
