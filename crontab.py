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
        elif '-' in condition:
            return match_range(condition, value)
        elif condition[:2] == '*/':
            return match_every(condition, value)
        else:
            return value == int(expression)


def crontab_match(entry: str, time: datetime.datetime) -> bool:
    if time.fold == 1:
        return False  # only trigger on the first one
    mapping_alias = {
        '@yearly': '0 0 1 1 *',
        '@annually': '0 0 1 1 *',
        '@monthly': '0 0 1 * *',
        '@weekly': '0 0 * * 0',  # on Sundays
        '@daily': '0 0 * * *',
        '@hourly': '0 * * * *',
    }

    mapping_mt = {
        'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
        'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12,
    }

    mapping_dow = {
        'mon': 1, 'tue': 2, 'wed': 3, 'thu': 4, 'fri': 5, 'sat': 6, 'sun': 7,
    }
    # like Vixie cron, strictly three letters abbrev.
    if entry[0] == '@':
        entry = mapping_alias[entry.lower()]
    mm, hh, dom, mt, dow = entry.split()
    for name, number in mapping_mt.items():
        dom = dom.replace(name, str(number))
    for name, number in mapping_dow.items():
        dow = dow.replace(name, str(number))
    dow = dow.replace('0', '7')  # because we are using ISO weekday
    # logic for dom/dow
    # small anecdote I found while surfing the webs:
    #  The reason for the below logic (see crontab(5) for details)
    #  is that in SysV they were trying to figure out when the next
    #  event happens, and sleeping for that long, instead of the usual
    #  cron logic of waking up every minute and check for matches.
    #  They said it was easier to implement that logic.
    #  See: https://stackoverflow.com/a/51345753
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
        t = t.replace(second=0, microsecond=0)
    minute = datetime.timedelta(minutes=1)
    while not crontab_match(entry, t):
        t += minute
    return t
