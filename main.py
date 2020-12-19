import datetime
import time

from crontab import crontab_match


if __name__ == '__main__':
    while(True):
        t = datetime.datetime.now()
        entry = "* * * * *"
        if crontab_match(entry, t):
            print(t)
        time.sleep(60 - t.second)
