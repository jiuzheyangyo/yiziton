import datetime
def dateTo8(needTrunDate):
    tzutc_8 = datetime.timezone(datetime.timedelta(hours=8))
    dt = needTrunDate
    dt = dt.replace(tzinfo=datetime.timezone.utc)
    dt = dt.astimezone(tzutc_8)
    return dt