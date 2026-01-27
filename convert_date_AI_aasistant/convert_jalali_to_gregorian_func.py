from datetime import datetime
from persiantools.jdatetime import JalaliDate

def persian_to_gregorian(persian_date: str):
    """
    Convert a Persian (Jalali) date string to Gregorian date.
    Expected format: "YYYY/MM/DD"
    Example: "1404/09/22" -> datetime.date(2025, 12, 13)
    """
    year, month, day = map(int, persian_date.split("/"))
    greg_date = JalaliDate(year, month, day).to_gregorian()
    return greg_date

def parse_timestamp(ts: str) -> datetime:
    """
    Parse multiple timestamp formats including:
    - Go-style: 2025-10-09-05.23.59.877774
    - 12h clock: 11/29/25 12:05 AM or 11/29/2025 12:05 AM
    - 24h clock: 11/29/2025 00:05:18
    - Persian 24h: 1404/09/22-23:23
    """
    ts = ts.strip()

    # 1) Go-style: 2025-10-09-05.23.59.877774
    parts = ts.split("-", 3)
    if len(parts) == 4 and "." in parts[3]:
        year, month, day, time_part = parts
        time_part = time_part.replace(".", ":", 2)
        ts_norm = f"{year}-{month}-{day} {time_part}"
        try:
            return datetime.strptime(ts_norm, "%Y-%m-%d %H:%M:%S.%f")
        except ValueError:
            pass

    # 2) Known Gregorian formats
    formats = [
        "%m/%d/%y %I:%M %p",
        "%m/%d/%Y %I:%M %p",
        "%m/%d/%y %H:%M",
        "%m/%d/%Y %H:%M",
        "%m/%d/%y %H:%M:%S",
        "%m/%d/%Y %H:%M:%S",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(ts, fmt)
        except ValueError:
            continue

    # 3) Persian date with 24h time: 1404/09/22-23:23
    if "-" in ts:
        date_part, time_part = ts.split("-", 1)
        if "/" in date_part:
            greg_date = persian_to_gregorian(date_part)
            hour, minute = map(int, time_part.split(":"))
            return datetime(
                greg_date.year, greg_date.month, greg_date.day, hour, minute
            )

    raise ValueError(f"Unsupported timestamp format: {ts}")






