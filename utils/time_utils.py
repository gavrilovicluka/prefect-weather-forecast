from datetime import datetime
from pytz import timezone

def convert_utc_to_local_time(iso_time):
    if isinstance(iso_time, str):
        iso_time = datetime.fromisoformat(iso_time)

    local_tz = timezone('Europe/Belgrade')

    local_time = iso_time.astimezone(local_tz)

    return local_time.strftime("%b %d, %Y %H:%M:%S")    # Format: Sep 24, 2024 13:04:30