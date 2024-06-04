import datetime
import numpy as np

def timestamp_to_datetime(ts):
    pass

def datetime_to_timestamp(ts_date):
    pass

def timestamp_to_readable_date(timestamp, format='%Y-%m-%d %H:%M:%S'):
    """
    Convert a Unix timestamp (in seconds or milliseconds) to a readable date string.

    :param timestamp: Unix timestamp in seconds or milliseconds.
    :param format: Format of the output date string.
    :return: Readable date string.
    """
    # Check if timestamp is in milliseconds (length > 10)
    if len(str(timestamp)) > 10:
        timestamp = timestamp / 1000  # Convert to seconds

    return datetime.fromtimestamp(timestamp).strftime(format)

async def now_ts(self) -> np.int64:
        now = datetime.datetime.now()
        return np.int64(now.timestamp() * 1000)

def convert_ints_to_np_int64(self, obj):
        if isinstance(obj, dict):
            return {k: self.convert_ints_to_np_int64(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.convert_ints_to_np_int64(v) for v in obj]
        elif isinstance(obj, int):
            return np.int64(obj)
        else:
            return obj