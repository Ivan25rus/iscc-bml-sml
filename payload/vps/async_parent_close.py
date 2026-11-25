"""Private terminal file transport for one durable group of exact closes."""
import math
import os
from pathlib import Path
import re
import time


BATCH_ID = re.compile(r'[a-f0-9]{64}')
MAX_TICKETS = 100
RESPONSE_TIMEOUT = 2.0


def terminal_folder(terminal_path, account):
    return Path(terminal_path) / 'MQL5' / 'Files' / 'ISCCAsyncClose' / str(account)


def ready_folder(terminal_path, account, server):
    folder = terminal_folder(terminal_path, account)
    try:
        marker = folder / 'ready'
        if not -1 <= time.time() - marker.stat().st_mtime <= 4:
            return None
        if marker.read_bytes().decode('utf-8').strip().split('\t') == ['1', str(account), server]:
            return folder
    except (OSError, UnicodeError):
        pass
    return None


def publish(folder, batch, account, server, expires_ms, requests):
    if any(re.search(r'[\r\n\t]', value) for value in (server, *(row['symbol'] for row in requests))):
        raise ValueError('Invalid close transport field')
    rows = ['\t'.join(('1', batch, str(account), server, str(int(expires_ms)), str(len(requests))))]
    for request in requests:
        rows.append('\t'.join(str(request[key]) for key in (
            'position', 'symbol', 'volume', 'type', 'type_filling', 'deviation', 'magic')))
    pending = folder / (batch + '.pending')
    with pending.open('xb') as stream:
        stream.write(('\r\n'.join(rows) + '\r\n').encode('utf-8'))
        stream.flush()
        os.fsync(stream.fileno())
    # The terminal watches this one file and enumerates only when woken. Publish
    # before waking, so concurrent requests cannot miss each other.
    os.replace(pending, folder / (batch + '.send'))
    with (folder / 'wake').open('ab'):
        pass


def wait_for_reply(location, batch):
    if not BATCH_ID.fullmatch(batch):
        raise ValueError('Invalid async close batch ID')
    reply = Path(location) / (batch + '.reply')
    deadline = time.monotonic() + RESPONSE_TIMEOUT
    while time.monotonic() < deadline:
        if reply.is_file():
            return
        time.sleep(.015)


def read_reply(folder, batch, requests):
    reply = folder / (batch + '.reply')
    if not reply.is_file():
        return None
    if reply.stat().st_size > 12288:
        raise ValueError('Oversized async close reply')
    lines = reply.read_bytes().decode('utf-8').splitlines()
    if not lines or lines[0] != '1\t' + batch or len(lines) != len(requests) + 1:
        raise ValueError('Invalid async close reply identity or count')
    results = {}
    for line in lines[1:]:
        ticket, submitted, retcode, order, filled, price, dispatch_ms = line.split('\t')
        if ticket not in requests or ticket in results or submitted not in ('0', '1'):
            raise ValueError('Invalid async close ticket reply')
        retcode, filled, price, dispatch_ms = int(retcode), float(filled), float(price), float(dispatch_ms)
        if not 0 <= retcode <= 65535 or not all(math.isfinite(x) and x >= 0 for x in (filled, price, dispatch_ms)):
            raise ValueError('Invalid async close terminal outcome')
        result = dict(status='unknown', operation='close', ticket=ticket, retcode=retcode,
                      dispatchOffsetMs=dispatch_ms, reason='Async close requires exact broker proof')
        if submitted == '0':
            result.update(status='rejected', dispatchNotSent=True, reason='Terminal did not submit this close')
        elif (retcode == 10009 and re.fullmatch(r'[1-9][0-9]{0,19}', order)
              and abs(filled - requests[ticket]) <= 1e-8 and price > 0):
            result.update(status='ok', providerOrderId=order, filledVolume=filled, fillPrice=price)
            result.pop('reason')
        results[ticket] = result
    return results
