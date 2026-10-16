"""Stamp each `kubectl get pods -w --output-watch-events -o json` event.

Reads the concatenated JSON stream on stdin and writes one JSONL line per
event: {"t": <host epoch seconds, ns precision>, "type": ADDED|MODIFIED|
DELETED, "obj": <full pod object>}. The stamp is taken the moment the event
is decoded, i.e. ~when the API server pushed it to kubectl.
"""
import json
import os
import sys
import time

dec = json.JSONDecoder()
buf = ''
out = sys.stdout
while True:
    chunk = os.read(0, 1 << 16)
    if not chunk:
        break
    t = time.time_ns() / 1e9
    buf += chunk.decode()
    while True:
        s = buf.lstrip()
        if not s:
            buf = ''
            break
        try:
            ev, end = dec.raw_decode(s)
        except json.JSONDecodeError:
            buf = s  # Incomplete object; wait for more bytes.
            break
        buf = s[end:]
        out.write(
            json.dumps({
                't': t,
                'type': ev.get('type'),
                'obj': ev.get('object')
            }) + '\n')
        out.flush()
