#!/usr/bin/env python3
import os
import time
import json
import datetime as dt
from web3 import Web3
from hexbytes import HexBytes

IPC_PATH    = '/data/geth-data/geth.ipc'
OUTPUT_DIR  = '/root/mempool-dumps'
INTERVAL    = 10  #seconds

# === SETUP ===
os.makedirs(OUTPUT_DIR, exist_ok=True)
w3 = Web3(Web3.IPCProvider(IPC_PATH))

def to_plain(obj):
    if isinstance(obj, HexBytes):
        return obj.hex()
    elif isinstance(obj, dict):
        return {k: to_plain(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [to_plain(v) for v in obj]
    # catch web3 AttributeDict (has .items())
    elif hasattr(obj, 'items'):
        return {k: to_plain(v) for k, v in obj.items()}
    else:
        return obj

def dump_once():
    now = dt.datetime.now(dt.timezone.utc)
    date = now.strftime('%Y-%m-%d')
    fn   = os.path.join(OUTPUT_DIR, f'{date}.log')

    try:
        # full txpool snapshot
        pool = w3.geth.txpool.content()
        # pending / queued counts
        st   = w3.geth.txpool.status() 
       # parse pending / queued
        def parse_count(x):
            if isinstance(x, str) and x.startswith('0x'):
                return int(x, 16)
            return int(x)

        record = {
            'timestamp':     now.isoformat(),
            'pending_count': parse_count(st['pending']),
            'queued_count':  parse_count(st['queued']),
            'snapshot':      to_plain(pool)
        }
    except Exception as e:
        record = {
            'timestamp': now.isoformat(),
            'error':     str(e)
        }

    # append to today's file
    with open(fn, 'a') as f:
        f.write(json.dumps(record) + "\n")
    # log to stdout/stderr so you can see it in nohup.out
    print(f"[{dt.datetime.utcnow().isoformat()}Z] wrote snapshot to {fn}", flush=True)

if __name__ == '__main__':
    while True:
        dump_once()
        time.sleep(INTERVAL)