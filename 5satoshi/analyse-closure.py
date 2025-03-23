from pyln.client import LightningRpc
from datetime import datetime
import os

RPC_PATH = os.environ['HOME']+"/.lightning/bitcoin/lightning-rpc"

def parse_close_info(channel):
    close_info = channel.get('close_info')
    if close_info:
        closed_at = close_info.get('closed_at')
        closed_at_str = datetime.utcfromtimestamp(closed_at).strftime('%Y-%m-%d %H:%M:%S') if closed_at else "unknown"
        return {
            'peer_id': channel.get('peer_id', 'unknown'),
            'short_channel_id': channel.get('short_channel_id', 'unknown'),
            'state': channel.get('state'),
            'closer': close_info.get('closer', 'unknown'),
            'reason': close_info.get('reason', 'unknown'),
            'closed_at': closed_at,
            'closed_at_str': closed_at_str
        }
    return None

def get_recent_closed_channels(rpc, limit=5):
    channels = rpc.call("listpeerchannels")['channels']
    closed = []

    for chan in channels:
        if chan.get('state', '').endswith('CLOSED') or chan.get('state', '') == 'ONCHAIN':
            info = parse_close_info(chan)
            if info:
                closed.append(info)

    closed.sort(key=lambda x: x.get('closed_at', 0), reverse=True)
    return closed[:limit]

def main():
    rpc = LightningRpc(RPC_PATH)
    closures = get_recent_closed_channels(rpc)

    if not closures:
        print("No recently closed channels found.")
        return

    print("\nMost Recent Channel Closures:\n")
    for c in closures:
        print(f"Peer ID           : {c['peer_id']}")
        print(f"Short Channel ID  : {c['short_channel_id']}")
        print(f"State             : {c['state']}")
        print(f"Closed At         : {c['closed_at_str']}")
        print(f"Closer            : {c['closer']}")
        print(f"Reason            : {c['reason']}\n")

if __name__ == "__main__":
    main()
