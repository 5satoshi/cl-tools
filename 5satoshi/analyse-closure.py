from pyln.client import LightningRpc
from datetime import datetime
import os

# Path to lightning-rpc socket (adjust if needed)
RPC_PATH = os.path.expanduser(os.environ['HOME']+"/.lightning/bitcoin/lightning-rpc")

def parse_close_info(channel):
    close_info = channel.get('close_info')
    if close_info:
        closed_at = close_info.get('closed_at')
        closed_at_str = datetime.utcfromtimestamp(closed_at).strftime('%Y-%m-%d %H:%M:%S') if closed_at else "unknown"
        return {
            'short_channel_id': channel.get('channel', 'unknown'),
            'state': channel.get('state'),
            'closer': close_info.get('closer', 'unknown'),
            'reason': close_info.get('reason', 'unknown'),
            'closed_at': closed_at,
            'closed_at_str': closed_at_str
        }
    return None

def get_recent_closures(rpc, limit=5):
    peers = rpc.listpeers()['peers']
    closures = []
    
    for peer in peers:
        for channel in peer.get('channels', []):
            state = channel.get('state', '')
            if state != 'CHANNELD_NORMAL':
                info = parse_close_info(channel)
                closures.append(info)
    
    # Sort by closure time (descending)
    closures.sort(key=lambda x: x.get('closed_at', 0), reverse=True)
    
    return closures[:limit]

def main():
    rpc = LightningRpc(RPC_PATH)
    closures = get_recent_closures(rpc)
    
    print("\nMost Recent Channel Closures:\n")
    for c in closures:
        print(f"Short Channel ID : {c['short_channel_id']}")
        print(f"Closed At        : {c['closed_at_str']}")
        print(f"State            : {c['state']}")
        print(f"Closer           : {c['closer']}")
        print(f"Reason           : {c['reason']}\n")

if __name__ == "__main__":
    main()
