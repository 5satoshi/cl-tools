#!/home/honc/.pyenv/versions/3.12.3/bin/python

import os
import math
import argparse
from pyln.client import Plugin, LightningRpc

# -----------------------------
# Default configuration
# -----------------------------
BUCKET_MSAT = 100_000_000    # 100k sat bucket
RESERVE_MSAT = 100_000_000   # 100k sat reserve

# -----------------------------
# Core logic
# -----------------------------
def compute_quantized_htlcmax(channel, bucket_msat=BUCKET_MSAT, reserve_msat=RESERVE_MSAT):
    msat_to_us = channel["to_us_msat"]
    available = max(msat_to_us - reserve_msat, 0)
    return (available // bucket_msat) * bucket_msat


def compute_power_fee_quantized(channel, bucket_msat=BUCKET_MSAT):
    msat_to_us = channel["to_us_msat"]
    msat_total = channel["total_msat"]

    if msat_total == 0:
        return None

    quantized_to_us = (msat_to_us // bucket_msat) * bucket_msat

    if quantized_to_us <= 0:
        quantized_to_us = bucket_msat

    balance = (quantized_to_us + 1) / msat_total
    fee = pow(math.floor(1 / balance), 2)

    return min(int(fee), 10000)


# -----------------------------
# Plugin execution
# -----------------------------
plugin = Plugin()


@plugin.subscribe("forward_event")
def on_forward_event(forward, plugin, **kwargs):

    if forward.get("status") != "settled":
        return

    for cid in [forward["channel_in"], forward["channel_out"]]:

        result = plugin.rpc.listpeerchannels(id=cid)
        channels = result.get("channels", [])

        if not channels:
            continue

        channel = channels[0]

        current_htlc = channel["maximum_htlc_out_msat"]
        current_fee = channel["fee_proportional_millionths"]

        new_htlc = compute_quantized_htlcmax(channel)
        new_fee = compute_power_fee_quantized(channel)

        if (new_htlc != current_htlc) or (new_fee != current_fee):

            plugin.rpc.setchannel(
                id=cid,
                feebase=0,
                feeppm=new_fee,
                htlcmax=new_htlc
            )

            plugin.log(
                f"Updated {cid}: "
                f"htlc {current_htlc}->{new_htlc}, "
                f"ppm {current_fee}->{new_fee}"
            )


@plugin.method("get_fee_config")
def get_fee_config(plugin):
    return {
        "bucket_msat": BUCKET_MSAT,
        "reserve_msat": RESERVE_MSAT
    }


# -----------------------------
# Ad-hoc execution
# -----------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Ad-hoc quantized fee updater"
    )

    parser.add_argument(
        "--rpc",
        type=str,
        default="~/.lightning/bitcoin/lightning-rpc",
        help="Path to lightning-rpc socket"
    )

    args = parser.parse_args()

    rpc_path = os.path.expanduser(args.rpc)
    rpc = LightningRpc(rpc_path)

    channels = rpc.listpeerchannels()["channels"]

    for channel in channels:

        new_htlc = compute_quantized_htlcmax(channel)
        new_fee = compute_power_fee_quantized(channel)

        if (new_htlc != channel["maximum_htlc_out_msat"]) or \
           (new_fee != channel["fee_proportional_millionths"]):

            rpc.setchannel(
                id=channel["channel_id"],
                feebase=0,
                feeppm=new_fee,
                htlcmax=new_htlc
            )

            print(
                f"Updated {channel['channel_id']}: "
                f"htlc {channel['maximum_htlc_out_msat']}->{new_htlc}, "
                f"ppm {channel['fee_proportional_millionths']}->{new_fee}"
            )


#main()
plugin.run()
