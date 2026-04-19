#!/home/honc/.pyenv/versions/3.12.3/bin/python
import os
import math
from pyln.client import Plugin, LightningRpc

def compute_quantized_htlcmax(channel):
    """
    Quantize HTLC max according to tiered levels:
    Micro: 200 sat → 200_000 msat
    Common: 50k sat → 50_000_000 msat
    Macro: 4M sat → 4_000_000_000 msat
    
    Quantize in steps of tier × 10.
    """
    msat_to_us = channel["to_us_msat"]
    
    # Define tiers in msat
    MICRO = 200_000
    COMMON = 50_000_000
    MACRO = 4_000_000_000
    
    # Determine which tier
    if msat_to_us < COMMON:
        tier = MICRO
    elif msat_to_us < MACRO:
        tier = COMMON
    else:
        tier = MACRO
    
    # Quantize up to 10× tier
    bucket = tier * 10
    quantized = (msat_to_us // bucket) * bucket
    
    # Minimum is the tier itself
    if quantized < tier:
        quantized = msat_to_us if msat_to_us < tier else tier
    
    return quantized


def compute_power_fee_quantized(channel, bucket):
    """Compute power-based fee using quantized liquidity."""
    msat_total = channel["total_msat"]

    if msat_total == 0:
        return None

    balance = (bucket + 1) / msat_total
    fee = pow(math.floor(1 / balance), 2)
    return min(int(fee), 10000)


# -----------------------------
# Shared update function
# -----------------------------
def update_channel(plugin, channel):

    cid = channel["channel_id"]

    new_htlc = compute_quantized_htlcmax(channel)
    current_htlc = channel["maximum_htlc_out_msat"]

    if new_htlc != current_htlc:

        new_fee = compute_power_fee_quantized(channel, new_htlc)

        plugin.rpc.setchannel(
            id=cid,
            feebase=0,
            feeppm=new_fee,
            htlcmax=new_htlc
        )

        plugin.log(
            f"Updated {cid} "
            f"htlc {current_htlc}→{new_htlc}, "
            f"ppm {channel['fee_proportional_millionths']}→{new_fee}"
        )


# -----------------------------
# Init
# -----------------------------
plugin = Plugin()

@plugin.init()
def init(options, configuration, plugin):
    plugin.log("Quantized fee updater initialized")


# -----------------------------
# Forward subscription
# -----------------------------
@plugin.subscribe("forward_event")
def on_forward_event(plugin, forward_event, **kwargs):

    if forward_event.get("status") != "settled":
        return

    for cid in [
        forward_event.get("channel_in"),
        forward_event.get("channel_out")
    ]:
        if not cid:
            continue

        result = plugin.rpc.listpeerchannels(id=cid)
        channels = result.get("channels", [])
        if not channels:
            continue

        update_channel(plugin, channels[0])


# -----------------------------
# Channel becomes usable
# -----------------------------
@plugin.subscribe("channel_state_changed")
def on_channel_state_changed(plugin, channel_state_changed, **kwargs):

    if channel_state_changed.get("new_state") != "CHANNELD_NORMAL":
        return

    cid = channel_state_changed.get("channel_id")
    if not cid:
        return

    result = plugin.rpc.listpeerchannels(id=cid)
    channels = result.get("channels", [])
    if not channels:
        return

    update_channel(plugin, channels[0])

# -----------------------------
# RPC method: show current HTLCs and fees
# -----------------------------
@plugin.method("list_channel_fees")
def list_channel_fees(plugin, short_channel_id=None):
    """
    Show current HTLC max and fee for all channels.
    Optional: filter by short_channel_id.
    """
    channels = plugin.rpc.listpeerchannels()["channels"]
    result = []

    for channel in channels:
        # If filtering by short_channel_id
        if short_channel_id and channel.get("short_channel_id") != short_channel_id:
            continue

        info = {
            "channel_id": channel["channel_id"],
            "short_channel_id": channel.get("short_channel_id"),
            "htlc_max": channel["maximum_htlc_out_msat"],
            "fee_ppm": channel["fee_proportional_millionths"],
            "to_us_msat": channel["to_us_msat"],
            "total_msat": channel["total_msat"],
            "state": channel["state"]
        }
        result.append(info)

    return {"channels": result}

# -----------------------------
# Manual RPC trigger
# -----------------------------
@plugin.method("update_fees")
def update_fees(plugin, channel_id=None):

    if channel_id:
        result = plugin.rpc.listpeerchannels(id=channel_id)
        channels = result.get("channels", [])
        if not channels:
            return {"error": "Channel not found"}

        update_channel(plugin, channels[0])
        return {"status": "updated", "channel": channel_id}

    channels = plugin.rpc.listpeerchannels()["channels"]

    for channel in channels:
        update_channel(plugin, channel)

    return {"status": "updated_all"}


plugin.run()
