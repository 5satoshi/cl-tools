#!/usr/bin/python

import csv
import json
import argparse
import logging
from collections import defaultdict

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("EquivPPMFinder")

def find_equivalent_ppms(input_csv, output_json):
    # Data structure to hold: data[ppm][channel] = {'cent': ..., 'rev': ...}
    sweep_data = defaultdict(lambda: defaultdict(dict))
    
    # Store all unique channels and ppms
    channels = set()
    ppms = set()
    
    logger.info(f"Loading sweep results from {input_csv}...")
    try:
        with open(input_csv, mode='r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                ppm = int(row["PPM"])
                ch_id = row["Channel"]
                cent = int(row["Edge_Centrality"])
                rev = float(row["Revenue_Potential"])
                
                sweep_data[ppm][ch_id] = {'cent': cent, 'rev': rev}
                channels.add(ch_id)
                ppms.add(ppm)
    except FileNotFoundError:
        logger.error(f"Could not find {input_csv}. Have you run revenue_optimizer.py yet?")
        return

    # 1. Find the overall best uniform PPM (maximum total revenue)
    best_ppm = -1
    max_total_rev = -1
    
    for ppm in sorted(ppms):
        total_rev = sum(ch_data['rev'] for ch_data in sweep_data[ppm].values())
        if total_rev > max_total_rev:
            max_total_rev = total_rev
            best_ppm = ppm
            
    logger.info(f"Overall maximum revenue ({max_total_rev}) was at uniform PPM {best_ppm}")
    
    # 2. Extract target centralities for each channel at the best uniform PPM
    target_centralities = {}
    for ch_id in channels:
        # If the channel wasn't recorded at best_ppm for some reason, default to 0
        target_centralities[ch_id] = sweep_data[best_ppm].get(ch_id, {'cent': 0})['cent']
        
    # 3. For each channel, find the max PPM where the centrality equals the target centrality
    equivalent_ppms = {}
    
    print("\n=== Equivalent PPM Adjustments ===")
    print(f"{'Channel':<15} | {'Target Cent':<12} | {'Orig PPM':<10} | {'New PPM':<10} | {'Rev Boost'}")
    print("-" * 65)
    
    total_rev_boost = 0
    
    for ch_id in sorted(channels):
        target_cent = target_centralities[ch_id]
        
        best_individual_ppm = best_ppm
        
        # We only care about optimizing if the centrality is > 0
        if target_cent > 0:
            for ppm in sorted(ppms):
                if ch_id in sweep_data[ppm]:
                    cent_at_ppm = sweep_data[ppm][ch_id]['cent']
                    # We want the highest PPM that retains the *exact* same centrality
                    if cent_at_ppm == target_cent and ppm > best_individual_ppm:
                        best_individual_ppm = ppm
        
        equivalent_ppms[ch_id] = best_individual_ppm
        
        orig_rev = target_cent * best_ppm
        new_rev = target_cent * best_individual_ppm
        rev_boost = new_rev - orig_rev
        total_rev_boost += rev_boost
        
        if best_individual_ppm != best_ppm:
            print(f"{ch_id:<15} | {target_cent:<12} | {best_ppm:<10} | {best_individual_ppm:<10} | +{rev_boost}")
        
    print("-" * 65)
    print(f"Total projected revenue boost from individual channel optimizations: +{total_rev_boost}")
    
    optimized_ppms = {}
    for ch_id, eq_ppm in equivalent_ppms.items():
        if eq_ppm > best_ppm:
            optimized_ppms[ch_id] = best_ppm + 1
        else:
            optimized_ppms[ch_id] = best_ppm

    # Save to JSON
    with open(output_json, mode='w') as f:
        json.dump(optimized_ppms, f, indent=4)
        
    logger.info(f"Optimized individual PPMs saved to {output_json}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Find higher PPMs that retain the same centrality as the optimal uniform PPM")
    parser.add_argument("--input-csv", type=str, default="centrality_sweep_results.csv", help="Input CSV from revenue_optimizer.py")
    parser.add_argument("--output-json", type=str, default="adjusted_best_ppms.json", help="Output JSON for optimized individual PPMs")
    args = parser.parse_args()
    
    find_equivalent_ppms(args.input_csv, args.output_json)
