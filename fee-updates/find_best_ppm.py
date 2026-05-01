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
logger = logging.getLogger("BestIndividualPPMFinder")

def find_best_individual_ppms(input_csv, output_json):
    # channel -> {max_rev: float, best_ppm: int, cent_at_best: int}
    best_data = {}
    
    logger.info(f"Loading sweep results from {input_csv}...")
    try:
        with open(input_csv, mode='r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                ppm = int(row["PPM"])
                ch_id = row["Channel"]
                cent = int(row["Edge_Centrality"])
                rev = float(row["Revenue_Potential"])
                
                if ch_id not in best_data:
                    best_data[ch_id] = {'max_rev': -1, 'best_ppm': -1, 'cent': 0}
                    
                # We want max revenue. If tied, pick the higher PPM to minimize routing overhead
                if rev > best_data[ch_id]['max_rev'] or (rev == best_data[ch_id]['max_rev'] and ppm > best_data[ch_id]['best_ppm']):
                    best_data[ch_id] = {'max_rev': rev, 'best_ppm': ppm, 'cent': cent}
                    
    except FileNotFoundError:
        logger.error(f"Could not find {input_csv}. Have you run revenue_optimizer.py yet?")
        return

    print("\n=== Best Individual PPM Adjustments ===")
    print(f"{'Channel':<15} | {'Best PPM':<10} | {'Max Revenue':<15} | {'Centrality'}")
    print("-" * 60)
    
    total_max_rev = 0
    optimized_ppms = {}
    
    for ch_id in sorted(best_data.keys()):
        data = best_data[ch_id]
        best_ppm = data['best_ppm']
        max_rev = data['max_rev']
        cent = data['cent']
        
        optimized_ppms[ch_id] = best_ppm
        total_max_rev += max_rev
        
        print(f"{ch_id:<15} | {best_ppm:<10} | {max_rev:<15.2f} | {cent}")
        
    print("-" * 60)
    print(f"Total theoretical revenue if all channels hit individual max: {total_max_rev:.2f}")
    
    # Save to JSON
    with open(output_json, mode='w') as f:
        json.dump(optimized_ppms, f, indent=4)
        
    logger.info(f"Best individual PPMs saved to {output_json}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Find the PPM that yields the maximum revenue for each individual channel")
    parser.add_argument("--input-csv", type=str, default="centrality_sweep_results.csv", help="Input CSV from revenue_optimizer.py")
    parser.add_argument("--output-json", type=str, default="individual_best_ppms.json", help="Output JSON for optimized individual PPMs")
    args = parser.parse_args()
    
    find_best_individual_ppms(args.input_csv, args.output_json)
