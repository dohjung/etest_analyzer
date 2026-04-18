#!/usr/bin/env python3
"""
STDF File Merger for Multiple Test Runs

This script merges multiple STDF (Standard Test Data Format) files from sequential test runs
where each subsequent file contains retests of failed parts/tests from previous runs.
The final output contains a complete dataset with only the most recent test result for each part.

Requirements:
- Python 3.6+
- Semi-STDF library (pip install semi-stdf)
"""

import os
import sys
import argparse
from datetime import datetime
from collections import defaultdict
from semi_stdf import stdf_file
from semi_stdf.stdf_types import *


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Merge multiple STDF files, keeping only the most recent test results")
    parser.add_argument("-i", "--input", required=True, nargs='+', 
                        help="Input STDF files in chronological order (oldest to newest)")
    parser.add_argument("-o", "--output", required=True, 
                        help="Output merged STDF file")
    parser.add_argument("-v", "--verbose", action="store_true", 
                        help="Enable verbose output")
    return parser.parse_args()


def merge_stdf_files(input_files, output_file, verbose=False):
    """
    Merge multiple STDF files, keeping only the most recent test results for each part.
    
    Args:
        input_files (list): List of input STDF files in chronological order
        output_file (str): Path to the output merged STDF file
        verbose (bool): Whether to print verbose information
    """
    if verbose:
        print(f"Processing {len(input_files)} STDF files...")
    
    # Dictionary to store the latest record for each part
    # Key: (head_num, site_num, part_id)
    # Value: {record_type: record}
    part_records = defaultdict(dict)
    
    # Dictionary to store MIR, MRR, PCR and other non-part-specific records
    global_records = {}
    
    # Track the latest timestamps for headers
    latest_mir = None
    latest_mrr = None
    
    # Process all input files in chronological order
    for file_index, input_file in enumerate(input_files):
        if verbose:
            print(f"Reading file {file_index+1}/{len(input_files)}: {input_file}")
        
        try:
            with stdf_file.open_stdf(input_file) as f:
                # Parse the STDF file record by record
                for rec in f:
                    # Handle MIR (Master Information Record)
                    if isinstance(rec, MIR):
                        latest_mir = rec
                    
                    # Handle MRR (Master Results Record)
                    elif isinstance(rec, MRR):
                        latest_mrr = rec
                    
                    # Handle PIR (Part Information Record) and store with its associated records
                    elif isinstance(rec, PIR):
                        # Extract key information to identify the part
                        head_num = rec.HEAD_NUM
                        site_num = rec.SITE_NUM
                        
                        # Get or create part ID
                        # We'll use PIR.PART_ID if available, otherwise a combination of other fields
                        if hasattr(rec, 'PART_ID') and rec.PART_ID is not None:
                            part_id = rec.PART_ID
                        else:
                            # Create a synthetic part ID from available information
                            # Use a combination that will be unique for each physical part
                            part_id = f"X{rec.X_COORD}Y{rec.Y_COORD}"
                        
                        part_key = (head_num, site_num, part_id)
                        
                        # Store this PIR and clear any previous PTR records for this part
                        # since we're getting a newer test of this part
                        part_records[part_key]['PIR'] = rec
                        # Keep existing PRR if it exists
                        if 'PTR' in part_records[part_key]:
                            del part_records[part_key]['PTR']
                    
                    # Handle PTR (Parametric Test Record)
                    elif isinstance(rec, PTR):
                        head_num = rec.HEAD_NUM
                        site_num = rec.SITE_NUM
                        test_num = rec.TEST_NUM
                        
                        # We need to find which part this PTR belongs to
                        # Scan through our part_records to find the matching part
                        for part_key, records in part_records.items():
                            if part_key[0] == head_num and part_key[1] == site_num:
                                # This PTR belongs to this part
                                if 'PIR' in records:  # Make sure we have a PIR for this part
                                    if 'PTR' not in records:
                                        records['PTR'] = {}
                                    # Store this test result, overwriting any previous result for this test number
                                    records['PTR'][test_num] = rec
                                    break
                    
                    # Handle PRR (Part Results Record)
                    elif isinstance(rec, PRR):
                        head_num = rec.HEAD_NUM
                        site_num = rec.SITE_NUM
                        
                        # Find the matching part
                        for part_key, records in part_records.items():
                            if part_key[0] == head_num and part_key[1] == site_num:
                                if 'PIR' in records:  # Make sure we have a PIR for this part
                                    # Store/update PRR for this part
                                    records['PRR'] = rec
                                    break
                    
                    # Store other global records (program info, hardware config, etc.)
                    else:
                        record_type = type(rec).__name__
                        if record_type not in ('MIR', 'MRR', 'PIR', 'PTR', 'PRR'):
                            if record_type not in global_records:
                                global_records[record_type] = []
                            global_records[record_type].append(rec)
        
        except Exception as e:
            print(f"Error processing file {input_file}: {e}", file=sys.stderr)
            if verbose:
                import traceback
                traceback.print_exc()
            return False
    
    # Write the merged STDF file
    try:
        if verbose:
            print(f"Writing merged data to {output_file}...")
            print(f"Total parts: {len(part_records)}")
        
        with stdf_file.open_stdf(output_file, "wb") as outf:
            # Write updated MIR with current timestamp
            if latest_mir:
                latest_mir.SETUP_T = datetime.now().strftime("%H:%M:%S %d-%b-%Y")
                latest_mir.START_T = datetime.now().strftime("%H:%M:%S %d-%b-%Y")
                outf.write_record(latest_mir)
            
            # Write global records
            for record_type, records in global_records.items():
                for rec in records:
                    outf.write_record(rec)
            
            # Write part-specific records
            for part_key, records in part_records.items():
                if 'PIR' in records:
                    # Write PIR
                    outf.write_record(records['PIR'])
                    
                    # Write all PTRs for this part
                    if 'PTR' in records:
                        for test_num, ptr in sorted(records['PTR'].items()):
                            outf.write_record(ptr)
                    
                    # Write PRR if available
                    if 'PRR' in records:
                        outf.write_record(records['PRR'])
            
            # Write updated MRR with current timestamp
            if latest_mrr:
                latest_mrr.FINISH_T = datetime.now().strftime("%H:%M:%S %d-%b-%Y")
                outf.write_record(latest_mrr)
        
        if verbose:
            print("Merge completed successfully!")
        return True
    
    except Exception as e:
        print(f"Error writing merged file: {e}", file=sys.stderr)
        if verbose:
            import traceback
            traceback.print_exc()
        return False


def main():
    """Main entry point of the script"""
    args = parse_arguments()
    
    # Sort input files by modification time if not already ordered
    input_files = args.input
    if len(input_files) > 1:
        input_files.sort(key=os.path.getmtime)
    
    # Merge STDF files
    success = merge_stdf_files(input_files, args.output, args.verbose)
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
