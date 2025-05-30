#!/usr/bin/env python3
"""
STDF File Merger for Multiple Test Runs

This script merges multiple STDF (Standard Test Data Format) files from sequential test runs
where each subsequent file contains retests of failed parts/tests from previous runs.
The final output contains a complete dataset with only the most recent test result for each part.

Requirements:
- Python 3.6+
- Semi-ATE Metis library (https://github.com/Semi-ATE/Metis)
  Install with: pip install Semi-ATE-Metis
"""

import os
import sys
import argparse
from datetime import datetime
from collections import defaultdict
import Semi_ATE.STDF as STDF


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
    
    # Store global records (non-part-specific)
    global_records = {
        'FAR': None,  # File Attributes Record
        'ATRs': [],   # Audit Trail Records
        'MIR': None,  # Master Information Record
        'MRR': None,  # Master Results Record
        'PCR': None,  # Part Count Record
        'HBRs': [],   # Hardware Bin Records
        'SBRs': [],   # Software Bin Records
        'PMRs': [],   # Pin Map Records
        'PGRs': [],   # Pin Group Records
        'PLRs': [],   # Pin List Records
        'RDRs': [],   # Retest Data Records
        'SDRs': [],   # Site Description Records
        'WIRs': [],   # Wafer Information Records
        'WRRs': [],   # Wafer Results Records
        'WCRs': [],   # Wafer Configuration Records
        'TSRs': [],   # Test Synopsis Records
        'DTRs': [],   # Datalog Text Records
        'GDRs': [],   # Generic Data Records
    }
    
    # Process all input files in chronological order
    for file_index, input_file in enumerate(input_files):
        if verbose:
            print(f"Reading file {file_index+1}/{len(input_files)}: {input_file}")
        
        try:
            # Open the STDF file
            with open(input_file, 'rb') as fp:
                stdf_reader = STDF.STDF(fp)
                
                # Process each record in the file
                for rec_type, rec_data in stdf_reader:
                    # Handle global records
                    if rec_type == 'FAR':
                        global_records['FAR'] = rec_data
                    elif rec_type == 'ATR':
                        global_records['ATRs'].append(rec_data)
                    elif rec_type == 'MIR':
                        global_records['MIR'] = rec_data
                    elif rec_type == 'MRR':
                        global_records['MRR'] = rec_data
                    elif rec_type == 'PCR':
                        global_records['PCR'] = rec_data
                    elif rec_type == 'HBR':
                        global_records['HBRs'].append(rec_data)
                    elif rec_type == 'SBR':
                        global_records['SBRs'].append(rec_data)
                    elif rec_type == 'PMR':
                        global_records['PMRs'].append(rec_data)
                    elif rec_type == 'PGR':
                        global_records['PGRs'].append(rec_data)
                    elif rec_type == 'PLR':
                        global_records['PLRs'].append(rec_data)
                    elif rec_type == 'RDR':
                        global_records['RDRs'].append(rec_data)
                    elif rec_type == 'SDR':
                        global_records['SDRs'].append(rec_data)
                    elif rec_type == 'WIR':
                        global_records['WIRs'].append(rec_data)
                    elif rec_type == 'WRR':
                        global_records['WRRs'].append(rec_data)
                    elif rec_type == 'WCR':
                        global_records['WCRs'].append(rec_data)
                    elif rec_type == 'TSR':
                        global_records['TSRs'].append(rec_data)
                    elif rec_type == 'DTR':
                        global_records['DTRs'].append(rec_data)
                    elif rec_type == 'GDR':
                        global_records['GDRs'].append(rec_data)
                    
                    # Handle PIR (Part Information Record)
                    elif rec_type == 'PIR':
                        head_num = rec_data.get('HEAD_NUM', 1)
                        site_num = rec_data.get('SITE_NUM', 1)
                        
                        # Get or create part ID
                        if 'PART_ID' in rec_data and rec_data['PART_ID'] is not None:
                            part_id = rec_data['PART_ID']
                        else:
                            # Create a synthetic part ID from X/Y coordinates
                            x_coord = rec_data.get('X_COORD', 0)
                            y_coord = rec_data.get('Y_COORD', 0)
                            part_id = f"X{x_coord}Y{y_coord}"
                        
                        part_key = (head_num, site_num, part_id)
                        
                        # Store this PIR and clear any previous PTR records for this part
                        # since we're getting a newer test of this part
                        part_records[part_key]['PIR'] = rec_data
                        part_records[part_key]['PTRs'] = []  # Reset PTRs for new test
                        part_records[part_key]['FTRs'] = []  # Reset FTRs for new test
                        # The PRR will be updated when we find it
                    
                    # Handle PTR (Parametric Test Record)
                    elif rec_type == 'PTR':
                        head_num = rec_data.get('HEAD_NUM', 1)
                        site_num = rec_data.get('SITE_NUM', 1)
                        test_num = rec_data.get('TEST_NUM', 0)
                        
                        # Find the correct part this belongs to
                        matching_part_key = None
                        for part_key in part_records:
                            if part_key[0] == head_num and part_key[1] == site_num:
                                # Check if we already have a PIR for this part in the current file
                                if 'PIR' in part_records[part_key]:
                                    matching_part_key = part_key
                                    break
                        
                        if matching_part_key:
                            # Add this PTR to the part's records
                            if 'PTRs' not in part_records[matching_part_key]:
                                part_records[matching_part_key]['PTRs'] = []
                            part_records[matching_part_key]['PTRs'].append(rec_data)
                    
                    # Handle FTR (Functional Test Record)
                    elif rec_type == 'FTR':
                        head_num = rec_data.get('HEAD_NUM', 1)
                        site_num = rec_data.get('SITE_NUM', 1)
                        test_num = rec_data.get('TEST_NUM', 0)
                        
                        # Find the correct part this belongs to
                        matching_part_key = None
                        for part_key in part_records:
                            if part_key[0] == head_num and part_key[1] == site_num:
                                # Check if we already have a PIR for this part in the current file
                                if 'PIR' in part_records[part_key]:
                                    matching_part_key = part_key
                                    break
                        
                        if matching_part_key:
                            # Add this FTR to the part's records
                            if 'FTRs' not in part_records[matching_part_key]:
                                part_records[matching_part_key]['FTRs'] = []
                            part_records[matching_part_key]['FTRs'].append(rec_data)
                    
                    # Handle PRR (Part Results Record)
                    elif rec_type == 'PRR':
                        head_num = rec_data.get('HEAD_NUM', 1)
                        site_num = rec_data.get('SITE_NUM', 1)
                        
                        # Find the correct part this belongs to
                        matching_part_key = None
                        for part_key in part_records:
                            if part_key[0] == head_num and part_key[1] == site_num:
                                # Check if we already have a PIR for this part in the current file
                                if 'PIR' in part_records[part_key]:
                                    matching_part_key = part_key
                                    break
                        
                        if matching_part_key:
                            # Update the PRR for this part
                            part_records[matching_part_key]['PRR'] = rec_data
                
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
        
        # Update PCR with correct part count
        if global_records['PCR']:
            global_records['PCR']['PART_CNT'] = len(part_records)
        
        # Update timestamps
        if global_records['MIR']:
            global_records['MIR']['START_T'] = datetime.now().strftime("%H:%M:%S %d-%b-%Y")
        
        if global_records['MRR']:
            global_records['MRR']['FINISH_T'] = datetime.now().strftime("%H:%M:%S %d-%b-%Y")
        
        # Create a new STDF writer
        with open(output_file, 'wb') as fp:
            stdf_writer = STDF.STDF_WRITER(fp)
            
            # Write FAR record
            if global_records['FAR']:
                stdf_writer.write_record('FAR', global_records['FAR'])
            
            # Write ATR records
            for atr in global_records['ATRs']:
                stdf_writer.write_record('ATR', atr)
            
            # Write MIR record
            if global_records['MIR']:
                stdf_writer.write_record('MIR', global_records['MIR'])
            
            # Write other global records
            for pgr in global_records['PGRs']:
                stdf_writer.write_record('PGR', pgr)
            
            for plr in global_records['PLRs']:
                stdf_writer.write_record('PLR', plr)
            
            for pmr in global_records['PMRs']:
                stdf_writer.write_record('PMR', pmr)
            
            for sdr in global_records['SDRs']:
                stdf_writer.write_record('SDR', sdr)
            
            for wir in global_records['WIRs']:
                stdf_writer.write_record('WIR', wir)
            
            for wcr in global_records['WCRs']:
                stdf_writer.write_record('WCR', wcr)
            
            # Write part-specific records
            for part_key, records in part_records.items():
                if 'PIR' in records:
                    # Write PIR
                    stdf_writer.write_record('PIR', records['PIR'])
                    
                    # Write all PTRs for this part
                    if 'PTRs' in records:
                        for ptr in records['PTRs']:
                            stdf_writer.write_record('PTR', ptr)
                    
                    # Write all FTRs for this part
                    if 'FTRs' in records:
                        for ftr in records['FTRs']:
                            stdf_writer.write_record('FTR', ftr)
                    
                    # Write PRR if available
                    if 'PRR' in records:
                        stdf_writer.write_record('PRR', records['PRR'])
            
            # Write remaining global records
            if global_records['PCR']:
                stdf_writer.write_record('PCR', global_records['PCR'])
            
            for hbr in global_records['HBRs']:
                stdf_writer.write_record('HBR', hbr)
            
            for sbr in global_records['SBRs']:
                stdf_writer.write_record('SBR', sbr)
            
            for tsr in global_records['TSRs']:
                stdf_writer.write_record('TSR', tsr)
            
            for wrr in global_records['WRRs']:
                stdf_writer.write_record('WRR', wrr)
            
            # Write MRR record
            if global_records['MRR']:
                stdf_writer.write_record('MRR', global_records['MRR'])
        
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
