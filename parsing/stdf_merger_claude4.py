#!/usr/bin/env python3
"""
STDF File Merger - Removes redundant test results and keeps only final results
Uses Semi-ATE STDF package to process Standard Test Data Format files
"""

import os
import sys
from pathlib import Path
from typing import List, Dict, Tuple, Set
from collections import defaultdict, OrderedDict
import argparse

try:
    from Semi_ATE.STDF import STDF
except ImportError:
    print("Error: Semi-ATE package not found. Install with: pip install Semi-ATE")
    sys.exit(1)


class STDFMerger:
    """
    Merges multiple STDF files, keeping only the final test results for each part.
    Removes redundant test data from retesting scenarios.
    """
    
    def __init__(self, input_files: List[str], output_file: str):
        self.input_files = input_files
        self.output_file = output_file
        self.merged_data = {}
        self.part_test_map = defaultdict(dict)  # part_id -> {test_id: latest_record}
        self.file_order = {}  # Track file processing order for chronological merging
        
    def validate_files(self) -> bool:
        """Validate that all input files exist and are readable"""
        for file_path in self.input_files:
            if not os.path.exists(file_path):
                print(f"Error: File not found: {file_path}")
                return False
            if not os.access(file_path, os.R_OK):
                print(f"Error: Cannot read file: {file_path}")
                return False
        return True
    
    def extract_part_id(self, record) -> str:
        """Extract unique part identifier from test record"""
        # Use combination of head/site/part numbers for unique identification
        head_num = getattr(record, 'HEAD_NUM', 0)
        site_num = getattr(record, 'SITE_NUM', 0)
        part_num = getattr(record, 'PART_NUM', 0)
        return f"{head_num}_{site_num}_{part_num}"
    
    def extract_test_id(self, record) -> str:
        """Extract unique test identifier from test record"""
        test_num = getattr(record, 'TEST_NUM', 0)
        test_name = getattr(record, 'TEST_TXT', '')
        return f"{test_num}_{test_name}"
    
    def process_stdf_file(self, file_path: str, file_index: int) -> Dict:
        """Process a single STDF file and extract test records"""
        print(f"Processing file {file_index + 1}/{len(self.input_files)}: {file_path}")
        
        file_data = {
            'header_records': [],
            'test_records': [],
            'summary_records': [],
            'part_records': {}
        }
        
        try:
            with STDF(file_path) as stdf_file:
                for record in stdf_file:
                    record_type = record.__class__.__name__
                    
                    # Handle different record types
                    if record_type in ['FAR', 'ATR', 'MIR', 'MRR', 'PCR', 'HBR', 'SBR', 'PMR', 'PGR', 'PLR']:
                        # Header and summary records
                        if record_type in ['MIR', 'MRR']:
                            file_data['summary_records'].append(record)
                        else:
                            file_data['header_records'].append(record)
                    
                    elif record_type in ['PIR', 'PRR']:
                        # Part information records
                        part_id = self.extract_part_id(record)
                        if part_id not in file_data['part_records']:
                            file_data['part_records'][part_id] = {}
                        file_data['part_records'][part_id][record_type] = record
                    
                    elif record_type in ['PTR', 'MPR', 'FTR']:
                        # Test result records - these are what we need to merge intelligently
                        part_id = self.extract_part_id(record)
                        test_id = self.extract_test_id(record)
                        
                        # Store with file order for chronological processing
                        record_key = f"{part_id}_{test_id}"
                        if record_key not in self.part_test_map:
                            self.part_test_map[record_key] = {}
                        
                        self.part_test_map[record_key][file_index] = record
                        file_data['test_records'].append(record)
                    
                    else:
                        # Other records (GDR, DTR, etc.)
                        file_data['header_records'].append(record)
                        
        except Exception as e:
            print(f"Error processing file {file_path}: {str(e)}")
            return {}
        
        return file_data
    
    def merge_files(self) -> bool:
        """Main method to merge all STDF files"""
        if not self.validate_files():
            return False
        
        print(f"Starting merge of {len(self.input_files)} STDF files...")
        all_file_data = []
        
        # Process each file
        for i, file_path in enumerate(self.input_files):
            file_data = self.process_stdf_file(file_path, i)
            if not file_data:
                print(f"Failed to process {file_path}")
                return False
            all_file_data.append(file_data)
        
        # Determine final test results (latest file wins for each part/test combination)
        final_test_records = []
        processed_combinations = set()
        
        print("Resolving duplicate test results...")
        for record_key, file_records in self.part_test_map.items():
            # Get the record from the highest numbered file (latest)
            latest_file_index = max(file_records.keys())
            final_record = file_records[latest_file_index]
            final_test_records.append(final_record)
            processed_combinations.add(record_key)
        
        # Collect all unique part records (latest version)
        final_part_records = {}
        for file_data in reversed(all_file_data):  # Process in reverse to get latest
            for part_id, part_info in file_data['part_records'].items():
                if part_id not in final_part_records:
                    final_part_records[part_id] = part_info
        
        # Use header records from the first file (they should be mostly the same)
        header_records = all_file_data[0]['header_records'] if all_file_data else []
        summary_records = all_file_data[-1]['summary_records'] if all_file_data else []
        
        print(f"Final result: {len(final_test_records)} test records for {len(final_part_records)} parts")
        
        # Write merged file
        return self.write_merged_file(header_records, final_part_records, final_test_records, summary_records)
    
    def write_merged_file(self, header_records: List, part_records: Dict, 
                         test_records: List, summary_records: List) -> bool:
        """Write the merged data to output STDF file"""
        try:
            print(f"Writing merged file: {self.output_file}")
            
            with STDF(self.output_file, mode='w') as output_stdf:
                # Write header records
                for record in header_records:
                    output_stdf.write(record)
                
                # Write part and test records in proper order
                # Group by part ID and write PIR, tests, PRR for each part
                for part_id, part_info in part_records.items():
                    # Write PIR (Part Information Record)
                    if 'PIR' in part_info:
                        output_stdf.write(part_info['PIR'])
                    
                    # Write all test records for this part
                    part_tests = [record for record in test_records 
                                if self.extract_part_id(record) == part_id]
                    
                    for test_record in part_tests:
                        output_stdf.write(test_record)
                    
                    # Write PRR (Part Result Record)
                    if 'PRR' in part_info:
                        output_stdf.write(part_info['PRR'])
                
                # Write summary records
                for record in summary_records:
                    output_stdf.write(record)
            
            print(f"Successfully created merged file: {self.output_file}")
            return True
            
        except Exception as e:
            print(f"Error writing merged file: {str(e)}")
            return False
    
    def print_summary(self):
        """Print summary of the merge operation"""
        total_original_records = 0
        for file_path in self.input_files:
            try:
                with STDF(file_path) as stdf_file:
                    count = sum(1 for _ in stdf_file)
                    total_original_records += count
                    print(f"  {file_path}: {count} records")
            except:
                print(f"  {file_path}: Could not count records")
        
        try:
            with STDF(self.output_file) as stdf_file:
                merged_count = sum(1 for _ in stdf_file)
                print(f"  {self.output_file}: {merged_count} records")
                print(f"  Reduction: {total_original_records - merged_count} records removed")
        except:
            print("  Could not count merged file records")


def main():
    """Main function with command line interface"""
    parser = argparse.ArgumentParser(
        description="Merge STDF files removing redundant test results",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python stdf_merger.py file1.stdf file2.stdf file3.stdf -o merged.stdf
  python stdf_merger.py *.stdf -o final_results.stdf
        """
    )
    
    parser.add_argument('input_files', nargs='+', 
                       help='Input STDF files (in chronological order)')
    parser.add_argument('-o', '--output', required=True,
                       help='Output merged STDF file')
    parser.add_argument('-v', '--verbose', action='store_true',
                       help='Verbose output')
    
    args = parser.parse_args()
    
    # Validate inputs
    if len(args.input_files) < 2:
        print("Error: At least 2 input files are required for merging")
        return 1
    
    # Create merger and process
    merger = STDFMerger(args.input_files, args.output)
    
    if merger.merge_files():
        print("\nMerge Summary:")
        merger.print_summary()
        print("Merge completed successfully!")
        return 0
    else:
        print("Merge failed!")
        return 1


if __name__ == "__main__":
    sys.exit(main())
