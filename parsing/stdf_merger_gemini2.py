import os
from collections import defaultdict
from stdf.stdf_writer import STDFWriter
from stdf.stdf_file import STDFFile
# No incorrect import like "from stdf.stdf_record_parser import مرحبا" is needed here

def merge_stdf_files(input_files, output_file):
    """
    Merges multiple STDF files, keeping only the final test results for each part.

    Args:
        input_files (list): A list of STDF file paths, ordered from the
                            earliest test to the latest re-test.
        output_file (str): The path for the merged output STDF file.
    """
    final_part_results = {}  # To store the latest PCR for each part, and act as anchor
    final_test_results = defaultdict(dict)  # {part_id_tuple: {test_num: latest_ptr_or_ftr_or_mpr}}
    sdr_records = {} # {sdr_key_tuple: sdr_record} to store unique SDRs

    # --- Pass 1: Read all files to gather the latest results ---
    print("--- Reading input files to gather latest results ---")
    for filename in input_files:
        if not os.path.exists(filename):
            print(f"Warning: File {filename} not found. Skipping.")
            continue
        print(f"Processing file: {filename}")
        
        # Corrected STDFFile instantiation:
        stdf_file = STDFFile() 
        
        try:
            stdf_file.open(filename)

            for record_type, records in stdf_file:
                for rec in records:
                    # We handle MIR and MRR separately by reading them from the first/last file
                    # during the writing phase.
                    if rec.id == 'SDR':
                        # Create a unique key for SDRs to avoid duplicates if they are identical
                        sdr_key = (
                            rec.fields.get('HEAD_NUM', b'\xff'), # Use a default if field missing
                            rec.fields.get('SITE_GRP', b'\xff'),
                            rec.fields.get('SITE_CNT', 0),
                            rec.fields.get('SDR_EXPR', b'') 
                            # Add more fields if needed for uniqueness, e.g., HAND_TYP, etc.
                        )
                        if sdr_key not in sdr_records:
                            sdr_records[sdr_key] = rec

                    elif rec.id == 'PCR': # Part Count Record
                        part_id_tuple = (rec.fields['HEAD_NUM'], rec.fields['SITE_NUM'])
                        # Store the latest PCR for this part
                        final_part_results[part_id_tuple] = {'pcr': rec}
                        # If a part is encountered via PCR, ensure its test dictionary exists
                        if part_id_tuple not in final_test_results:
                            final_test_results[part_id_tuple] = {}


                    elif rec.id in ['PTR', 'FTR', 'MPR']: # Test Records
                        part_id_tuple = (rec.fields['HEAD_NUM'], rec.fields['SITE_NUM'])
                        test_num = rec.fields['TEST_NUM']
                        
                        # Ensure this part is known (e.g. if only test records seen before PCR)
                        if part_id_tuple not in final_part_results:
                           final_part_results[part_id_tuple] = {'pcr': None} # Placeholder for PCR

                        # Overwrite with the latest result for this test on this part
                        final_test_results[part_id_tuple][test_num] = rec
        
        except Exception as e:
            print(f"Error processing file {filename}: {e}")
        finally:
            stdf_file.close()
            
    print("--- Finished reading input files ---")

    # --- Pass 2: Write the merged STDF file ---
    print(f"--- Writing merged results to {output_file} ---")
    with STDFWriter(output_file) as writer:
        first_file_mir = None
        last_file_mrr = None

        # Get MIR from the first file
        if input_files:
            first_file_path = None
            for f_path in input_files:
                if os.path.exists(f_path):
                    first_file_path = f_path
                    break
            if first_file_path:
                mir_stdf_file = STDFFile()
                try:
                    mir_stdf_file.open(first_file_path)
                    for record_type, records in mir_stdf_file:
                        if record_type == 'MIR':
                            first_file_mir = records[0]
                            break 
                except Exception as e:
                    print(f"Error reading MIR from {first_file_path}: {e}")
                finally:
                    mir_stdf_file.close()

        if first_file_mir:
            writer.write_record(first_file_mir)
        else:
            print("Warning: No MIR record found in the first valid input file. Output MIR will be missing.")

        # Write unique SDRs collected
        # Sorting SDRs by head number then site group for some consistency, if desired
        # This is optional, as order might not be strictly critical for all SDRs
        sorted_sdr_keys = sorted(sdr_records.keys()) 
        for sdr_key in sorted_sdr_keys:
            writer.write_record(sdr_records[sdr_key])

        # Get all part IDs that have final results, sort for consistent output
        parts_to_write = sorted(final_part_results.keys()) 

        for part_id_tuple in parts_to_write:
            # Note: This script does not explicitly reconstruct or ensure a PIR record
            # for each part. For a fully compliant STDF, a PIR should precede
            # test records for each part. You might need to enhance this part
            # if strict PIR handling is required (e.g., by capturing the last seen PIR
            # for each part during the reading phase).

            # Write test results for the current part
            if part_id_tuple in final_test_results:
                tests_for_part = final_test_results[part_id_tuple]
                # Sort tests by test number for consistent output
                sorted_test_nums = sorted(tests_for_part.keys())
                for test_num in sorted_test_nums:
                    writer.write_record(tests_for_part[test_num])

            # Write PCR for the current part (if it was found)
            if final_part_results[part_id_tuple].get('pcr'):
                writer.write_record(final_part_results[part_id_tuple]['pcr'])
            else:
                # This case might occur if a part had test records but no PCR was found for it
                # in any of the files, which would be unusual for a complete part test.
                print(f"Warning: No PCR record was stored for part {part_id_tuple}. "
                      "Its entry in the merged file might be incomplete regarding part counts.")

        # Get MRR from the last file
        if input_files:
            last_file_path = None
            for f_path in reversed(input_files):
                if os.path.exists(f_path):
                    last_file_path = f_path
                    break
            if last_file_path:
                mrr_stdf_file = STDFFile()
                try:
                    mrr_stdf_file.open(last_file_path)
                    for record_type, records in mrr_stdf_file:
                        if record_type == 'MRR':
                            last_file_mrr = records[0]
                            break
                except Exception as e:
                    print(f"Error reading MRR from {last_file_path}: {e}")
                finally:
                    mrr_stdf_file.close()
        
        if last_file_mrr:
            writer.write_record(last_file_mrr)
        else:
            print("Warning: No MRR record found in the last valid input file. Output MRR will be missing.")

    print(f"--- Merged STDF file created: {output_file} ---")
    print(f"Processed data for {len(final_part_results)} unique part identifiers (HEAD_NUM, SITE_NUM).")


if __name__ == '__main__':
    # --- Configuration ---
    # List your STDF files here, from OLDEST to NEWEST 
    # (original test to latest retest)
    # EXAMPLE:
    # input_stdf_files = [
    #     "lot_A_initial_test.stdf", 
    #     "lot_A_retest_failed_only.stdf", 
    #     "lot_A_final_retest.stdf"
    # ]
    input_stdf_files = [
        "path/to/your/first_test.stdf",   # Replace with your actual file path
        "path/to/your/second_retest.stdf", # Replace with your actual file path
        "path/to/your/third_retest.stdf"   # Add more files if needed
    ]
    
    output_stdf_file = "merged_final_results.stdf" # Desired output file name

    # Basic check if placeholder paths are still there
    if any("path/to/your/" in f for f in input_stdf_files):
        print("ERROR: Please update 'input_stdf_files' in the script with your actual STDF file paths before running.")
    else:
        # Make sure the output directory exists if specified in output_stdf_file path
        output_dir = os.path.dirname(output_stdf_file)
        if output_dir and not os.path.exists(output_dir):
            try:
                os.makedirs(output_dir)
                print(f"Created output directory: {output_dir}")
            except OSError as e:
                print(f"Error creating output directory {output_dir}: {e}")
                # Potentially exit or handle error as appropriate

        if not output_dir or os.path.exists(output_dir): # Proceed if no dir needed or dir exists/was created
             merge_stdf_files(input_stdf_files, output_stdf_file)