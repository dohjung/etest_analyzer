import os
import pandas as pd
from Semi_ATE.STDF import V4

def parse_stdf_to_dataframe(stdf_file_path):
    """
    Parses an STDF file and extracts relevant records into a pandas DataFrame.
    Focuses on PTR records for test results.
    You might need to adjust this based on what records you need.
    """
    data = []
    try:
        with open(stdf_file_path, 'rb') as f:
            for record in V4.records_from_file(f):
                if isinstance(record, V4.PTR):
                    # Extract key information from PTR record
                    data.append({
                        'FILE_PATH': stdf_file_path, # Keep track of the source file
                        'REC_TYP': record.REC_TYP,
                        'REC_SUB': record.REC_SUB,
                        'HEAD_NUM': record.HEAD_NUM,
                        'SITE_NUM': record.SITE_NUM,
                        'TEST_NUM': record.TEST_NUM,
                        'TEST_NAM': record.TEST_NAM if hasattr(record, 'TEST_NAM') else None,
                        'HARD_BIN': record.HARD_BIN if hasattr(record, 'HARD_BIN') else None,
                        'SOFT_BIN': record.SOFT_BIN if hasattr(record, 'SOFT_BIN') else None,
                        'RESULT': record.RESULT, # The test result (e.g., value, pass/fail code)
                        'TEST_FLG': record.TEST_FLG, # Test flags (e.g., pass/fail status)
                        'RTN_ICNT': record.RTN_ICNT, # Retest count - very important for your case
                        'X_COORD': record.X_COORD if hasattr(record, 'X_COORD') else None,
                        'Y_COORD': record.Y_COORD if hasattr(record, 'Y_COORD') else None,
                        # Add other relevant fields from PTR or other records like PRR if needed
                        # For PRR (Part Results Record), you might also want to track PART_ID
                        # To get PART_ID, you'd need to link PRR records to PTRs.
                        # This example simplifies by only getting PTRs.
                    })
                # You might need to process PRR records to get PART_ID
                # For simplicity, we'll assume HEAD_NUM and SITE_NUM are enough for part identification for this example.
                # If PART_ID is crucial, you'll need to store PRR data and merge it based on sequence.
                # Or, if your PTRs contain unique part IDs, use that.
    except Exception as e:
        print(f"Error processing {stdf_file_path}: {e}")
    return pd.DataFrame(data)

def merge_stdf_files_final_results(stdf_files_paths, output_csv_path="merged_final_results.csv"):
    """
    Merges multiple STDF files, keeping only the final test result for each
    part/test combination.

    Args:
        stdf_files_paths (list): A list of paths to the STDF files, ordered
                                 from oldest to newest (first is largest, last is smallest).
        output_csv_path (str): The path to save the merged results as a CSV.
    """
    all_dataframes = []

    print("Parsing STDF files...")
    for file_path in stdf_files_paths:
        print(f"  - Parsing {file_path}")
        df = parse_stdf_to_dataframe(file_path)
        if not df.empty:
            df['SOURCE_FILE_INDEX'] = stdf_files_paths.index(file_path) # Helps in ordering later
            all_dataframes.append(df)

    if not all_dataframes:
        print("No data parsed from any STDF files.")
        return

    # Concatenate all dataframes
    merged_df = pd.concat(all_dataframes, ignore_index=True)
    print(f"Total records after initial merge: {len(merged_df)}")

    # Sort to ensure later files' results come after earlier ones for the same part/test.
    # The order of sorting keys is crucial for correct deduplication.
    # We sort by:
    # 1. Part identifier (HEAD_NUM, SITE_NUM - if PART_ID is available, use it!)
    # 2. Test identifier (TEST_NUM, TEST_NAM)
    # 3. Source file index (to prioritize later files in case of exact duplicates in earlier files)
    # 4. Retest count (RTN_ICNT) - higher retest count indicates a later run for the same part/test within a file or across files if the part was re-tested.
    merged_df.sort_values(
        by=['HEAD_NUM', 'SITE_NUM', 'TEST_NUM', 'TEST_NAM', 'SOURCE_FILE_INDEX', 'RTN_ICNT'],
        inplace=True,
        ascending=[True, True, True, True, True, True] # Ascending for all to ensure later tests are at the bottom
    )

    # Drop duplicates, keeping the last occurrence.
    # 'HEAD_NUM', 'SITE_NUM', 'TEST_NUM', 'TEST_NAM' define a unique test for a unique part.
    # Because of the sorting, `keep='last'` will select the entry from the latest file (highest SOURCE_FILE_INDEX)
    # and the highest RTN_ICNT for that part/test combination.
    final_results_df = merged_df.drop_duplicates(
        subset=['HEAD_NUM', 'SITE_NUM', 'TEST_NUM', 'TEST_NAM'],
        keep='last'
    )
    print(f"Total records after deduplication (final results): {len(final_results_df)}")

    # Clean up and save
    final_results_df = final_results_df.drop(columns=['FILE_PATH', 'SOURCE_FILE_INDEX'])
    final_results_df.to_csv(output_csv_path, index=False)
    print(f"Merged final results saved to {output_csv_path}")

# --- Example Usage ---
if __name__ == "__main__":
    # Create dummy STDF files for demonstration
    # In a real scenario, you would replace these with your actual file paths.

    # File 1 (largest): Initial run, some failures
    # File 2 (smaller): Retest of some failures from File 1
    # File 3 (smallest): Retest of some remaining failures from File 2

    # Dummy data for demonstration
    # For simplicity, we'll manually create some STDF records.
    # In a real scenario, these would be generated by your tester.

    # Helper to write dummy STDF files
    def write_dummy_stdf(file_path, records):
        with open(file_path, 'wb') as f:
            for rec in records:
                f.write(rec.to_bytes())

    # Dummy records for File 1
    # Part 1, Site 0: Test 1 (Pass), Test 2 (Fail)
    # Part 2, Site 1: Test 1 (Pass), Test 2 (Pass)
    dummy_records_1 = [
        V4.FAR(CPU_TYPE=2, STDF_VER=4),
        V4.MIR(LOT_ID='LOT_ABC', NODE_NAM='Tester1', TSTR_TYP='ATE', TEST_TIM=1678886400),
        V4.SDR(HEAD_NUM=1, SITE_GRP=0, SITE_NUM=1), # Dummy site information
        V4.PRR(HEAD_NUM=1, SITE_NUM=0, PART_FLG=0x00, NUM_TEST=2, HARD_BIN=1, SOFT_BIN=1), # Part 1 (Site 0)
        V4.PTR(HEAD_NUM=1, SITE_NUM=0, TEST_NUM=1, TEST_FLG=0x00, RESULT=10.5, RTN_ICNT=0), # Part 1 Test 1 (Pass)
        V4.PTR(HEAD_NUM=1, SITE_NUM=0, TEST_NUM=2, TEST_FLG=0x80, RESULT=1.2, RTN_ICNT=0), # Part 1 Test 2 (Fail)
        V4.PRR(HEAD_NUM=1, SITE_NUM=1, PART_FLG=0x00, NUM_TEST=2, HARD_BIN=1, SOFT_BIN=1), # Part 2 (Site 1)
        V4.PTR(HEAD_NUM=1, SITE_NUM=1, TEST_NUM=1, TEST_FLG=0x00, RESULT=20.1, RTN_ICNT=0), # Part 2 Test 1 (Pass)
        V4.PTR(HEAD_NUM=1, SITE_NUM=1, TEST_NUM=2, TEST_FLG=0x00, RESULT=5.0, RTN_ICNT=0),  # Part 2 Test 2 (Pass)
        V4.MRR(FINL_PMOD=0)
    ]
    stdf_file_1 = "lot_test_1.stdf"
    write_dummy_stdf(stdf_file_1, dummy_records_1)

    # Dummy records for File 2 (retest of Part 1, Test 2)
    # Part 1, Site 0: Test 2 (Pass) - this is the retest, so RTN_ICNT should be > 0 or it's a new run
    dummy_records_2 = [
        V4.FAR(CPU_TYPE=2, STDF_VER=4),
        V4.MIR(LOT_ID='LOT_ABC', NODE_NAM='Tester2', TSTR_TYP='ATE', TEST_TIM=1678886500),
        V4.SDR(HEAD_NUM=1, SITE_GRP=0, SITE_NUM=1),
        V4.PRR(HEAD_NUM=1, SITE_NUM=0, PART_FLG=0x00, NUM_TEST=1, HARD_BIN=1, SOFT_BIN=1), # Part 1 (Site 0)
        V4.PTR(HEAD_NUM=1, SITE_NUM=0, TEST_NUM=2, TEST_FLG=0x00, RESULT=1.5, RTN_ICNT=1), # Part 1 Test 2 (Retest, now Pass)
        V4.MRR(FINL_PMOD=0)
    ]
    stdf_file_2 = "lot_test_2.stdf"
    write_dummy_stdf(stdf_file_2, dummy_records_2)

    # Dummy records for File 3 (a different part retest)
    # Part 3, Site 0: Test 1 (Pass)
    dummy_records_3 = [
        V4.FAR(CPU_TYPE=2, STDF_VER=4),
        V4.MIR(LOT_ID='LOT_ABC', NODE_NAM='Tester3', TSTR_TYP='ATE', TEST_TIM=1678886600),
        V4.SDR(HEAD_NUM=1, SITE_GRP=0, SITE_NUM=1),
        V4.PRR(HEAD_NUM=1, SITE_NUM=2, PART_FLG=0x00, NUM_TEST=1, HARD_BIN=1, SOFT_BIN=1), # Part 3 (Site 2)
        V4.PTR(HEAD_NUM=1, SITE_NUM=2, TEST_NUM=1, TEST_FLG=0x00, RESULT=7.8, RTN_ICNT=0), # Part 3 Test 1 (Pass)
        V4.MRR(FINL_PMOD=0)
    ]
    stdf_file_3 = "lot_test_3.stdf"
    write_dummy_stdf(stdf_file_3, dummy_records_3)


    # List of STDF files in the order they were generated (or re-tested)
    # This order is crucial for correctly identifying "final" results.
    stdf_files = [stdf_file_1, stdf_file_2, stdf_file_3]
    output_csv = "merged_final_stdf_results.csv"

    merge_stdf_files_final_results(stdf_files, output_csv)

    print("\nContent of the merged CSV file:")
    print(pd.read_csv(output_csv))

    # Clean up dummy files
    os.remove(stdf_file_1)
    os.remove(stdf_file_2)
    os.remove(stdf_file_3)