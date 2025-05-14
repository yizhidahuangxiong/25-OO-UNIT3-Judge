import os
import subprocess
import time
import shutil
import argparse
import concurrent.futures
import threading
import re
from collections import defaultdict

# --- Configuration --- (Keep existing configuration)
CODE_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = r"E:\PyCharmPjs\对拍" # <<< 修改为你的基础路径
DATA_FOLDER = os.path.join(BASE_DIR, "data")
OUTPUT_FOLDER = os.path.join(BASE_DIR, "output")
ANSWERS_FOLDER = os.path.join(BASE_DIR, "answers")
JARS_DIR = os.path.join(BASE_DIR, "jars")
ERROR_FOLDER = os.path.join(BASE_DIR, "errors")
DATA_GENERATOR_SCRIPT = os.path.join(CODE_DIR, "generator11.py") # <<< 确保这是你修改后的生成器脚本名
STRONG_DATA_FOLDER = os.path.join(BASE_DIR, "strong")
STANDARD_JAR_PATH = os.path.join(BASE_DIR, "standard.jar") # <<< 标准答案 Jar 路径
JAR_TIMEOUT = 10 # seconds
PUBLIC_MAX_INSTRUCTIONS = 10000
MUTUAL_MAX_INSTRUCTIONS = 3000
# NEW: Configuration for data generation retries
MAX_GEN_RETRIES_PER_INDEX = 1000 # Number of times to retry generating data for a specific index
RETRY_DELAY_SECONDS = 0     # Optional delay between retry batches

# --- Utility Functions --- (Keep existing functions: setup_directories, get_jar_files, extract_index_from_filename)
def setup_directories(jar_files):
    os.makedirs(DATA_FOLDER, exist_ok=True)
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    os.makedirs(ANSWERS_FOLDER, exist_ok=True)
    os.makedirs(JARS_DIR, exist_ok=True)
    os.makedirs(ERROR_FOLDER, exist_ok=True)
    os.makedirs(STRONG_DATA_FOLDER, exist_ok=True)
    for jar_name in jar_files:
        os.makedirs(os.path.join(OUTPUT_FOLDER, os.path.splitext(jar_name)[0]), exist_ok=True)

def get_jar_files():
    jars = [f for f in os.listdir(JARS_DIR) if f.endswith(".jar")]
    if not jars:
        print(f"Error: No .jar files found in {JARS_DIR}")
        exit(1)
    print(f"Found JAR files: {', '.join(jars)}")
    return jars

def extract_index_from_filename(filename):
    match_gen = re.search(r"test_data_(\d+)\.txt$", os.path.basename(filename))
    if match_gen:
        return int(match_gen.group(1))
    return None


# --- Run Standard Jar and Save Answer Function --- (Keep existing function)
def run_standard_jar_and_save_answer(input_path, correct_test_set_index):
    answer_filename = f"answer_set{correct_test_set_index}.txt"
    answer_filepath = os.path.join(ANSWERS_FOLDER, answer_filename)
    standard_jar_exists = os.path.exists(STANDARD_JAR_PATH)
    elapsed_time = None

    if not standard_jar_exists:
        print(f"Error: Standard JAR not found at {STANDARD_JAR_PATH}")
        return correct_test_set_index, False, None

    start_time = time.time()
    process = None
    try:
        with open(input_path, 'r', encoding='utf-8', errors='replace') as infile:
            input_data = infile.read()

        process = subprocess.run(
            ['java', '-jar', STANDARD_JAR_PATH],
            input=input_data,
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace',
            timeout=JAR_TIMEOUT * 2
        )
        elapsed_time = time.time() - start_time

        if process.returncode != 0 or (process.stderr and not process.stderr.isspace()):
            print(f"Error running standard JAR for input {os.path.basename(input_path)}:")
            print(f"  Return Code: {process.returncode}")
            print(f"  Stderr:\n{process.stderr or 'None'}")
            # Still create an empty answer file to indicate failure? Optional.
            # open(answer_filepath, 'w').close()
            return correct_test_set_index, False, None

        os.makedirs(ANSWERS_FOLDER, exist_ok=True)
        with open(answer_filepath, 'w', encoding='utf-8', errors='replace') as f_ans:
            f_ans.write(process.stdout)
        return correct_test_set_index, True, elapsed_time

    except subprocess.TimeoutExpired:
        elapsed_time = time.time() - start_time
        print(f"Error: Standard JAR timed out for input {os.path.basename(input_path)}")
        # open(answer_filepath, 'w').close() # Indicate failure
        return correct_test_set_index, False, None
    except FileNotFoundError:
         print(f"Error: Cannot find input file {input_path} when running standard JAR.")
         # open(answer_filepath, 'w').close() # Indicate failure
         return correct_test_set_index, False, None
    except Exception as e:
        print(f"Error running standard JAR for {os.path.basename(input_path)}: {e}")
        # open(answer_filepath, 'w').close() # Indicate failure
        return correct_test_set_index, False, None

# --- Validator Function --- (Keep existing function)
def validate_output(input_path, output_path, correct_test_set_index):
    """
    Compares the pre-generated standard answer file with the actual output.
    Calculates approximate input line number for errors, accounting for 'ln'.
    """
    answer_filename = f"answer_set{correct_test_set_index}.txt"
    answer_filepath = os.path.join(ANSWERS_FOLDER, answer_filename)
    input_line_offset = 0 # Default offset if no 'ln'

    try:
        # --- Calculate Input Line Offset ---
        try:
            with open(input_path, 'r', encoding='utf-8', errors='replace') as f_in:
                first_line = f_in.readline().strip()
                if first_line.startswith("ln "):
                    try:
                        parts = first_line.split()
                        if len(parts) >= 2:
                            n_load = int(parts[1])
                            # ln block uses N + 3 lines total (incl. ln N).
                            # ln outputs "Ok" on output line 1.
                            # First command *after* ln block is at input line N + 4.
                            # Output line j (j >= 2) corresponds to input line (N+3) + (j-1) = N + j + 2.
                            # Offset to add to output line j (1-based) is N + 2.
                            input_line_offset = n_load + 2 # CORRECTED OFFSET
                            # print(f"Debug: Found ln {n_load}, offset calculated as {input_line_offset}") # Optional debug
                        else:
                             print(f"Warning: Malformed ln command found in {input_path}: '{first_line}'")
                    except ValueError:
                        print(f"Warning: Could not parse N from ln command in {input_path}: '{first_line}'")
                    except Exception as e_ln:
                         print(f"Warning: Error processing ln command in {input_path}: {e_ln}")
                # else: print("Debug: No ln command found, offset is 0") # Optional debug
        except FileNotFoundError:
             print(f"Warning: Input file {input_path} not found during offset calculation.")
        except Exception as e_offset:
             print(f"Warning: Error calculating input line offset for {input_path}: {e_offset}")

        # --- Read Expected Answer ---
        if not os.path.exists(answer_filepath):
            # If standard jar failed, we might not have an answer file. Report this differently?
            return False, f"VF Standard Error: Expected answer file missing ({answer_filename}). Standard JAR likely failed."
        with open(answer_filepath, 'r', encoding='utf-8', errors='replace') as f_ans:
            # Read all lines, including potentially empty ones if needed for exact comparison
            expected_output_lines = [line.rstrip('\n\r') for line in f_ans] # Keep structure
            # Filter empty lines *after* reading if desired, e.g.:
            # expected_output_lines = [line for line in expected_output_lines if line.strip()]

        # --- Read Actual Output ---
        if not os.path.exists(output_path):
            return False, "VF Error: Output file missing (likely RE/Timeout)"
        with open(output_path, 'r', encoding='utf-8', errors='replace') as f_out:
            actual_output_lines = [line.rstrip('\n\r') for line in f_out]
            # actual_output_lines = [line for line in actual_output_lines if line.strip()] # Filter if matching above

        # --- Compare ---
        len_expected = len(expected_output_lines)
        len_actual = len(actual_output_lines)

        if len_expected != len_actual:
            # Provide approx input line for length mismatch based on where it likely diverged
            approx_error_line_index = min(len_expected, len_actual) # 0-based index of first differing/missing line
            approx_input_line = input_line_offset + (approx_error_line_index + 1) # Convert to 1-based and add offset
            return False, f"VF Error: Mismatched output lines (Expected {len_expected}, Got {len_actual}). Input approx line {approx_input_line}"

        for i in range(len_expected):
            if expected_output_lines[i] != actual_output_lines[i]:
                output_error_line = i + 1 # 1-based line number
                approx_input_line = input_line_offset + output_error_line # Calculate approx input line using CORRECTED offset
                input_cmd_approx = f"Input approx line {approx_input_line}"
                # Use repr() to show hidden characters like trailing spaces
                return False, f"VF Error line {output_error_line}: Expected {repr(expected_output_lines[i])}, Got {repr(actual_output_lines[i])} ({input_cmd_approx})"

        return True, "Ok"

    except FileNotFoundError:
        return False, f"VF Critical Error: File missing during comparison ({answer_filepath} or {output_path})"
    except Exception as e:
        return False, f"VF Critical Error during comparison: {e}"

# --- Test Execution Function --- (Keep existing function)
def run_single_test(jar_name, input_path, base_output_dir, base_error_dir, correct_test_set_index):
    jar_path = os.path.join(JARS_DIR, jar_name)
    jar_name_no_ext = os.path.splitext(jar_name)[0]
    jar_output_folder = os.path.join(base_output_dir, jar_name_no_ext)
    output_filename = f"output_set{correct_test_set_index}.txt"
    output_path = os.path.join(jar_output_folder, output_filename)
    start_time = time.time()
    process = None
    stdout_content = ""
    stderr_content = ""
    result_type = "Tester Error"
    message = "Initialization Error"
    elapsed_time = 0
    try:
        try:
            with open(input_path, 'r', encoding='utf-8', errors='replace') as infile:
                input_data = infile.read()
        except Exception as e:
            # Raise a more specific error or return a tester error immediately
            result_type = "Tester Error"
            message = f"Input Read Error: {e}"
            elapsed_time = time.time() - start_time
            # Try to save what we can
            save_error_case(jar_name, input_path, None, str(e), message, base_error_dir, correct_test_set_index)
            return jar_name, correct_test_set_index, result_type, elapsed_time, message

        process = subprocess.run(
            ['java', '-jar', jar_path], input=input_data, capture_output=True,
            text=True, encoding='utf-8', errors='replace', timeout=JAR_TIMEOUT
        )
        end_time = time.time()
        elapsed_time = end_time - start_time
        stdout_content = process.stdout
        stderr_content = process.stderr

        # Create output folder before writing
        os.makedirs(jar_output_folder, exist_ok=True)
        # Write output even if there are errors, might contain partial info
        try:
            with open(output_path, 'w', encoding='utf-8', errors='replace') as outfile:
                outfile.write(stdout_content)
        except Exception as write_e:
            print(f"Warning: Failed to write output for {jar_name}, set {correct_test_set_index}: {write_e}")
            # Ensure output_path doesn't falsely exist if write failed
            if os.path.exists(output_path): os.remove(output_path)


        # --- Determine Result Type ---
        if process.returncode != 0:
            result_type = "Runtime Error"
            message = f"RE: Exit Code {process.returncode}"
            save_error_case(jar_name, input_path, output_path, stderr_content, message, base_error_dir, correct_test_set_index)
        # Check stderr more carefully, ignore common Java VM messages
        elif stderr_content and not stderr_content.isspace() and not re.match(r"Picked up _JAVA_OPTIONS:", stderr_content.strip(), re.IGNORECASE):
            result_type = "Runtime Error"
            message = f"RE: Non-empty stderr (check stderr.txt)"
            save_error_case(jar_name, input_path, output_path, stderr_content, message, base_error_dir, correct_test_set_index)
        else:
            # Only validate if JAR ran without explicit RE
            is_valid, validation_message = validate_output(input_path, output_path, correct_test_set_index)
            result_type = "Pass" if is_valid else "Validation Failed"
            message = validation_message # Use detailed message from validator
            if not is_valid:
                # Pass stderr even if it was empty/ignored previously, might have context
                save_error_case(jar_name, input_path, output_path, stderr_content, message, base_error_dir, correct_test_set_index)

    except subprocess.TimeoutExpired:
        end_time = time.time()
        elapsed_time = end_time - start_time
        result_type = "Timeout"
        message = f"Timeout after {JAR_TIMEOUT}s"
        os.makedirs(jar_output_folder, exist_ok=True)
        # Capture partial output/error if available
        captured_stdout = process.stdout if process else ""
        captured_stderr = process.stderr if process else ""
        # Try writing partial output
        try:
            with open(output_path, 'w', encoding='utf-8', errors='replace') as outfile:
                outfile.write(captured_stdout)
        except Exception as write_e:
             print(f"Warning: Failed to write partial output on Timeout for {jar_name}, set {correct_test_set_index}: {write_e}")
        save_error_case(jar_name, input_path, output_path, captured_stderr, message, base_error_dir, correct_test_set_index)

    except Exception as e:
        # Catch other potential errors during subprocess handling
        end_time = time.time()
        elapsed_time = time.time() - start_time # Use start_time here
        result_type = "Tester Error"
        message = f"Tester Error: {e}"
        # Ensure output folder exists
        os.makedirs(jar_output_folder, exist_ok=True)
        # Create an empty output file marker if it doesn't exist
        if not os.path.exists(output_path):
            try: open(output_path, 'w').close()
            except Exception: pass
        save_error_case(jar_name, input_path, output_path, str(e), message, base_error_dir, correct_test_set_index)

    return jar_name, correct_test_set_index, result_type, elapsed_time, message

# --- Data Generation Task --- (Keep existing function)
def generate_data_task(test_index, test_mode, num_instr_per_test, data_folder):
    data_filename = os.path.join(data_folder, f"test_data_{test_index}.txt")
    generator_cmd = ["python", DATA_GENERATOR_SCRIPT, "-m", test_mode, "-n", str(num_instr_per_test), "-o", data_filename]
    try:
        # Use a timeout for the generator as well? Optional.
        gen_proc = subprocess.run(
            generator_cmd, capture_output=True, text=True, check=True,
            encoding='utf-8', errors='replace', timeout=JAR_TIMEOUT * 3 # Generous timeout for generator
        )
        # NEW: Check if the output file actually exists after successful run
        if os.path.exists(data_filename) and os.path.getsize(data_filename) > 0:
             return test_index, data_filename, "Success", ""
        else:
             error_message = f"Gen Failed set {test_index}: Generator finished but output file is missing or empty."
             # print(error_message)
             # Clean up empty file if it exists
             if os.path.exists(data_filename):
                 try: os.remove(data_filename)
                 except OSError: pass
             return test_index, data_filename, "Gen Failed", error_message

    except subprocess.TimeoutExpired:
        error_message = f"Gen Failed set {test_index}: Generator timed out."
        # print(error_message)
        if os.path.exists(data_filename): # Clean up potentially partial file
             try: os.remove(data_filename)
             except OSError: pass
        return test_index, data_filename, "Gen Failed", error_message
    except subprocess.CalledProcessError as e:
        error_message = f"Gen Failed set {test_index}: Code {e.returncode}\nStderr:\n{e.stderr}\nStdout:\n{e.stdout}"
        # print(error_message)
        if os.path.exists(data_filename): # Clean up potentially partial file
             try: os.remove(data_filename)
             except OSError: pass
        return test_index, data_filename, "Gen Failed", error_message
    except Exception as e:
        error_message = f"Gen Failed set {test_index}: Error {e}"
        # print(error_message)
        if os.path.exists(data_filename): # Clean up potentially partial file
             try: os.remove(data_filename)
             except OSError: pass
        return test_index, data_filename, "Gen Failed", error_message


# --- Error Saving Function --- (Keep existing function)
def save_error_case(jar_name, input_path, output_path, stderr_content, reason, base_error_dir, correct_test_set_index):
    # (Function unchanged, logic is robust)
    jar_name_no_ext = os.path.splitext(jar_name)[0]
    jar_error_dir = os.path.join(base_error_dir, jar_name_no_ext)
    test_case_name = f"set_{correct_test_set_index}"
    error_case_dir = os.path.join(jar_error_dir, test_case_name)
    os.makedirs(error_case_dir, exist_ok=True)
    try:
        # Input
        if input_path and os.path.exists(input_path):
            shutil.copy2(input_path, os.path.join(error_case_dir, "input.txt"))
        elif input_path:
            with open(os.path.join(error_case_dir, "input_NOT_FOUND.txt"), 'w') as f: f.write(f"Input {input_path} not found.")
        else:
             with open(os.path.join(error_case_dir, "input_PATH_NONE.txt"), 'w') as f: f.write("Input path was None.")
        # Output (handle potentially missing output_path on RE/Timeout before write)
        if output_path and os.path.exists(output_path):
             shutil.copy2(output_path, os.path.join(error_case_dir, "output.txt"))
        elif output_path: # Path provided but file doesn't exist (likely write failure or early exit)
             with open(os.path.join(error_case_dir, "output_MISSING_OR_WRITE_FAILED.txt"), 'w') as f: f.write("Output file missing or write failed.")
        else: # output_path itself was None (e.g., input read error)
             with open(os.path.join(error_case_dir, "output_PATH_NONE.txt"), 'w') as f: f.write("Output path was None.")
        # Stderr
        with open(os.path.join(error_case_dir, "stderr.txt"), 'w', encoding='utf-8', errors='replace') as f: f.write(stderr_content or "")
        # Reason
        with open(os.path.join(error_case_dir, "reason.txt"), 'w', encoding='utf-8', errors='replace') as f: f.write(reason)
        # Expected Answer
        answer_filename = f"answer_set{correct_test_set_index}.txt"
        answer_filepath = os.path.join(ANSWERS_FOLDER, answer_filename)
        if os.path.exists(answer_filepath):
            shutil.copy2(answer_filepath, os.path.join(error_case_dir, "expected_answer.txt"))
        else:
            with open(os.path.join(error_case_dir, "expected_answer_NOT_FOUND.txt"), 'w') as f: f.write(f"Expected answer file {answer_filepath} not found or standard jar failed.")
    except Exception as e:
        print(f"Warning: Failed to save error case {jar_name} - {test_case_name}: {e}")


# --- Main Function (MODIFIED FOR GENERATION RETRY) ---
def main():
    # --- Setup ---
    jar_files = get_jar_files()
    setup_directories(jar_files)

    # --- Get Parameters ---
    use_local = input(f"Use local data from '{os.path.basename(STRONG_DATA_FOLDER)}' folder (Y/N) ? ").strip().upper()
    num_tests_requested = 0 # MODIFIED: Renamed from num_tests
    test_mode = 'P'
    run_mode = ""
    num_logical_instr_per_test = 0
    successfully_generated_files = {} # MODIFIED: Will be populated carefully

    if use_local == 'Y':
        if not os.path.isdir(STRONG_DATA_FOLDER):
            print(f"Error: Local data folder not found: {STRONG_DATA_FOLDER}")
            print("Please create the 'strong' folder and place test files inside.")
            return

        strong_files_paths = []
        try:
            strong_files_paths = [os.path.join(STRONG_DATA_FOLDER, f) for f in os.listdir(STRONG_DATA_FOLDER)
                            if os.path.isfile(os.path.join(STRONG_DATA_FOLDER, f))]
            # NEW: Try to extract index from filename for consistent numbering
            temp_local_files = {}
            processed_indices = set()
            for file_path in strong_files_paths:
                extracted_index = extract_index_from_filename(file_path)
                if extracted_index:
                    if extracted_index not in processed_indices:
                        temp_local_files[extracted_index] = file_path
                        processed_indices.add(extracted_index)
                    else:
                        print(f"Warning: Duplicate index {extracted_index} found in local files. Skipping {os.path.basename(file_path)}.")
                else:
                    # Assign next available index if pattern doesn't match
                    next_idx = 1
                    while next_idx in temp_local_files or next_idx in processed_indices:
                        next_idx += 1
                    print(f"Warning: Could not extract index from '{os.path.basename(file_path)}'. Assigning index {next_idx}.")
                    temp_local_files[next_idx] = file_path
                    processed_indices.add(next_idx)
            successfully_generated_files = dict(sorted(temp_local_files.items())) # Store sorted by index

        except Exception as e:
            print(f"Error reading strong data folder {STRONG_DATA_FOLDER}: {e}")
            return

        if not successfully_generated_files:
            print(f"Error: No files found or processed in the strong data folder: {STRONG_DATA_FOLDER}")
            return

        num_tests_requested = len(successfully_generated_files) # MODIFIED: Use count of processed files
        print(f"Using {num_tests_requested} local data file(s) from: {STRONG_DATA_FOLDER}")
        run_mode = f"Local Folder ({os.path.basename(STRONG_DATA_FOLDER)})"

    else: # Generation logic
        while True:
            mode_input=input("Testing mode (M/P)? ").strip().upper()
            if mode_input in ['M','P']: test_mode=mode_input; break
            else: print("Invalid.")
        while True:
            try:
                num_tests_requested=int(input("Number of data sets TO GENERATE? ")) # MODIFIED: Clarified input
                if num_tests_requested > 0: break
                else: print("Positive needed.")
            except ValueError: print("Invalid.")
        while True:
            try:
                 limit = PUBLIC_MAX_INSTRUCTIONS if test_mode == 'P' else MUTUAL_MAX_INSTRUCTIONS
                 num_logical_instr_per_test=int(input(f"LOGICAL instructions per set (max {limit})? "))
                 if 0 < num_logical_instr_per_test <= limit: break
                 else: print(f"Enter positive up to {limit}.")
            except ValueError: print("Invalid.")
        run_mode = f"Generate (Mode: {test_mode}, Instr: {num_logical_instr_per_test})"

    print(f"\nStarting tests for {len(jar_files)} JAR(s). Run Mode: {run_mode}")
    max_workers = os.cpu_count() or 1 # Ensure at least 1 worker
    print(f"Using up to {max_workers} worker threads.")
    results = defaultdict(lambda: defaultdict(list))
    results_lock = threading.Lock()
    start_run_time = time.time()
    total_jar_tests_submitted = 0
    completed_jar_tests = 0
    progress_lock = threading.Lock()
    standard_jar_times = []

    # --- Execution Flow ---
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        generation_futures = []
        answer_futures = []
        test_futures = []
        indices_with_valid_answers = set()

        # --- Phase 1: Data Generation (WITH RETRY LOGIC) ---
        if run_mode.startswith("Generate"):
            print(f"\nPhase 1: Generating {num_tests_requested} data sets with retries...")
            indices_to_generate = set(range(1, num_tests_requested + 1))
            generation_attempts = defaultdict(int)
            permanently_failed_indices = set()

            while len(successfully_generated_files) < num_tests_requested and \
                  len(successfully_generated_files) + len(permanently_failed_indices) < num_tests_requested:

                indices_needing_retry_this_batch = []
                for index in indices_to_generate:
                    if index not in successfully_generated_files and index not in permanently_failed_indices:
                        if generation_attempts[index] < MAX_GEN_RETRIES_PER_INDEX:
                            indices_needing_retry_this_batch.append(index)
                        else:
                            if index not in permanently_failed_indices:
                                # print(f"Data Gen: Index {index} exceeded max retries ({MAX_GEN_RETRIES_PER_INDEX}). Marking as failed.")
                                permanently_failed_indices.add(index)

                if not indices_needing_retry_this_batch:
                    # This condition means all remaining indices have hit the retry limit
                    # print("Data Gen: No more indices eligible for retry.")
                    break # Exit the while loop

                # print(f"Data Gen: Attempting generation for indices: {sorted(indices_needing_retry_this_batch)} "
                      # f"(Success: {len(successfully_generated_files)}, Failed: {len(permanently_failed_indices)})")

                current_batch_futures = []
                for index in indices_needing_retry_this_batch:
                     # Increment attempt counter *before* submitting
                     generation_attempts[index] += 1
                     attempt_num = generation_attempts[index]
                     # print(f"  - Submitting index {index} (Attempt {attempt_num}/{MAX_GEN_RETRIES_PER_INDEX})")
                     current_batch_futures.append(
                         executor.submit(generate_data_task, index, test_mode, num_logical_instr_per_test, DATA_FOLDER)
                     )

                for future in concurrent.futures.as_completed(current_batch_futures):
                    try:
                        test_index, data_filename, status, message = future.result()
                        if status == "Success":
                            if test_index not in successfully_generated_files: # Avoid duplicates if somehow submitted twice
                                # print(f"Data Gen: SUCCESS for index {test_index} (path: {data_filename})")
                                successfully_generated_files[test_index] = data_filename
                        else: pass
                             # Gen Failed
                             # print(f"Data Gen: FAILED for index {test_index} (Attempt {generation_attempts[test_index]}/{MAX_GEN_RETRIES_PER_INDEX}). Reason: {message.splitlines()[0]}") # Show first line of error
                             # No action needed here, it will be retried if attempts remain or marked failed later
                    except Exception as e:
                        # Log error processing the future, but don't know which index it was for easily
                        print(f"Error retrieving generation result (unknown index): {e}")

                # Optional delay between batches
                if len(successfully_generated_files) < num_tests_requested and \
                   len(successfully_generated_files) + len(permanently_failed_indices) < num_tests_requested:
                    # print(f"Data Gen: Completed batch. Waiting {RETRY_DELAY_SECONDS}s before next attempt...")
                    time.sleep(RETRY_DELAY_SECONDS)


            # --- Report final generation status ---
            final_success_count = len(successfully_generated_files)
            print(f"\nPhase 1 Complete: {final_success_count}/{num_tests_requested} data sets generated successfully.")
            if permanently_failed_indices:
                print(f"Warning: Failed to generate data for indices after {MAX_GEN_RETRIES_PER_INDEX} retries: {sorted(list(permanently_failed_indices))}")
            if not successfully_generated_files:
                print("No data generated successfully. Aborting testing.")
                return
            # Sort the successful files by index for consistent processing order later
            successfully_generated_files = dict(sorted(successfully_generated_files.items()))

        # --- Phase 1.5: Answer Generation (Uses successfully_generated_files) ---
        if not successfully_generated_files:
             print("No data files available (either local or generated/retried). Aborting.")
             return

        # MODIFIED: Use the count of *actually* successful generations
        num_successful_sets = len(successfully_generated_files)
        print(f"\nPhase 1.5: Generating expected answers for {num_successful_sets} successfully generated data set(s) using standard.jar...")
        # The loop now iterates over the potentially smaller set of successful files
        for test_index, data_path in successfully_generated_files.items():
            answer_futures.append(executor.submit(run_standard_jar_and_save_answer, data_path, test_index))

        ans_success_count = 0
        for future in concurrent.futures.as_completed(answer_futures):
            try:
                ans_index, ans_ok, standard_time_for_this_run = future.result()
                if ans_ok:
                    indices_with_valid_answers.add(ans_index)
                    ans_success_count += 1
                    if standard_time_for_this_run is not None:
                         standard_jar_times.append(standard_time_for_this_run)
                # else: Failure message printed by task
            except Exception as e:
                print(f"Error retrieving standard answer generation result for index (unknown): {e}")
        # MODIFIED: Report based on successful generations
        print(f"\nPhase 1.5 Complete: {ans_success_count}/{num_successful_sets} standard answer sets generated successfully.")

        # --- Phase 2: Testing (Uses indices_with_valid_answers) ---
        if not indices_with_valid_answers:
            print("No valid standard answers generated. Cannot proceed with testing.")
        else:
            # MODIFIED: Report based on valid answers count
            num_sets_to_test = len(indices_with_valid_answers)
            print(f"\nPhase 2: Submitting tests for {num_sets_to_test} data set(s) with valid answers...")
            total_jar_tests_submitted = 0
            for test_index in sorted(list(indices_with_valid_answers)):
                if test_index not in successfully_generated_files:
                    # This might happen if standard.jar succeeded but the original file was somehow lost (unlikely with current flow)
                    print(f"Warning: Index {test_index} has valid answer but no corresponding data file found in successful list. Skipping.")
                    continue
                data_file_path = successfully_generated_files[test_index]
                for jar_name in jar_files:
                    test_futures.append(executor.submit(run_single_test, jar_name, data_file_path, OUTPUT_FOLDER, ERROR_FOLDER, test_index))
                    total_jar_tests_submitted += 1

            print(f"Submitted {total_jar_tests_submitted} total JAR tests.")

            # --- Process Test Results (Logic Unchanged) ---
            for future in concurrent.futures.as_completed(test_futures):
                try:
                    jar_name, test_set_idx_res, result_type, elapsed_time, message = future.result()
                    with progress_lock:
                        completed_jar_tests += 1
                        progress = f"{completed_jar_tests}/{total_jar_tests_submitted}" if total_jar_tests_submitted > 0 else "N/A"
                        result_icon_map = {"Pass": "✅", "Validation Failed": "❌", "Runtime Error": "💥", "Timeout": "💥", "Tester Error": "❓"}
                        result_icon = result_icon_map.get(result_type, "❓")
                    result_key_map = {"Pass": "pass", "Validation Failed": "vf", "Runtime Error": "re", "Timeout": "timeout", "Tester Error": "tester_error"}
                    result_key = result_key_map.get(result_type, "tester_error")
                    with results_lock:
                        results[jar_name][result_key].append(1)
                        results[jar_name]["times"].append(elapsed_time)
                        if result_type == "Timeout":
                             # Correctly count Timeout under 're' bucket as well
                             if "re" not in results[jar_name]: results[jar_name]["re"] = []
                             results[jar_name]["re"].append(1) # Increment RE/Timeout count
                             # Store timeout specific count if needed separately (optional)
                             # if "timeout" not in results[jar_name]: results[jar_name]["timeout"] = []
                             # results[jar_name]["timeout"].append(1)

                    print(f"[{progress}] JAR: {jar_name:<20} | Set: {test_set_idx_res:<5} | Result: {result_icon:<2} | Time: {elapsed_time:.3f}s | Info: {message}")
                except Exception as e:
                    # Handle potential errors retrieving results from futures
                    print(f"Error processing test future result: {e}")
                    with results_lock: # Count as a tester error
                        # Need a way to associate this error with a JAR if possible, otherwise log generally
                         print("Tester Error: Could not retrieve result from a test future.")
                         # Or try to infer JAR if only one is running? Difficult.


    # --- Final Summary (MODIFIED: Use actual tested count) ---
    end_run_time = time.time()
    total_run_duration = end_run_time - start_run_time
    print("\n--- Testing Summary ---")
    print(f"Total time: {total_run_duration:.2f} seconds")
    # MODIFIED: Base summary on the number of sets for which valid answers were generated
    data_tested_count = len(indices_with_valid_answers)
    print(f"Tested {len(jar_files)} JAR(s) against {data_tested_count} data set(s) with valid standard answers.") # Clarified meaning

    if standard_jar_times:
        avg_standard_time = sum(standard_jar_times) / len(standard_jar_times)
        print(f"Standard JAR Average Time: {avg_standard_time:.3f}s")
    else:
        print("Standard JAR Average Time: N/A (No successful runs or standard.jar missing/failed)")

    for jar_name in jar_files:
        stats = results[jar_name]
        passed = len(stats.get("pass", []))
        vf = len(stats.get("vf", []))
        re_timeout_count = len(stats.get("re", [])) # Includes timeouts
        tester_errors = len(stats.get("tester_error", []))
        # MODIFIED: Calculate total results received for this jar specifically
        total_results_received_for_jar = passed + vf + re_timeout_count + tester_errors

        times = stats.get("times", [])
        avg_time = sum(times) / len(times) if times else 0
        # MODIFIED: Calculate accuracy based on the number of tests actually run for this jar against valid data
        accuracy = (passed / data_tested_count * 100) if data_tested_count > 0 else 0

        print(f"\nJAR: {jar_name}")
        # MODIFIED: Compare results received with expected number of tests (data_tested_count)
        if total_results_received_for_jar != data_tested_count and data_tested_count > 0 :
             print(f"  Warn: Results received ({total_results_received_for_jar}) != Data sets attempted ({data_tested_count}). Check for errors during processing.")

        summary_line = f"  Ran: {total_results_received_for_jar} sets | ✅ Pass: {passed} ({accuracy:.1f}%) | ❌ VF: {vf} | 💥 RE/Timeout: {re_timeout_count}"
        if tester_errors > 0:
            summary_line += f" | ❓ Tester Errors: {tester_errors}"
        print(summary_line)
        print(f"  Average Time per Test: {avg_time:.3f}s")
        if vf > 0 or re_timeout_count > 0 or tester_errors > 0:
            jar_error_path = os.path.join(ERROR_FOLDER, os.path.splitext(jar_name)[0])
            print(f"  Check errors in: {jar_error_path}")
            print(f"  Compare with expected answers in: {ANSWERS_FOLDER}")

if __name__ == "__main__":
    main()