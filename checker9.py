import os
import subprocess
import time
import shutil
import argparse
import concurrent.futures
import threading
import re
from collections import defaultdict
# Removed: deque, copy (no longer needed after removing simulation)

# --- Configuration ---
CODE_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = r"E:\PyCharmPjs\对拍"
DATA_FOLDER = os.path.join(BASE_DIR, "data")
OUTPUT_FOLDER = os.path.join(BASE_DIR, "output")
ANSWERS_FOLDER = os.path.join(BASE_DIR, "answers")
JARS_DIR = os.path.join(BASE_DIR, "jars")
ERROR_FOLDER = os.path.join(BASE_DIR, "errors")
DATA_GENERATOR_SCRIPT = os.path.join(CODE_DIR, "generator9.py")
CUSTOM_DATA_PATH = os.path.join(BASE_DIR, "MyData.txt")
STANDARD_JAR_PATH = os.path.join(BASE_DIR, "standard.jar") # <<< 标准答案 Jar 路径
JAR_TIMEOUT = 10
PUBLIC_MAX_INSTRUCTIONS = 10000
MUTUAL_MAX_INSTRUCTIONS = 3000

# --- CHECK MODE (REMOVED - Forced to COMPARE) ---
# CHECKMODE = 'COMPARE' # <<< REMOVED


# --- Utility Functions ---
def setup_directories(jar_files):
    os.makedirs(DATA_FOLDER, exist_ok=True)
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    os.makedirs(ANSWERS_FOLDER, exist_ok=True)
    os.makedirs(JARS_DIR, exist_ok=True)
    os.makedirs(ERROR_FOLDER, exist_ok=True)
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
    match = re.search(r"test_data_(\d+)\.txt$", os.path.basename(filename))
    return int(match.group(1)) if match else None


# --- Exception Counter (REMOVED - Not needed for compare mode) ---
# class ExceptionCounter: ... (Removed)


# --- Simulation Classes (REMOVED - Not needed for compare mode) ---
# class SimTag: ... (Removed)
# class SimPerson: ... (Removed)
# class SimNetwork: ... (Removed)


# --- Simulate and Save Answer Function (REMOVED) ---
# def simulate_and_save_answer(...): ... (Removed)


# --- Run Standard Jar and Save Answer Function (Unchanged, but now the only answer gen method) ---
def run_standard_jar_and_save_answer(input_path, correct_test_set_index):
    """
    Runs the standard.jar with input_path and saves its stdout to the ANSWERS_FOLDER.
    Returns (index, True, elapsed_time) on success, (index, False, None) on failure.
    """
    answer_filename = f"answer_set{correct_test_set_index}.txt"
    answer_filepath = os.path.join(ANSWERS_FOLDER, answer_filename)
    standard_jar_exists = os.path.exists(STANDARD_JAR_PATH)
    elapsed_time = None # Initialize elapsed time

    if not standard_jar_exists:
        print(f"Error: Standard JAR not found at {STANDARD_JAR_PATH}")
        return correct_test_set_index, False, None # Cannot generate answer

    start_time = time.time()
    process = None
    try:
        with open(input_path, 'r', encoding='utf-8') as infile:
            input_data = infile.read()

        process = subprocess.run(
            ['java', '-jar', STANDARD_JAR_PATH],
            input=input_data,
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace',
            timeout=JAR_TIMEOUT * 2 # Give standard JAR more time? Optional.
        )
        elapsed_time = time.time() - start_time # Calculate time regardless of outcome initially

        if process.returncode != 0 or (process.stderr and not process.stderr.isspace()):
            print(f"Error running standard JAR for input {os.path.basename(input_path)}:")
            print(f"  Return Code: {process.returncode}")
            print(f"  Stderr:\n{process.stderr or 'None'}")
            return correct_test_set_index, False, None # Answer generation failed

        # Save successful output as answer
        os.makedirs(ANSWERS_FOLDER, exist_ok=True)
        with open(answer_filepath, 'w', encoding='utf-8', errors='replace') as f_ans:
            f_ans.write(process.stdout)
        # print(f"Generated answer {answer_filename} using standard.jar (Time: {elapsed_time:.2f}s)") # Optional verbose
        return correct_test_set_index, True, elapsed_time # Return time on success

    except subprocess.TimeoutExpired:
        elapsed_time = time.time() - start_time # Record time even on timeout if needed
        print(f"Error: Standard JAR timed out for input {os.path.basename(input_path)}")
        return correct_test_set_index, False, None # Return None for time on failure
    except FileNotFoundError:
         print(f"Error: Cannot find input file {input_path} when running standard JAR.")
         return correct_test_set_index, False, None
    except Exception as e:
        print(f"Error running standard JAR for {os.path.basename(input_path)}: {e}")
        return correct_test_set_index, False, None


# --- Validator Function (SIMPLIFIED - Reads Answer File) ---
def validate_output(input_path, output_path, correct_test_set_index):
    """Compares the pre-generated standard answer file with the actual output."""
    answer_filename = f"answer_set{correct_test_set_index}.txt"
    answer_filepath = os.path.join(ANSWERS_FOLDER, answer_filename)

    try:
        # Read Expected Answer
        if not os.path.exists(answer_filepath):
            # If answer file doesn't exist, it means standard jar failed earlier
            return False, f"VF Standard Error: Expected answer file missing ({answer_filename})"
        with open(answer_filepath, 'r', encoding='utf-8') as f_ans:
            expected_output_lines = [line.strip() for line in f_ans if line.strip()]
            # REMOVED: Check for SIMULATION_ERROR in answer file

        # Read Actual Output
        if not os.path.exists(output_path):
            # Output missing, but answer file exists. Likely RE/Timeout.
            return False, "VF Error: Output file missing (likely RE/Timeout)"
        with open(output_path, 'r', encoding='utf-8') as f_out:
            actual_output_lines = [line.strip() for line in f_out if line.strip()]

        # Compare line counts
        if len(expected_output_lines) != len(actual_output_lines):
            return False, f"VF Error: Mismatched output lines (Expected {len(expected_output_lines)}, Got {len(actual_output_lines)})"

        # Compare line by line
        for i in range(len(expected_output_lines)):
            if expected_output_lines[i] != actual_output_lines[i]:
                input_cmd_approx = f"Input approx line {i + 1}" # Still useful context
                return False, f"VF Error line {i + 1}: Expected '{expected_output_lines[i]}', Got '{actual_output_lines[i]}' ({input_cmd_approx})"

        return True, "Ok"  # All comparisons passed

    except FileNotFoundError:
        # This might happen if output_path disappears between check and open
        return False, f"VF Critical Error: File missing during comparison ({answer_filepath} or {output_path})"
    except Exception as e:
        return False, f"VF Critical Error during comparison: {e}"


# --- Test Execution Function ---
def run_single_test(jar_name, input_path, base_output_dir, base_error_dir, correct_test_set_index):
    # subprocess run logic remains the same
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
            with open(input_path, 'r', encoding='utf-8') as infile:
                input_data = infile.read()
        except Exception as e:
            raise Exception(f"Error reading input {input_path}: {e}")
        process = subprocess.run(
            ['java', '-jar', jar_path], input=input_data, capture_output=True,
            text=True, encoding='utf-8', errors='replace', timeout=JAR_TIMEOUT
        )
        end_time = time.time()
        elapsed_time = end_time - start_time
        stdout_content = process.stdout
        stderr_content = process.stderr
        os.makedirs(jar_output_folder, exist_ok=True)
        with open(output_path, 'w', encoding='utf-8', errors='replace') as outfile:
            outfile.write(stdout_content)

        # Determine Result using simplified validator
        if process.returncode != 0:  # Runtime Error takes precedence
            result_type = "Runtime Error"
            message = f"RE: Exit Code {process.returncode}"
            save_error_case(jar_name, input_path, output_path, stderr_content, message, base_error_dir,
                            correct_test_set_index)
        elif stderr_content and not stderr_content.isspace() and "Picked up _JAVA_OPTIONS" not in stderr_content:
            result_type = "Runtime Error"
            message = f"RE: Non-empty stderr"
            save_error_case(jar_name, input_path, output_path, stderr_content, message, base_error_dir,
                            correct_test_set_index)
        else:  # JAR ran without apparent errors, now validate output against standard answer
            is_valid, validation_message = validate_output(input_path, output_path, correct_test_set_index)
            result_type = "Pass" if is_valid else "Validation Failed"
            message = validation_message  # Use detailed message from validator
            if not is_valid:
                save_error_case(jar_name, input_path, output_path, stderr_content, message, base_error_dir,
                                correct_test_set_index)

    except subprocess.TimeoutExpired:
        # Timeout handling unchanged
        end_time = time.time()
        elapsed_time = end_time - start_time
        result_type = "Timeout"
        message = f"RE: Timeout after {JAR_TIMEOUT} seconds"
        os.makedirs(jar_output_folder, exist_ok=True)
        captured_stdout = process.stdout if process else ""
        captured_stderr = process.stderr if process else ""
        try:
            with open(output_path, 'w', encoding='utf-8', errors='replace') as outfile:
                outfile.write(captured_stdout)
        except Exception:
            pass
        save_error_case(jar_name, input_path, output_path, captured_stderr, message, base_error_dir,
                        correct_test_set_index)
    except Exception as e:
        # Other exception handling unchanged
        end_time = time.time()
        elapsed_time = time.time() - start_time
        result_type = "Tester Error"
        message = f"Tester Error: {e}"
        os.makedirs(jar_output_folder, exist_ok=True)
        if not os.path.exists(output_path):
            try:
                open(output_path, 'w').close()
            except:
                pass
        save_error_case(jar_name, input_path, output_path, str(e), message, base_error_dir, correct_test_set_index)
    return jar_name, correct_test_set_index, result_type, elapsed_time, message


# --- Data Generation Task --- (Unchanged)
def generate_data_task(test_index, test_mode, num_instr_per_test, data_folder):
    data_filename = os.path.join(data_folder, f"test_data_{test_index}.txt")
    generator_cmd = ["python", DATA_GENERATOR_SCRIPT, "-m", test_mode, "-n", str(num_instr_per_test), "-o",
                     data_filename]
    try:
        gen_proc = subprocess.run(
            generator_cmd, capture_output=True, text=True, check=True, encoding='utf-8', errors='replace'
        )
        return test_index, data_filename, "Success", ""
    except subprocess.CalledProcessError as e:
        error_message = f"Gen Failed set {test_index}: Code {e.returncode}\nStderr:\n{e.stderr}\nStdout:\n{e.stdout}"
        print(error_message)
        return test_index, data_filename, "Gen Failed", error_message
    except Exception as e:
        error_message = f"Gen Failed set {test_index}: Error {e}"
        print(error_message)
        return test_index, data_filename, "Gen Failed", error_message


# --- Error Saving Function --- (Unchanged)
def save_error_case(jar_name, input_path, output_path, stderr_content, reason, base_error_dir, correct_test_set_index):
    jar_name_no_ext = os.path.splitext(jar_name)[0]
    jar_error_dir = os.path.join(base_error_dir, jar_name_no_ext)
    test_case_name = f"set_{correct_test_set_index}"
    error_case_dir = os.path.join(jar_error_dir, test_case_name)
    os.makedirs(error_case_dir, exist_ok=True)
    try:
        if input_path and os.path.exists(input_path):
            shutil.copy2(input_path, os.path.join(error_case_dir, "input.txt"))
        elif input_path:
            with open(os.path.join(error_case_dir, "input_NOT_FOUND.txt"), 'w') as f:
                f.write(f"Input {input_path} not found.")
        else:
            with open(os.path.join(error_case_dir, "input_PATH_NONE.txt"), 'w') as f:
                f.write("Input path was None.")
        if output_path and os.path.exists(output_path):
            shutil.copy2(output_path, os.path.join(error_case_dir, "output.txt"))
        elif output_path:
            with open(os.path.join(error_case_dir, "output_EMPTY_OR_MISSING.txt"), 'w') as f:
                f.write("Output file missing/empty or write failed.")
        else:
            with open(os.path.join(error_case_dir, "output_PATH_NONE.txt"), 'w') as f:
                f.write("Output path was None.")
        with open(os.path.join(error_case_dir, "stderr.txt"), 'w', encoding='utf-8', errors='replace') as f:
            f.write(stderr_content or "")
        with open(os.path.join(error_case_dir, "reason.txt"), 'w', encoding='utf-8') as f:
            f.write(reason)
        answer_filename = f"answer_set{correct_test_set_index}.txt"
        answer_filepath = os.path.join(ANSWERS_FOLDER, answer_filename)
        if os.path.exists(answer_filepath):
            shutil.copy2(answer_filepath, os.path.join(error_case_dir, "expected_answer.txt"))
        else:
            # Still try to indicate that the answer file wasn't found for this error case
            with open(os.path.join(error_case_dir, "expected_answer_NOT_FOUND.txt"), 'w') as f:
                f.write(f"Expected answer file {answer_filepath} not found or standard jar failed.")
    except Exception as e:
        print(f"Warning: Failed save error case {jar_name} - {test_case_name}: {e}")


# --- Main Function (MODIFIED) ---
def main():
    # --- Setup ---
    os.makedirs(DATA_FOLDER, exist_ok=True)
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    os.makedirs(ANSWERS_FOLDER, exist_ok=True)
    os.makedirs(JARS_DIR, exist_ok=True)
    os.makedirs(ERROR_FOLDER, exist_ok=True)
    jar_files = get_jar_files()
    setup_directories(jar_files)
    # REMOVED: print(f"--- Running in {CHECKMODE} mode ---") # No longer needed

    # --- Get Parameters ---
    # (Parameter gathering unchanged)
    use_local = input("Use local data file (Y/N) ? ").strip().upper()
    num_tests = 0
    test_mode = 'P'
    data_files_to_test = []
    run_mode = ""
    num_logical_instr_per_test = 0
    if use_local == 'Y':
        if not os.path.exists(CUSTOM_DATA_PATH):
            print(f"Error: Local file not found: {CUSTOM_DATA_PATH}")
            return
        data_files_to_test.append(CUSTOM_DATA_PATH)
        num_tests = 1
        print(f"Using local data file: {CUSTOM_DATA_PATH}")
        run_mode = "Local File"
    else:
        while True: # Mode input
            mode_input=input("Testing mode (M/P)? ").strip().upper()
            if mode_input in ['M','P']:
                test_mode=mode_input
                break
            else:
                print("Invalid.")
        while True: # Num tests input
            try:
                num_tests=int(input("Number of data sets? "))
                if num_tests>0:
                    break
                else:
                    print("Positive needed.")
            except ValueError:
                print("Invalid.")
        while True: # Num instructions input
            try:
                 limit = PUBLIC_MAX_INSTRUCTIONS if test_mode == 'P' else MUTUAL_MAX_INSTRUCTIONS
                 num_logical_instr_per_test=int(input(f"LOGICAL instructions per set (max {limit})? "))
                 if 0 < num_logical_instr_per_test <= limit:
                    break
                 else:
                    print(f"Enter positive up to {limit}.")
            except ValueError:
                print("Invalid.")
        run_mode = f"Generate (Mode: {test_mode}, Instr: {num_logical_instr_per_test})"

    print(f"\nStarting tests for {len(jar_files)} JAR(s). Run Mode: {run_mode}")
    max_workers = os.cpu_count() or 1
    print(f"Using up to {max_workers} worker threads.")
    results = defaultdict(lambda: defaultdict(list))
    results_lock = threading.Lock()
    start_run_time = time.time()
    total_jar_tests_submitted = 0
    completed_jar_tests = 0
    progress_lock = threading.Lock()
    standard_jar_times = [] # Stores standard jar times

    # --- Execution Flow ---
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        generation_futures = []
        answer_futures = []
        test_futures = []
        successfully_generated_files = {}
        indices_with_valid_answers = set() # Indices for which answer generation succeeded

        # --- Phase 1: Data Generation ---
        if run_mode.startswith("Generate"):
            # (Data generation logic unchanged)
            print(f"\nPhase 1: Generating {num_tests} data sets...")
            for i in range(1, num_tests + 1):
                 generation_futures.append(executor.submit(generate_data_task, i, test_mode, num_logical_instr_per_test, DATA_FOLDER))
            gen_success_count = 0
            temp_data_files = {}
            for future in concurrent.futures.as_completed(generation_futures):
                try:
                    test_index, data_filename, status, message = future.result()
                    if status == "Success":
                        temp_data_files[test_index] = data_filename
                        gen_success_count += 1
                except Exception as e:
                    print(f"Error retrieving gen result: {e}")
            successfully_generated_files = temp_data_files
            print(f"\nPhase 1 Complete: {gen_success_count}/{num_tests} data sets generated successfully.")
            if not successfully_generated_files:
                print("No data available. Aborting.")
                return
        elif run_mode == "Local File":
             local_idx = 1 # Assign an index for the local file
             if os.path.exists(CUSTOM_DATA_PATH):
                 successfully_generated_files[local_idx] = CUSTOM_DATA_PATH
             else:
                 print(f"Error: Cannot find local file {CUSTOM_DATA_PATH}")
                 return

        # --- Phase 1.5: Answer Generation (SIMPLIFIED) ---
        print(f"\nPhase 1.5: Generating expected answers for {len(successfully_generated_files)} data set(s) using standard.jar...")
        # REMOVED: check for CHECKMODE and answer_generation_function selection

        for test_index, data_path in successfully_generated_files.items():
            # Directly submit the standard jar runner
            answer_futures.append(executor.submit(run_standard_jar_and_save_answer, data_path, test_index))

        ans_success_count = 0
        for future in concurrent.futures.as_completed(answer_futures):
            try:
                # Always expect 3 values now (index, ok, time)
                ans_index, ans_ok, standard_time_for_this_run = future.result()

                if ans_ok:
                    indices_with_valid_answers.add(ans_index)
                    ans_success_count += 1
                    if standard_time_for_this_run is not None:
                         standard_jar_times.append(standard_time_for_this_run)
                # else: Failures already printed by the task function

            except Exception as e:
                print(f"Error retrieving standard answer generation result: {e}")
        print(f"\nPhase 1.5 Complete: {ans_success_count}/{len(successfully_generated_files)} standard answer sets generated successfully.")


        # --- Phase 2: Testing ---
        print(f"\nPhase 2: Submitting tests for {len(indices_with_valid_answers)} data set(s) with valid answers...")
        total_jar_tests_submitted = 0
        for test_index in sorted(list(indices_with_valid_answers)):
            if test_index not in successfully_generated_files:
                continue
            data_file_path = successfully_generated_files[test_index]
            for jar_name in jar_files:
                test_futures.append(executor.submit(run_single_test, jar_name, data_file_path, OUTPUT_FOLDER, ERROR_FOLDER, test_index))
                total_jar_tests_submitted += 1

        print(f"Submitted {total_jar_tests_submitted} total JAR tests.")

        # --- Process Test Results ---
        # (Result processing unchanged)
        for future in concurrent.futures.as_completed(test_futures):
            try:
                jar_name, test_set_idx_res, result_type, elapsed_time, message = future.result()
                with progress_lock:
                    completed_jar_tests += 1
                    progress = f"{completed_jar_tests}/{total_jar_tests_submitted}"
                    result_icon_map = {"Pass": "✅", "Validation Failed": "❌", "Runtime Error": "💥", "Timeout": "💥", "Tester Error": "❓"}
                    result_icon = result_icon_map.get(result_type, "❓")
                result_key_map = {"Pass": "pass", "Validation Failed": "vf", "Runtime Error": "re", "Timeout": "timeout", "Tester Error": "tester_error"}
                result_key = result_key_map.get(result_type, "tester_error")
                with results_lock:
                    results[jar_name][result_key].append(1)
                    results[jar_name]["times"].append(elapsed_time)
                    if result_type == "Timeout":
                        results[jar_name]["re"].append(1) # Count timeout as RE for summary
                print(f"[{progress}] JAR: {jar_name:<20} | Set: {test_set_idx_res:<5} | Result: {result_icon:<2} | Time: {elapsed_time:.3f}s | Info: {message}")
            except Exception as e:
                print(f"Error processing test future: {e}")


    # --- Final Summary (MODIFIED) ---
    end_run_time = time.time()
    total_run_duration = end_run_time - start_run_time
    print("\n--- Testing Summary ---")
    print(f"Total time: {total_run_duration:.2f} seconds")
    data_tested_count = len(indices_with_valid_answers)
    print(f"Tested {len(jar_files)} JAR(s) against {data_tested_count} data set(s) with valid standard answers.")

    # Print standard jar average time
    if standard_jar_times:
        avg_standard_time = sum(standard_jar_times) / len(standard_jar_times)
        print(f"Standard JAR Average Time: {avg_standard_time:.3f}s")
    else:
        print("Standard JAR Average Time: N/A (No successful runs or standard.jar missing/failed)")

    # (Rest of summary printing unchanged)
    for jar_name in jar_files:
        stats = results[jar_name]
        passed = len(stats["pass"])
        vf = len(stats["vf"])
        re_timeout_count = len(stats["re"]) # Includes timeouts now
        tester_errors = len(stats["tester_error"])
        total_run_for_jar = passed + vf + re_timeout_count + tester_errors
        times = stats["times"]
        avg_time = sum(times) / len(times) if times else 0
        accuracy = (passed / data_tested_count * 100) if data_tested_count > 0 else 0 # Accuracy based on successfully answered tests
        print(f"\nJAR: {jar_name}")
        # if total_run_for_jar != data_tested_count: print(f"  Warn: Results ({total_run_for_jar}) != Data sets tested ({data_tested_count}).")
        summary_line = f"  Ran: {total_run_for_jar} sets | ✅ Pass: {passed} ({accuracy:.1f}%) | ❌ VF: {vf} | 💥 RE/Timeout: {re_timeout_count}"
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