import os
import subprocess
from concurrent.futures import ThreadPoolExecutor
import json
import argparse
import logging
from tqdm import tqdm
import shutil
import acoustid
import collections
import time # For mtime

# --- Global Configuration ---
# BITRATE_THRESHOLD = 160000 ## Remove hardcoded threshold
FINGERPRINT_AUDIO_MAX_LENGTH_SECONDS = 0 # 0 means process the whole file
CACHE_FILENAME = ".musicscan_fp_cache.json"
UNSORTED_PATH_MARKER_SEGMENTS = ("Music", "Unsorted") # Segments to identify an "unsorted" path
MP3VAL_PATH = None
FFMPEG_PATH = None

# --- Logging Setup ---
# Placed here so it's configured before any logging calls, even from functions
# if script is imported (though it's primarily a CLI tool).
logging.basicConfig(filename='musicscan.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - [%(module)s.%(funcName)s] %(message)s', filemode='w')

# --- Mutagen/EasyID3 related functions ---
try:
    from mutagen.easyid3 import EasyID3
    from mutagen import File as MutagenFile
except ImportError:
    print("CRITICAL: Mutagen library not found. Please install it: pip install mutagen")
    logging.critical("Mutagen library not found. Renaming and some bitrate checks will fail or be inaccurate.")
    # Define dummy classes if you want the script to limp along, or exit.
    # For now, this will cause errors later if MutagenFile is used directly.
    # It's better to check explicitly before using Mutagen functionality if this is a soft dependency.
    # However, it's used in core parts like renaming and bitrate.
    # A more robust script might disable features if dependencies are missing.
    # For this script, let's assume Mutagen is a hard dependency.
    raise # Re-raise the ImportError to stop the script if Mutagen isn't there.


# --- Helper function to check for fpcalc ---
def check_fpcalc_executable():
    fpcalc_path = shutil.which("fpcalc")
    if not fpcalc_path:
        error_msg = "CRITICAL: The 'fpcalc' utility (part of Chromaprint) was not found in your system's PATH."
        recommend_msg = ("Please install Chromaprint (which includes fpcalc) from https://acoustid.org/chromaprint "
                         "or via your system's package manager (e.g., 'brew install chromaprint' on macOS, "
                         "'sudo apt-get install fpcalc' on Debian/Ubuntu).")
        print(f"\n{error_msg}\n{recommend_msg}\n")
        logging.critical(error_msg)
        logging.critical(recommend_msg)
        return False
    logging.info(f"fpcalc utility found at: {fpcalc_path}")
    return True

def check_mp3val_executable():
    """Checks if mp3val is installed and accessible, sets MP3VAL_PATH global."""
    global MP3VAL_PATH
    MP3VAL_PATH = shutil.which("mp3val")
    if not MP3VAL_PATH:
        # This message is informational; a more prominent warning will appear if --auto-repair-mp3 is used.
        logging.info("mp3val utility not found in PATH. Automatic MP3 repair will be disabled if attempted.")
        return False
    logging.info(f"mp3val utility found at: {MP3VAL_PATH}. Automatic MP3 repair is available if enabled via CLI.")
    return True

def check_ffmpeg_executable():
    """Checks if ffmpeg is installed and accessible, sets FFMPEG_PATH global."""
    global FFMPEG_PATH
    FFMPEG_PATH = shutil.which("ffmpeg")
    if not FFMPEG_PATH:
        logging.info("ffmpeg utility not found in PATH. FFmpeg-based repair will be disabled if attempted.")
        return False
    logging.info(f"ffmpeg utility found at: {FFMPEG_PATH}. FFmpeg-based repair is available if enabled via CLI.")
    return True

# --- Cache Functions ---
def load_fingerprint_cache(cache_file_path):
    if os.path.exists(cache_file_path):
        try:
            with open(cache_file_path, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            logging.info(f"Fingerprint cache loaded from {cache_file_path} with {len(cache_data)} entries.")
            return cache_data
        except (json.JSONDecodeError, IOError) as e:
            logging.warning(f"Could not load or parse fingerprint cache from {cache_file_path}: {e}. Starting with an empty cache.")
    else:
        logging.info(f"Fingerprint cache file not found at {cache_file_path}. A new cache will be created.")
    return {}

def save_fingerprint_cache(cache_file_path, cache_data):
    temp_cache_file_path = cache_file_path + ".tmp" # Write to a temporary file first
    try:
        with open(temp_cache_file_path, 'w', encoding='utf-8') as f:
            json.dump(cache_data, f, indent=4)
        # If writing to temp file is successful, atomically replace the old cache file
        os.replace(temp_cache_file_path, cache_file_path) 
        logging.info(f"Fingerprint cache with {len(cache_data)} entries saved to {cache_file_path}")
    except (IOError, json.JSONDecodeError) as e: # json.JSONDecodeError is for loading, but good to be broad for dump too
        logging.error(f"Could not save fingerprint cache to {cache_file_path}: {e}")
        if os.path.exists(temp_cache_file_path):
            try:
                os.remove(temp_cache_file_path) # Clean up temp file on error
            except OSError as e_rm:
                logging.error(f"Could not remove temporary cache file {temp_cache_file_path}: {e_rm}")
    except Exception as e: # Catch any other unexpected errors during save/replace
        logging.error(f"Unexpected error saving fingerprint cache: {e}")
        if os.path.exists(temp_cache_file_path):
             try:
                os.remove(temp_cache_file_path)
             except OSError as e_rm:
                logging.error(f"Could not remove temporary cache file {temp_cache_file_path}: {e_rm}")

def ensure_unique_quarantine_filename(quarantine_dir, original_filename):
    """
    Ensures the destination filename in the quarantine directory is unique.
    If 'filename.ext' exists, it tries 'filename (1).ext', 'filename (2).ext', etc.
    Returns the full unique destination path.
    """
    base, ext = os.path.splitext(original_filename)
    counter = 1
    # Sanitize base name slightly for the counter part, though os.path.join handles most things
    # This is a basic approach for problematic characters if base itself is very strange.
    # For simplicity, we assume 'base' is reasonable here.
    
    dest_filename = original_filename
    full_dest_path = os.path.join(quarantine_dir, dest_filename)
    
    while os.path.exists(full_dest_path):
        dest_filename = f"{base} ({counter}){ext}"
        full_dest_path = os.path.join(quarantine_dir, dest_filename)
        counter += 1
    return full_dest_path

def move_file_to_quarantine(source_filepath, quarantine_base_dir, dry_run=False):
    """
    Moves a file to the quarantine directory.
    Ensures the quarantine directory exists and handles potential filename collisions.
    Returns True if successful (or would be successful in dry_run), False otherwise.
    """
    if not os.path.exists(source_filepath):
        logging.warning(f"Source file for quarantine not found (already moved or deleted?): {source_filepath}")
        print(f"\tWARNING: Source file not found: {source_filepath}")
        return False

    original_filename = os.path.basename(source_filepath)
    action_prefix_moving = "DRY RUN: Would move" if dry_run else "Moving"
    
    try:
        if not dry_run:
            os.makedirs(quarantine_base_dir, exist_ok=True)
        else:
            # In dry_run, we don't create the directory, but ensure_unique_quarantine_filename will still check
            # against the live file system if the quarantine_base_dir happens to exist.
            logging.info(f"DRY RUN: Would ensure quarantine directory exists: {quarantine_base_dir}")
        
        unique_dest_path = ensure_unique_quarantine_filename(quarantine_base_dir, original_filename)
        
        # This print now occurs within the calling function for better context
        # print(f"\t{action_prefix_moving} '{source_filepath}' to '{unique_dest_path}'") 
        logging.info(f"{action_prefix_moving} '{source_filepath}' to '{unique_dest_path}'")
        
        if not dry_run:
            shutil.move(source_filepath, unique_dest_path)
        return True
        
    except Exception as e:
        error_msg = f"Error moving '{source_filepath}' to quarantine '{quarantine_base_dir}': {e}"
        print(f"\tERROR: {error_msg}")
        logging.error(error_msg)
        return False

def is_in_target_path_pattern(filepath, pattern_segments):
    """
    Checks if a filepath string contains a specific sequence of directory name segments.
    This check is case-insensitive and handles both / and \\ path separators.
    Example: pattern_segments = ("Music", "Unsorted") will match /path/to/Music/Unsorted/song.mp3
    """
    if not filepath or not pattern_segments:
        return False
    try:
        # Normalize the filepath: lower case and split into parts
        # os.path.normcase might be better for direct case-insensitivity of the whole path for comparison
        # but splitting and lowercasing parts is robust for segment matching.
        normalized_filepath_parts = [part.lower() for part in os.path.normpath(filepath).split(os.sep) if part] # Ensure no empty parts from multiple slashes

        # Normalize pattern segments
        normalized_pattern_segments = [part.lower() for part in pattern_segments]
        
        pattern_len = len(normalized_pattern_segments)
        if pattern_len == 0:
            return False

        for i in range(len(normalized_filepath_parts) - pattern_len + 1):
            if normalized_filepath_parts[i:i+pattern_len] == normalized_pattern_segments:
                return True
        return False
    except Exception as e:
        logging.warning(f"Error checking path pattern for '{filepath}' with pattern '{pattern_segments}': {e}")
        return False # Default to not matching if an error occurs during path processing

# --- Audio Fingerprinting Function ---
def get_audio_fingerprint(filepath, attempt_repair_mp3val_if_needed=False, attempt_repair_ffmpeg_if_needed=False):
    """
    Calculates the audio fingerprint and duration of a file.
    Optionally attempts to repair MP3s with mp3val, and then with FFmpeg if fingerprinting fails and repair is enabled.
    Manages temporary backups during repair attempts.
    """
    try:
        # Initial fingerprint attempt
        duration, fp_bytes = acoustid.fingerprint_file(filepath, maxlength=FINGERPRINT_AUDIO_MAX_LENGTH_SECONDS)
        logging.debug(f"Successfully fingerprinted (initial attempt): {os.path.basename(filepath)}, Duration: {duration:.2f}s")
        return duration, fp_bytes
    except acoustid.FingerprintGenerationError as e_initial_fp:
        logging.warning(f"Initial fingerprint generation failed for {filepath}: {e_initial_fp}")
        original_error_for_ffmpeg_check = str(e_initial_fp).lower() # Store initial error for ffmpeg condition

        # --- STAGE 1: mp3val Repair Attempt (if enabled and applicable) ---
        should_try_mp3val = (
            attempt_repair_mp3val_if_needed and
            MP3VAL_PATH and
            filepath.lower().endswith(".mp3") and
            ("fpcalc exited with status" in original_error_for_ffmpeg_check or \
             "error decoding" in original_error_for_ffmpeg_check or \
             "header missing" in original_error_for_ffmpeg_check)
        )

        if should_try_mp3val:
            logging.info(f"Attempting to repair {os.path.basename(filepath)} with mp3val...")
            print(f"INFO: Fingerprint failed for {os.path.basename(filepath)}, attempting repair with mp3val...")
            
            backup_filepath_mp3val = filepath + ".musicscan_mp3val_repair.bak"
            original_file_backed_up_mp3val = False
            mp3val_repair_led_to_fp = False

            try:
                shutil.copy2(filepath, backup_filepath_mp3val)
                original_file_backed_up_mp3val = True
                logging.info(f"Backup for mp3val of {filepath} created at {backup_filepath_mp3val}")

                mp3val_cmd = [MP3VAL_PATH, "-f", "-nb", "-si", filepath]
                process = subprocess.run(mp3val_cmd, capture_output=True, text=True, check=False, encoding='utf-8', errors='replace')
                
                # Log mp3val output
                log_mp3val_out = process.stdout.strip() if process.stdout else ""
                if process.stderr: log_mp3val_out += (f"\nStderr: {process.stderr.strip()}")
                logging.debug(f"mp3val output for {filepath} (RC: {process.returncode}):\n{log_mp3val_out}")

                if process.returncode == 0: # mp3val ran without crashing
                    if "FIXED" in (process.stdout.upper() if process.stdout else ""):
                        logging.info(f"mp3val reported fixing errors for {filepath}.")
                        print(f"INFO: mp3val reported fixing errors for {os.path.basename(filepath)}.")
                    else:
                        logging.info(f"mp3val processed {filepath}. Retrying fingerprint.")
                        print(f"INFO: mp3val processed {os.path.basename(filepath)}. Retrying fingerprint.")
                    
                    try: # Retry fingerprinting
                        duration_rep, fp_bytes_rep = acoustid.fingerprint_file(filepath, maxlength=FINGERPRINT_AUDIO_MAX_LENGTH_SECONDS)
                        logging.info(f"Successfully fingerprinted file after mp3val attempt: {filepath}")
                        print(f"INFO: Successfully fingerprinted file after mp3val attempt: {os.path.basename(filepath)}.")
                        mp3val_repair_led_to_fp = True
                        return duration_rep, fp_bytes_rep # SUCCESS after mp3val
                    except acoustid.FingerprintGenerationError as e_after_mp3val:
                        logging.warning(f"Fingerprinting STILL FAILED for {filepath} after mp3val attempt: {e_after_mp3val}")
                    except Exception as e_fp_mp3val_retry:
                        logging.error(f"Unexpected error re-fingerprinting {filepath} after mp3val: {e_fp_mp3val_retry}")
                else:
                    logging.warning(f"mp3val failed for {filepath} with exit code {process.returncode}.")
                    print(f"WARNING: mp3val repair attempt failed for {os.path.basename(filepath)} (code: {process.returncode}).")
            
            except FileNotFoundError:
                logging.error(f"mp3val command not found at {MP3VAL_PATH}. Cannot attempt mp3val repair.")
                print(f"ERROR: mp3val not found at {MP3VAL_PATH}. Cannot attempt repair for {os.path.basename(filepath)}.")
            except Exception as repair_exception_mp3val:
                logging.error(f"An error occurred during the mp3val repair process for {filepath}: {repair_exception_mp3val}")
            finally:
                if original_file_backed_up_mp3val:
                    if mp3val_repair_led_to_fp: # Success, remove our backup
                        try: os.remove(backup_filepath_mp3val)
                        except OSError as e_rm_bak: logging.warning(f"Could not remove mp3val backup {backup_filepath_mp3val}: {e_rm_bak}")
                    else: # mp3val path failed, restore original file from our backup
                        try:
                            if os.path.exists(backup_filepath_mp3val):
                                shutil.move(backup_filepath_mp3val, filepath)
                                logging.info(f"Restored original {filepath} from mp3val backup as repair didn't lead to successful FP.")
                            # else: backup was already moved or deleted, problem
                        except Exception as e_restore_mp3val:
                            logging.error(f"CRITICAL: Failed to restore {filepath} from mp3val backup {backup_filepath_mp3val}: {e_restore_mp3val}")
                            print(f"ERROR: Failed to restore original {os.path.basename(filepath)} from mp3val backup. Backup: {backup_filepath_mp3val}")
            
            if mp3val_repair_led_to_fp: # Should have already returned if successful
                 return None, None # Should ideally not be reached if logic above is perfect

        # --- STAGE 2: FFmpeg Repair Attempt ---
        # This runs if:
        # 1. Initial fingerprinting failed (e_initial_fp is defined).
        # 2. EITHER mp3val repair was not attempted (e.g., not enabled, not MP3, MP3VAL_PATH not set).
        # 3. OR mp3val repair was attempted but did not result in a successful return of fingerprint (mp3val_repair_led_to_fp is False).
        #    (The file at 'filepath' should be in its pre-mp3val state if mp3val's 'finally' block worked correctly on failure).
        
        should_try_ffmpeg = (
            attempt_repair_ffmpeg_if_needed and
            FFMPEG_PATH and # Check if ffmpeg path is known
            ("fpcalc exited with status" in original_error_for_ffmpeg_check or \
             "error decoding" in original_error_for_ffmpeg_check or \
             "header missing" in original_error_for_ffmpeg_check) # Use similar error triggers
        )

        if should_try_ffmpeg:
            logging.info(f"Attempting to repair {os.path.basename(filepath)} with FFmpeg re-encode...")
            print(f"INFO: mp3val repair failed/skipped for {os.path.basename(filepath)}, attempting FFmpeg re-encode (POTENTIAL QUALITY LOSS)...")

            backup_filepath_ffmpeg = filepath + ".musicscan_ffmpeg_repair.bak"
            # Use a distinct temp name to avoid collision if original had ".ffmpeg_repaired_temp"
            temp_output_filename = os.path.basename(filepath) + ".ffmpeg_temp_out" + os.path.splitext(filepath)[1]
            temp_output_filepath = os.path.join(os.path.dirname(filepath), temp_output_filename)

            original_file_backed_up_ffmpeg = False
            ffmpeg_repair_led_to_fp = False

            try:
                shutil.copy2(filepath, backup_filepath_ffmpeg)
                original_file_backed_up_ffmpeg = True
                logging.info(f"Backup for FFmpeg of {filepath} created at {backup_filepath_ffmpeg}")

                # For MP3s, re-encode to MP3. For other (potentially lossless) formats, try -c:a copy first.
                # This is a simplified approach; more format-specific handling might be better.
                if filepath.lower().endswith(".mp3"):
                    ffmpeg_cmd = [FFMPEG_PATH, "-y", "-i", filepath, "-codec:a", "libmp3lame", "-qscale:a", "2", "-loglevel", "error", temp_output_filepath]
                    logging.debug("Using FFmpeg re-encode for MP3.")
                else: # For FLAC, WAV etc., first try a stream copy (remux)
                    ffmpeg_cmd = [FFMPEG_PATH, "-y", "-i", filepath, "-codec:a", "copy", "-loglevel", "error", temp_output_filepath]
                    logging.debug(f"Using FFmpeg stream copy for non-MP3: {os.path.basename(filepath)}")
                
                process_ffmpeg = subprocess.run(ffmpeg_cmd, capture_output=True, text=True, check=False, encoding='utf-8', errors='replace')

                if process_ffmpeg.returncode == 0 and os.path.exists(temp_output_filepath) and os.path.getsize(temp_output_filepath) > 0:
                    logging.info(f"FFmpeg processing to {temp_output_filepath} successful for {filepath}.")
                    # Replace original with the re-encoded/remuxed temporary file
                    shutil.move(temp_output_filepath, filepath) # Original filepath is now the processed one
                    logging.info(f"Replaced {filepath} with FFmpeg processed version.")
                    print(f"INFO: FFmpeg processing seemed successful for {os.path.basename(filepath)}. Retrying fingerprint.")

                    try: # Retry fingerprinting
                        duration_ffmpeg_rep, fp_bytes_ffmpeg_rep = acoustid.fingerprint_file(filepath, maxlength=FINGERPRINT_AUDIO_MAX_LENGTH_SECONDS)
                        logging.info(f"Successfully fingerprinted after FFmpeg processing: {filepath}")
                        print(f"INFO: Successfully fingerprinted after FFmpeg processing: {os.path.basename(filepath)}.")
                        ffmpeg_repair_led_to_fp = True
                        return duration_ffmpeg_rep, fp_bytes_ffmpeg_rep # SUCCESS after FFmpeg
                    except acoustid.FingerprintGenerationError as e_after_ffmpeg:
                        logging.warning(f"Fingerprinting STILL FAILED for {filepath} after FFmpeg processing: {e_after_ffmpeg}")
                    except Exception as e_fp_ffmpeg_retry:
                        logging.error(f"Unexpected error re-fingerprinting {filepath} after FFmpeg: {e_fp_ffmpeg_retry}")
                else: # FFmpeg command failed or produced empty file
                    logging.warning(f"FFmpeg processing failed for {filepath} or produced empty file. RC: {process_ffmpeg.returncode}. Stderr: {process_ffmpeg.stderr.strip() if process_ffmpeg.stderr else 'N/A'}")
                    print(f"WARNING: FFmpeg processing failed for {os.path.basename(filepath)}.")
            
            except FileNotFoundError:
                logging.error(f"ffmpeg command not found at {FFMPEG_PATH}. Cannot attempt FFmpeg repair.")
                print(f"ERROR: ffmpeg not found at {FFMPEG_PATH}. Cannot attempt repair for {os.path.basename(filepath)}.")
            except Exception as ffmpeg_repair_exception:
                logging.error(f"An error occurred during the FFmpeg repair process for {filepath}: {ffmpeg_repair_exception}")
            finally:
                if os.path.exists(temp_output_filepath): # Clean up temp output if it still exists
                    try: os.remove(temp_output_filepath)
                    except OSError: pass
                if original_file_backed_up_ffmpeg:
                    if ffmpeg_repair_led_to_fp: # Success, remove our backup
                        try: os.remove(backup_filepath_ffmpeg)
                        except OSError as e: logging.warning(f"Could not remove FFmpeg backup {backup_filepath_ffmpeg}: {e}")
                    else: # FFmpeg path failed, restore original file from our backup
                        try:
                            if os.path.exists(backup_filepath_ffmpeg):
                                shutil.move(backup_filepath_ffmpeg, filepath)
                                logging.info(f"Restored {filepath} from FFmpeg backup as repair didn't lead to successful FP.")
                                print(f"INFO: Original file {os.path.basename(filepath)} restored as FFmpeg repair did not lead to successful fingerprinting.")
                            # else: backup was already moved or deleted, problem
                        except Exception as e_restore_ff:
                            logging.error(f"CRITICAL: Failed to restore {filepath} from FFmpeg backup {backup_filepath_ffmpeg}: {e_restore_ff}")
                            print(f"ERROR: Failed to restore original from FFmpeg backup. Backup: {backup_filepath_ffmpeg}")
        
        # If all repair attempts failed or were not applicable/enabled from the initial FingerprintGenerationError
        return None, None 

    except Exception as general_initial_e: 
        # Catch other potential errors during the very first fingerprinting attempt (e.g., file not found if path is bad before any repair attempt)
        logging.error(f"Unexpected error during initial fingerprinting of {filepath} (before any repair logic): {general_initial_e}")
        return None, None

def prompt_to_remove_duplicates(duplicates, quarantine_path, dry_run=False): # Added quarantine_path
    if not duplicates:
        logging.info("No duplicates found to prompt for quarantining.")
        return

    print('\nThe following acoustically similar files (duplicates) were found and will be processed individually:')
    logging.info("Presenting duplicates to user for individual quarantine decision.")
    
    files_quarantined_count = 0 # Renamed
    processed_in_prompt_loop = 0 
    remove_all_mode = False      
    quit_mode = False            

    sorted_duplicate_sets = sorted(duplicates.items())

    for canonical_file, dupe_list in sorted_duplicate_sets:
        if quit_mode:
            break 
        
        print(f"\n--- Processing Duplicates for Canonical File ---")
        print(f"  Keeping (Canonical): {canonical_file}")
        logging.info(f"Processing duplicate set. Canonical (to keep): '{canonical_file}'")
        
        if not dupe_list:
            logging.debug(f"  No duplicates listed for {canonical_file} in this set.")
            continue

        print(f"  The following are considered duplicates of it (to be quarantined):") # Updated text
        
        for i, file_to_quarantine in enumerate(dupe_list): # Renamed file_to_remove
            if quit_mode: 
                break 
            
            if not os.path.exists(file_to_quarantine):
                logging.warning(f"Duplicate file {file_to_quarantine} (for canonical {canonical_file}) no longer exists. Skipping.")
                print(f"\t- {file_to_quarantine} (INFO: File already removed or moved)")
                continue

            processed_in_prompt_loop += 1
            should_quarantine_current_file = False # Renamed
            
            print(f"\n\tConsidering duplicate {i+1} of {len(dupe_list)} for '{os.path.basename(canonical_file)}':")
            print(f"\t  File to potentially quarantine: {file_to_quarantine}") # Updated text

            if remove_all_mode:
                should_quarantine_current_file = True
                logging.info(f"Auto-processing (due to 'yes to all') duplicate file for quarantine: {file_to_quarantine}")
            else:
                # Updated prompt text
                user_response = input("\tMove this duplicate file to quarantine? (y/n/a/q - yes/no/yes to ALL subsequent/quit ALL subsequent): ").strip().lower()
                
                if user_response == 'y':
                    should_quarantine_current_file = True
                    logging.info(f"User chose 'yes' to quarantine duplicate file: {file_to_quarantine}")
                elif user_response == 'a':
                    should_quarantine_current_file = True
                    remove_all_mode = True
                    logging.info(f"User chose 'yes to all' to quarantine. Will quarantine current and all subsequent duplicate files, starting with: {file_to_quarantine}")
                elif user_response == 'q':
                    quit_mode = True
                    logging.info("User chose 'quit'. Halting all duplicate file quarantine processing.")
                    print("\tQuitting duplicate file quarantine.")
                    break 
                elif user_response == 'n':
                    logging.info(f"User chose 'no' for quarantining duplicate file: {file_to_quarantine}")
                    print(f"\tSkipped: {file_to_quarantine}")
                else:
                    print(f"\tInvalid input '{user_response}'. Skipped: {file_to_quarantine}")
                    logging.warning(f"Invalid input '{user_response}' for duplicate file {file_to_quarantine}. Skipped.")

            if should_quarantine_current_file:
                # Call the helper function to move the file
                if move_file_to_quarantine(file_to_quarantine, quarantine_path, dry_run):
                    files_quarantined_count += 1 
        
    # Summarize actions (updated text)
    if files_quarantined_count > 0:
        action_verb = "simulated moving to quarantine" if dry_run else "moved to quarantine"
        print(f"\nFinished duplicate processing. {action_verb.capitalize()} {files_quarantined_count} file(s).")
        logging.info(f"Finished duplicate processing. {action_verb.capitalize()} {files_quarantined_count} file(s).")
    elif processed_in_prompt_loop > 0 and not quit_mode:
        print("\nFinished duplicate processing. No files were moved to quarantine based on your choices.")
        logging.info("Finished duplicate processing. No files moved to quarantine by user choice.")
    elif quit_mode and files_quarantined_count == 0:
         print("\nDuplicate file quarantine process was quit by user; no files were moved to quarantine during this phase.")
         logging.info("Duplicate file quarantine process was quit by user; no files were moved to quarantine during this phase.")
    elif processed_in_prompt_loop == 0 and duplicates: # Duplicates dict was not empty initially
        print("\nFinished duplicate processing. No duplicate files were available for interaction (perhaps already removed or paths were invalid).")
        logging.info("Finished duplicate processing. No duplicate files were available for interaction.")

def rename_files_from_metadata(filepath, dry_run=False):
    try:
        audio_meta = MutagenFile(filepath, easy=True)
        if not audio_meta:
            logging.debug(f"Could not load metadata for {os.path.basename(filepath)} using MutagenFile.")
            return False

        artist_list = audio_meta.get('artist')
        title_list = audio_meta.get('title')

        if not artist_list or not title_list:
            logging.debug(f"Missing artist or title metadata for {os.path.basename(filepath)}.")
            return False

        artist = artist_list[0].replace('/', '&').strip()
        title = title_list[0].replace('/', '&').strip()
        
        if artist and title:
            file_dir = os.path.dirname(filepath)
            file_ext = os.path.splitext(filepath)[1]
            
            sanitized_artist = "".join(c if c.isalnum() or c in " &'-_" else "_" for c in artist)
            sanitized_title = "".join(c if c.isalnum() or c in " &'-_" else "_" for c in title)

            if not sanitized_artist or not sanitized_title :
                logging.debug(f"Artist or Title became empty after sanitization for {os.path.basename(filepath)}")
                return False

            new_basename = f'{sanitized_artist} - {sanitized_title}{file_ext}'
            new_filepath = os.path.join(file_dir, new_basename)
            
            if filepath.lower() != new_filepath.lower():
                action_prefix = "DRY RUN: Would rename" if dry_run else "Renaming"
                
                if os.path.exists(new_filepath):
                    if filepath.lower() == new_filepath.lower():
                        logging.debug(f"File {os.path.basename(filepath)} already matches metadata name (case difference). No actual rename needed.")
                        return False 
                    msg = f"TARGET EXISTS: Cannot rename {os.path.basename(filepath)} to {new_basename} because target already exists."
                    # print(msg) # Can be noisy
                    logging.warning(msg)
                    return False
                
                print(f"{action_prefix}: '{os.path.basename(filepath)}' to '{new_basename}' in '{file_dir}'")
                logging.info(f"{action_prefix}: '{filepath}' to '{new_filepath}'")
                if not dry_run:
                    try:
                        os.rename(filepath, new_filepath)
                    except OSError as e:
                        error_msg = f"Error renaming file {os.path.basename(filepath)} to {new_basename}: {e}"
                        print(error_msg)
                        logging.error(error_msg)
                        return False
                return True
            else:
                logging.debug(f"Filename for {os.path.basename(filepath)} already matches metadata. No rename needed.")
        else:
            logging.debug(f"Could not find valid artist/title metadata for {os.path.basename(filepath)}")
            
    except Exception as e:
        logging.error(f"Error processing metadata for renaming {os.path.basename(filepath)}: {e}")
    return False


def check_bitrate(filepath, current_bitrate_threshold_bps): # Added threshold parameter
    """
    Checks if an audio file has a bitrate lower than the provided threshold.
    Returns True if bitrate is lower, False otherwise or if error.
    """
    try:
        audio_m = MutagenFile(filepath)
        if audio_m and hasattr(audio_m, 'info') and hasattr(audio_m.info, 'bitrate'):
            # Ensure bitrate is positive before comparing
            if audio_m.info.bitrate > 0 and audio_m.info.bitrate < current_bitrate_threshold_bps:
                logging.debug(f"Mutagen: Low bitrate ({audio_m.info.bitrate/1000:.0f}kbps) for {os.path.basename(filepath)} (Threshold: <{current_bitrate_threshold_bps/1000:.0f}kbps)")
                return True
            elif audio_m.info.bitrate >= current_bitrate_threshold_bps:
                logging.debug(f"Mutagen: Sufficient bitrate ({audio_m.info.bitrate/1000:.0f}kbps) for {os.path.basename(filepath)} (Threshold: <{current_bitrate_threshold_bps/1000:.0f}kbps)")
                return False
            # If bitrate is 0 or not determinable by mutagen, fall through to ffprobe
    except Exception as e:
        logging.debug(f"Mutagen could not determine bitrate for {os.path.basename(filepath)}: {e}. Trying ffprobe.")

    cmd = f"ffprobe -v quiet -print_format json -show_streams \"{filepath}\""
    try:
        output = subprocess.check_output(cmd, shell=True, stderr=subprocess.PIPE)
        data = json.loads(output)
        if data and 'streams' in data:
            for stream in data['streams']:
                if stream.get('codec_type') == 'audio' and 'bit_rate' in stream:
                    try:
                        bitrate = int(stream['bit_rate'])
                        if bitrate < current_bitrate_threshold_bps:
                            logging.debug(f"ffprobe: Low bitrate ({bitrate/1000:.0f}kbps) for {os.path.basename(filepath)} (Threshold: <{current_bitrate_threshold_bps/1000:.0f}kbps)")
                            return True
                        logging.debug(f"ffprobe: Sufficient bitrate ({bitrate/1000:.0f}kbps) for {os.path.basename(filepath)} (Threshold: <{current_bitrate_threshold_bps/1000:.0f}kbps)")
                        return False 
                    except ValueError:
                        logging.warning(f"ffprobe: Could not parse bit_rate '{stream['bit_rate']}' for {os.path.basename(filepath)}")
        logging.debug(f"ffprobe: No suitable audio stream with bitrate found for {os.path.basename(filepath)}")
        return False # Default to not low if info missing or unparsable
    except subprocess.CalledProcessError as e:
        logging.warning(f"ffprobe command failed for {os.path.basename(filepath)}. Output: {e.stderr.decode(errors='ignore').strip()}")
        return False
    except (json.JSONDecodeError, ValueError, KeyError) as e: # Added KeyError for safety
        logging.warning(f"Could not determine bitrate via ffprobe for {os.path.basename(filepath)}: {e}")
        return False
    return False # Default case if everything fails

# --- Main script ---
def main():
    parser = argparse.ArgumentParser(
        description=(
            "Music Scan Tool (musicscan.py)\n"
            "--------------------------------\n"
            "Scans a specified music library to identify and manage audio files. \n"
            "Features include:\n"
            "  - Acoustic duplicate detection using audio fingerprints.\n"
            "    (Requires 'fpcalc' utility from Chromaprint: https://acoustid.org/chromaprint).\n"
            "  - Identification of files with bitrates below a (now configurable) defined threshold.\n"
            "  - Optional renaming of files based on their 'Artist - Title' metadata.\n"
            "  - Caching of audio fingerprints in the scanned directory \n"
            "    (in a file named '" + CACHE_FILENAME + "') to significantly speed up subsequent scans.\n"
            "  - Files marked for removal are moved to a quarantine directory.\n\n"
            "  - Optional automatic repair of corrupted MP3s using mp3val during fingerprinting.\n"
            "A detailed log of operations is saved to 'musicscan.log' in the directory \n"
            "from which the script is run.\n\n"
            "Required Python libraries: mutagen, pyacoustid, tqdm."
        ),
        epilog=(
            "Usage Examples:\n"
            "  1. Basic scan (files will be quarantined to 'Deletions' folder in music dir):\n"
            "     python musicscan.py \"/path/to/your/music\"\n\n"
            "  2. Dry run, specify a custom quarantine path:\n"
            "     python musicscan.py \"/path/to/your/music\" --dry-run --quarantine-path \"/tmp/my_quarantine\"\n\n"
            "  3. Scan, enable metadata renaming, limit to 2 worker threads, skip low bitrate check:\n"
            "     python musicscan.py \"/path/to/your/music\" --rename-metadata --max-workers 2 --skip-low-bitrate\n\n"
            "  4. Force re-fingerprinting of all files (ignore cache) and skip duplicate processing:\n" # Clarified skip
            "     python musicscan.py \"/path/to/your/music\" --force-re-fingerprint --skip-duplicates\n"
            "  5. Attempt MP3 auto-repair during scan (use with caution):\n"
            "     python musicscan.py \"/path/to/your/music\" --auto-repair-mp3\n"
        ),
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        'directory',
        nargs='?',
        default=None,
        metavar='DIRECTORY_PATH',
        help='The full path to the music directory you want to scan. \nIf not provided, the script will prompt you to enter it. \nExample: "/mnt/music" or "C:\\Users\\YourName\\Music".'
    )
    parser.add_argument(
        '--rename-metadata',
        action='store_true',
        help='Enable renaming of audio files based on their metadata. \nFiles will be renamed to "Artist - Title.ext" format in their current directory.'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Perform a dry run. The script will analyze files and report what actions \n(e.g., quarantines, renames) it would take, but will NOT make any actual \nchanges to your files or create directories. Highly recommended for first-time use.' # Updated help
    )
    parser.add_argument(
        '--quarantine-path', # NEW ARGUMENT
        type=str,
        default=None,
        metavar='QUARANTINE_DIR_PATH',
        help='Path to a directory where files marked for removal will be moved (quarantined). \n'
             'If a file with the same name exists in the quarantine directory, a number \n'
             'will be appended to the new file (e.g., song (1).mp3).\n'
             'If this option is NOT specified, files will be moved to a default folder named \n'
             '"Deletions" created in the root of the scanned music directory.'
    )
    parser.add_argument(
        '--skip-duplicates',
        action='store_true',
        help='Skip the acoustic duplicate detection and quarantining phase entirely.' # Updated help
    )
    parser.add_argument(
        '--skip-low-bitrate',
        action='store_true',
        help='Skip the low bitrate file detection and associated quarantining prompts. \nUses the threshold from --bitrate (default: 256 kbps).'
    )
    parser.add_argument(
        '--bitrate',
        type=int,
        default=256,  # Default in kbps
        metavar='KBPS',
        help='Minimum bitrate threshold in kbps. Files below this will be flagged as low bitrate. \n'
             'Default: 256 kbps.'
    )
    parser.add_argument(
        '--force-re-fingerprint',
        action='store_true',
        help='Force re-fingerprinting of all audio files, ignoring any existing entries \nin the fingerprint cache (' + CACHE_FILENAME + '). The cache will then be \nrebuilt with fresh fingerprints.'
    )
    parser.add_argument(
        '--max-workers',
        type=int,
        default=None,
        metavar='N',
        help='Maximum number of worker threads for parallel tasks (e.g., fingerprinting). \nIf not specified, defaults to using half of the available CPU cores \n(minimum 1) to maintain system responsiveness. \nExample: --max-workers 2'
    )
    parser.add_argument( 
        '--auto-repair-mp3',
        action='store_true',
        help='EXPERIMENTAL: Attempt to automatically repair MP3 files that fail fingerprinting using mp3val. \n'
             'A temporary backup of the original file (ending in ".musicscan_repair.bak") will be \n'
             'made by this script before repair. This backup is removed if repair and \n'
             're-fingerprinting succeed, otherwise the original is restored from it. \n'
             'Use with extreme caution and ensure your library is backed up. Requires mp3val in PATH.'
    )
    parser.add_argument(
        '--auto-repair-ffmpeg',
        action='store_true',
        help='EXPERIMENTAL: If mp3val repair fails or is not applicable/enabled, attempt to repair files \n'
             'that fail fingerprinting by re-encoding them with FFmpeg. \n'
             'NOTE: This is often a LOSSY process for formats like MP3 and may reduce audio quality. \n'
             'A temporary backup of the original file will be made by this script before repair. \n'
             'Use with EXTREME CAUTION. Requires FFmpeg in PATH.'
    )

    args = parser.parse_args()

    if args.dry_run:
        print("--- DRY RUN MODE ENABLED: No actual file changes will be made. ---")
        logging.info("Dry run mode enabled.")

    # Determine the number of worker threads
    num_workers_cli = args.max_workers
    if num_workers_cli is not None and num_workers_cli <= 0:
        print("Warning: --max-workers must be a positive integer. Using a conservative default instead.")
        logging.warning(f"--max-workers input '{num_workers_cli}' was <= 0. Reverting to default calculation.")
        num_workers_cli = None 

    if num_workers_cli is None:
        num_workers = max(1, os.cpu_count() // 2)
        logging.info(f"--max-workers not specified or invalid, defaulting to {num_workers} workers.")
    else:
        num_workers = num_workers_cli
        logging.info(f"Using {num_workers} worker threads based on --max-workers input.")
    
    print(f"Using up to {num_workers} worker threads for parallel tasks.")

    # Determine effective bitrate threshold in BPS
    if args.bitrate <= 0:
        print(f"Warning: --bitrate value '{args.bitrate}' must be a positive integer. Using default of 256 kbps.")
        logging.warning(f"--bitrate input '{args.bitrate}' was invalid. Reverting to default 256 kbps.")
        effective_bitrate_threshold_bps = 256 * 1000
    else:
        effective_bitrate_threshold_bps = args.bitrate * 1000
        logging.info(f"Using custom bitrate threshold: {args.bitrate} kbps ({effective_bitrate_threshold_bps} bps).")
    
    print(f"Low bitrate threshold set to: {effective_bitrate_threshold_bps / 1000:.0f} kbps.")

    directory_to_scan = args.directory
    if not directory_to_scan:
        directory_to_scan = input('Enter directory path to scan: ')
    directory_to_scan = os.path.abspath(directory_to_scan) 

    if not os.path.isdir(directory_to_scan):
        error_msg = f"Error: Directory '{directory_to_scan}' not found."
        print(error_msg); logging.critical(error_msg); exit(1)
    
    logging.info(f"Starting scan in directory: {directory_to_scan}")

    # Determine effective quarantine path
    if args.quarantine_path:
        effective_quarantine_path = os.path.abspath(args.quarantine_path)
        logging.info(f"Using custom quarantine path: {effective_quarantine_path}")
    else:
        effective_quarantine_path = os.path.join(directory_to_scan, "Deletions") # Default
        logging.info(f"Using default quarantine path: {effective_quarantine_path}")
    
    norm_effective_quarantine_path = os.path.normcase(effective_quarantine_path) # Already absolute from above
    print(f"Quarantine Active: Files will be moved to: {effective_quarantine_path}")
    
    # Check for external executables
    can_fingerprint_system_ok = check_fpcalc_executable()
    can_repair_mp3_system_ok = check_mp3val_executable() # Check for mp3val
    can_repair_ffmpeg_system_ok = check_ffmpeg_executable() # Check for ffmpeg

    # Determine if repair should be attempted based on CLI flag and mp3val availability
    attempt_mp3_repair_globally = args.auto_repair_mp3 and can_repair_mp3_system_ok
    if args.auto_repair_mp3 and not can_repair_mp3_system_ok:
        print("WARNING: MP3 auto-repair was requested (--auto-repair-mp3), but mp3val utility was not found in PATH. Repair feature will be disabled.")
        logging.warning("MP3 auto-repair disabled because mp3val was not found in PATH.")
    elif args.auto_repair_mp3:
        print("INFO: MP3 auto-repair enabled. Problematic MP3s will be backed up and repair will be attempted with mp3val.")
        logging.info("MP3 auto-repair enabled by user and mp3val found.")

    attempt_ffmpeg_repair_globally = args.auto_repair_ffmpeg and can_repair_ffmpeg_system_ok
    if args.auto_repair_ffmpeg and not can_repair_ffmpeg_system_ok:
        print("WARNING: FFmpeg auto-repair requested, but ffmpeg not found. Will be disabled.")
        logging.warning("FFmpeg auto-repair disabled: ffmpeg not found.")
    elif args.auto_repair_ffmpeg:
        print("INFO: FFmpeg auto-repair enabled (used if mp3val fails/skipped and if applicable). This may be LOSSY.")
        logging.info("FFmpeg auto-repair enabled by user.")

    # --- Initial File Scan (excluding quarantine) ---
    print(f"Scanning for audio files in: {directory_to_scan} (excluding quarantine path: {effective_quarantine_path})...")
    logging.info(f"Initial file scan. Root: {directory_to_scan}. Excluding: {effective_quarantine_path}")
    audio_files = []
    for root, dirs, files_in_root in os.walk(directory_to_scan, topdown=True):
        abs_current_root = os.path.abspath(root)
        norm_current_root = os.path.normcase(abs_current_root)
        if norm_current_root == norm_effective_quarantine_path or \
           norm_current_root.startswith(norm_effective_quarantine_path + os.sep):
            logging.debug(f"Skipping scan within quarantine directory: {root}"); dirs[:]=[] ; continue
        
        original_dirs = list(dirs) # Iterate over a copy for safe modification
        dirs[:] = [] # Clear dirs to rebuild it
        for d in original_dirs:
            prospective_path = os.path.normcase(os.path.abspath(os.path.join(root, d)))
            if prospective_path == norm_effective_quarantine_path:
                logging.debug(f"Pruning quarantine directory from sub-traversal of {root}: {os.path.join(root, d)}")
            else:
                dirs.append(d)

        for file_basename in files_in_root:
            if file_basename.lower().endswith(('.mp3', '.wav', '.flac', '.m4a', '.ogg', '.aac', '.opus', '.wma', '.aiff', '.ape')):
                audio_files.append(os.path.join(root, file_basename))

    if not audio_files:
        msg = f'No audio files found in "{directory_to_scan}" (excluding quarantine path).'; print(msg); logging.info(msg); exit(0)
    print(f"Found {len(audio_files)} audio files (excluding quarantine path). Starting analysis...")

    # --- Fingerprint Cache Handling & Initialization of current_run_valid_cache_entries ---
    current_run_valid_cache_entries = {}
    fp_cache_from_disk = {}
    cache_file_path = os.path.join(directory_to_scan, CACHE_FILENAME)
    # Variable to determine later if cache needs saving (initialized assuming low bitrate scan is skipped)
    response_check_low_br_for_save_logic = "n" 

    if not args.force_re_fingerprint:
        fp_cache_from_disk = load_fingerprint_cache(cache_file_path)

    logging.info("Initializing working cache with valid entries from disk or current file stats.")
    for f_path in tqdm(audio_files, desc="Validating cache state", unit="file", smoothing=0.1, bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]'):
        abs_f_path = os.path.abspath(f_path) # Ensure we use absolute paths as cache keys
        if os.path.exists(abs_f_path): # Should always be true if audio_files is from os.walk and not modified
            try:
                current_mtime = os.path.getmtime(abs_f_path)
                current_size = os.path.getsize(abs_f_path)
                disk_cached_entry = fp_cache_from_disk.get(abs_f_path)

                if disk_cached_entry and \
                   disk_cached_entry.get("mtime") == current_mtime and \
                   disk_cached_entry.get("size") == current_size:
                    current_run_valid_cache_entries[abs_f_path] = disk_cached_entry.copy() # Copy full valid entry
                else: # No valid entry on disk, or file changed. Store current mtime/size.
                      # Fingerprint/duration/ignore flags will be added/updated later if needed.
                    current_run_valid_cache_entries[abs_f_path] = {"mtime": current_mtime, "size": current_size}
                    if disk_cached_entry: logging.debug(f"Cache for {os.path.basename(abs_f_path)} was stale/incomplete.")
            except OSError as e:
                logging.warning(f"Could not stat file {abs_f_path} during initial cache population: {e}")
    
    # --- Duplicate Detection ---
    duplicates = {}
    if args.skip_duplicates:
        print("\nSkipping duplicate detection as per --skip-duplicates flag.")
        logging.info("Skipping duplicate detection.")
    elif not can_fingerprint_system_ok: # fpcalc availability checked earlier
        print("\nSkipping duplicate detection: 'fpcalc' utility not available.")
        logging.warning("Skipping duplicate detection: fpcalc not found.")
    else:
        fingerprint_map = collections.defaultdict(list)
        files_needing_fingerprinting = []
        print("\n-- Checking which files need fingerprinting (based on cache and --force-re-fingerprint)...")
        
        for abs_filepath, entry_data in tqdm(current_run_valid_cache_entries.items(), desc="Preparing for fingerprinting", unit="file", smoothing=0.1, bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]'):
            # Check if file still exists before deciding to fingerprint (it might have been an invalid entry initially)
            if not os.path.exists(abs_filepath):
                logging.debug(f"File {abs_filepath} no longer exists. Skipping fingerprint prep.")
                continue

            if args.force_re_fingerprint or "fingerprint_hex" not in entry_data or "duration" not in entry_data:
                files_needing_fingerprinting.append(abs_filepath) 
                if args.force_re_fingerprint: logging.debug(f"Forcing re-fingerprint for {os.path.basename(abs_filepath)}.")
                else: logging.debug(f"Queued for fingerprinting (missing data): {os.path.basename(abs_filepath)}.")
            else: # Use existing valid fingerprint from current_run_valid_cache_entries
                try:
                    fp_bytes = bytes.fromhex(entry_data["fingerprint_hex"])
                    fingerprint_map[(fp_bytes, round(entry_data["duration"]))].append(abs_filepath)
                except (ValueError, TypeError) as e: # Error decoding hex or other issues
                    logging.warning(f"Error using cached FP for {abs_filepath}: {e}. Queuing for re-FP.")
                    files_needing_fingerprinting.append(abs_filepath)
        
        if files_needing_fingerprinting:
            print(f"\n-- Generating {len(files_needing_fingerprinting)} audio fingerprints (using {num_workers} workers)...")
            logging.info(f"Generating {len(files_needing_fingerprinting)} new/updated audio fingerprints using {num_workers} workers. mp3val repair: {attempt_mp3_repair_globally}, ffmpeg repair: {attempt_ffmpeg_repair_globally}")
            
            with ThreadPoolExecutor(max_workers=num_workers) as executor:
                future_to_filepath = { 
                    executor.submit(
                        get_audio_fingerprint, 
                        fp_path, # This should be the path string
                        attempt_mp3_repair_globally, 
                        attempt_ffmpeg_repair_globally 
                    ): fp_path 
                    for fp_path in files_needing_fingerprinting 
                }
                for future in tqdm(future_to_filepath, desc="Fingerprinting files", total=len(future_to_filepath), unit="file", smoothing=0.1, bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]'):
                    abs_filepath_processed = future_to_filepath[future]
                    try:
                        duration, fp_bytes = future.result()
                        if fp_bytes is not None and duration is not None: # Successfully fingerprinted
                            fingerprint_map[(fp_bytes, round(duration))].append(abs_filepath_processed)
                            
                            # Update working cache with new fingerprint data
                            # Get current mtime and size again as repair might have changed them
                            current_mtime_fp = os.path.getmtime(abs_filepath_processed)
                            current_size_fp = os.path.getsize(abs_filepath_processed)
                            
                            entry = current_run_valid_cache_entries.setdefault(abs_filepath_processed, {})
                            entry.update({ 
                                "fingerprint_hex": fp_bytes.hex(), 
                                "duration": duration,
                                "mtime": current_mtime_fp, 
                                "size": current_size_fp
                            })
                            # If successfully fingerprinted (even after repair), reset ignore flag,
                            # as content might have changed enough or it's a fresh FP.
                            entry.pop("low_bitrate_ignored", None) 
                    except OSError as e: # Catch if getmtime/getsize fails (e.g. file gone after repair attempt)
                        logging.error(f"OSError accessing {abs_filepath_processed} after fingerprint attempt: {e}")
                    except Exception as e: 
                        logging.error(f"Error processing fingerprint result for {abs_filepath_processed}: {e}")
        elif not args.force_re_fingerprint and (not args.skip_duplicates and can_fingerprint_system_ok) : 
            # Only print this if fingerprinting was supposed to run but no files needed it
            print("\n-- No new files needed fingerprinting based on cache status.")
            logging.info("No new files needed fingerprinting based on cache status.")
        
        # --- Duplicate Identification (from fingerprint_map) ---
        # This part runs if duplicate detection was not skipped and fpcalc is available
        logging.info("Fingerprint processing complete. Identifying duplicates from map.")
        print("\n-- Identifying duplicates from fingerprints...")
        for (fp_bytes, duration_group), files_list in tqdm(fingerprint_map.items(), desc="Processing fingerprints", unit="group", smoothing=0.1, bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]'):
            if len(files_list) > 1:
                def sort_key_for_duplicates(filepath_str_sort): # Renamed to avoid clash
                    in_unsorted_folder = is_in_target_path_pattern(filepath_str_sort, UNSORTED_PATH_MARKER_SEGMENTS)
                    size_sort = 0 # Default size if not accessible
                    if os.path.exists(filepath_str_sort):
                        try: size_sort = os.path.getsize(filepath_str_sort)
                        except OSError as e_sort: logging.warning(f"Could not get size for {filepath_str_sort} during duplicate sort: {e_sort}")
                    return (in_unsorted_folder, -size_sort, filepath_str_sort)
                files_list.sort(key=sort_key_for_duplicates) 
                canonical_file = files_list[0]
                duplicate_copies = files_list[1:]
                if duplicate_copies: duplicates[canonical_file] = duplicate_copies
        
        if duplicates: 
            prompt_to_remove_duplicates(duplicates, effective_quarantine_path, args.dry_run)
        else: 
            print('No acoustically similar duplicate audio files found.')
            logging.info('No duplicates found.')

    # --- Low Bitrate File Check ---
    # This 'response_check_low_br_for_save_logic' should have been defined before this block starts.
    # If skip_low_bitrate is True, it remains "n". Otherwise, it's set by user input.
    # Initialize it here if it might not be set due to skip_duplicates path.
    if 'response_check_low_br_for_save_logic' not in locals():
        response_check_low_br_for_save_logic = "n" 

    if args.skip_low_bitrate:
        print("\nSkipping low bitrate file check.")
        logging.info("Low bitrate check skipped by user.")
    else:
        # Use the effective_bitrate_threshold_bps for the prompt
        prompt_bitrate_kbps = effective_bitrate_threshold_bps / 1000
        response_check_low_br = input(f'\nScan for files with bitrates < {prompt_bitrate_kbps:.0f}kbps (to quarantine)? (y/n): ').strip().lower()
        response_check_low_br_for_save_logic = response_check_low_br # Store for save condition
        
        if response_check_low_br == 'y':
            low_bitrate_files_initially_detected = []
            print(f"\n-- Checking for files with bitrates lower than {prompt_bitrate_kbps:.0f}kbps (using {num_workers} workers)...")
            logging.info(f"Starting low bitrate file check (threshold: {effective_bitrate_threshold_bps}bps) using {num_workers} workers.")
            
            files_for_br_check = [f for f in audio_files if os.path.exists(f)]
            with ThreadPoolExecutor(max_workers=num_workers) as executor:
                future_to_fp_br = { 
                    # Pass the effective_bitrate_threshold_bps to check_bitrate
                    executor.submit(check_bitrate, fp_br, effective_bitrate_threshold_bps): fp_br 
                    for fp_br in files_for_br_check 
                }
                for future in tqdm(future_to_fp_br, desc=f"Checking bitrates (<{prompt_bitrate_kbps:.0f}kbps)", total=len(future_to_fp_br), unit="file", smoothing=0.1, bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]'):
                    filepath_br = future_to_fp_br[future]
                    try:
                        if future.result(): low_bitrate_files_initially_detected.append(filepath_br)
                    except Exception as exc_br: logging.error(f'{os.path.basename(filepath_br)} generated an exception during bitrate check: {exc_br}')
            
            if low_bitrate_files_initially_detected:
                low_bitrate_files_to_prompt = []
                low_br_q_path = os.path.join(effective_quarantine_path, "low-bitrate")
                
                print(f"\n-- Filtering {len(low_bitrate_files_initially_detected)} potential low bitrate files against cache 'ignore' status...")
                for file_path_lb in tqdm(low_bitrate_files_initially_detected, desc="Checking ignored low bitrate", unit="file", smoothing=0.1, bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]'):
                    abs_filepath_lb = os.path.abspath(file_path_lb)
                    try:
                        if not os.path.exists(abs_filepath_lb): continue
                        current_mtime_lb = os.path.getmtime(abs_filepath_lb)
                        current_size_lb = os.path.getsize(abs_filepath_lb)
                        cached_entry_lb = current_run_valid_cache_entries.get(abs_filepath_lb)
                        
                        if cached_entry_lb and \
                           cached_entry_lb.get("mtime") == current_mtime_lb and \
                           cached_entry_lb.get("size") == current_size_lb and \
                           cached_entry_lb.get("low_bitrate_ignored") is True:
                            logging.info(f"Skipping low bitrate prompt for {os.path.basename(abs_filepath_lb)} (previously ignored and unchanged).")
                        else:
                            low_bitrate_files_to_prompt.append(file_path_lb)
                            entry = current_run_valid_cache_entries.setdefault(abs_filepath_lb, {})
                            entry["mtime"] = current_mtime_lb 
                            entry["size"] = current_size_lb
                            entry["low_bitrate_ignored"] = False 
                    except OSError as e_lb_stat: logging.warning(f"Could not stat {abs_filepath_lb} for low bitrate ignore check: {e_lb_stat}")
                
                if low_bitrate_files_to_prompt:
                    print(f'\nFound {len(low_bitrate_files_to_prompt)} file(s) needing review for low bitrate (threshold < {prompt_bitrate_kbps:.0f}kbps).') # Use prompt_bitrate_kbps
                    print(f"Low bitrate files will be quarantined to: {low_br_q_path}")
                    # ... (y/n/a/q loop for low_bitrate_files_to_prompt as before, it doesn't need the threshold directly) ...
                    # ... (its summary messages also refer to 'low bitrate' generally)
                    quarantined_lb_count = 0; processed_lb_prompt = 0
                    all_mode_lb = False; quit_mode_lb = False       
                    for i, fp_lb_prompt_path in enumerate(low_bitrate_files_to_prompt):
                        if quit_mode_lb: break
                        if not os.path.exists(fp_lb_prompt_path): continue
                        processed_lb_prompt += 1
                        should_q_lb = False; abs_fp_lb_prompt = os.path.abspath(fp_lb_prompt_path)
                        print(f"\n--- File {i+1} of {len(low_bitrate_files_to_prompt)} ---\nLow bitrate candidate (below {prompt_bitrate_kbps:.0f}kbps): {fp_lb_prompt_path}") # Added threshold to prompt
                        
                        if all_mode_lb: should_q_lb = True
                        else:
                            resp_lb = input(f"Move to '{os.path.basename(low_br_q_path)}' quarantine? (y/n/a/q): ").strip().lower()
                            if resp_lb == 'y': 
                                should_q_lb = True
                                entry = current_run_valid_cache_entries.setdefault(abs_fp_lb_prompt, {})
                                entry["low_bitrate_ignored"] = False 
                                try: entry.update({"mtime": os.path.getmtime(abs_fp_lb_prompt), "size": os.path.getsize(abs_fp_lb_prompt)})
                                except OSError: pass
                            elif resp_lb == 'a': 
                                should_q_lb = True; all_mode_lb = True
                                entry = current_run_valid_cache_entries.setdefault(abs_fp_lb_prompt, {})
                                entry["low_bitrate_ignored"] = False 
                                try: entry.update({"mtime": os.path.getmtime(abs_fp_lb_prompt), "size": os.path.getsize(abs_fp_lb_prompt)})
                                except OSError: pass
                            elif resp_lb == 'q': 
                                quit_mode_lb = True; print("Quitting low bitrate quarantine."); break
                            elif resp_lb == 'n':
                                print(f"Skipped (will be ignored next time if unchanged): {fp_lb_prompt_path}")
                                entry = current_run_valid_cache_entries.setdefault(abs_fp_lb_prompt, {})
                                entry["low_bitrate_ignored"] = True
                                try: 
                                    entry["mtime"] = os.path.getmtime(abs_fp_lb_prompt)
                                    entry["size"] = os.path.getsize(abs_fp_lb_prompt)
                                except OSError as e_ign: logging.error(f"Could not update mtime/size for ignored {abs_fp_lb_prompt}: {e_ign}")
                            else: print(f"Invalid input. Skipped: {fp_lb_prompt_path}")
                        
                        if should_q_lb:
                            if move_file_to_quarantine(fp_lb_prompt_path, low_br_q_path, args.dry_run):
                                quarantined_lb_count += 1
                    
                    if quarantined_lb_count > 0: 
                        print(f"\nLow Bitrate: {'Simulated moving' if args.dry_run else 'Moved'} {quarantined_lb_count} file(s) to '{low_br_q_path}'.")
                        logging.info(f"Low Bitrate: {'Simulated moving' if args.dry_run else 'Moved'} {quarantined_lb_count} file(s) to '{low_br_q_path}'.")
                    elif processed_lb_prompt > 0 and not quit_mode_lb: 
                        print("\nLow Bitrate: No files moved to quarantine by choice.")
                        logging.info("Low Bitrate: No files moved to quarantine by choice.")
                    elif quit_mode_lb and quarantined_lb_count == 0: 
                        print("\nLow Bitrate: Quarantine quit by user; no files moved.")
                        logging.info("Low Bitrate: Quarantine quit by user; no files moved.")
                else: 
                    if low_bitrate_files_initially_detected : 
                         print(f"\nAll {len(low_bitrate_files_initially_detected)} files identified as low bitrate were previously 'ignored' and unchanged, or no longer exist.")
                         logging.info("All potential low bitrate files were previously 'ignored' and unchanged, or no longer exist.")
            else: 
                print(f'\nNo files initially identified with bitrates < {prompt_bitrate_kbps:.0f}kbps.') # Use prompt_bitrate_kbps
                logging.info(f'No files initially identified with bitrates < {prompt_bitrate_kbps:.0f}kbps.')
        else: 
            print('\nScan for low bitrate files skipped by user.')
            logging.info("User chose not to scan for low bitrate files.")

    # --- Rename files based on metadata (conditionally) ---
    if args.rename_metadata:
        print("\n-- Renaming files based on metadata --")
        logging.info("Starting metadata-based renaming process.")
        # Use a fresh list of existing audio files for renaming, as some might have been quarantined
        files_to_rename_after_ops = [f for f in audio_files if os.path.exists(f)] # Check original audio_files list
        
        if not files_to_rename_after_ops:
            print("No audio files remaining to consider for renaming.")
            logging.info("No audio files remaining for renaming after prior operations.")
        else:
            print(f"Checking {len(files_to_rename_after_ops)} audio file(s) for renaming...")
            renamed_c = 0
            for fp_rename in tqdm(files_to_rename_after_ops, desc="Renaming files", unit="file", smoothing=0.1, bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]'):
                if rename_files_from_metadata(fp_rename, args.dry_run): 
                    renamed_c += 1
            
            if renamed_c > 0: 
                summary_msg = f"Successfully {'simulated renaming of' if args.dry_run else 'renamed'} {renamed_c} file(s) based on metadata."
                print(summary_msg); logging.info(summary_msg)
            elif args.dry_run and len(files_to_rename_after_ops) > 0 : 
                print("Dry run: No files were actually renamed."); logging.info("Dry run: No files were actually renamed.")
            elif len(files_to_rename_after_ops) > 0 : # Files were checked but none needed renaming
                print("No files needed renaming based on metadata (or errors occurred).")
                logging.info("No files needed renaming based on metadata (or errors occurred).")
    else:
        print("\nSkipping renaming files based on metadata.")
        logging.info("Renaming based on metadata disabled.")

    # --- FINAL CACHE SAVE ---
    # Prune entries from current_run_valid_cache_entries for files that no longer exist (e.g. were quarantined)
    # or for which we couldn't get mtime/size (e.g. if they vanished during script run).
    final_cache_to_save = {}
    logging.info(f"Preparing final cache. Starting with {len(current_run_valid_cache_entries)} working entries.")
    for fp_abs_save, data_save in current_run_valid_cache_entries.items():
        if os.path.exists(fp_abs_save): # Only save cache entries for files that still exist at their original path
            # Ensure mtime and size are present if we are saving this entry, 
            # especially if it only has an ignore flag from an old cache and fingerprinting was skipped.
            if "mtime" not in data_save or "size" not in data_save: # Should be rare given pre-population
                try:
                    data_save["mtime"] = os.path.getmtime(fp_abs_save)
                    data_save["size"] = os.path.getsize(fp_abs_save)
                    final_cache_to_save[fp_abs_save] = data_save
                except OSError as e_stat_save:
                    logging.warning(f"Could not stat existing file {fp_abs_save} before saving its cache entry: {e_stat_save}. Entry skipped.")
            else: # mtime and size are already there
                 final_cache_to_save[fp_abs_save] = data_save
        else:
            logging.info(f"Pruning cache entry for non-existent file (e.g., quarantined or deleted): {fp_abs_save}")
    
    # Determine if any cache-relevant operation was actually attempted by the user or forced
    did_fingerprint_stage_run_attempted = not args.skip_duplicates and can_fingerprint_system_ok
    did_low_bitrate_scan_run_attempted = not args.skip_low_bitrate and response_check_low_br_for_save_logic.lower() == 'y'

    if args.force_re_fingerprint or did_fingerprint_stage_run_attempted or did_low_bitrate_scan_run_attempted:
        if final_cache_to_save: # Only save if there's something to save
            save_fingerprint_cache(cache_file_path, final_cache_to_save)
        else:
            logging.info("Final cache to save is empty (e.g., all files removed or no valid entries). Not writing empty cache file.")
            # Optionally, delete the cache file if it's now empty:
            # if os.path.exists(cache_file_path):
            #     try: os.remove(cache_file_path); logging.info(f"Removed empty cache file: {cache_file_path}")
            #     except OSError as e: logging.error(f"Could not remove empty cache file {cache_file_path}: {e}")
    else:
        logging.info("Skipping final cache save as no cache-relevant operations were performed or enabled this session.")

    print('\nFinished scanning and processing.')
    logging.info("Script finished.")

if __name__ == "__main__":
    try: import mutagen 
    except ImportError: print("CRITICAL: Mutagen not installed. Run: pip install mutagen"); exit(1)
    main()
