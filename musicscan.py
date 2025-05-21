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
BITRATE_THRESHOLD = 256000
FINGERPRINT_AUDIO_MAX_LENGTH_SECONDS = 0 # 0 means process the whole file
CACHE_FILENAME = ".musicscan_fp_cache.json"

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
    try:
        with open(cache_file_path, 'w', encoding='utf-8') as f:
            json.dump(cache_data, f, indent=4)
        logging.info(f"Fingerprint cache with {len(cache_data)} entries saved to {cache_file_path}")
    except IOError as e:
        logging.error(f"Could not save fingerprint cache to {cache_file_path}: {e}")

# --- Audio Fingerprinting Function ---
def get_audio_fingerprint(filepath):
    try:
        duration, fp_bytes = acoustid.fingerprint_file(filepath, maxlength=FINGERPRINT_AUDIO_MAX_LENGTH_SECONDS)
        logging.debug(f"Successfully fingerprinted: {os.path.basename(filepath)}, Duration: {duration:.2f}s")
        return duration, fp_bytes
    except acoustid.FingerprintGenerationError as e:
        logging.warning(f"Fingerprint generation failed for {filepath}: {e}")
        return None, None
    except Exception as e:
        logging.error(f"Unexpected error fingerprinting {filepath}: {e}")
        return None, None

def prompt_to_remove_duplicates(duplicates, dry_run=False):
    if not duplicates:
        logging.info("No duplicates found to prompt for removal.")
        return
    print('\nThe following acoustically similar files (duplicates) were found:')
    logging.info("Presenting duplicates to user for removal decision.")
    for canonical_file, dupe_list in duplicates.items():
        print(f"\n  Keeping: {canonical_file}")
        logging.info(f"Duplicate set: Keeping '{canonical_file}'")
        print(f"  Marked as duplicates of it:")
        for file_to_remove in dupe_list:
            print(f"\t- {file_to_remove}")
            logging.info(f"\t- Marked for potential removal: '{file_to_remove}'")
            
    response = input('\nWould you like to remove ALL marked duplicate files (those listed with "-")? (y/n): ')
    if response.lower() == 'y':
        files_removed_count = 0
        action_prefix = "DRY RUN: Would remove" if dry_run else "Removing"
        
        for _, files_to_remove_list in duplicates.items():
            for file_to_remove in files_to_remove_list:
                if os.path.exists(file_to_remove):
                    try:
                        print(f"{action_prefix}: {file_to_remove}")
                        logging.info(f"{action_prefix}: {file_to_remove}")
                        if not dry_run:
                            os.remove(file_to_remove)
                        files_removed_count +=1
                    except OSError as e:
                        error_msg = f"Error removing {file_to_remove}: {e}"
                        print(error_msg)
                        logging.error(error_msg)
                else:
                    logging.warning(f"Attempted to remove non-existent file (already removed or moved?): {file_to_remove}")
        
        if files_removed_count > 0:
            summary_msg = f"Successfully {'simulated removal of' if dry_run else 'removed'} {files_removed_count} duplicate file(s)."
            print(summary_msg)
            logging.info(summary_msg)
        elif dry_run and len(duplicates)>0 : # Check if there were duplicates to simulate removing
             print("Dry run: No files were actually removed.")
             logging.info("Dry run: No files were actually removed.")
        elif not duplicates:
            pass # No duplicates were presented in the first place
        else:
            print("No duplicate files were removed (perhaps they were already gone or an error occurred).")
            logging.info("No duplicate files actually removed in this session.")
    else:
        print('Duplicate files not removed.')
        logging.info("User chose not to remove duplicate files.")


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


def check_bitrate(filepath):
    try:
        audio_m = MutagenFile(filepath)
        if audio_m and hasattr(audio_m, 'info') and hasattr(audio_m.info, 'bitrate'):
            if audio_m.info.bitrate > 0 and audio_m.info.bitrate < BITRATE_THRESHOLD:
                logging.debug(f"Mutagen: Low bitrate ({audio_m.info.bitrate/1000:.0f}kbps) for {os.path.basename(filepath)}")
                return True
            elif audio_m.info.bitrate >= BITRATE_THRESHOLD:
                logging.debug(f"Mutagen: Sufficient bitrate ({audio_m.info.bitrate/1000:.0f}kbps) for {os.path.basename(filepath)}")
                return False
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
                        if bitrate < BITRATE_THRESHOLD:
                            logging.debug(f"ffprobe: Low bitrate ({bitrate/1000:.0f}kbps) for {os.path.basename(filepath)}")
                            return True
                        logging.debug(f"ffprobe: Sufficient bitrate ({bitrate/1000:.0f}kbps) for {os.path.basename(filepath)}")
                        return False 
                    except ValueError:
                        logging.warning(f"ffprobe: Could not parse bit_rate '{stream['bit_rate']}' for {os.path.basename(filepath)}")
        logging.debug(f"ffprobe: No suitable audio stream with bitrate found for {os.path.basename(filepath)}")
        return False 
    except subprocess.CalledProcessError as e:
        logging.warning(f"ffprobe command failed for {os.path.basename(filepath)}. Output: {e.stderr.decode(errors='ignore').strip()}")
        return False
    except (json.JSONDecodeError, ValueError, KeyError) as e:
        logging.warning(f"Could not determine bitrate via ffprobe for {os.path.basename(filepath)}: {e}")
        return False

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
            "  - Identification of files with bitrates below a defined threshold.\n"
            "  - Optional renaming of files based on their 'Artist - Title' metadata.\n"
            "  - Caching of audio fingerprints in the scanned directory \n"
            "    (in a file named '" + CACHE_FILENAME + "') to significantly speed up subsequent scans.\n\n"
            "A detailed log of operations is saved to 'musicscan.log' in the directory \n"
            "from which the script is run.\n\n"
            "Required Python libraries: mutagen, pyacoustid, tqdm."
        ),
        epilog=(
            "Usage Examples:\n"
            "  1. Basic scan of a directory (will prompt for actions):\n"
            "     python musicscan.py \"/path/to/your/music\"\n\n"
            "  2. Interactive prompt for directory, then perform a dry run:\n"
            "     python musicscan.py --dry-run\n\n"
            "  3. Scan, enable metadata renaming, limit to 2 worker threads, and skip low bitrate check:\n"
            "     python musicscan.py \"/path/to/your/music\" --rename-metadata --max-workers 2 --skip-low-bitrate\n\n"
            "  4. Force re-fingerprinting of all files (ignore cache) and skip duplicate removal prompts:\n"
            "     python musicscan.py \"/path/to/your/music\" --force-re-fingerprint --skip-duplicates\n"
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
        help='Perform a dry run. The script will analyze files and report what actions \n(e.g., deletions, renames) it would take, but will NOT make any actual \nchanges to your files. Highly recommended for first-time use or when \nunsure about settings.'
    )
    parser.add_argument(
        '--skip-duplicates',
        action='store_true',
        help='Skip the acoustic duplicate detection phase entirely. \nThis can save considerable time if you only intend to use other features \nlike bitrate checking or metadata renaming for this run.'
    )
    parser.add_argument(
        '--skip-low-bitrate',
        action='store_true',
        help=f'Skip the low bitrate file detection and associated removal prompts. \nThe current bitrate threshold is set to {BITRATE_THRESHOLD/1000:.0f}kbps.'
    )
    parser.add_argument(
        '--force-re-fingerprint',
        action='store_true',
        help='Force re-fingerprinting of all audio files, ignoring any existing entries \nin the fingerprint cache (' + CACHE_FILENAME + '). The cache will then be \nrebuilt with fresh fingerprints. Use this if you suspect the cache \nis corrupted or if the fingerprinting algorithm/settings have changed.'
    )
    parser.add_argument(
        '--max-workers',
        type=int,
        default=None,
        metavar='N',
        help='Maximum number of worker threads for parallel tasks, primarily for \nCPU-intensive audio fingerprinting and I/O-bound bitrate checks. \nIf not specified, defaults to using half of the available CPU cores \n(with a minimum of 1 worker) to help maintain system responsiveness. \nExample: --max-workers 2'
    )
    args = parser.parse_args()

    if args.dry_run:
        print("--- DRY RUN MODE ENABLED: No actual file changes will be made. ---")
        logging.info("Dry run mode enabled.")

    # Determine the number of worker threads
    num_workers_cli = args.max_workers
    if num_workers_cli is not None:
        if num_workers_cli <= 0:
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


    directory_to_scan = args.directory
    if not directory_to_scan:
        directory_to_scan = input('Enter directory path to scan: ')
    
    directory_to_scan = os.path.abspath(directory_to_scan) # Use absolute path for consistency

    if not os.path.isdir(directory_to_scan):
        error_msg = f"Error: Directory '{directory_to_scan}' not found."
        print(error_msg)
        logging.critical(error_msg)
        exit(1) # Exit if directory is invalid
    
    # Configure logging file path to be inside the scanned directory if possible,
    # otherwise, it defaults to CWD due to initial basicConfig call.
    # For simplicity, the initial basicConfig will create 'musicscan.log' in CWD.
    # A more advanced setup would delay basicConfig or add a specific FileHandler.
    # The help text mentions CWD for the log file for now.
    
    logging.info(f"Starting scan in directory: {directory_to_scan}")
    print(f"Scanning for audio files in: {directory_to_scan} ...")
    
    audio_files = []
    for root, _, files_in_root in os.walk(directory_to_scan):
        for file_basename in files_in_root:
            # Expanded list of common audio extensions
            if file_basename.lower().endswith(('.mp3', '.wav', '.flac', '.m4a', '.ogg', '.aac', '.opus', '.wma', '.aiff', '.ape')):
                audio_files.append(os.path.join(root, file_basename))

    if not audio_files:
        msg = 'No audio files found in the directory or its subfolders.'
        print(msg)
        logging.info(msg)
        exit(0) # Exit if no audio files are found

    print(f"Found {len(audio_files)} audio files. Starting analysis...")
    logging.info(f"Found {len(audio_files)} audio files for analysis.")

    # --- Duplicate Detection (Acoustic Fingerprinting with Caching) ---
    duplicates = {}
    can_fingerprint_system_ok = check_fpcalc_executable() # Ensure fpcalc is available

    if args.skip_duplicates:
        print("\nSkipping duplicate detection as per --skip-duplicates flag.")
        logging.info("Skipping duplicate detection as per --skip-duplicates flag.")
    elif not can_fingerprint_system_ok:
        print("\nSkipping duplicate detection via audio fingerprinting as 'fpcalc' utility is not available.")
        logging.warning("Skipping duplicate detection: fpcalc not found.")
    else:
        cache_file_path = os.path.join(directory_to_scan, CACHE_FILENAME)
        fp_cache_from_disk = {}
        if not args.force_re_fingerprint:
            fp_cache_from_disk = load_fingerprint_cache(cache_file_path)
        else:
            logging.info("Forcing re-fingerprint of all files, cache will be ignored for loading but rebuilt.")
            print("\nForcing re-fingerprint of all files, ignoring existing cache.")

        current_run_valid_cache_entries = {} 
        fingerprint_map = collections.defaultdict(list)
        files_needing_fingerprinting = []

        print("\n-- Checking fingerprint cache and identifying files for new fingerprinting...")
        logging.info("Checking fingerprint cache.")
        
        valid_audio_files_for_processing = [f for f in audio_files if os.path.exists(f)]

        for filepath in tqdm(valid_audio_files_for_processing, desc="Cache Check", unit="file", smoothing=0.1, bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]'):
            try:
                abs_filepath = os.path.abspath(filepath) 
                file_mtime = os.path.getmtime(abs_filepath)
                file_size = os.path.getsize(abs_filepath)
                
                cached_data = fp_cache_from_disk.get(abs_filepath)
                
                if cached_data and \
                   cached_data.get("mtime") == file_mtime and \
                   cached_data.get("size") == file_size and \
                   cached_data.get("fingerprint_hex") and \
                   cached_data.get("duration") is not None:
                    
                    logging.debug(f"Cache hit for {os.path.basename(abs_filepath)}")
                    fp_bytes = bytes.fromhex(cached_data["fingerprint_hex"])
                    duration = cached_data["duration"]
                    duration_key = round(duration)
                    fingerprint_map[(fp_bytes, duration_key)].append(abs_filepath)
                    current_run_valid_cache_entries[abs_filepath] = cached_data 
                else:
                    if cached_data: 
                        logging.debug(f"Cache stale/incomplete for {os.path.basename(abs_filepath)}. Queued for re-fingerprinting.")
                    else: 
                        logging.debug(f"Cache miss for {os.path.basename(abs_filepath)}. Queued for fingerprinting.")
                    files_needing_fingerprinting.append({'path': abs_filepath, 'mtime': file_mtime, 'size': file_size})
            except OSError as e:
                logging.warning(f"Could not stat file {filepath} for cache check: {e}")

        if files_needing_fingerprinting:
            print(f"\n-- Generating {len(files_needing_fingerprinting)} new/updated audio fingerprints (using {num_workers} workers, this may take a while)...")
            logging.info(f"Generating {len(files_needing_fingerprinting)} new/updated audio fingerprints using {num_workers} workers.")
            
            with ThreadPoolExecutor(max_workers=num_workers) as executor:
                future_to_fileinfo = {
                    executor.submit(get_audio_fingerprint, fileinfo['path']): fileinfo
                    for fileinfo in files_needing_fingerprinting
                }
                
                for future in tqdm(future_to_fileinfo, desc="Fingerprinting files", total=len(future_to_fileinfo), unit="file", smoothing=0.1, bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]'):
                    fileinfo = future_to_fileinfo[future]
                    abs_filepath = fileinfo['path']
                    try:
                        duration, fp_bytes = future.result()
                        if fp_bytes is not None and duration is not None:
                            duration_key = round(duration)
                            fingerprint_map[(fp_bytes, duration_key)].append(abs_filepath)
                            current_run_valid_cache_entries[abs_filepath] = { 
                                "fingerprint_hex": fp_bytes.hex(),
                                "duration": duration,
                                "mtime": fileinfo['mtime'],
                                "size": fileinfo['size']
                            }
                    except Exception as e:
                        logging.error(f"Error processing fingerprint result for {abs_filepath}: {e}")
        elif not args.force_re_fingerprint: 
            print("\n-- No new files to fingerprint. All valid fingerprints loaded from cache.")
            logging.info("No new files to fingerprint. All valid fingerprints loaded from cache.")
        
        save_fingerprint_cache(cache_file_path, current_run_valid_cache_entries)

        logging.info("Fingerprint processing complete. Identifying duplicates from map.")
        print("\n-- Identifying duplicates from fingerprints...")
        for (fp_bytes, duration_group), files_list in tqdm(fingerprint_map.items(), desc="Processing fingerprints", unit="group", smoothing=0.1, bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]'):
            if len(files_list) > 1:
                files_list.sort(key=lambda x: (os.path.getsize(x) if os.path.exists(x) else 0, x), reverse=True) 
                canonical_file = files_list[0]
                duplicate_copies = files_list[1:]
                if duplicate_copies:
                    duplicates[canonical_file] = duplicate_copies
                    logging.debug(f"Duplicate set found: Key='{canonical_file}', Duplicates='{', '.join(duplicate_copies)}'")

        if duplicates:
            prompt_to_remove_duplicates(duplicates, args.dry_run)
        else:
            msg = 'No acoustically similar duplicate audio files found.'
            print(msg)
            logging.info(msg)

    # --- Low Bitrate File Check ---
    # --- Low Bitrate File Check ---
    if args.skip_low_bitrate:
        print("\nSkipping low bitrate file check as per --skip-low-bitrate flag.")
        logging.info("Skipping low bitrate file check as per --skip-low-bitrate flag.")
    else:
        response_check_low_br = input(f'\nWould you like to scan for files with bitrates lower than {BITRATE_THRESHOLD/1000:.0f}kbps? (y/n): ')
        if response_check_low_br.lower() == 'y':
            low_bitrate_files = [] # This list will store paths of low bitrate files
            print(f"\n-- Checking for files with bitrates lower than {BITRATE_THRESHOLD/1000:.0f}kbps (using {num_workers} workers, this may take a while)...")
            logging.info(f"Starting low bitrate file check (threshold: {BITRATE_THRESHOLD}bps) using {num_workers} workers.")
            
            files_for_bitrate_check = [f for f in audio_files if os.path.exists(f)]
            with ThreadPoolExecutor(max_workers=num_workers) as executor:
                future_to_file = {
                    executor.submit(check_bitrate, file_path): file_path 
                    for file_path in files_for_bitrate_check
                }
                for future in tqdm(future_to_file, desc=f"Checking bitrates (<{BITRATE_THRESHOLD/1000:.0f}kbps)", total=len(future_to_file), unit="file", smoothing=0.1, bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]'):
                    file_path = future_to_file[future]
                    try:
                        if future.result(): # True if bitrate is low
                             low_bitrate_files.append(file_path)
                    except Exception as exc:
                        logging.error(f'{os.path.basename(file_path)} generated an exception during bitrate check: {exc}')
            
            if low_bitrate_files:
                print(f'\nFound {len(low_bitrate_files)} file(s) with bitrates lower than {BITRATE_THRESHOLD/1000:.0f}kbps.')
                logging.info(f"Found {len(low_bitrate_files)} low bitrate files. Prompting for individual removal.")
                
                removed_count = 0
                processed_in_prompt_loop = 0 # To know if any files were presented to the user in the loop
                remove_all_mode = False # Flag for 'yes to all' (a)
                quit_mode = False       # Flag for 'quit' (q)

                for i, file_path in enumerate(low_bitrate_files):
                    if quit_mode:
                        break 
                    
                    if not os.path.exists(file_path):
                        logging.warning(f"Low bitrate file {file_path} no longer exists (perhaps removed as duplicate). Skipping.")
                        continue
                    
                    processed_in_prompt_loop += 1
                    should_remove_current_file = False
                    
                    # Display which file is being considered
                    print(f"\n--- File {i+1} of {len(low_bitrate_files)} ---")
                    print(f"Low bitrate candidate: {file_path}")

                    if remove_all_mode:
                        should_remove_current_file = True
                        logging.info(f"Auto-processing (due to 'yes to all') low bitrate file: {file_path}")
                        # No print here, action print happens if should_remove_current_file is true
                    else:
                        user_response = input("Remove this file? (y/n/a/q - yes/no/yes to ALL subsequent/quit ALL subsequent): ").strip().lower()
                        
                        if user_response == 'y':
                            should_remove_current_file = True
                            logging.info(f"User chose 'yes' for low bitrate file: {file_path}")
                        elif user_response == 'a':
                            should_remove_current_file = True
                            remove_all_mode = True
                            logging.info(f"User chose 'yes to all'. Will remove current and all subsequent low bitrate files: {file_path}")
                        elif user_response == 'q':
                            quit_mode = True
                            logging.info("User chose 'quit'. Halting low bitrate file removal process.")
                            print("Quitting low bitrate file removal.")
                            break 
                        elif user_response == 'n':
                            logging.info(f"User chose 'no' for low bitrate file: {file_path}")
                            print(f"Skipped: {file_path}")
                        else:
                            print(f"Invalid input '{user_response}'. Skipped: {file_path}")
                            logging.warning(f"Invalid input '{user_response}' for low bitrate file {file_path}. Skipped.")

                    if should_remove_current_file:
                        action_prefix = "DRY RUN: Would remove" if args.dry_run else "Removing"
                        print(f"{action_prefix}: {file_path}")
                        logging.info(f"{action_prefix} low bitrate file: {file_path}")
                        if not args.dry_run:
                            try:
                                os.remove(file_path)
                                removed_count += 1
                            except OSError as e:
                                error_msg = f"Error removing {file_path}: {e}"
                                print(error_msg)
                                logging.error(error_msg)
                        elif args.dry_run: # Count simulated removals in dry run
                            removed_count += 1
                
                # Summarize actions after the loop
                if removed_count > 0:
                    action_verb = "simulated removing" if args.dry_run else "removed"
                    print(f"\nFinished low bitrate processing. {action_verb.capitalize()} {removed_count} file(s).")
                    logging.info(f"Finished low bitrate processing. {action_verb.capitalize()} {removed_count} file(s).")
                elif processed_in_prompt_loop > 0 and not quit_mode: 
                    # This means user was prompted for at least one file but chose not to remove any (or any that were chosen had errors)
                    # and didn't quit early.
                    print("\nFinished low bitrate processing. No files were removed based on your choices.")
                    logging.info("Finished low bitrate processing. No files were removed based on user choices (all 'n' or invalid responses).")
                elif quit_mode:
                    # Message for quitting is already printed. If removed_count > 0, that's covered.
                    # If removed_count is 0 and quit_mode, means quit before any 'y' or 'a'.
                    if removed_count == 0: # Ensure a message if quit before any action
                         print("\nLow bitrate file removal process was quit by user; no files were removed during this phase.")
                         logging.info("Low bitrate file removal process was quit by user; no files were removed during this phase.")
                # If processed_in_prompt_loop is 0, it implies low_bitrate_files was populated but all files vanished before prompt loop.
                # That case is covered by the os.path.exists check and the outer else.
            
            else: # No low_bitrate_files found after scan
                msg = f'No files identified with bitrates lower than {BITRATE_THRESHOLD/1000:.0f}kbps (or they were already removed by other operations).'
                print(msg)
                logging.info(msg)
        else: # User chose not to scan for low bitrate files
            print('\nScan for low bitrate files skipped by user.')
            logging.info("User chose not to scan for low bitrate files.")

    # --- Rename files based on metadata (conditionally) ---
    if args.rename_metadata:
        print("\n-- Renaming files based on metadata --")
        logging.info("Starting metadata-based renaming process.")
        files_to_rename_check = [f for f in audio_files if os.path.exists(f)]
        
        if not files_to_rename_check:
            msg = "No audio files found/remaining to consider for renaming."
            print(msg)
            logging.info(msg)
        else:
            print(f"Checking {len(files_to_rename_check)} audio file(s) for renaming (if metadata available)...")
            renamed_count = 0
            for file_path in tqdm(files_to_rename_check, desc="Renaming files", unit="file", smoothing=0.1, bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]'):
                if rename_files_from_metadata(file_path, args.dry_run):
                    renamed_count += 1
            
            if renamed_count > 0:
                summary_msg = f"Successfully {'simulated renaming of' if args.dry_run else 'renamed'} {renamed_count} file(s) based on metadata."
                print(summary_msg)
                logging.info(summary_msg)
            elif args.dry_run and len(files_to_rename_check) > 0 : 
                print("Dry run: No files were actually renamed.")
                logging.info("Dry run: No files were actually renamed.")
            else:
                msg = "No files were renamed based on metadata (either already correctly named, no metadata, errors, or dry run)."
                print(msg)
                logging.info(msg)
    else:
        print("\nSkipping renaming files based on metadata (use --rename-metadata to enable).")
        logging.info("Renaming based on metadata disabled by command-line flag or default.")

    print('\nFinished scanning and processing.')
    logging.info("Script finished.")

if __name__ == "__main__":
    try:
        import mutagen 
    except ImportError:
        print("CRITICAL: Mutagen library is not installed. This script cannot run without it.")
        print("Please install it using: pip install mutagen")
        exit(1) # Exit if mutagen cannot be imported
    main()
