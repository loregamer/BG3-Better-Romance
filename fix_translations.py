#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
import re
import xml.etree.ElementTree as ET
import shutil
import keyring
import json
import subprocess # Added for Divine.exe
import multiprocessing
from pathlib import Path
from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
                           QLabel, QLineEdit, QPushButton, QTextEdit, QFileDialog,
                           QProgressBar, QMessageBox, QCheckBox, QGridLayout, QStatusBar)
from PyQt6.QtCore import Qt, QThread, pyqtSignal

# Helper function for multiprocessing LSX conversion
def process_lsx_file_conversion(args):
    divine_exe_path, file_path_str, delete_original = args
    file_path_obj = Path(file_path_str)
    source_path = str(file_path_obj)
    filename = file_path_obj.name
    logs = []

    if filename.lower() == "meta.lsx":
        logs.append(f"Skipping: {source_path} (meta.lsx)")
        return {"status": "skipped", "path": source_path, "logs": logs}

    destination_path = file_path_obj.with_suffix(".lsf")
    command = [
        divine_exe_path,
        "--action", "convert-resource",
        "--game", "bg3",
        "--source", source_path,
        "--destination", str(destination_path),
        "--loglevel", "error"
    ]

    try:
        logs.append(f"Converting: {source_path} -> {destination_path}")
        process_run_args = {"capture_output": True, "text": True, "check": False}
        if sys.platform != "win32": # shell=False is default and preferred
            process_run_args["shell"] = False
        else: # On Windows, sometimes shell=True is needed for .exe if not in PATH
             # However, Divine.exe is called by full path, so shell=False should be fine.
             # Forcing shell=False for security and consistency.
            process_run_args["shell"] = False

        process = subprocess.run(command, **process_run_args)

        if process.returncode == 0:
            logs.append(f"Successfully converted: {destination_path}")
            if delete_original:
                try:
                    os.remove(source_path)
                    logs.append(f"Successfully deleted original file: {source_path}")
                except OSError as e:
                    logs.append(f"Error deleting original file {source_path}: {e}")
            return {"status": "converted", "path": source_path, "logs": logs}
        else:
            log_msg = f"Error converting {source_path}:\n"
            log_msg += f"  Return code: {process.returncode}\n"
            if process.stdout:
                log_msg += f"  Stdout: {process.stdout.strip()}\n"
            if process.stderr:
                log_msg += f"  Stderr: {process.stderr.strip()}\n"
            logs.append(log_msg)
            return {"status": "error", "path": source_path, "details": log_msg, "logs": logs}
    except Exception as e:
        error_msg = f"Exception during conversion of {source_path}: {e}"
        logs.append(error_msg)
        return {"status": "error", "path": source_path, "details": error_msg, "logs": logs}

class LsxConverterWorker(QThread):
    """Worker thread for converting LSX to LSF files."""
    progress_update = pyqtSignal(str)
    progress_percent = pyqtSignal(int)
    finished_signal = pyqtSignal(dict)
    error_signal = pyqtSignal(str)

    def __init__(self, search_dir, recursive=True):
        super().__init__()
        self.search_dir = search_dir
        self.recursive = recursive
        self.running = True
        self.divine_exe_path = os.path.join(os.getcwd(), "Tools", "Divine.exe")

    def run(self):
        try:
            if not os.path.exists(self.divine_exe_path):
                self.error_signal.emit(f"Error: Divine.exe not found at {self.divine_exe_path}")
                return

            self.progress_update.emit(f"Starting LSX to LSF conversion using Divine.exe from: {self.divine_exe_path}")
            self.progress_update.emit(f"Scanning directory: {self.search_dir}")

            search_path_obj = Path(self.search_dir)
            if self.recursive:
                self.progress_update.emit(f"Scanning directory recursively: {search_path_obj}")
                all_files_in_dir = [f for f in search_path_obj.rglob("*.lsx") if f.is_file()]
            else:
                self.progress_update.emit(f"Scanning directory (non-recursively): {search_path_obj}")
                all_files_in_dir = [f for f in search_path_obj.glob("*.lsx") if f.is_file()]
            
            files_to_scan = [
                f for f in all_files_in_dir
                if ".git" not in f.parts and "Tools" not in f.parts
            ]

            total_files = len(files_to_scan)
            self.progress_update.emit(f"Found {total_files} .lsx files to potentially convert.")

            if total_files == 0:
                self.progress_percent.emit(100)
                self.finished_signal.emit({"converted_files": 0, "skipped_files": 0, "error_files": [], "total_scanned": 0})
                return

            converted_files = 0
            skipped_files = 0
            error_files = []
            
            # True for delete_original, matching original behavior
            tasks = [(self.divine_exe_path, str(f_obj), True) for f_obj in files_to_scan]

            # Determine number of processes: min of files, cpu_count, or a sensible max like 8 if cpu_count is very high
            # This prevents creating too many processes for few files or overwhelming system with too many.
            # A practical limit like 16 or 32 could also be considered if cpu_count() is excessively large.
            max_processes = multiprocessing.cpu_count()
            num_processes = min(total_files, max_processes)
            if num_processes == 0 and total_files > 0 : # Ensure at least one process if there are files
                num_processes = 1

            self.progress_update.emit(f"Using {num_processes} worker processes.")

            processed_count = 0
            try:
                with multiprocessing.Pool(processes=num_processes) as pool:
                    results_iterator = pool.imap_unordered(process_lsx_file_conversion, tasks)
                    
                    for i, result_dict in enumerate(results_iterator):
                        processed_count = i + 1
                        if not self.running:
                            self.progress_update.emit("LSX Conversion Canceled by user.")
                            break
                        
                        for log_message in result_dict.get("logs", []):
                            self.progress_update.emit(log_message)

                        status = result_dict["status"]
                        if status == "converted":
                            converted_files += 1
                        elif status == "skipped":
                            skipped_files += 1
                        elif status == "error":
                            error_files.append(result_dict["path"])
                        
                        self.progress_percent.emit(int(((processed_count) / total_files) * 100))
            except Exception as e: # Catch exceptions during pool operations
                self.error_signal.emit(f"Error during multiprocessing pool execution: {str(e)}")
                # Fall through to emit finished_signal with current counts

            # Ensure progress bar reaches 100% if not cancelled early and all files processed
            if self.running and processed_count == total_files:
                 self.progress_percent.emit(100)
            elif not self.running : # If cancelled, emit current progress
                 self.progress_percent.emit(int(((processed_count) / total_files) * 100))


            final_result = {
                "converted_files": converted_files,
                "skipped_files": skipped_files,
                "error_files": error_files,
                "total_scanned": processed_count # Use processed_count in case of cancellation
            }
            self.finished_signal.emit(final_result)

        except Exception as e:
            self.error_signal.emit(f"Error in LsxConverterWorker: {str(e)}")
            # Emit finished with whatever data is available if an unexpected error occurs early
            # This might be redundant if the inner try-except for the pool handles it.
            # Consider if a default/empty result should be emitted here.
            # For now, relying on error_signal and the pool's own final_result emission.

    def stop(self):
        """Stop the worker thread."""
        self.running = False

# Helper function for multiprocessing file content replacement
def process_single_file_for_xml_replacement(args):
    """Process a single file for XML content replacement (used with multiprocessing)"""
    file_path, replacements, original_contents, backup, loglevel = args
    result = {
        "file_path": str(file_path),
        "modified": False,
        "error": None,
        "logs": [],
        "debug_info": {"file": str(file_path), "matching_ids": [], "changes": []}
    }
    
    def log(message, level=0, prefix=""):
        if level <= loglevel:
            log_entry = f"{prefix}{message}"
            result["logs"].append(log_entry)
    
    # Skip .git directories and common binary files
    str_path = str(file_path)
    if ".git" in str_path:
        return result
        
    file_extension = file_path.suffix.lower() if isinstance(file_path, Path) else Path(file_path).suffix.lower()
    # Fast binary check - skip common binary extensions
    if file_extension in ['.pak', '.lsf', '.bin', '.exe', '.dll', '.so', '.dylib', '.jpg', '.png', '.ttf', '.dat', '.db']:
        return result
        
    # Check if this is an LSX or LSJ file for special handling
    is_lsx_file = file_extension == '.lsx'
    is_lsj_file = file_extension == '.lsj'
    
    # Add debug info about file type
    if is_lsx_file:
        log(f"Processing LSX file: {file_path}", 2)
    elif is_lsj_file:
        log(f"Processing LSJ file: {file_path}", 2)
        
    try:
        # Try to read file as text
        content = None
        encoding_used = None
        for encoding in ['utf-8', 'latin-1']:
            try:
                with open(file_path, 'r', encoding=encoding) as f:
                    content = f.read()
                encoding_used = encoding
                break
            except UnicodeDecodeError:
                continue
            except Exception as e:
                log(f"Error reading {file_path} with {encoding}: {str(e)}", 1)
                continue
        
        if content is None:
            # Couldn't read file with any encoding
            return result
        
        # Check if any replacement is needed
        needs_update = False
        matching_ids = []
        for old_uid in replacements.keys():
            if old_uid in content:
                needs_update = True
                matching_ids.append(old_uid)
        
        if not needs_update:
            return result
            
        # Create backup if needed
        if backup:
            backup_path = f"{file_path}.backup"
            try:
                import shutil
                shutil.copy2(file_path, backup_path)
                log(f"Created backup: {backup_path}", 1)
            except Exception as e:
                log(f"Error creating backup for {file_path}: {str(e)}", 0, "Error: ")
                # Continue execution even if backup fails
        
        # Add matching IDs to debug info
        result["debug_info"]["matching_ids"] = matching_ids
        
        # Make replacements
        modified_content = content
        for old_uid, new_uid in replacements.items():
            if old_uid in content:
                # For specific file types, log additional debug info
                if is_lsx_file:
                    log(f"Found ID '{old_uid}' in LSX file, will replace with '{new_uid}'", 2)
                elif is_lsj_file:
                    log(f"Found ID '{old_uid}' in LSJ file, will replace with '{new_uid}'", 2)
                # Store original version from original XML
                original_version = original_contents[new_uid]["version"]
                
                # Apply common patterns for all file types
                # Pattern 1: contentuid="ID" version="VER"
                pattern1 = fr'contentuid="{re.escape(old_uid)}"\s*version="[^"]*"'
                replacement1 = f'contentuid="{new_uid}" version="{original_version}"'
                modified_content_1 = re.sub(pattern1, replacement1, modified_content)
                if modified_content_1 != modified_content:
                    result["debug_info"]["changes"].append(f"Pattern 1 matched for {old_uid}")
                modified_content = modified_content_1
                
                # Pattern 2: contentuid="ID"
                pattern2 = fr'contentuid="{re.escape(old_uid)}"'
                replacement2 = f'contentuid="{new_uid}"'
                modified_content_2 = re.sub(pattern2, replacement2, modified_content)
                if modified_content_2 != modified_content:
                    result["debug_info"]["changes"].append(f"Pattern 2 matched for {old_uid}")
                modified_content = modified_content_2
                
                # Pattern 3: ID LSX style
                pattern3 = fr'id="{re.escape(old_uid)}"'
                replacement3 = f'id="{new_uid}"'
                modified_content_3 = re.sub(pattern3, replacement3, modified_content)
                if modified_content_3 != modified_content:
                    result["debug_info"]["changes"].append(f"Pattern 3 matched for {old_uid}")
                modified_content = modified_content_3
                
                # Apply file-specific patterns
                if is_lsx_file:
                    # LSX-specific patterns
                    # Pattern 5: LSX TagText TranslatedString handle format
                    pattern5 = fr'<attribute id="TagText" type="TranslatedString" handle="{re.escape(old_uid)}" version="[^"]*" />'
                    replacement5 = f'<attribute id="TagText" type="TranslatedString" handle="{new_uid}" version="{original_version}" />'
                    modified_content_5 = re.sub(pattern5, replacement5, modified_content)
                    if modified_content_5 != modified_content:
                        result["debug_info"]["changes"].append(f"Pattern 5 matched for {old_uid} - LSX TranslatedString handle")
                    modified_content = modified_content_5
                    
                    # Pattern 6: LSX TranslatedString handle format - alternate
                    pattern6 = fr'<attribute id="TagText" type="TranslatedString" handle="{re.escape(old_uid)}" version="(\d+)"'
                    replacement6 = f'<attribute id="TagText" type="TranslatedString" handle="{new_uid}" version="{original_version}"'
                    modified_content_6 = re.sub(pattern6, replacement6, modified_content)
                    if modified_content_6 != modified_content:
                        result["debug_info"]["changes"].append(f"Pattern 6 matched for {old_uid} - LSX TranslatedString handle alternate")
                    modified_content = modified_content_6
                    
                    # Pattern 9: Specific format from the example (.lsx)
                    pattern9 = fr'<node id="TagText">\s+<attribute id="TagText" type="TranslatedString" handle="{re.escape(old_uid)}" version="[^"]*" />'
                    replacement9 = f'<node id="TagText">\n\t\t\t\t\t\t\t\t\t\t\t<attribute id="TagText" type="TranslatedString" handle="{new_uid}" version="{original_version}" />'
                    modified_content_9 = re.sub(pattern9, replacement9, modified_content)
                    if modified_content_9 != modified_content:
                        result["debug_info"]["changes"].append(f"Pattern 9 matched for {old_uid} - Specific LSX example format")
                    modified_content = modified_content_9
                
                elif is_lsj_file:
                    # LSJ-specific patterns
                    # Pattern 7: LSJ TranslatedString handle format in JSON
                    pattern7 = fr'"handle" : "{re.escape(old_uid)}",\s*"type" : "TranslatedString",\s*"version" : \d+'
                    replacement7 = f'"handle" : "{new_uid}", "type" : "TranslatedString", "version" : {original_version}'
                    modified_content_7 = re.sub(pattern7, replacement7, modified_content)
                    if modified_content_7 != modified_content:
                        result["debug_info"]["changes"].append(f"Pattern 7 matched for {old_uid} - LSJ TranslatedString handle")
                    modified_content = modified_content_7
                    
                    # Pattern 8: LSJ TranslatedString handle format - alternate
                    pattern8 = fr'"handle" : "{re.escape(old_uid)}"'
                    replacement8 = f'"handle" : "{new_uid}"'
                    modified_content_8 = re.sub(pattern8, replacement8, modified_content)
                    if modified_content_8 != modified_content:
                        result["debug_info"]["changes"].append(f"Pattern 8 matched for {old_uid} - LSJ handle only")
                    modified_content = modified_content_8
                    
                    # Pattern 10: Specific format from the example (.lsj)
                    pattern10 = fr'"TagText" : {{\s+"handle" : "{re.escape(old_uid)}",\s+"type" : "TranslatedString",\s+"version" : \d+\s+}}'
                    replacement10 = f'"TagText" : {{\n                                                   "handle" : "{new_uid}",\n                                                   "type" : "TranslatedString",\n                                                   "version" : {original_version}\n                                                }}'
                    modified_content_10 = re.sub(pattern10, replacement10, modified_content)
                    if modified_content_10 != modified_content:
                        result["debug_info"]["changes"].append(f"Pattern 10 matched for {old_uid} - Specific LSJ example format")
                    modified_content = modified_content_10
                
                else:
                    # For other file types, just check for quoted IDs
                    pattern4 = fr'"{re.escape(old_uid)}"'
                    replacement4 = f'"{new_uid}"'
                    modified_content_4 = re.sub(pattern4, replacement4, modified_content)
                    if modified_content_4 != modified_content:
                        result["debug_info"]["changes"].append(f"Pattern 4 matched for {old_uid} - quoted ID in other file type")
                    modified_content = modified_content_4
        
        # Only write if content changed
        if modified_content != content:
            try:
                with open(file_path, 'w', encoding=encoding_used) as f:
                    f.write(modified_content)
                result["modified"] = True
                log(f"Updated file: {file_path}", 1)
                return result
            except Exception as e:
                error_msg = f"Error writing to {file_path}: {str(e)}"
                log(error_msg, 0, "Error: ")
                result["error"] = error_msg
                return result
        else:
            return result
        
    except Exception as e:
        error_msg = f"Error in process_single_file_for_xml_replacement for {file_path}: {str(e)}"
        log(error_msg, 0, "Error: ")
        result["error"] = error_msg
        return result

# Helper function for multiprocessing LSF conversion
def process_lsf_file_conversion(args):
    divine_exe_path, file_path_str, delete_original = args
    file_path_obj = Path(file_path_str)
    source_path = str(file_path_obj)
    filename = file_path_obj.name
    logs = []

    if filename.lower() == "meta.lsf": # Note: .lsf check
        logs.append(f"Skipping: {source_path} (meta.lsf)")
        return {"status": "skipped", "path": source_path, "logs": logs}

    destination_path = file_path_obj.with_suffix(".lsx") # Note: .lsx destination
    command = [
        divine_exe_path,
        "--action", "convert-resource",
        "--game", "bg3",
        "--source", source_path,
        "--destination", str(destination_path),
        "--loglevel", "error"
    ]

    try:
        logs.append(f"Converting: {source_path} -> {destination_path}")
        process_run_args = {"capture_output": True, "text": True, "check": False}
        if sys.platform != "win32":
            process_run_args["shell"] = False
        else:
            process_run_args["shell"] = False # Keep shell=False for Divine.exe

        process = subprocess.run(command, **process_run_args)

        if process.returncode == 0:
            logs.append(f"Successfully converted: {destination_path}")
            if delete_original:
                try:
                    os.remove(source_path)
                    logs.append(f"Successfully deleted original file: {source_path}")
                except OSError as e:
                    logs.append(f"Error deleting original file {source_path}: {e}")
            return {"status": "converted", "path": source_path, "logs": logs}
        else:
            log_msg = f"Error converting {source_path}:\n"
            log_msg += f"  Return code: {process.returncode}\n"
            if process.stdout:
                log_msg += f"  Stdout: {process.stdout.strip()}\n"
            if process.stderr:
                log_msg += f"  Stderr: {process.stderr.strip()}\n"
            logs.append(log_msg)
            return {"status": "error", "path": source_path, "details": log_msg, "logs": logs}
    except Exception as e:
        error_msg = f"Exception during conversion of {source_path}: {e}"
        logs.append(error_msg)
        return {"status": "error", "path": source_path, "details": error_msg, "logs": logs}

class LsfConverterWorker(QThread):
    """Worker thread for converting LSF to LSX files."""
    progress_update = pyqtSignal(str)
    progress_percent = pyqtSignal(int)
    finished_signal = pyqtSignal(dict)
    error_signal = pyqtSignal(str)

    def __init__(self, search_dir, recursive=True):
        super().__init__()
        self.search_dir = search_dir
        self.recursive = recursive
        self.running = True
        self.divine_exe_path = os.path.join(os.getcwd(), "Tools", "Divine.exe")

    def run(self):
        try:
            if not os.path.exists(self.divine_exe_path):
                self.error_signal.emit(f"Error: Divine.exe not found at {self.divine_exe_path}")
                return

            self.progress_update.emit(f"Starting LSF to LSX conversion using Divine.exe from: {self.divine_exe_path}")
            self.progress_update.emit(f"Scanning directory: {self.search_dir}")

            search_path_obj = Path(self.search_dir)
            if self.recursive:
                self.progress_update.emit(f"Scanning directory recursively: {search_path_obj}")
                all_files_in_dir = [f for f in search_path_obj.rglob("*.lsf") if f.is_file()] # Note: .lsf
            else:
                self.progress_update.emit(f"Scanning directory (non-recursively): {search_path_obj}")
                all_files_in_dir = [f for f in search_path_obj.glob("*.lsf") if f.is_file()] # Note: .lsf
            
            files_to_scan = [
                f for f in all_files_in_dir
                if ".git" not in f.parts and "Tools" not in f.parts
            ]

            total_files = len(files_to_scan)
            self.progress_update.emit(f"Found {total_files} .lsf files to potentially convert.")

            if total_files == 0:
                self.progress_percent.emit(100)
                self.finished_signal.emit({"converted_files": 0, "skipped_files": 0, "error_files": [], "total_scanned": 0})
                return

            converted_files = 0
            skipped_files = 0
            error_files = []
            
            tasks = [(self.divine_exe_path, str(f_obj), True) for f_obj in files_to_scan]

            max_processes = multiprocessing.cpu_count()
            num_processes = min(total_files, max_processes)
            if num_processes == 0 and total_files > 0 :
                num_processes = 1
            
            self.progress_update.emit(f"Using {num_processes} worker processes.")

            processed_count = 0
            try:
                with multiprocessing.Pool(processes=num_processes) as pool:
                    results_iterator = pool.imap_unordered(process_lsf_file_conversion, tasks) # Use new helper
                    
                    for i, result_dict in enumerate(results_iterator):
                        processed_count = i + 1
                        if not self.running:
                            self.progress_update.emit("LSF Conversion Canceled by user.")
                            break
                        
                        for log_message in result_dict.get("logs", []):
                            self.progress_update.emit(log_message)

                        status = result_dict["status"]
                        if status == "converted":
                            converted_files += 1
                        elif status == "skipped":
                            skipped_files += 1
                        elif status == "error":
                            error_files.append(result_dict["path"])
                        
                        self.progress_percent.emit(int(((processed_count) / total_files) * 100))
            except Exception as e:
                self.error_signal.emit(f"Error during multiprocessing pool execution: {str(e)}")


            if self.running and processed_count == total_files:
                 self.progress_percent.emit(100)
            elif not self.running :
                 self.progress_percent.emit(int(((processed_count) / total_files) * 100))

            final_result = {
                "converted_files": converted_files,
                "skipped_files": skipped_files,
                "error_files": error_files,
                "total_scanned": processed_count
            }
            self.finished_signal.emit(final_result)

        except Exception as e:
            self.error_signal.emit(f"Error in LsfConverterWorker: {str(e)}")

    def stop(self):
        """Stop the worker thread."""
        self.running = False


class XMLWorker(QThread):
    """Worker thread for processing XML files."""
    progress_update = pyqtSignal(str)
    progress_percent = pyqtSignal(int)
    finished_signal = pyqtSignal(dict)
    error_signal = pyqtSignal(str)
    
    def __init__(self, original_file, new_file, search_dir, recursive=True, backup=True, processes=None):
        super().__init__()
        self.original_file = original_file
        self.new_file = new_file
        self.search_dir = search_dir
        self.recursive = recursive
        self.backup = backup
        self.running = True
        # Set the number of processes for multiprocessing
        self.processes = processes if processes is not None else multiprocessing.cpu_count()
        self.loglevel = 1  # Default log level for worker
        
    def run(self):
        try:
            # Read XML files
            self.progress_update.emit("Reading original XML file...")
            original_tree = ET.parse(self.original_file)
            original_root = original_tree.getroot()
            
            self.progress_update.emit("Reading new XML file...")
            new_tree = ET.parse(self.new_file)
            new_root = new_tree.getroot()
            
            # Extract content nodes
            self.progress_update.emit("Extracting content nodes from original XML...")
            original_contents = {}
            for elem in original_root.findall(".//content"):
                if "contentuid" in elem.attrib:
                    contentuid = elem.attrib["contentuid"]
                    version = elem.attrib.get("version", "")
                    original_contents[contentuid] = {
                        "element": elem,
                        "version": version,
                        "text": elem.text
                    }
            
            self.progress_update.emit(f"Found {len(original_contents)} content nodes in original XML.")
            
            self.progress_update.emit("Extracting content nodes from new XML...")
            new_contents = {}
            nodes_to_delete = []
            replacements = {}
            
            for elem in new_root.findall(".//content"):
                if "contentuid" in elem.attrib:
                    contentuid = elem.attrib["contentuid"]
                    version = elem.attrib.get("version", "")
                    new_contents[contentuid] = {
                        "element": elem,
                        "version": version,
                        "text": elem.text
                    }
                    
                    # Check if this contentuid exists in original with different version
                    if contentuid in original_contents:
                        orig_version = original_contents[contentuid]["version"]
                        orig_text = original_contents[contentuid]["text"]
                        curr_text = elem.text
                        # Only revert the version if the contents are the same
                        if version != orig_version and orig_text == curr_text:
                            self.progress_update.emit(f"Found match with different version but same content: {contentuid}")
                            self.progress_update.emit(f"  Original version: {orig_version}, New version: {version}")
                            nodes_to_delete.append(elem)
                            replacements[contentuid] = contentuid  # Store original ID for replacement
                        elif version != orig_version:
                            self.progress_update.emit(f"Found match with different version and different content: {contentuid}")
                            self.progress_update.emit(f"  Not reverting version as content is different")
            
            self.progress_update.emit(f"Found {len(new_contents)} content nodes in new XML.")
            self.progress_update.emit(f"Identified {len(nodes_to_delete)} nodes to delete.")
            self.progress_update.emit(f"IDs to replace: {list(replacements.keys())[:5]}..." if replacements else "No replacements needed.")
            
            # Delete nodes from new XML
            if nodes_to_delete:
                self.progress_update.emit("Deleting nodes from new XML...")
                for elem in nodes_to_delete:
                    parent = elem.getparent() if hasattr(elem, 'getparent') else self._find_parent(new_root, elem)
                    if parent is not None:
                        parent.remove(elem)
                
                # Save modified new XML
                if self.backup:
                    backup_path = f"{self.new_file}.backup"
                    self.progress_update.emit(f"Creating backup of new XML at {backup_path}")
                    shutil.copy2(self.new_file, backup_path)
                
                self.progress_update.emit(f"Saving modified new XML to {self.new_file}")
                new_tree.write(self.new_file, encoding="utf-8", xml_declaration=True)
            
            # Replace contentuid in all files
            if replacements:
                self.progress_update.emit("Replacing contentuid in files...")
                self._replace_in_files(replacements, original_contents)
            
            result = {
                "nodes_deleted": len(nodes_to_delete),
                "replacements": len(replacements),
                "files_modified": self.files_modified if hasattr(self, "files_modified") else 0
            }
            self.finished_signal.emit(result)
            
        except Exception as e:
            self.error_signal.emit(f"Error: {str(e)}")
    
    def _find_parent(self, root, elem):
        """Find parent of an element in ElementTree."""
        for parent in root.iter():
            for child in list(parent):
                if child == elem:
                    return parent
        return None
    
    def _replace_in_files(self, replacements, original_contents):
        """Replace contentuid in all files in search directory using multiprocessing (except english.xml)."""
        search_path = Path(self.search_dir)
        files_to_process = []
        
        # Get list of files
        if self.recursive:
            self.progress_update.emit(f"Scanning directory recursively: {search_path}")
            all_files = [f for f in search_path.rglob("*") if f.is_file()]
        else:
            self.progress_update.emit(f"Scanning directory: {search_path}")
            all_files = [f for f in search_path.glob("*") if f.is_file()]
        
        # Filter out english.xml files and .git directory files
        filtered_files = [f for f in all_files if f.name.lower() != "english.xml" and ".git" not in str(f)]
        
        self.progress_update.emit(f"Found {len(filtered_files)} files to process (excluding english.xml files).")
        
        if not filtered_files:
            self.progress_update.emit("No files to process.")
            return True

        # Initialize counters
        self.files_modified = 0
        self.debug_info = []  # Store debug info for each file
        
        # Create a list of arguments for multiprocessing
        args_list = [(file_path, replacements, original_contents, self.backup, self.loglevel) 
                     for file_path in filtered_files]
        
        # Calculate optimal number of processes
        total_files = len(filtered_files)
        # Adjust process count to avoid creating too many processes for few files
        num_processes = min(total_files, self.processes)
        if num_processes <= 0 and total_files > 0:
            num_processes = 1
            
        self.progress_update.emit(f"Starting file processing with {num_processes} worker processes.")
        
        processed_count = 0
        try:
            with multiprocessing.Pool(processes=num_processes) as pool:
                # Use imap_unordered for better performance with incremental results
                results_iterator = pool.imap_unordered(process_single_file_for_xml_replacement, args_list)
                
                for i, result in enumerate(results_iterator):
                    if not self.running:
                        self.progress_update.emit("Operation canceled.")
                        # This will not immediately stop running workers, but no new tasks will be started
                        pool.terminate()
                        break
                    
                    processed_count = i + 1
                    progress = int((processed_count / total_files) * 100)
                    self.progress_percent.emit(progress)
                    
                    # Process logs
                    for log_entry in result.get("logs", []):
                        self.progress_update.emit(log_entry)
                    
                    # Update stats
                    if result["modified"]:
                        self.files_modified += 1
                        if self.files_modified <= 5:  # Show only first 5 for brevity
                            self.progress_update.emit(f"Modified file: {result['file_path']}")
                        elif self.files_modified == 6:
                            self.progress_update.emit("More files modified...")
                    
                    # Store debug info if there are changes
                    if result["debug_info"]["changes"]:
                        self.debug_info.append(result["debug_info"])
                    
                    # Handle errors
                    if result["error"]:
                        self.progress_update.emit(f"Error: {result['error']}")
        except Exception as e:
            self.progress_update.emit(f"Error during multiprocessing: {str(e)}")
            self.error_signal.emit(f"Error during multiprocessing: {str(e)}")
            return False
        
        # Ensure progress bar reaches 100% if not cancelled
        if self.running and processed_count == total_files:
            self.progress_percent.emit(100)
        elif not self.running:
            # If cancelled, emit current progress
            self.progress_percent.emit(int((processed_count / total_files) * 100))
        
        self.progress_update.emit(f"Replacement complete. Modified {self.files_modified} files.")
        
        # Display debug info for the first few modified files
        if self.debug_info:
            for info in self.debug_info[:3]:  # Show only first 3 for brevity
                self.progress_update.emit(f"Debug info: {info}")
                
        return True
    
    def _process_file(self, file_path, replacements, original_contents):
        """Process a single file and return True if it was modified."""
        # Skip .git directories and common binary files
        str_path = str(file_path)
        if ".git" in str_path:
            return False
            
        file_extension = file_path.suffix.lower()
        # Fast binary check - skip common binary extensions
        if file_extension in ['.pak', '.lsf', '.bin', '.exe', '.dll', '.so', '.dylib', '.jpg', '.png', '.ttf', '.dat', '.db']:
            return False
            
        # Check if this is an LSX or LSJ file for special handling
        is_lsx_file = file_extension == '.lsx'
        is_lsj_file = file_extension == '.lsj'
        
        # Add debug info about file type
        if is_lsx_file:
            self.progress_update.emit(f"Processing LSX file: {file_path}")
        elif is_lsj_file:
            self.progress_update.emit(f"Processing LSJ file: {file_path}")
        else:
            # Normal file processing without additional debug info
            pass
            
        try:
            # Try to read file as text
            content = None
            encoding_used = None
            for encoding in ['utf-8', 'latin-1']:
                try:
                    with open(file_path, 'r', encoding=encoding) as f:
                        content = f.read()
                    encoding_used = encoding
                    break
                except UnicodeDecodeError:
                    continue
                except Exception as e:
                    self.progress_update.emit(f"Error reading {file_path} with {encoding}: {str(e)}")
                    continue
            
            if content is None:
                # Couldn't read file with any encoding
                return False
            
            # Check if any replacement is needed
            needs_update = False
            matching_ids = []
            for old_uid in replacements.keys():
                if old_uid in content:
                    needs_update = True
                    matching_ids.append(old_uid)
            
            if not needs_update:
                return False
                
            # Create backup if needed
            if self.backup:
                backup_path = f"{file_path}.backup"
                shutil.copy2(file_path, backup_path)
            
            # Prepare debug info
            debug_info = {"file": str(file_path), "matching_ids": matching_ids, "changes": []}
            
            # Make replacements
            modified_content = content
            for old_uid, new_uid in replacements.items():
                if old_uid in content:
                    # For specific file types, log additional debug info
                    if is_lsx_file:
                        self.progress_update.emit(f"Found ID '{old_uid}' in LSX file, will replace with '{new_uid}'")
                    elif is_lsj_file:
                        self.progress_update.emit(f"Found ID '{old_uid}' in LSJ file, will replace with '{new_uid}'")
                    # Store original version from original XML
                    original_version = original_contents[new_uid]["version"]
                    
                    # Apply common patterns for all file types
                    # Pattern 1: contentuid="ID" version="VER"
                    pattern1 = fr'contentuid="{re.escape(old_uid)}"\s*version="[^"]*"'
                    replacement1 = f'contentuid="{new_uid}" version="{original_version}"'
                    modified_content_1 = re.sub(pattern1, replacement1, modified_content)
                    if modified_content_1 != modified_content:
                        debug_info["changes"].append(f"Pattern 1 matched for {old_uid}")
                    modified_content = modified_content_1
                    
                    # Pattern 2: contentuid="ID"
                    pattern2 = fr'contentuid="{re.escape(old_uid)}"'
                    replacement2 = f'contentuid="{new_uid}"'
                    modified_content_2 = re.sub(pattern2, replacement2, modified_content)
                    if modified_content_2 != modified_content:
                        debug_info["changes"].append(f"Pattern 2 matched for {old_uid}")
                    modified_content = modified_content_2
                    
                    # Pattern 3: ID LSX style
                    pattern3 = fr'id="{re.escape(old_uid)}"'
                    replacement3 = f'id="{new_uid}"'
                    modified_content_3 = re.sub(pattern3, replacement3, modified_content)
                    if modified_content_3 != modified_content:
                        debug_info["changes"].append(f"Pattern 3 matched for {old_uid}")
                    modified_content = modified_content_3
                    
                    # Apply file-specific patterns
                    if is_lsx_file:
                        # LSX-specific patterns
                        # Pattern 5: LSX TagText TranslatedString handle format
                        pattern5 = fr'<attribute id="TagText" type="TranslatedString" handle="{re.escape(old_uid)}" version="[^"]*" />'
                        replacement5 = f'<attribute id="TagText" type="TranslatedString" handle="{new_uid}" version="{original_version}" />'
                        modified_content_5 = re.sub(pattern5, replacement5, modified_content)
                        if modified_content_5 != modified_content:
                            debug_info["changes"].append(f"Pattern 5 matched for {old_uid} - LSX TranslatedString handle")
                        modified_content = modified_content_5
                        
                        # Pattern 6: LSX TranslatedString handle format - alternate
                        pattern6 = fr'<attribute id="TagText" type="TranslatedString" handle="{re.escape(old_uid)}" version="(\d+)"'
                        replacement6 = f'<attribute id="TagText" type="TranslatedString" handle="{new_uid}" version="{original_version}"'
                        modified_content_6 = re.sub(pattern6, replacement6, modified_content)
                        if modified_content_6 != modified_content:
                            debug_info["changes"].append(f"Pattern 6 matched for {old_uid} - LSX TranslatedString handle alternate")
                        modified_content = modified_content_6
                        
                        # Pattern 9: Specific format from the example (.lsx)
                        pattern9 = fr'<node id="TagText">\s+<attribute id="TagText" type="TranslatedString" handle="{re.escape(old_uid)}" version="[^"]*" />'
                        replacement9 = f'<node id="TagText">\n\t\t\t\t\t\t\t\t\t\t\t<attribute id="TagText" type="TranslatedString" handle="{new_uid}" version="{original_version}" />'
                        modified_content_9 = re.sub(pattern9, replacement9, modified_content)
                        if modified_content_9 != modified_content:
                            debug_info["changes"].append(f"Pattern 9 matched for {old_uid} - Specific LSX example format")
                        modified_content = modified_content_9
                    
                    elif is_lsj_file:
                        # LSJ-specific patterns
                        # Pattern 7: LSJ TranslatedString handle format in JSON
                        pattern7 = fr'"handle" : "{re.escape(old_uid)}",\s*"type" : "TranslatedString",\s*"version" : \d+'
                        replacement7 = f'"handle" : "{new_uid}", "type" : "TranslatedString", "version" : {original_version}'
                        modified_content_7 = re.sub(pattern7, replacement7, modified_content)
                        if modified_content_7 != modified_content:
                            debug_info["changes"].append(f"Pattern 7 matched for {old_uid} - LSJ TranslatedString handle")
                        modified_content = modified_content_7
                        
                        # Pattern 8: LSJ TranslatedString handle format - alternate
                        pattern8 = fr'"handle" : "{re.escape(old_uid)}"'
                        replacement8 = f'"handle" : "{new_uid}"'
                        modified_content_8 = re.sub(pattern8, replacement8, modified_content)
                        if modified_content_8 != modified_content:
                            debug_info["changes"].append(f"Pattern 8 matched for {old_uid} - LSJ handle only")
                        modified_content = modified_content_8
                        
                        # Pattern 10: Specific format from the example (.lsj)
                        pattern10 = fr'"TagText" : {{\s+"handle" : "{re.escape(old_uid)}",\s+"type" : "TranslatedString",\s+"version" : \d+\s+}}'
                        replacement10 = f'"TagText" : {{\n                                                   "handle" : "{new_uid}",\n                                                   "type" : "TranslatedString",\n                                                   "version" : {original_version}\n                                                }}'
                        modified_content_10 = re.sub(pattern10, replacement10, modified_content)
                        if modified_content_10 != modified_content:
                            debug_info["changes"].append(f"Pattern 10 matched for {old_uid} - Specific LSJ example format")
                        modified_content = modified_content_10
                    
                    else:
                        # For other file types, just check for quoted IDs
                        pattern4 = fr'"{re.escape(old_uid)}"'
                        replacement4 = f'"{new_uid}"'
                        modified_content_4 = re.sub(pattern4, replacement4, modified_content)
                        if modified_content_4 != modified_content:
                            debug_info["changes"].append(f"Pattern 4 matched for {old_uid} - quoted ID in other file type")
                        modified_content = modified_content_4
            
            # Store debug info if changes were made
            if len(debug_info["changes"]) > 0:
                self.debug_info.append(debug_info)
            
            # Only write if content changed
            if modified_content != content:
                with open(file_path, 'w', encoding=encoding_used) as f:
                    f.write(modified_content)
                return True
            else:
                return False
            
        except Exception as e:
            self.progress_update.emit(f"Error in _process_file for {file_path}: {str(e)}")
            return False
    
    def stop(self):
        """Stop the worker thread."""
        self.running = False


class XMLContentManager(QMainWindow):
    """Main application window."""
    
    # Keyring service name
    KEYRING_SERVICE = "XMLContentManager"
    
    # Keyring keys
    KEYRING_KEY = "saved_settings"
    
    def __init__(self):
        super().__init__()
        self.init_ui()
        self.xml_worker = None
        self.lsf_worker = None
        self.lsx_worker = None # Added for LSX to LSF conversion
        self.load_saved_settings()
    
    def init_ui(self):
        """Initialize the user interface."""
        self.setWindowTitle("XML Content Manager - Fixed Version")
        self.setGeometry(100, 100, 800, 600)
        
        # Main widget and layout
        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        main_layout = QVBoxLayout(main_widget)
        
        # Input section
        input_layout = QGridLayout()
        
        # Original XML file
        input_layout.addWidget(QLabel("Original XML File:"), 0, 0)
        self.original_file_edit = QLineEdit()
        input_layout.addWidget(self.original_file_edit, 0, 1)
        browse_original_btn = QPushButton("Browse...")
        browse_original_btn.clicked.connect(self.browse_original_file)
        input_layout.addWidget(browse_original_btn, 0, 2)
        
        # New XML file
        input_layout.addWidget(QLabel("New XML File:"), 1, 0)
        self.new_file_edit = QLineEdit()
        input_layout.addWidget(self.new_file_edit, 1, 1)
        browse_new_btn = QPushButton("Browse...")
        browse_new_btn.clicked.connect(self.browse_new_file)
        input_layout.addWidget(browse_new_btn, 1, 2)
        
        # Search directory
        input_layout.addWidget(QLabel("Search Directory:"), 2, 0)
        self.search_dir_edit = QLineEdit()
        input_layout.addWidget(self.search_dir_edit, 2, 1)
        browse_dir_btn = QPushButton("Browse...")
        browse_dir_btn.clicked.connect(self.browse_search_dir)
        input_layout.addWidget(browse_dir_btn, 2, 2)
        
        main_layout.addLayout(input_layout)
        
        # Options section
        options_layout = QHBoxLayout()
        
        self.recursive_check = QCheckBox("Search Recursively")
        self.recursive_check.setChecked(True)
        self.recursive_check.setToolTip("Search in all subdirectories")
        options_layout.addWidget(self.recursive_check)
        
        self.backup_check = QCheckBox("Create Backups")
        self.backup_check.setChecked(True)
        self.backup_check.setToolTip("Create backup files before modifying")
        options_layout.addWidget(self.backup_check)
        
        main_layout.addLayout(options_layout)
        
        # Action buttons
        buttons_layout = QHBoxLayout()
        
        # self.analyze_btn = QPushButton("Analyze Files") # Removed
        # self.analyze_btn.clicked.connect(self.analyze_files) # Removed
        # buttons_layout.addWidget(self.analyze_btn) # Removed
        
        self.process_btn = QPushButton("Process Files")
        self.process_btn.clicked.connect(self.process_files)
        buttons_layout.addWidget(self.process_btn)
        
        self.cancel_btn = QPushButton("Cancel")
        self.cancel_btn.clicked.connect(self.cancel_operation)
        self.cancel_btn.setEnabled(False)
        buttons_layout.addWidget(self.cancel_btn)
        
        clear_log_btn = QPushButton("Clear Log")
        clear_log_btn.clicked.connect(self.clear_log)
        buttons_layout.addWidget(clear_log_btn)
        
        save_settings_btn = QPushButton("Save Settings")
        save_settings_btn.clicked.connect(self.save_settings)
        save_settings_btn.setToolTip("Save current settings to keyring")
        buttons_layout.addWidget(save_settings_btn)

        self.convert_lsf_btn = QPushButton("Convert LSF to LSX")
        self.convert_lsf_btn.clicked.connect(self.run_lsf_conversion)
        self.convert_lsf_btn.setToolTip("Convert all .lsf files to .lsx in the search directory (excluding meta.lsf)")
        buttons_layout.addWidget(self.convert_lsf_btn)

        self.convert_lsx_btn = QPushButton("Convert LSX to LSF")
        self.convert_lsx_btn.clicked.connect(self.run_lsx_conversion)
        self.convert_lsx_btn.setToolTip("Convert all .lsx files to .lsf in the search directory")
        buttons_layout.addWidget(self.convert_lsx_btn)
        
        main_layout.addLayout(buttons_layout)
        
        # Progress bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        main_layout.addWidget(self.progress_bar)
        
        # Log area
        self.log_edit = QTextEdit()
        self.log_edit.setReadOnly(True)
        main_layout.addWidget(self.log_edit)
        
        # Status bar
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        self.status_bar.showMessage("Ready")
        
        # Show initial message
        self.log("XML Content Manager (Fixed Version) started. Please select files to process.")
        self.log("NOTE: This tool will search ALL files in the selected directory and its subdirectories.")
        self.log("NOTE: Files named 'english.xml' will be automatically ignored during replacement.")
        self.log("NOTE: Files in .git directories will be skipped.")
        self.log("NOTE: This tool will process all file types, including XML, LSX, and any other text-based files.")
        self.log("NOTE: This version includes better debugging to show what's being changed.")
    
    def browse_original_file(self):
        """Open file dialog to select original XML file."""
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Select Original XML File", "", "XML Files (*.xml);;LSX Files (*.lsx);;All Files (*)"
        )
        if file_path:
            self.original_file_edit.setText(file_path)
            self.save_settings()
    
    def browse_new_file(self):
        """Open file dialog to select new XML file."""
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Select New XML File", "", "XML Files (*.xml);;LSX Files (*.lsx);;All Files (*)"
        )
        if file_path:
            self.new_file_edit.setText(file_path)
            self.save_settings()
    
    def browse_search_dir(self):
        """Open directory dialog to select search directory."""
        dir_path = QFileDialog.getExistingDirectory(
            self, "Select Search Directory", ""
        )
        if dir_path:
            self.search_dir_edit.setText(dir_path)
            self.save_settings()
    
    def save_settings(self):
        """Save current settings to keyring."""
        settings = {
            "original_file": self.original_file_edit.text(),
            "new_file": self.new_file_edit.text(),
            "search_dir": self.search_dir_edit.text(),
            "recursive": self.recursive_check.isChecked(),
            "backup": self.backup_check.isChecked()
        }
        
        try:
            # Convert settings to JSON string
            settings_json = json.dumps(settings)
            # Save to keyring
            keyring.set_password(self.KEYRING_SERVICE, self.KEYRING_KEY, settings_json)
            self.log("Settings saved to keyring")
        except Exception as e:
            self.log(f"Error saving settings: {str(e)}")
    
    def load_saved_settings(self):
        """Load settings from keyring."""
        try:
            # Get settings from keyring
            settings_json = keyring.get_password(self.KEYRING_SERVICE, self.KEYRING_KEY)
            
            if settings_json:
                # Parse JSON
                settings = json.loads(settings_json)
                
                # Apply settings
                if "original_file" in settings and os.path.exists(settings["original_file"]):
                    self.original_file_edit.setText(settings["original_file"])
                
                if "new_file" in settings and os.path.exists(settings["new_file"]):
                    self.new_file_edit.setText(settings["new_file"])
                
                if "search_dir" in settings and os.path.exists(settings["search_dir"]):
                    self.search_dir_edit.setText(settings["search_dir"])
                
                if "recursive" in settings:
                    self.recursive_check.setChecked(settings["recursive"])
                
                if "backup" in settings:
                    self.backup_check.setChecked(settings["backup"])
                
                self.log("Settings loaded from keyring")
            
        except Exception as e:
            self.log(f"Note: No saved settings found or error loading settings: {str(e)}")
            # This is not a critical error, so just log it
    
    def log(self, message):
        """Add message to log area."""
        self.log_edit.append(message)
        # Ensure the latest message is visible
        cursor = self.log_edit.textCursor()
        cursor.movePosition(cursor.MoveOperation.End)
        self.log_edit.setTextCursor(cursor)
    
    def clear_log(self):
        """Clear the log area."""
        self.log_edit.clear()
    
    def validate_inputs(self):
        """Validate user inputs."""
        original_file = self.original_file_edit.text().strip()
        new_file = self.new_file_edit.text().strip()
        search_dir = self.search_dir_edit.text().strip()
        
        if not original_file:
            QMessageBox.warning(self, "Input Error", "Original XML file is required.")
            return False
        
        if not os.path.isfile(original_file):
            QMessageBox.warning(self, "Input Error", f"Original XML file not found: {original_file}")
            return False
        
        if not new_file:
            QMessageBox.warning(self, "Input Error", "New XML file is required.")
            return False
        
        if not os.path.isfile(new_file):
            QMessageBox.warning(self, "Input Error", f"New XML file not found: {new_file}")
            return False
        
        if not search_dir:
            QMessageBox.warning(self, "Input Error", "Search directory is required.")
            return False
        
        if not os.path.isdir(search_dir):
            QMessageBox.warning(self, "Input Error", f"Search directory not found: {search_dir}")
            return False
        
        return True
    
    # def analyze_files(self): # Removed
    #     """Analyze files without making changes.""" # Removed
    #     if not self.validate_inputs(): # Removed
    #         return # Removed
    #      # Removed
    #     # Create worker thread for analysis only # Removed
    #     self.start_worker(analysis_only=True) # Removed
    
    def process_files(self):
        """Process files and make changes."""
        if not self.validate_inputs():
            return
        
        # Ask for confirmation
        reply = QMessageBox.question(
            self, "Confirm Operation",
            "This will modify XML files. Do you want to continue?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            # Create worker thread for full processing
            self.start_xml_worker() # Changed from self.start_worker(analysis_only=False)
    
    def start_xml_worker(self): # Renamed from start_worker, removed analysis_only
        """Start the XML processing worker thread."""
        # Disable UI elements
        # self.analyze_btn.setEnabled(False) # Removed
        self.process_btn.setEnabled(False)
        self.convert_lsf_btn.setEnabled(False)
        self.convert_lsx_btn.setEnabled(False)
        self.cancel_btn.setEnabled(True)
        
        # Update status
        self.status_bar.showMessage("Processing XML files...") # Simplified message
        self.progress_bar.setValue(0)
        
        # Create and start worker
        # Determine number of processes based on CPU count
        processes = multiprocessing.cpu_count()
        self.log(f"Using {processes} CPU cores for multiprocessing")
        
        self.xml_worker = XMLWorker(
            self.original_file_edit.text(),
            self.new_file_edit.text(),
            self.search_dir_edit.text(),
            self.recursive_check.isChecked(),
            self.backup_check.isChecked(),
            processes
        )
        
        # Connect signals
        self.xml_worker.progress_update.connect(self.log)
        self.xml_worker.progress_percent.connect(self.progress_bar.setValue)
        self.xml_worker.finished_signal.connect(self.process_finished)
        self.xml_worker.error_signal.connect(self.handle_error)
        
        # Start worker
        self.log("XML Processing started...") # Simplified message
        self.xml_worker.start()

    def run_lsf_conversion(self):
        """Start the LSF to LSX conversion worker."""
        search_dir = self.search_dir_edit.text().strip()
        if not search_dir:
            QMessageBox.warning(self, "Input Error", "Search directory is required for LSF conversion.")
            return
        if not os.path.isdir(search_dir):
            QMessageBox.warning(self, "Input Error", f"Search directory not found: {search_dir}")
            return

        reply = QMessageBox.question(
            self, "Confirm LSF Conversion",
            "This will convert .lsf files to .lsx in the specified directory. Do you want to continue?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No
        )
        if reply != QMessageBox.StandardButton.Yes:
            return

        # self.analyze_btn.setEnabled(False) # Removed
        self.process_btn.setEnabled(False)
        self.convert_lsf_btn.setEnabled(False)
        self.convert_lsx_btn.setEnabled(False)
        self.cancel_btn.setEnabled(True)
        self.status_bar.showMessage("Converting LSF to LSX...")
        self.progress_bar.setValue(0)

        self.lsf_worker = LsfConverterWorker(
            search_dir,
            self.recursive_check.isChecked()
        )
        self.lsf_worker.progress_update.connect(self.log)
        self.lsf_worker.progress_percent.connect(self.progress_bar.setValue)
        self.lsf_worker.finished_signal.connect(self.lsf_conversion_finished)
        self.lsf_worker.error_signal.connect(self.handle_error) # Can reuse handle_error

        self.log("LSF to LSX conversion started...")
        self.lsf_worker.start()

    def lsf_conversion_finished(self, result):
        """Handle LSF conversion finished event."""
        # self.analyze_btn.setEnabled(True) # Removed
        self.process_btn.setEnabled(True)
        self.convert_lsf_btn.setEnabled(True)
        self.convert_lsx_btn.setEnabled(True)
        self.cancel_btn.setEnabled(False)
        self.status_bar.showMessage("Ready")
        self.progress_bar.setValue(100)

        self.log("\nLSF to LSX Conversion completed.")
        self.log(f"Files scanned: {result['total_scanned']}")
        self.log(f"Successfully converted: {result['converted_files']}")
        self.log(f"Skipped (meta.lsf): {result['skipped_files']}")
        if result['error_files']:
            self.log(f"Files with errors ({len(result['error_files'])}):")
            for f_path in result['error_files']:
                self.log(f"  - {f_path}")
        else:
            self.log("No errors encountered during LSF conversion.")
        
        QMessageBox.information(
            self, "LSF Conversion Completed",
            f"LSF to LSX conversion finished.\n\n"
            f"Files scanned: {result['total_scanned']}\n"
            f"Converted: {result['converted_files']}\n"
            f"Skipped: {result['skipped_files']}\n"
            f"Errors: {len(result['error_files'])}"
        )

    def run_lsx_conversion(self):
        """Start the LSX to LSF conversion worker."""
        search_dir = self.search_dir_edit.text().strip()
        if not search_dir:
            QMessageBox.warning(self, "Input Error", "Search directory is required for LSX conversion.")
            return
        if not os.path.isdir(search_dir):
            QMessageBox.warning(self, "Input Error", f"Search directory not found: {search_dir}")
            return

        reply = QMessageBox.question(
            self, "Confirm LSX Conversion",
            "This will convert .lsx files to .lsf in the specified directory. Do you want to continue?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No
        )
        if reply != QMessageBox.StandardButton.Yes:
            return

        # self.analyze_btn.setEnabled(False) # Removed
        self.process_btn.setEnabled(False)
        self.convert_lsf_btn.setEnabled(False)
        self.convert_lsx_btn.setEnabled(False)
        self.cancel_btn.setEnabled(True)
        self.status_bar.showMessage("Converting LSX to LSF...")
        self.progress_bar.setValue(0)

        self.lsx_worker = LsxConverterWorker( # Use LsxConverterWorker
            search_dir,
            self.recursive_check.isChecked()
        )
        self.lsx_worker.progress_update.connect(self.log)
        self.lsx_worker.progress_percent.connect(self.progress_bar.setValue)
        self.lsx_worker.finished_signal.connect(self.lsx_conversion_finished) # New handler
        self.lsx_worker.error_signal.connect(self.handle_error)

        self.log("LSX to LSF conversion started...")
        self.lsx_worker.start()

    def lsx_conversion_finished(self, result):
        """Handle LSX conversion finished event."""
        # self.analyze_btn.setEnabled(True) # Removed
        self.process_btn.setEnabled(True)
        self.convert_lsf_btn.setEnabled(True)
        self.convert_lsx_btn.setEnabled(True)
        self.cancel_btn.setEnabled(False)
        self.status_bar.showMessage("Ready")
        self.progress_bar.setValue(100)

        self.log("\nLSX to LSF Conversion completed.")
        self.log(f"Files scanned: {result['total_scanned']}")
        self.log(f"Successfully converted: {result['converted_files']}")
        self.log(f"Skipped (meta.lsx): {result['skipped_files']}") # Display skipped meta.lsx
        if result['error_files']:
            self.log(f"Files with errors ({len(result['error_files'])}):")
            for f_path in result['error_files']:
                self.log(f"  - {f_path}")
        else:
            self.log("No errors encountered during LSX conversion.")
        
        QMessageBox.information(
            self, "LSX Conversion Completed",
            f"LSX to LSF conversion finished.\n\n"
            f"Files scanned: {result['total_scanned']}\n"
            f"Converted: {result['converted_files']}\n"
            f"Skipped (meta.lsx): {result['skipped_files']}\n"
            f"Errors: {len(result['error_files'])}"
        )
    
    def cancel_operation(self):
        """Cancel the current operation."""
        worker_to_cancel = None
        operation_name = "Unknown operation"
        if self.xml_worker and self.xml_worker.isRunning():
            worker_to_cancel = self.xml_worker
            operation_name = "XML processing"
        elif self.lsf_worker and self.lsf_worker.isRunning():
            worker_to_cancel = self.lsf_worker
            operation_name = "LSF to LSX conversion"
        elif self.lsx_worker and self.lsx_worker.isRunning(): # Added check for lsx_worker
            worker_to_cancel = self.lsx_worker
            operation_name = "LSX to LSF conversion"
        else:
            self.log("No operation currently running to cancel.")
            return

        if worker_to_cancel:
            reply = QMessageBox.question(
                self, "Confirm Cancellation",
                f"Do you want to cancel the current {operation_name} operation?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No
            )
            
            if reply == QMessageBox.StandardButton.Yes:
                self.log(f"Cancelling {operation_name}...")
                worker_to_cancel.stop()
    
    def process_finished(self, result):
        """Handle process finished event."""
        # Re-enable UI elements
        # self.analyze_btn.setEnabled(True) # Removed
        self.process_btn.setEnabled(True)
        self.convert_lsf_btn.setEnabled(True) # Ensure this is re-enabled
        self.convert_lsx_btn.setEnabled(True) # Ensure this is re-enabled
        self.cancel_btn.setEnabled(False)
        
        # Update status
        self.status_bar.showMessage("Ready")
        
        # Log results
        self.log("\nOperation completed successfully.")
        self.log(f"Nodes deleted: {result['nodes_deleted']}")
        self.log(f"ContentUID replacements: {result['replacements']}")
        self.log(f"Files modified: {result['files_modified']}")
        
        # Save settings after successful operation
        self.save_settings()
        
        # Show result dialog
        QMessageBox.information(
            self, "Operation Completed",
            f"Operation completed successfully.\n\n"
            f"Nodes deleted: {result['nodes_deleted']}\n"
            f"ContentUID replacements: {result['replacements']}\n"
            f"Files modified: {result['files_modified']}"
        )
    
    def handle_error(self, error_message):
        """Handle error from worker thread."""
        # Re-enable UI elements
        # self.analyze_btn.setEnabled(True) # Removed
        self.process_btn.setEnabled(True)
        self.convert_lsf_btn.setEnabled(True) # Ensure this is re-enabled
        self.convert_lsx_btn.setEnabled(True) # Ensure this is re-enabled
        self.cancel_btn.setEnabled(False)
        
        # Update status
        self.status_bar.showMessage("Error")
        
        # Log error
        self.log(f"ERROR: {error_message}")
        
        # Show error dialog
        QMessageBox.critical(self, "Error", error_message)


def main():
    # On Windows, protect the entry point to avoid recursive spawning with multiprocessing
    if sys.platform == 'win32':
        multiprocessing.freeze_support()
        
    app = QApplication(sys.argv)
    
    # Set the application name (used by keyring)
    app.setApplicationName("XMLContentManager")
    app.setOrganizationName("XMLTools")
    
    window = XMLContentManager()
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()