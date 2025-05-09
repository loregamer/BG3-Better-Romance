#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
import re
import xml.etree.ElementTree as ET
import shutil
import keyring
import json
import multiprocessing
import mmap
import concurrent.futures
from io import StringIO
from pathlib import Path
from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
                           QLabel, QLineEdit, QPushButton, QTextEdit, QFileDialog, 
                           QProgressBar, QMessageBox, QCheckBox, QGridLayout, QStatusBar)
from PyQt6.QtCore import Qt, QThread, pyqtSignal


class XMLWorker(QThread):
    """Worker thread for processing XML files."""
    progress_update = pyqtSignal(str)
    progress_percent = pyqtSignal(int)
    finished_signal = pyqtSignal(dict)
    error_signal = pyqtSignal(str)
    
    def __init__(self, original_file, new_file, search_dir, recursive=True, backup=True):
        super().__init__()
        self.original_file = original_file
        self.new_file = new_file
        self.search_dir = search_dir
        self.recursive = recursive
        self.backup = backup
        self.running = True
        # Pre-compile regex patterns for performance
        self.xml_pattern_template = r'contentuid="{0}"\s+version="[^"]*"'
        self.lsx_pattern_template = r'id="{0}"'
        
    def run(self):
        try:
            # Read XML files
            self.progress_update.emit("Reading original XML file...")
            original_tree = ET.parse(self.original_file)
            original_root = original_tree.getroot()
            
            self.progress_update.emit("Reading new XML file...")
            new_tree = ET.parse(self.new_file)
            new_root = new_tree.getroot()
            
            # Extract content nodes - use dictionary comprehension for speed
            self.progress_update.emit("Extracting content nodes from original XML...")
            original_contents = {
                elem.attrib["contentuid"]: {
                    "element": elem,
                    "version": elem.attrib.get("version", ""),
                    "text": elem.text
                }
                for elem in original_root.findall(".//content") 
                if "contentuid" in elem.attrib
            }
            
            self.progress_update.emit(f"Found {len(original_contents)} content nodes in original XML.")
            
            self.progress_update.emit("Extracting content nodes from new XML...")
            new_contents = {}
            nodes_to_delete = []
            replacements = {}
            
            # Fast extraction of new content nodes
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
                        if version != orig_version:
                            nodes_to_delete.append(elem)
                            replacements[contentuid] = contentuid  # Store original ID for replacement
            
            self.progress_update.emit(f"Found {len(new_contents)} content nodes in new XML.")
            self.progress_update.emit(f"Identified {len(nodes_to_delete)} nodes to delete.")
            
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
    
    def _is_binary_file(self, file_path):
        """Fast check if a file is binary."""
        try:
            with open(file_path, 'rb') as f:
                chunk = f.read(1024)
                if b'\0' in chunk:  # Null bytes indicate binary file
                    return True
                if not chunk:  # Empty file
                    return False
                # Check for high number of non-printable characters
                return sum(c < 9 or 13 < c < 32 or c > 126 for c in chunk) / len(chunk) > 0.3
        except:
            return True  # If error, treat as binary

    def _process_file(self, file_path, replacements, original_contents):
        """Process a single file - separated for parallelization."""
        if not self.running:
            return None
            
        try:
            if self._is_binary_file(file_path):
                return None  # Skip binary files quickly
                
            # Quick check for any matches before reading full file
            file_size = os.path.getsize(file_path)
            if file_size > 10 * 1024 * 1024:  # For files > 10MB, use memory mapping
                with open(file_path, 'r+b') as f:
                    mm = mmap.mmap(f.fileno(), 0)
                    any_match = any(uid.encode() in mm for uid in replacements.keys())
                    if not any_match:
                        mm.close()
                        return None
                    content = mm.read().decode('utf-8', errors='ignore')
                    mm.close()
            else:
                # For smaller files, read directly
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                except UnicodeDecodeError:
                    try:
                        with open(file_path, 'r', encoding='latin-1') as f:
                            content = f.read()
                    except:
                        return None  # Skip if can't read
            
            # Check if any replacement is needed - optimized check
            needs_update = False
            for old_uid in replacements.keys():
                if old_uid in content:
                    needs_update = True
                    break
            
            if needs_update:
                # Create backup if needed
                if self.backup:
                    backup_path = f"{file_path}.backup"
                    shutil.copy2(file_path, backup_path)
                
                # Pre-compile patterns for this file's replacements
                compiled_patterns = []
                for old_uid, new_uid in replacements.items():
                    if old_uid in content:  # Only compile patterns needed for this file
                        # XML pattern
                        xml_pattern = re.compile(self.xml_pattern_template.format(re.escape(old_uid)))
                        xml_replacement = f'contentuid="{new_uid}" version="{original_contents[new_uid]["version"]}"'
                        
                        # LSX pattern
                        lsx_pattern = re.compile(self.lsx_pattern_template.format(re.escape(old_uid)))
                        lsx_replacement = f'id="{new_uid}"'
                        
                        # Direct reference pattern
                        compiled_patterns.append((xml_pattern, xml_replacement, lsx_pattern, lsx_replacement, old_uid, new_uid))
                
                # Make replacements - batch all patterns at once
                modified_content = content
                for xml_pattern, xml_replacement, lsx_pattern, lsx_replacement, old_uid, new_uid in compiled_patterns:
                    modified_content = xml_pattern.sub(xml_replacement, modified_content)
                    modified_content = lsx_pattern.sub(lsx_replacement, modified_content)
                    modified_content = modified_content.replace(f'"{old_uid}"', f'"{new_uid}"')
                
                # Write back to file
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(modified_content)
                
                return file_path  # Return path of modified file
        except Exception as e:
            return None
        
        return None
    
    def _replace_in_files(self, replacements, original_contents):
        """Replace contentuid in all files in search directory using parallel processing."""
        search_path = Path(self.search_dir)
        
        # Fast file collection
        self.progress_update.emit(f"Scanning directory {'recursively' if self.recursive else ''}: {search_path}")
        
        # Get list of files - use generator to avoid loading all paths in memory
        if self.recursive:
            def get_files():
                for root, _, files in os.walk(str(search_path)):
                    for file in files:
                        if file.lower() != "english.xml":
                            yield os.path.join(root, file)
        else:
            def get_files():
                for file in os.listdir(str(search_path)):
                    file_path = os.path.join(str(search_path), file)
                    if os.path.isfile(file_path) and file.lower() != "english.xml":
                        yield file_path
                        
        # Process files in parallel
        max_workers = max(4, multiprocessing.cpu_count())
        modified_files = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all files for processing
            future_to_file = {}
            files_to_process = list(get_files())
            num_files = len(files_to_process)
            self.progress_update.emit(f"Found {num_files} files to process")
            
            # Submit in batches to avoid memory issues with very large directories
            batch_size = min(10000, num_files)
            for i in range(0, num_files, batch_size):
                batch = files_to_process[i:i+batch_size]
                for file_path in batch:
                    if not self.running:
                        break
                    future = executor.submit(self._process_file, file_path, replacements, original_contents)
                    future_to_file[future] = file_path
                
                # Process completed futures
                for i, future in enumerate(concurrent.futures.as_completed(future_to_file)):
                    if not self.running:
                        executor.shutdown(wait=False)
                        self.progress_update.emit("Operation canceled.")
                        return
                    
                    file_path = future_to_file[future]
                    try:
                        result = future.result()
                        if result:
                            modified_files.append(result)
                        
                        # Update progress less frequently for better performance
                        if i % 100 == 0 or i == num_files - 1:
                            progress = int((i / num_files) * 100)
                            self.progress_percent.emit(progress)
                    except Exception as e:
                        # Skip error reporting for better performance
                        pass
        
        self.files_modified = len(modified_files)
        self.progress_percent.emit(100)
        self.progress_update.emit(f"Replacement complete. Modified {self.files_modified} files.")
    
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
        self.worker = None
        self.load_saved_settings()
    
    def init_ui(self):
        """Initialize the user interface."""
        self.setWindowTitle("XML Content Manager - Optimized")
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
        
        self.analyze_btn = QPushButton("Analyze Files")
        self.analyze_btn.clicked.connect(self.analyze_files)
        buttons_layout.addWidget(self.analyze_btn)
        
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
        self.log("XML Content Manager (Optimized) started. Please select files to process.")
        self.log("NOTE: This tool will search ALL files in the selected directory and its subdirectories.")
        self.log("NOTE: Files named 'english.xml' will be automatically ignored during replacement.")
        self.log("NOTE: This tool will process all file types, including XML, LSX, and any other text-based files.")
        self.log("NOTE: Optimized for high performance on large directories.")
    
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
    
    def analyze_files(self):
        """Analyze files without making changes."""
        if not self.validate_inputs():
            return
        
        # Create worker thread for analysis only
        self.start_worker(analysis_only=True)
    
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
            self.start_worker(analysis_only=False)
    
    def start_worker(self, analysis_only=False):
        """Start the worker thread."""
        # Disable UI elements
        self.analyze_btn.setEnabled(False)
        self.process_btn.setEnabled(False)
        self.cancel_btn.setEnabled(True)
        
        # Update status
        self.status_bar.showMessage("Processing..." if not analysis_only else "Analyzing...")
        
        # Create and start worker
        self.worker = XMLWorker(
            self.original_file_edit.text(),
            self.new_file_edit.text(),
            self.search_dir_edit.text(),
            self.recursive_check.isChecked(),
            self.backup_check.isChecked()
        )
        
        # Connect signals
        self.worker.progress_update.connect(self.log)
        self.worker.progress_percent.connect(self.progress_bar.setValue)
        self.worker.finished_signal.connect(self.process_finished)
        self.worker.error_signal.connect(self.handle_error)
        
        # Start worker
        self.log(f"{'Analysis' if analysis_only else 'Processing'} started...")
        self.worker.start()
    
    def cancel_operation(self):
        """Cancel the current operation."""
        if self.worker and self.worker.isRunning():
            reply = QMessageBox.question(
                self, "Confirm Cancellation",
                "Do you want to cancel the current operation?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No
            )
            
            if reply == QMessageBox.StandardButton.Yes:
                self.log("Cancelling operation...")
                self.worker.stop()
    
    def process_finished(self, result):
        """Handle process finished event."""
        # Re-enable UI elements
        self.analyze_btn.setEnabled(True)
        self.process_btn.setEnabled(True)
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
        self.analyze_btn.setEnabled(True)
        self.process_btn.setEnabled(True)
        self.cancel_btn.setEnabled(False)
        
        # Update status
        self.status_bar.showMessage("Error")
        
        # Log error
        self.log(f"ERROR: {error_message}")
        
        # Show error dialog
        QMessageBox.critical(self, "Error", error_message)


def main():
    app = QApplication(sys.argv)
    
    # Set the application name (used by keyring)
    app.setApplicationName("XMLContentManager")
    app.setOrganizationName("XMLTools")
    
    window = XMLContentManager()
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()