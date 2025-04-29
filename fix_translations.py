#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
import re
import xml.etree.ElementTree as ET
import shutil
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
                        if version != orig_version:
                            self.progress_update.emit(f"Found match with different version: {contentuid}")
                            self.progress_update.emit(f"  Original version: {orig_version}, New version: {version}")
                            nodes_to_delete.append(elem)
                            replacements[contentuid] = contentuid  # Same ID in this case
            
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
                    self.progress_update.emit(f"Creating backup of new XML at {backup_path}")
                    shutil.copy2(self.new_file, backup_path)
                
                self.progress_update.emit(f"Saving modified new XML to {self.new_file}")
                new_tree.write(self.new_file, encoding="utf-8", xml_declaration=True)
            
            # Replace contentuid in all files
            if replacements:
                self.progress_update.emit("Replacing contentuid in files...")
                self._replace_in_files(replacements)
            
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
    
    def _replace_in_files(self, replacements):
        """Replace contentuid in all files in search directory."""
        search_path = Path(self.search_dir)
        files_to_process = []
        
        # Get list of files
        if self.recursive:
            self.progress_update.emit(f"Scanning directory recursively: {search_path}")
            xml_files = list(search_path.rglob("*.xml"))
        else:
            self.progress_update.emit(f"Scanning directory: {search_path}")
            xml_files = list(search_path.glob("*.xml"))
        
        self.progress_update.emit(f"Found {len(xml_files)} XML files to process.")
        
        # Process each file
        self.files_modified = 0
        for i, file_path in enumerate(xml_files):
            if not self.running:
                self.progress_update.emit("Operation canceled.")
                return
            
            progress = int((i / len(xml_files)) * 100)
            self.progress_percent.emit(progress)
            
            try:
                self.progress_update.emit(f"Processing file: {file_path}")
                
                # Read file content
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # Check if any replacement is needed
                needs_update = False
                for old_uid, new_uid in replacements.items():
                    if old_uid in content:
                        needs_update = True
                        break
                
                if needs_update:
                    self.progress_update.emit(f"Found matches in {file_path}")
                    
                    # Create backup if needed
                    if self.backup:
                        backup_path = f"{file_path}.backup"
                        self.progress_update.emit(f"Creating backup at {backup_path}")
                        shutil.copy2(file_path, backup_path)
                    
                    # Make replacements
                    modified_content = content
                    for old_uid, new_uid in replacements.items():
                        modified_content = modified_content.replace(old_uid, new_uid)
                    
                    # Write back to file
                    with open(file_path, 'w', encoding='utf-8') as f:
                        f.write(modified_content)
                    
                    self.files_modified += 1
                    self.progress_update.emit(f"Updated file: {file_path}")
            
            except Exception as e:
                self.progress_update.emit(f"Error processing {file_path}: {str(e)}")
        
        self.progress_percent.emit(100)
        self.progress_update.emit(f"Replacement complete. Modified {self.files_modified} files.")
    
    def stop(self):
        """Stop the worker thread."""
        self.running = False


class XMLContentManager(QMainWindow):
    """Main application window."""
    
    def __init__(self):
        super().__init__()
        self.init_ui()
        self.worker = None
    
    def init_ui(self):
        """Initialize the user interface."""
        self.setWindowTitle("XML Content Manager")
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
        options_layout.addWidget(self.recursive_check)
        
        self.backup_check = QCheckBox("Create Backups")
        self.backup_check.setChecked(True)
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
        self.log("XML Content Manager started. Please select files to process.")
    
    def browse_original_file(self):
        """Open file dialog to select original XML file."""
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Select Original XML File", "", "XML Files (*.xml);;All Files (*)"
        )
        if file_path:
            self.original_file_edit.setText(file_path)
    
    def browse_new_file(self):
        """Open file dialog to select new XML file."""
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Select New XML File", "", "XML Files (*.xml);;All Files (*)"
        )
        if file_path:
            self.new_file_edit.setText(file_path)
    
    def browse_search_dir(self):
        """Open directory dialog to select search directory."""
        dir_path = QFileDialog.getExistingDirectory(
            self, "Select Search Directory", ""
        )
        if dir_path:
            self.search_dir_edit.setText(dir_path)
    
    def log(self, message):
        """Add message to log area."""
        self.log_edit.append(message)
        # Ensure the latest message is visible
        self.log_edit.ensureCursorVisible()
    
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
    window = XMLContentManager()
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()