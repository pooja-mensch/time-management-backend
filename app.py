#!/usr/bin/env python3
"""
File: app.py
Main Flask server with timeout handling and progress tracking
"""

from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
from werkzeug.utils import secure_filename
import tempfile
import uuid
import shutil
import os
import logging
import signal
import time
from holiday_distribution import HolidayTool
import random
from threading import Thread
import threading

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Dynamic CORS configuration
FRONTEND_URLS = os.getenv('FRONTEND_URLS', 'http://localhost:3000').split(',')
CORS(app, origins=FRONTEND_URLS)
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

# Configuration
ALLOWED_EXTENSIONS = {'xlsx', 'xls'}
MAX_EMPLOYEES = 30
REQUIRED_SHEETS = ["MA Übersicht", "IST Stunden"]
HOLIDAY_FILE = "Feiertage.xlsx"
DEFAULT_OUTPUT_SUFFIX = "_holidays_added"

# Store temporary files and processing status
temp_files = {}
processing_status = {}

class TimeoutError(Exception):
    pass

def timeout_handler(signum, frame):
    raise TimeoutError("Processing timeout")

def process_file_with_timeout(tool, output_file, timeout_seconds=240):
    """Process file with timeout handling"""
    result = None
    exception = None
    
    def target():
        nonlocal result, exception
        try:
            result = tool.execute(output_file)
        except Exception as e:
            exception = e
    
    thread = Thread(target=target)
    thread.daemon = True
    thread.start()
    thread.join(timeout_seconds)
    
    if thread.is_alive():
        # Thread is still running, timeout occurred
        logger.error(f"Processing timeout after {timeout_seconds} seconds")
        raise TimeoutError(f"Processing took longer than {timeout_seconds} seconds")
    
    if exception:
        raise exception
    
    return result

def allowed_file(filename):
    """Check if uploaded file has valid extension"""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/')
def home():
    """Simple home page to test if server is running"""
    return f"""
    <h1>Holiday Processor Server is running! 🎉</h1>
    <p>German Holiday Distribution System</p>
    <ul>
        <li>Max employees: {MAX_EMPLOYEES}</li>
        <li>Required sheets: {', '.join(REQUIRED_SHEETS)}</li>
        <li>Holiday file: {HOLIDAY_FILE}</li>
        <li>Allowed origins: {', '.join(FRONTEND_URLS)}</li>
    </ul>
    <p>Use your React frontend to upload files!</p>
    """

@app.route('/health')
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy', 
        'message': 'Server is running',
        'max_employees': MAX_EMPLOYEES,
        'required_sheets': REQUIRED_SHEETS,
        'holiday_file': HOLIDAY_FILE,
        'holiday_file_exists': os.path.exists(HOLIDAY_FILE),
        'allowed_cors_origins': FRONTEND_URLS
    })

@app.route('/process-holidays', methods=['POST'])
def process_holidays():
    """Main endpoint to process uploaded Excel files with timeout handling"""
    processing_id = str(uuid.uuid4())
    
    try:
        # Fun messages
        holiday_lines = [
            "Spreading holidays like cheese on a hot pizza 🍕📅",
            "Processing your data faster than Santa delivers presents 🎅📊",
            "Crunching numbers like autumn leaves 🍂🔢"
        ]
        fun_message = random.choice(holiday_lines)
        
        logger.info(f"[{processing_id}] Received request to process holidays")
        
        # Check if file is in the request
        if 'file' not in request.files:
            logger.error(f"[{processing_id}] No file in request")
            return jsonify({
                'success': False,
                'message': 'No file uploaded'
            }), 400

        file = request.files['file']
        logger.info(f"[{processing_id}] Received file: {file.filename} ({file.content_length} bytes)")
        
        # Check if file is actually selected
        if file.filename == '':
            return jsonify({
                'success': False,
                'message': 'No file selected'
            }), 400

        # Check file extension
        if not file.filename.endswith('.xlsx'):
            return jsonify({
                'success': False,
                'message': 'Currently only accepting Excel Files (.xlsx)'
            }), 400

        # Create temporary directory for processing
        temp_dir = tempfile.mkdtemp()
        input_file = os.path.join(temp_dir, secure_filename(file.filename))
        file.save(input_file)
        
        logger.info(f"[{processing_id}] Saved file to: {input_file}")
        
        # Get max employees parameter
        max_employees = request.form.get('max_employees', MAX_EMPLOYEES, type=int)
        
        # Initialize HolidayTool
        logger.info(f"[{processing_id}] Initializing HolidayTool...")
        tool = HolidayTool(input_file)
        
        # Set max employees if different from default
        if max_employees != MAX_EMPLOYEES:
            tool.change_max(max_employees)
        
        # Generate output filename
        base_filename = file.filename.rsplit('.', 1)[0]
        output_filename = f"{base_filename}{DEFAULT_OUTPUT_SUFFIX}_{uuid.uuid4().hex[:8]}.xlsx"
        output_file = os.path.join(temp_dir, output_filename)
        
        logger.info(f"[{processing_id}] Processing file: {input_file} -> {output_file}")
        
        # Store processing status
        processing_status[processing_id] = {
            'status': 'processing',
            'start_time': time.time(),
            'message': 'Processing your file...'
        }
        
        # Execute with timeout handling
        try:
            result = process_file_with_timeout(tool, output_file, timeout_seconds=240)  # 4 minutes
        except TimeoutError as e:
            logger.error(f"[{processing_id}] {str(e)}")
            shutil.rmtree(temp_dir, ignore_errors=True)
            return jsonify({
                'success': False,
                'message': 'Processing timeout. Your file might be too large or complex. Please try with a smaller file or contact support.'
            }), 408  # Request Timeout
        
        if result:
            # Store file info for download
            file_id = str(uuid.uuid4())
            temp_files[file_id] = {
                'path': result,
                'filename': output_filename,
                'temp_dir': temp_dir
            }
            
            processing_status[processing_id] = {
                'status': 'completed',
                'end_time': time.time(),
                'message': 'Processing completed successfully'
            }
            
            logger.info(f"[{processing_id}] Processing completed successfully. File ID: {file_id}")
            
            return jsonify({
                'success': True,
                'message': fun_message,
                'employees_processed': len(tool.emp_list),
                'download_url': f'/download/{file_id}',
                'filename': output_filename,
                'processing_id': processing_id
            })
        else:
            # Clean up on failure
            shutil.rmtree(temp_dir, ignore_errors=True)
            logger.error(f"[{processing_id}] Processing failed")
            return jsonify({
                'success': False,
                'message': 'Failed to process the file. Please check the file format and data.'
            }), 500
        
    except Exception as e:
        logger.error(f"[{processing_id}] Error occurred: {str(e)}")
        return jsonify({
            'success': False,
            'message': f'An error occurred: {str(e)}'
        }), 500

@app.route('/download/<file_id>')
def download_file(file_id):
    """Endpoint to download processed files"""
    try:
        if file_id not in temp_files:
            logger.error(f"File ID not found: {file_id}")
            return jsonify({
                'success': False,
                'message': 'File not found or expired'
            }), 404
        
        file_info = temp_files[file_id]
        logger.info(f"Download requested for: {file_info['filename']}")
        
        if not os.path.exists(file_info['path']):
            logger.error(f"File not found on disk: {file_info['path']}")
            return jsonify({
                'success': False,
                'message': 'File not found on server'
            }), 404
            
        return send_file(
            file_info['path'],
            as_attachment=True,
            download_name=file_info['filename'],
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        
    except Exception as e:
        logger.error(f"Download error: {str(e)}")
        return jsonify({
            'success': False,
            'message': f'Download failed: {str(e)}'
        }), 500

@app.route('/cleanup/<file_id>', methods=['DELETE'])
def cleanup_file(file_id):
    """Endpoint to clean up temporary files"""
    try:
        if file_id in temp_files:
            file_info = temp_files[file_id]
            shutil.rmtree(file_info['temp_dir'], ignore_errors=True)
            del temp_files[file_id]
            logger.info(f"Cleaned up file: {file_id}")
            return jsonify({'success': True, 'message': 'File cleaned up'})
        else:
            return jsonify({'success': False, 'message': 'File not found'}), 404
    except Exception as e:
        logger.error(f"Cleanup error: {str(e)}")
        return jsonify({'success': False, 'message': str(e)}), 500

# Run the application
if __name__ == '__main__':
    print("🚀 Starting Holiday Distribution Flask Server...")
    print("=" * 60)
    print(f"Server will be available at: http://localhost:8000")
    print(f"Max employees: {MAX_EMPLOYEES}")
    print(f"Required sheets: {REQUIRED_SHEETS}")
    print(f"Holiday file: {HOLIDAY_FILE}")
    print(f"Processing timeout: 4 minutes")
    
    if os.path.exists(HOLIDAY_FILE):
        print(f"✅ Holiday file found: {HOLIDAY_FILE}")
    else:
        print(f"⚠️  Holiday file not found: {HOLIDAY_FILE}")
    
    print("=" * 60)