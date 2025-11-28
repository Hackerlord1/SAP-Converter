from flask import Flask, request, send_file, render_template, redirect, url_for, jsonify, Response
import pandas as pd
import io
import zipfile
import os
import uuid
import csv
import re
import traceback
from datetime import datetime, timedelta
from werkzeug.utils import secure_filename
import logging
import gc
import psutil
import signal
from contextlib import contextmanager
import queue
from threading import Thread

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'temp_uploads'
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB
app.config['SESSION_TIMEOUT'] = timedelta(hours=2)
app.config['ALLOWED_EXTENSIONS'] = {'xlsx', 'xls'}

os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
sessions = {}

# Background processing setup
processing_queue = queue.Queue()
results_store = {}

class TimeoutException(Exception):
    pass

@contextmanager
def time_limit(seconds):
    def signal_handler(signum, frame):
        raise TimeoutException(f"Processing timed out after {seconds} seconds")
    signal.signal(signal.SIGALRM, signal_handler)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)

class BackgroundProcessor(Thread):
    """Background thread for processing files without timeout"""
    def __init__(self):
        Thread.__init__(self)
        self.daemon = True
        
    def run(self):
        while True:
            try:
                task_id, session_id, exclude_returns, exclude_invoices = processing_queue.get(timeout=30)
                try:
                    output_files, processed, errors = generate_output_files(
                        session_id, exclude_returns, exclude_invoices
                    )
                    results_store[task_id] = {
                        'status': 'completed',
                        'output_files': output_files,
                        'processed': processed,
                        'errors': errors,
                        'completed_at': datetime.now()
                    }
                except Exception as e:
                    results_store[task_id] = {
                        'status': 'error',
                        'error': str(e),
                        'completed_at': datetime.now()
                    }
                finally:
                    processing_queue.task_done()
                    
            except queue.Empty:
                continue

# Start background processor
bg_processor = BackgroundProcessor()
bg_processor.start()

def get_memory_usage():
    """Get current memory usage in MB"""
    try:
        process = psutil.Process()
        return process.memory_info().rss / 1024 / 1024
    except:
        return 0

def memory_safe_chunk_processing(filepath, chunk_size=5000):
    """Process large files in chunks to avoid memory exhaustion"""
    chunks = []
    total_rows = 0
    
    try:
        # Read in chunks
        for chunk in pd.read_excel(
            filepath, 
            engine='openpyxl', 
            dtype=str, 
            keep_default_na=False,
            chunksize=chunk_size
        ):
            # Process chunk immediately
            processed_chunk = process_data_chunk(chunk)
            chunks.append(processed_chunk)
            total_rows += len(chunk)
            
            # Force garbage collection every few chunks
            if len(chunks) % 3 == 0:
                gc.collect()
            
            # Check memory usage and reduce chunk size if needed
            current_memory = get_memory_usage()
            if current_memory > 512:  # 512MB threshold
                chunk_size = max(1000, chunk_size // 2)
                logger.warning(f"High memory usage ({current_memory:.1f}MB), reducing chunk size to {chunk_size}")
            
            # Emergency break if memory gets too high
            if current_memory > 800:  # 800MB emergency threshold
                logger.error("Memory threshold exceeded, stopping processing")
                break
                
        # Combine chunks efficiently
        if chunks:
            final_df = pd.concat(chunks, ignore_index=True)
            # Final memory cleanup
            del chunks
            gc.collect()
            return final_df
        else:
            return pd.DataFrame()
            
    except Exception as e:
        logger.error(f"Chunk processing failed: {e}")
        # Clean up on failure
        del chunks
        gc.collect()
        raise

def process_data_chunk(chunk):
    """Process a single chunk of data"""
    # Apply your existing processing logic to each chunk
    if 'Route Name' in chunk.columns:
        chunk['Route Name'] = chunk['Route Name'].apply(clean_route_name)
    
    # Process quantities and filter
    chunk = process_quantities(chunk)
    
    # Convert numeric columns
    chunk = process_numeric_columns(chunk)
    
    return chunk

def allowed_file(filename):
    """Check if file extension is allowed"""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']

def clean_route_name(value):
    """Clean invisible characters from Route Name only"""
    if pd.isna(value) or str(value).strip() in ['', '0']:
        return value
    return re.sub(r'[\u200B-\u200D\u2060\uFEFF\u00A0\r\n\t\x00-\x1F\x7F]', '', str(value)).strip()

def cleanup_old_sessions():
    """Clean up expired sessions and their files"""
    now = datetime.now()
    expired = [sid for sid, data in sessions.items() if now - data['created'] > app.config['SESSION_TIMEOUT']]
    for sid in expired:
        pattern = f"{sid}_"
        for f in os.listdir(app.config['UPLOAD_FOLDER']):
            if f.startswith(pattern):
                try: 
                    os.remove(os.path.join(app.config['UPLOAD_FOLDER'], f))
                except Exception as e:
                    logger.error(f"Error deleting file {f}: {e}")
        sessions.pop(sid, None)
        logger.info(f"Cleaned up expired session: {sid}")

def validate_dataframe(df, required_cols):
    """Validate dataframe structure and content"""
    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {', '.join(missing_cols)}")
    
    if df.empty:
        raise ValueError("File contains no data")

def process_quantities(df):
    """Process quantity columns and filter out zero quantities"""
    for col in ['Sale_Qty_Pcs', 'Free_Total_Qty']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    # Filter out rows where both quantities are zero
    return df[~((df['Sale_Qty_Pcs'] == 0) & (df['Free_Total_Qty'] == 0))]

def process_numeric_columns(df):
    """Convert numeric columns to appropriate data types"""
    numeric_columns = [
        'Pieces per Case', 'List Price per case', 'Gross Sale', 'Net Sale',
        'Bonus_Discount', 'Trade_Discount', 'Cash_Discount', 'Total Discount'
    ]
    
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    return df

def process_text_columns(df):
    """Clean and format text columns"""
    if 'Salesrep Code' in df.columns:
        df['Salesrep Code'] = df['Salesrep Code'].astype(str).str.zfill(3)
    
    if 'Salesrep Name' in df.columns:
        df['Salesrep Name'] = df['Salesrep Name'].str.strip().str.replace(r'\s+', ' ', regex=True)
    
    # Process line number column
    if len(df.columns) > 7:
        line_col = 'LineNum' if 'LineNum' in df.columns else df.columns[7]
        df[line_col] = df[line_col].astype(str).str.zfill(2)
    
    return df

def fix_date_value(val):
    """Optimized date parsing to avoid timeouts"""
    if pd.isna(val):
        return '0'
    
    s = str(val).strip()
    if s in ['', '0', '0.0', 'NaT', 'None', 'nan']:
        return '0'
    
    # Quick checks for common invalid values
    if len(s) < 5 or s.lower() in ['nat', 'none', 'null']:
        return '0'
    
    # Handle Excel serial dates first (most common case)
    try:
        n = float(s)
        if 1000 < n < 50000:  # Reasonable Excel date range
            try:
                dt = pd.Timestamp('1899-12-30') + pd.Timedelta(days=n)
                return dt.strftime('%d/%m/%Y')
            except:
                pass
    except (ValueError, TypeError):
        pass
    
    # Handle string dates with simpler parsing
    if '/' in s or '-' in s:
        # Try common formats directly without expensive guessing
        formats = [
            '%d/%m/%Y', '%d-%m-%Y', '%Y-%m-%d', '%Y/%m/%d',
            '%d/%m/%y', '%d-%m-%y', '%m/%d/%Y', '%m-%d-%Y'
        ]
        
        for fmt in formats:
            try:
                dt = datetime.strptime(s, fmt)
                return dt.strftime('%d/%m/%Y')
            except ValueError:
                continue
    
    return '0'

def process_dates(df):
    """Optimized date processing"""
    date_columns = ['Invoice Date', 'Reference Doc Date']
    
    # Process only non-empty columns
    for col in date_columns:
        if col in df.columns and not df[col].empty:
            # Use vectorized operations where possible
            df[col] = df[col].astype(str).str.strip()
            
            # Filter out obviously invalid values first
            mask = ~df[col].isin(['', '0', '0.0', 'NaT', 'None', 'nan'])
            df.loc[mask, col] = df.loc[mask, col].apply(fix_date_value)
            df.loc[~mask, col] = '0'
    
    # Simplified date column selection
    if 'Document Type Descrw' not in df.columns:
        return 'Invoice Date'
    
    invoice_count = (df['Document Type Descrw'] == 'Invoice').sum()
    returns_count = (df['Document Type Descrw'] == 'Credit for Returns').sum()
    
    return 'Invoice Date' if invoice_count >= returns_count else 'Reference Doc Date'

def get_session_files(session_id):
    """Safely get files for a session"""
    try:
        pattern = f"{session_id}_"
        files = []
        for f in os.listdir(app.config['UPLOAD_FOLDER']):
            if f.startswith(pattern):
                filepath = os.path.join(app.config['UPLOAD_FOLDER'], f)
                if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                    files.append(f)
                else:
                    logger.warning(f"Invalid file found: {filepath}")
        return files
    except Exception as e:
        logger.error(f"Error getting session files: {e}")
        return []

def safe_file_cleanup(filepath):
    """Safely cleanup files with error handling"""
    try:
        if os.path.exists(filepath):
            os.remove(filepath)
    except Exception as e:
        logger.error(f"Cleanup failed for {filepath}: {e}")

def validate_upload(files):
    """Validate upload before processing"""
    max_files = 5  # Reduce from 10 to 5 for Render
    max_total_size = 20 * 1024 * 1024  # 20MB total
    
    if len(files) > max_files:
        raise ValueError(f"Too many files. Maximum {max_files} files allowed.")
    
    total_size = 0
    for file in files:
        file.seek(0, 2)  # Seek to end
        size = file.tell()
        file.seek(0)  # Reset
        total_size += size
        
        if size > 10 * 1024 * 1024:  # 10MB per file
            raise ValueError(f"File {file.filename} is too large (max 10MB)")
    
    if total_size > max_total_size:
        raise ValueError(f"Total upload size too large (max {max_total_size//1024//1024}MB)")
    
    return True

def process_single_file_memory_safe(filepath, orig_name, exclude_returns, exclude_invoices):
    """Process a single file with memory safety"""
    required_cols = ['Document Type Descrw', 'Storage Location', 'Sale_Qty_Pcs', 'Free_Total_Qty']
    location_map = {
        '15330599': 'lodwar', '15510868': 'kapsabet', '15393486': 'bomet', '50260522': 'nakuru',
        '50260577': 'savemore', '18010415': 'kisii', '50260971': 'molo', '15580524': 'naivasha',
        '18041985': 'rongo', '15580523': 'nyahururu', '15393485': 'kericho', '15510770': 'eldoret',
    }
    
    # Check file size to decide processing method
    file_size = os.path.getsize(filepath)
    if file_size > 5 * 1024 * 1024:  # 5MB threshold for chunked processing
        logger.info(f"Using chunked processing for large file: {orig_name} ({file_size//1024//1024}MB)")
        df = memory_safe_chunk_processing(filepath)
    else:
        # Use normal processing for smaller files
        df = pd.read_excel(filepath, engine='openpyxl', dtype=str, keep_default_na=False)
    
    # Validate basic structure
    validate_dataframe(df, required_cols)
    
    # Clean Route Name
    if 'Route Name' in df.columns:
        df['Route Name'] = df['Route Name'].apply(clean_route_name)
    
    # Process quantities and filter
    df = process_quantities(df)
    
    # Apply exclusions
    if exclude_returns:
        df = df[df['Document Type Descrw'] != 'Credit for Returns']
    if exclude_invoices:
        df = df[df['Document Type Descrw'] != 'Invoice']
    
    if df.empty:
        raise ValueError("No data remaining after filtering")
    
    # Process various column types
    df = process_numeric_columns(df)
    df = process_text_columns(df)
    
    # Process dates with optimized function
    date_col = process_dates(df)
    use_invoice = date_col == 'Invoice Date'
    suffix = 'Inv' if use_invoice else 'Returns'
    
    # Generate filename with date
    valid_dates = [d for d in df[date_col] if d != '0']
    if valid_dates:
        try:
            date_str = datetime.strptime(valid_dates[0], '%d/%m/%Y').strftime('%Y%m%d')
        except:
            date_str = datetime.now().strftime('%Y%m%d')
    else:
        date_str = datetime.now().strftime('%Y%m%d')
        logger.warning(f"No valid dates found in {orig_name}, using current date")
    
    # Determine location
    loc_code = df['Storage Location'].dropna().astype(str)
    location = location_map.get(loc_code.iloc[0] if len(loc_code) > 0 else '', 'Unknown').title()
    
    # Generate CSV
    csv_name = f"UKL_{location}{date_str}{suffix}.csv"
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False, quoting=csv.QUOTE_NONE, escapechar='\\', na_rep='')
    csv_bytes = buffer.getvalue().encode('utf-8-sig')
    
    return {
        'name': csv_name,
        'content': csv_bytes,
        'type': 'csv'
    }

def generate_output_files(session_id, exclude_returns, exclude_invoices):
    """Memory-optimized file processing"""
    files = get_session_files(session_id)
    
    if not files:
        raise ValueError("No files found for session")
    
    # Sort files by size (process smaller files first)
    files_with_size = []
    for filename in files:
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        size = os.path.getsize(filepath)
        files_with_size.append((filename, size))
    
    # Process smaller files first to avoid memory spikes
    files_with_size.sort(key=lambda x: x[1])
    
    processed = 0
    errors = []
    output_files = []
    
    for filename, size in files_with_size:
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        orig_name = filename[len(f"{session_id}_"):]
        
        try:
            # Check available memory before processing each file
            if get_memory_usage() > 600:  # 600MB threshold
                logger.warning("Memory high, forcing garbage collection")
                gc.collect()
            
            # Add timeout for individual file processing (45 seconds per file)
            with time_limit(45):
                result = process_single_file_memory_safe(
                    filepath, orig_name, exclude_returns, exclude_invoices
                )
            
            if result:
                output_files.append(result)
                processed += 1
                logger.info(f"Successfully processed: {orig_name} (Memory: {get_memory_usage():.1f}MB)")
                
        except TimeoutException as e:
            error_msg = f"FILE: {orig_name}\nERROR: Processing timeout - file too large or complex\n"
            errors.append(error_msg)
            logger.error(f"Timeout processing {orig_name}: {e}")
            
        except MemoryError:
            error_msg = f"FILE: {orig_name}\nERROR: File too large - out of memory\n"
            errors.append(error_msg)
            logger.error(f"Memory error processing {orig_name}")
            
            # Emergency memory cleanup
            gc.collect()
            continue
            
        except Exception as e:
            error_msg = f"FILE: {orig_name}\nERROR: {e}\n"
            errors.append(error_msg)
            logger.error(f"Processing failed for {orig_name}: {e}")
            
        finally:
            # Always clean up temporary file
            safe_file_cleanup(filepath)

    # Create error report if needed
    if errors:
        error_report = f"SAP CONVERTER ERRORS - {datetime.now():%Y-%m-%d %H:%M}\n"
        error_report += f"Success: {processed} | Failed: {len(errors)}\n\n"
        error_report += "\n".join(errors)
        output_files.append({
            'name': "ERRORS_AND_WARNINGS.txt",
            'content': error_report.encode('utf-8'),
            'type': 'error'
        })

    return output_files, processed, errors

@app.route('/cron')
def cron_health_check():
    """Health check endpoint with session cleanup"""
    cleanup_old_sessions()
    return jsonify({
        'status': 'healthy', 
        'time': datetime.now().isoformat(),
        'active_sessions': len(sessions),
        'memory_usage_mb': get_memory_usage()
    })

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/', methods=['POST'])
def upload():
    """Upload with memory-aware processing choice"""
    if 'files' not in request.files:
        return jsonify({'error': 'No files part'}), 400

    files = request.files.getlist('files')
    valid_files = [f for f in files if f and f.filename and allowed_file(f.filename)]
    
    if not valid_files:
        return jsonify({'error': 'No valid files selected. Please upload Excel files (.xlsx, .xls)'}), 400

    try:
        validate_upload(valid_files)
    except ValueError as e:
        return jsonify({'error': str(e)}), 400

    preview = 'preview' in request.form
    exclude_returns = 'exclude_returns' in request.form
    exclude_invoices = 'exclude_invoices' in request.form

    # Memory-aware decision making
    total_size = 0
    for file in valid_files:
        file.seek(0, 2)
        total_size += file.tell()
        file.seek(0)
    
    # Use async processing for larger uploads
    use_async = (len(valid_files) > 3 or total_size > 10 * 1024 * 1024)  # 10MB threshold

    session_id = str(uuid.uuid4())
    sessions[session_id] = {
        'created': datetime.now(),
        'exclude_returns': exclude_returns,
        'exclude_invoices': exclude_invoices
    }

    saved_files = []
    previews = {}

    for file in valid_files:
        safe_name = secure_filename(file.filename)
        if not safe_name:
            continue
            
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], f"{session_id}_{safe_name}")
        file.save(filepath)
        saved_files.append((safe_name, filepath))

        if preview:
            try:
                dfp = pd.read_excel(filepath, engine='openpyxl', dtype=str, nrows=5)
                previews[safe_name] = {
                    'html': dfp.head(5).to_html(classes='table table-sm', index=False, border=0),
                    'columns': list(dfp.columns)
                }
            except Exception as e:
                logger.error(f"Preview error for {safe_name}: {e}")
                previews[safe_name] = {
                    'html': f"<p class='text-danger'>Preview error: {str(e)}</p>",
                    'columns': []
                }

    if preview:
        return render_template('preview.html',
                               previews=previews.items(),
                               session_id=session_id,
                               exclude_returns=exclude_returns,
                               exclude_invoices=exclude_invoices)

    # Choose processing method based on size
    if use_async:
        # Redirect to async processing
        return redirect(url_for('async_process_files', session_id=session_id))
    else:
        # Use direct processing for small files
        if len(valid_files) == 1:
            return redirect(url_for('download_direct', session_id=session_id))
        else:
            return redirect(url_for('process_files', session_id=session_id))

@app.route('/async-process/<session_id>')
def async_process_files(session_id):
    """Async processing endpoint to avoid timeouts"""
    session = sessions.get(session_id)
    if not session:
        return jsonify({'error': 'Session expired'}), 404
    
    # Create async task
    task_id = str(uuid.uuid4())
    processing_queue.put((
        task_id,
        session_id,
        session.get('exclude_returns', False),
        session.get('exclude_invoices', False)
    ))
    
    # Clean session immediately since we're processing async
    sessions.pop(session_id, None)
    
    return jsonify({
        'task_id': task_id,
        'status': 'processing',
        'message': 'Files are being processed in background'
    })

@app.route('/async-status/<task_id>')
def async_status(task_id):
    """Check async processing status"""
    result = results_store.get(task_id)
    
    if not result:
        return jsonify({'status': 'unknown'})
    
    if result['status'] == 'completed':
        # Return download link or results
        return jsonify({
            'status': 'completed',
            'processed': result['processed'],
            'error_count': len(result['errors']),
            'download_url': f"/async-download/{task_id}"
        })
    
    return jsonify(result)

@app.route('/async-download/<task_id>')
def async_download(task_id):
    """Download results from async processing"""
    result = results_store.get(task_id)
    
    if not result or result['status'] != 'completed':
        return "Result not available", 404
    
    output_files = result['output_files']
    csv_files = [f for f in output_files if f['type'] == 'csv']
    
    if len(csv_files) == 1:
        # Single file download
        csv_file = csv_files[0]
        return Response(
            csv_file['content'],
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename="{csv_file["name"]}"'}
        )
    else:
        # Multiple files - return as ZIP
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
            for file_info in output_files:
                zf.writestr(file_info['name'], file_info['content'])
        
        zip_buffer.seek(0)
        
        # Clean up
        if task_id in results_store:
            del results_store[task_id]
        
        return send_file(
            zip_buffer,
            as_attachment=True,
            download_name=f"UKL_Processed_{datetime.now():%Y%m%d_%H%M%S}.zip",
            mimetype='application/zip'
        )

@app.route('/process/<session_id>')
def process_files(session_id):
    """Process uploaded files and return converted CSVs - for multiple files"""
    session = sessions.get(session_id)
    if not session:
        cleanup_old_sessions()
        return "Session expired or invalid", 404

    exclude_returns = session.get('exclude_returns', False)
    exclude_invoices = session.get('exclude_invoices', False)

    # Generate all output files
    output_files, processed, errors = generate_output_files(session_id, exclude_returns, exclude_invoices)

    # Clean session only after processing
    sessions.pop(session_id, None)

    if processed == 0:
        return "All files failed to process. Check the error log for details.", 400

    # Get only CSV files for decision making
    csv_files = [f for f in output_files if f['type'] == 'csv']
    
    # DEBUG: Log what files we have
    logger.info(f"Output files: {[f['name'] for f in output_files]}")
    logger.info(f"CSV files: {[f['name'] for f in csv_files]}")
    
    # For multiple files, always return as ZIP
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
        for file_info in output_files:
            zf.writestr(file_info['name'], file_info['content'])

    zip_buffer.seek(0)
    logger.info(f"Downloading ZIP with {len(output_files)} files")
    
    # For ZIP files, set proper headers
    zip_filename = f"UKL_Processed_{datetime.now():%Y%m%d_%H%M%S}.zip"
    response = send_file(
        zip_buffer,
        as_attachment=True,
        download_name=zip_filename,
        mimetype='application/zip'
    )
    
    # Add cache control headers
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    
    return response

@app.route('/download/<session_id>')
def download_direct(session_id):
    """Direct download endpoint with guaranteed filename handling - for single files"""
    session = sessions.get(session_id)
    if not session:
        return "Session expired or invalid", 404

    exclude_returns = session.get('exclude_returns', False)
    exclude_invoices = session.get('exclude_invoices', False)

    # Generate all output files
    output_files, processed, errors = generate_output_files(session_id, exclude_returns, exclude_invoices)

    # Clean session
    sessions.pop(session_id, None)

    if processed == 0:
        return "All files failed to process.", 400

    csv_files = [f for f in output_files if f['type'] == 'csv']
    if len(csv_files) >= 1:
        # Use the first CSV file for single file download
        csv_file = csv_files[0]
        logger.info(f"Direct download: {csv_file['name']}")
        
        # Return with explicit filename in headers
        return Response(
            csv_file['content'],
            mimetype='text/csv',
            headers={
                'Content-Disposition': f'attachment; filename="{csv_file["name"]}"',
                'Content-Type': 'text/csv; charset=utf-8',
                'Cache-Control': 'no-cache, no-store, must-revalidate',
                'Pragma': 'no-cache', 
                'Expires': '0'
            }
        )
    
    return "No CSV files generated", 500

@app.errorhandler(413)
def too_large(e):
    return "File too large (max 50MB)", 413

@app.errorhandler(500)
def internal_error(e):
    logger.error(f"Internal server error: {e}")
    logger.error(traceback.format_exc())
    return "Internal Server Error — check server logs", 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)