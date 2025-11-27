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
    """Fix various date formats to DD/MM/YYYY"""
    if pd.isna(val):
        return '0'
    
    s = str(val).strip()
    if s in ['', '0', '0.0', 'NaT']:
        return '0'
    
    # Handle Excel serial date numbers
    try:
        n = float(s)
        if n > 59:  # Reasonable date threshold
            dt = pd.to_datetime(n, unit='D', origin='1899-12-30', errors='coerce')
            if pd.notna(dt):
                return dt.strftime('%d/%m/%Y')
    except (ValueError, TypeError):
        pass
    
    # Handle string dates
    try:
        dt = pd.to_datetime(s, dayfirst=True, errors='coerce')
        if pd.notna(dt):
            return dt.strftime('%d/%m/%Y')
    except (ValueError, TypeError):
        pass
    
    return '0'

def process_dates(df):
    """Process date columns and determine which date to use"""
    date_columns = ['Invoice Date', 'Reference Doc Date']
    
    for col in date_columns:
        if col in df.columns:
            df[col] = df[col].apply(fix_date_value)
    
    # Determine which date column to use based on document type counts
    counts = df['Document Type Descrw'].value_counts()
    use_invoice = counts.get('Invoice', 0) >= counts.get('Credit for Returns', 0)
    
    return 'Invoice Date' if use_invoice else 'Reference Doc Date'

def generate_output_files(session_id, exclude_returns, exclude_invoices):
    """Process files and return list of (filename, content) tuples"""
    pattern = f"{session_id}_"
    files = [f for f in os.listdir(app.config['UPLOAD_FOLDER']) if f.startswith(pattern)]
    
    processed = 0
    errors = []
    output_files = []

    required_cols = ['Document Type Descrw', 'Storage Location', 'Sale_Qty_Pcs', 'Free_Total_Qty']
    location_map = {
        '15330599': 'lodwar', '15510868': 'kapsabet', '15393486': 'bomet', '50260522': 'nakuru',
        '50260577': 'savemore', '18010415': 'kisii', '50260971': 'molo', '15580524': 'naivasha',
        '18041985': 'rongo', '15580523': 'nyahururu', '15393485': 'kericho', '15510770': 'eldoret',
    }

    for filename in files:
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        orig_name = filename[len(pattern):]

        try:
            # Read Excel file
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
            
            # Process dates and determine which to use
            date_col = process_dates(df)
            use_invoice = date_col == 'Invoice Date'
            suffix = 'Inv' if use_invoice else 'Returns'
            
            # Generate filename with date
            valid_dates = [d for d in df[date_col] if d != '0']
            if valid_dates:
                date_str = datetime.strptime(valid_dates[0], '%d/%m/%Y').strftime('%Y%m%d')
            else:
                date_str = datetime.now().strftime('%Y%m%d')
                logger.warning(f"No valid dates found in {orig_name}, using current date")
            
            # Determine location
            loc_code = df['Storage Location'].dropna().astype(str)
            location = location_map.get(loc_code.iloc[0] if len(loc_code) > 0 else '', 'Unknown').title()
            
            # Generate CSV with proper naming convention
            csv_name = f"UKL_{location}{date_str}{suffix}.csv"
            buffer = io.StringIO()
            df.to_csv(buffer, index=False, header=False, quoting=csv.QUOTE_NONE, escapechar='\\', na_rep='')
            csv_bytes = buffer.getvalue().encode('utf-8-sig')
            
            output_files.append({
                'name': csv_name,
                'content': csv_bytes,
                'type': 'csv'
            })
            processed += 1
            logger.info(f"Successfully processed: {orig_name} -> {csv_name}")

        except Exception as e:
            error_msg = f"FILE: {orig_name}\nERROR: {e}\n{traceback.format_exc()}\n{'-'*60}\n"
            errors.append(error_msg)
            logger.error(f"Processing failed for {orig_name}: {e}")
        finally:
            # Clean up temporary file
            try:
                os.remove(filepath)
            except Exception as e:
                logger.error(f"Error deleting temporary file {filepath}: {e}")

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
        'active_sessions': len(sessions)
    })

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/', methods=['POST'])
def upload():
    """Handle file uploads with optional preview"""
    if 'files' not in request.files:
        return jsonify({'error': 'No files part'}), 400

    files = request.files.getlist('files')
    valid_files = [f for f in files if f and f.filename and allowed_file(f.filename)]
    
    if not valid_files:
        return jsonify({'error': 'No valid files selected. Please upload Excel files (.xlsx, .xls)'}), 400

    preview = 'preview' in request.form
    exclude_returns = 'exclude_returns' in request.form
    exclude_invoices = 'exclude_invoices' in request.form

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

    # For single file, redirect to download endpoint for better filename handling
    # For multiple files, use the regular process endpoint
    if len(valid_files) == 1:
        return redirect(url_for('download_direct', session_id=session_id))
    else:
        return redirect(url_for('process_files', session_id=session_id))

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
    logger.error(traceback.format_exc())  # Add this to see the full traceback
    return "Internal Server Error — check server logs", 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)  # Set debug=True to see detailed errors