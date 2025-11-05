from flask import Flask, request, send_file, render_template, redirect, url_for
import pandas as pd
import io
from datetime import datetime
import zipfile
import os
import uuid
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'temp_uploads'
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

sessions = {}

@app.route('/', methods=['GET'])
def index():
    return render_template('index.html')

@app.route('/', methods=['POST'])
def upload():
    files = request.files.getlist('files')
    if not files or all(f.filename == '' for f in files):
        return "No files selected", 400
    
    preview = 'preview' in request.form
    exclude_returns = 'exclude_returns' in request.form
    exclude_invoices = 'exclude_invoices' in request.form
    session_id = str(uuid.uuid4())
    sessions[session_id] = {
        'exclude_returns': exclude_returns,
        'exclude_invoices': exclude_invoices
    }
    previews = {}
    
    for file in files:
        if file.filename == '':
            continue
        secure_name = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], f"{session_id}_{secure_name}")
        file.save(filepath)
        
        if preview:
            try:
                # Read as strings to preserve raw format
                df = pd.read_excel(filepath, engine='openpyxl', dtype=str)
                preview_df = df.head(5).to_html(
                    classes='table table-striped table-hover table-sm', 
                    index=False, 
                    escape=False, 
                    border=0,
                    na_rep=''
                )
                previews[secure_name] = preview_df
            except Exception as e:
                previews[secure_name] = f"<p class='text-danger'><i class='fas fa-exclamation-triangle me-1'></i>Error loading preview: {str(e)}</p>"
    
    if preview:
        return render_template('preview.html', previews=previews.items(), session_id=session_id, 
                             exclude_returns=exclude_returns, exclude_invoices=exclude_invoices)
    else:
        return redirect(url_for('process_files', session_id=session_id))

@app.route('/process/<session_id>')
def process_files(session_id):
    session_data = sessions.get(session_id, {})
    exclude_returns = session_data.get('exclude_returns', False)
    exclude_invoices = session_data.get('exclude_invoices', False)
    
    files = [f for f in os.listdir(app.config['UPLOAD_FOLDER']) if f.startswith(session_id + '_')]
    if not files:
        return "Session not found or files already processed", 404
    
    zip_buffer = io.BytesIO()
    processed_files = 0
    error_files = []
    required_cols = ['Document Type Descrw', 'Storage Location', 'Sale_Qty_Pcs', 'Free_Total_Qty']
    
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        for filename in files:
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            try:
                # READ EVERYTHING AS STRINGS - NO DATE INTERPRETATION
                with open(filepath, 'rb') as f:
                    df = pd.read_excel(f, engine='openpyxl', dtype=str)
                
                missing_cols = [col for col in required_cols if col not in df.columns]
                if missing_cols:
                    raise ValueError(f"Missing required columns: {', '.join(missing_cols)}")
                
                # Debug: Show raw date values before any processing
                for date_col in ['Invoice Date', 'Reference Doc Date']:
                    if date_col in df.columns:
                        sample_values = df[date_col].head(3).tolist()
                        print(f"DEBUG {filename} - Raw {date_col} values: {sample_values}")
                        print(f"DEBUG {filename} - Raw {date_col} types: {[type(x).__name__ for x in sample_values]}")
                
                # Step 1: Filter rows where both Sale_Qty_Pcs and Free_Total_Qty are 0
                df['Sale_Qty_Pcs'] = pd.to_numeric(df['Sale_Qty_Pcs'], errors='coerce').fillna(0)
                df['Free_Total_Qty'] = pd.to_numeric(df['Free_Total_Qty'], errors='coerce').fillna(0)
                df = df[~((df['Sale_Qty_Pcs'] == 0) & (df['Free_Total_Qty'] == 0))]
                
                if exclude_returns:
                    initial_rows = len(df)
                    df = df[df['Document Type Descrw'] != 'Credit for Returns']
                    returns_removed = initial_rows - len(df)
                    if returns_removed > 0:
                        print(f"Info: Removed {returns_removed} return rows from {filename}")
                
                if exclude_invoices:
                    initial_rows = len(df)
                    df = df[df['Document Type Descrw'] != 'Invoice']
                    invoices_removed = initial_rows - len(df)
                    if invoices_removed > 0:
                        print(f"Info: Removed {invoices_removed} invoice rows from {filename}")
                
                # Step 2: Replace empties with 0 in specified columns
                target_cols = ['Pieces per Case', 'List Price per case', 'Sale_Qty_Pcs', 'Free_Total_Qty',
                               'Gross Sale', 'Net Sale', 'Bonus_Discount', 'Trade_Discount',
                               'Cash_Discount', 'Total Discount']
                for col in target_cols:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                
                # Standardize Salesrep Code
                if 'Salesrep Code' in df.columns:
                    df['Salesrep Code'] = df['Salesrep Code'].astype(str)
                    mask_numeric = df['Salesrep Code'].str.isdigit()
                    df.loc[mask_numeric, 'Salesrep Code'] = df.loc[mask_numeric, 'Salesrep Code'].str.zfill(3)
                
                # Standardize Salesrep Name
                if 'Salesrep Name' in df.columns:
                    df['Salesrep Name'] = df['Salesrep Name'].str.strip().str.replace(r'\s+', ' ', regex=True)
                
                # Pad LineNum to 2 digits
                if len(df.columns) > 7:
                    line_col = df.columns[7] if 'LineNum' not in df.columns else 'LineNum'
                    if line_col in df.columns:
                        df[line_col] = df[line_col].astype(str).str.zfill(2)
                
                if len(df) == 0:
                    error_files.append(f"{filename}: No data after filtering")
                    continue
                
                # Step 3: Rename logic
                doc_types = df['Document Type Descrw'].value_counts()
                invoice_count = doc_types.get('Invoice', 0)
                credit_count = doc_types.get('Credit for Returns', 0)
                
                if invoice_count > credit_count:
                    date_col = 'Invoice Date'
                    suffix = 'Inv'
                else:
                    date_col = 'Reference Doc Date'
                    suffix = 'Returns'
                
                # Location mapping
                location_map = {
                    '15330599': 'lodwar', '15510868': 'kapsabet', '15393486': 'bomet', '50260522': 'nakuru',
                    '50260577': 'savemore', '18010415': 'kisii', '50260971': 'molo', '15580524': 'naivasha',
                    '18041985': 'rongo', '15580523': 'nyahururu', '15393485': 'kericho', '15510770': 'eldoret',
                }
                
                storage_locs = df['Storage Location'].astype(str).dropna().unique()
                location = location_map.get(storage_locs[0] if len(storage_locs) > 0 else 'Unknown', 'Unknown').title()
                
                # FIXED DATE PROCESSING - Treat as raw strings and parse as DD/MM/YYYY
                for dc in ['Invoice Date', 'Reference Doc Date']:
                    if dc in df.columns:
                        print(f"DEBUG: Processing {dc} column as raw strings")
                        
                        def parse_and_format_date(raw_date):
                            if pd.isna(raw_date) or raw_date in ['', '0', 'NaN', 'NaT']:
                                return '0'
                            
                            # If it's already a string in DD/MM/YYYY format, use it as is
                            if isinstance(raw_date, str):
                                # Check if it's already in DD/MM/YYYY format
                                if '/' in raw_date:
                                    parts = raw_date.split('/')
                                    if len(parts) == 3:
                                        day, month, year = parts
                                        # Validate it's DD/MM/YYYY (first part <= 31, second part <= 12)
                                        if (day.isdigit() and month.isdigit() and year.isdigit() and
                                            len(day) == 2 and len(month) == 2 and len(year) == 4 and
                                            int(day) <= 31 and int(month) <= 12):
                                            return f"{day}/{month}/{year}"
                                
                                # If it's in MM/DD/YYYY format, convert to DD/MM/YYYY
                                if '/' in raw_date:
                                    parts = raw_date.split('/')
                                    if len(parts) == 3:
                                        month, day, year = parts
                                        if (month.isdigit() and day.isdigit() and year.isdigit() and
                                            len(month) == 2 and len(day) == 2 and len(year) == 4 and
                                            int(month) <= 12 and int(day) <= 31):
                                            return f"{day}/{month}/{year}"
                                
                                # If it's in YYYY-MM-DD format (pandas default), convert to DD/MM/YYYY
                                if '-' in raw_date:
                                    parts = raw_date.split('-')
                                    if len(parts) == 3:
                                        year, month, day = parts
                                        if (year.isdigit() and month.isdigit() and day.isdigit() and
                                            len(year) == 4 and len(month) == 2 and len(day) == 2 and
                                            int(month) <= 12 and int(day) <= 31):
                                            return f"{day}/{month}/{year}"
                            
                            # If we can't parse it, return as is (will be '0' if invalid)
                            return str(raw_date)
                        
                        # Apply the parsing and formatting
                        df[dc] = df[dc].apply(parse_and_format_date)
                        
                        # Debug final output
                        final_sample = df[dc].head(3).tolist()
                        print(f"DEBUG: Final formatted {dc}: {final_sample}")
                
                # Get date for filename - use first valid date
                if date_col in df.columns:
                    # Find first valid DD/MM/YYYY date for filename
                    valid_dates = []
                    for date_str in df[date_col]:
                        if (date_str != '0' and '/' in date_str and 
                            len(date_str.split('/')) == 3):
                            day, month, year = date_str.split('/')
                            if (day.isdigit() and month.isdigit() and year.isdigit() and
                                len(year) == 4):
                                valid_dates.append(f"{year}{month}{day}")
                                break
                    
                    if valid_dates:
                        date_str = valid_dates[0]
                    else:
                        date_str = datetime.now().strftime('%Y%m%d')
                else:
                    date_str = datetime.now().strftime('%Y%m%d')
                
                csv_filename = f"UKL_{location}{date_str}{suffix}.csv"
                
                # Write to CSV in ZIP
                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, index=False, header=False, quoting=3, na_rep='')
                csv_content = csv_buffer.getvalue().encode('utf-8')
                zip_file.writestr(csv_filename, csv_content)
                processed_files += 1
                
                print(f"SUCCESS: Processed {filename} -> {csv_filename}")
                
            except Exception as e:
                print(f"ERROR processing {filename}: {str(e)}")
                import traceback
                print(f"TRACEBACK: {traceback.format_exc()}")
                error_files.append(f"{filename}: {str(e)}")
                continue
            finally:
                if os.path.exists(filepath):
                    os.remove(filepath)
    
    # Clean up session
    if session_id in sessions:
        del sessions[session_id]
    
    # Clean up temp folder if empty
    if os.path.exists(app.config['UPLOAD_FOLDER']) and not os.listdir(app.config['UPLOAD_FOLDER']):
        try:
            os.rmdir(app.config['UPLOAD_FOLDER'])
        except OSError:
            pass
    
    if processed_files > 0:
        error_summary = "_with_warnings" if error_files else ""
        zip_buffer.seek(0)
        return send_file(
            zip_buffer,
            as_attachment=True,
            download_name=f'Processed_Files{error_summary}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.zip',
            mimetype='application/zip'
        )
    else:
        error_msg = "No valid data found after processing."
        if error_files:
            error_msg += f" Errors: {'; '.join(error_files)}"
        return error_msg, 400

@app.errorhandler(413)
def too_large(e):
    return "File too large. Maximum size is 50MB.", 413

@app.errorhandler(500)
def internal_error(error):
    return "An internal error occurred. Please try again.", 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)