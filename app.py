
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
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB max file size
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

@app.route('/', methods=['GET'])
def index():
    return render_template('index.html')

@app.route('/', methods=['POST'])
def upload():
    files = request.files.getlist('files')
    if not files or all(f.filename == '' for f in files):
        return "No files selected", 400
    
    preview = 'preview' in request.form
    session_id = str(uuid.uuid4())
    previews = {}
    
    for file in files:
        if file.filename == '':
            continue
        secure_name = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], f"{session_id}_{secure_name}")
        file.save(filepath)
        
        if preview:
            try:
                df = pd.read_excel(filepath, engine='openpyxl', dtype={'Document Type Descrw': str})
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
        return render_template('preview.html', previews=previews.items(), session_id=session_id)
    else:
        return redirect(url_for('process_files', session_id=session_id))

@app.route('/process/<session_id>')
def process_files(session_id):
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
                with open(filepath, 'rb') as f:
                    df = pd.read_excel(f, engine='openpyxl', dtype={'Document Type Descrw': str})
                
                missing_cols = [col for col in required_cols if col not in df.columns]
                if missing_cols:
                    raise ValueError(f"Missing required columns: {', '.join(missing_cols)}")
                
                # Step 1: Filter rows where both Sale_Qty_Pcs and Free_Total_Qty are 0
                df['Sale_Qty_Pcs'] = pd.to_numeric(df['Sale_Qty_Pcs'], errors='coerce').fillna(0)
                df['Free_Total_Qty'] = pd.to_numeric(df['Free_Total_Qty'], errors='coerce').fillna(0)
                df = df[~((df['Sale_Qty_Pcs'] == 0) & (df['Free_Total_Qty'] == 0))]
                
                # Step 2: Replace empties with 0 in specified columns
                target_cols = ['Pieces per Case', 'List Price per case', 'Sale_Qty_Pcs', 'Free_Total_Qty',
                               'Gross Sale', 'Net Sale', 'Bonus_Discount', 'Trade_Discount',
                               'Cash_Discount', 'Total Discount']
                for col in target_cols:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                
                # Standardize Salesrep Code (pad numeric codes to 3 digits with leading zeros)
                if 'Salesrep Code' in df.columns:
                    df['Salesrep Code'] = df['Salesrep Code'].astype(str)
                    mask_numeric = df['Salesrep Code'].str.isdigit()
                    df.loc[mask_numeric, 'Salesrep Code'] = df.loc[mask_numeric, 'Salesrep Code'].str.zfill(3)
                
                # Standardize Salesrep Name (trim extra spaces if present)
                if 'Salesrep Name' in df.columns:
                    df['Salesrep Name'] = df['Salesrep Name'].str.strip().str.replace(r'\s+', ' ', regex=True)
                
                # Pad LineNum to 2 digits (assuming column index 7 or named 'LineNum')
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
                
                # Convert date column to datetime for filename (with NaT handling)
                df_temp = df[date_col].copy()
                df_temp = pd.to_datetime(df_temp, errors='coerce')
                dates = df_temp.dropna().unique()
                if len(dates) > 1:
                    print(f"Warning: Multiple dates detected in {filename}; using first: {dates[0]}")
                date_str = dates[0].strftime('%Y%m%d') if len(dates) >= 1 and pd.notna(dates[0]) else datetime.now().strftime('%Y%m%d')
                csv_filename = f"UKL_{location}{date_str}{suffix}.csv"
                
                # Format dates to YYYY-MM-DD (ISO) for SQL/SAP compatibility; invalids (incl. 1970-01-01/zero) to '0'
                for dc in ['Invoice Date', 'Reference Doc Date']:
                    if dc in df.columns:
                        df[dc] = pd.to_datetime(df[dc], errors='coerce', dayfirst=True)  # Handles DD/MM/YYYY input
                        
                        # Mask epoch (from zero inputs) as invalid
                        epoch_mask = (df[dc] == pd.Timestamp('1970-01-01'))
                        df[dc] = df[dc].where(~epoch_mask, pd.NaT)
                        
                        invalid_mask = df[dc].isna()
                        if invalid_mask.sum() > 0:
                            print(f"Info: {invalid_mask.sum()} invalid dates (incl. zeros/epoch) in {dc} for {filename}; setting to '0'")
                        
                        df[dc] = df[dc].dt.strftime('%Y-%m-%d').where(df[dc].notna(), '0')
                        df[dc] = df[dc].astype(str)  # '0' for invalids in CSV
                # Write to CSV in ZIP
                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, index=False, header=False, quoting=3, na_rep='')
                csv_content = csv_buffer.getvalue().encode('utf-8')
                zip_file.writestr(csv_filename, csv_content)
                processed_files += 1
                
            except Exception as e:
                print(f"Error processing {filename}: {str(e)}")
                error_files.append(f"{filename}: {str(e)}")
                continue
            finally:
                if os.path.exists(filepath):
                    os.remove(filepath)
    
    # Clean up
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