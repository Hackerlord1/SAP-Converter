from flask import Flask, request, send_file, render_template, redirect, url_for, jsonify
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

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'temp_uploads'
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB
app.config['SESSION_TIMEOUT'] = timedelta(hours=2)

os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# Session store with timestamp
sessions = {}  # {session_id: {'created': dt, 'exclude_returns': bool, ...}}

def clean_route_name(value):
    """Remove invisible Ctrl+J characters and Unicode gremlins ONLY from Route Name"""
    if pd.isna(value) or value == '':
        return value
    text = str(value)
    # Remove zero-width, non-breaking spaces, BOM, control chars — exactly what Ctrl+J finds
    cleaned = re.sub(r'[\u200B-\u200D\u2060\uFEFF\u00A0\r\n\t\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', text)
    return cleaned.strip()

def cleanup_old_sessions():
    now = datetime.now()
    expired = [sid for sid, data in sessions.items() if now - data['created'] > app.config['SESSION_TIMEOUT']]
    for sid in expired:
        pattern = f"{sid}_"
        for f in os.listdir(app.config['UPLOAD_FOLDER']):
            if f.startswith(pattern):
                try:
                    os.remove(os.path.join(app.config['UPLOAD_FOLDER'], f))
                except:
                    pass
        sessions.pop(sid, None)

@app.route('/cron')
def cron_health_check():
    cleanup_old_sessions()
    try:
        test_path = os.path.join(app.config['UPLOAD_FOLDER'], 'health.test')
        with open(test_path, 'w') as f:
            f.write('ok')
        os.remove(test_path)
        return jsonify({
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'active_sessions': len(sessions),
            'temp_files': len(os.listdir(app.config['UPLOAD_FOLDER']))
        }), 200
    except Exception as e:
        return jsonify({'status': 'error', 'error': str(e)}), 500

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
        'created': datetime.now(),
        'exclude_returns': exclude_returns,
        'exclude_invoices': exclude_invoices
    }

    previews = {}
    for file in files:
        if not file or not file.filename:
            continue
        safe_name = secure_filename(file.filename)
        if not safe_name:
            continue
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], f"{session_id}_{safe_name}")
        file.save(filepath)

        if preview:
            try:
                df_preview = pd.read_excel(filepath, engine='openpyxl', dtype=str, nrows=5)
                previews[safe_name] = df_preview.head(5).to_html(
                    classes='table table-striped table-sm', index=False, border=0, na_rep=''
                )
            except Exception as e:
                previews[safe_name] = f"<p class='text-danger'>Preview error: {e}</p>"

    if preview:
        return render_template('preview.html', previews=previews.items(), session_id=session_id,
                               exclude_returns=exclude_returns, exclude_invoices=exclude_invoices)
    return redirect(url_for('process_files', session_id=session_id))

@app.route('/process/<session_id>')
def process_files(session_id):
    cleanup_old_sessions()

    session = sessions.get(session_id)
    if not session:
        return "Session expired or invalid", 404

    exclude_returns = session.get('exclude_returns', False)
    exclude_invoices = session.get('exclude_invoices', False)

    pattern = f"{session_id}_"
    files = [f for f in os.listdir(app.config['UPLOAD_FOLDER']) if f.startswith(pattern)]
    if not files:
        return "No files found", 404

    zip_buffer = io.BytesIO()
    zip_buffer = io.BytesIO()
    processed_count = 0
    errors = []

    required_cols = ['Document Type Descrw', 'Storage Location', 'Sale_Qty_Pcs', 'Free_Total_Qty']
    location_map = {
        '15330599': 'lodwar', '15510868': 'kapsabet', '15393486': 'bomet', '50260522': 'nakuru',
        '50260577': 'savemore', '18010415': 'kisii', '50260971': 'molo', '15580524': 'naivasha',
        '18041985': 'rongo', '15580523': 'nyahururu', '15393485': 'kericho', '15510770': 'eldoret',
    }

    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
        for filename in files:
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            orig_filename = filename[len(pattern):]  # human readable name
            try:
                df = pd.read_excel(filepath, engine='openpyxl', dtype=str, keep_default_na=False)

                # === CLEAN ONLY Route Name column
                if 'Route Name' in df.columns:
                    df['Route Name'] = df['Route Name'].apply(clean_route_name)
                # Optional: warn if missing
                # else:
                #     print(f"'Route Name' column missing in {orig_filename}")

                missing = [c for c in required_cols if c not in df.columns]
                if missing:
                    raise ValueError(f"Missing required columns: {', '.join(missing)}")

                # Convert quantities
                for col in ['Sale_Qty_Pcs', 'Free_Total_Qty']:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                df = df[~((df['Sale_Qty_Pcs'] == 0) & (df['Free_Total_Qty'] == 0))]

                if exclude_returns:
                    df = df[df['Document Type Descrw'] != 'Credit for Returns']
                if exclude_invoices:
                    df = df[df['Document Type Descrw'] != 'Invoice']

                if df.empty:
                    raise ValueError("No data remaining after filtering")

                # Normalize numeric columns
                num_cols = ['Pieces per Case', 'List Price per case', 'Gross Sale', 'Net Sale',
                            'Bonus_Discount', 'Trade_Discount', 'Cash_Discount', 'Total Discount']
                for col in num_cols:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

                if 'Salesrep Code' in df.columns:
                    df['Salesrep Code'] = df['Salesrep Code'].astype(str).str.zfill(3)
                if 'Salesrep Name' in df.columns:
                    df['Salesrep Name'] = df['Salesrep Name'].str.strip().str.replace(r'\s+', ' ', regex=True)

                line_col = 'LineNum' if 'LineNum' in df.columns else (df.columns[7] if len(df.columns) > 7 else None)
                if line_col:
                    df[line_col] = df[line_col].astype(str).str.zfill(2)

                # Smart date column selection
                doc_counts = df['Document Type Descrw'].value_counts()
                use_invoice = doc_counts.get('Invoice', 0) >= doc_counts.get('Credit for Returns', 0)
                date_col = 'Invoice Date' if use_invoice else 'Reference Doc Date'
                date_col = 'Invoice Date' if use_invoice else 'Reference Doc Date'
                suffix = 'Inv' if use_invoice else 'Returns'

                # Robust date formatting (handles Excel serials + strings)
                for dc in ['Invoice Date', 'Reference Doc Date']:
                    if dc in df.columns:
                        # Excel serial numbers
                        numeric = pd.to_numeric(df[dc], errors='coerce')
                        mask = numeric.notna()
                        if mask.any():
                            df.loc[mask, dc] = pd.to_datetime(numeric[mask], unit='D', origin='1899-12-30').dt.strftime('%d/%m/%Y')

                        # String dates
                        df[dc] = pd.to_datetime(df[dc], dayfirst=True, errors='coerce').dt.strftime('%d/%m/%Y')
                        df[dc] = df[dc].fillna('0')

                # Filename date
                valid_date = df[date_col][df[date_col] != '0'].iloc[0] if not df[date_col][df[date_col] != '0'].empty else None
                if valid_date and valid_date != '0':
                    try:
                        date_str = datetime.strptime(valid_date, '%d/%m/%Y').strftime('%Y%m%d')
                    except:
                        date_str = datetime.now().strftime('%Y%m%d')
                else:
                    date_str = datetime.now().strftime('%Y%m%d')

                loc_code = df['Storage Location'].dropna().astype(str).iloc[0] if not df['Storage Location'].dropna().empty else ''
                location = location_map.get(loc_code, 'Unknown').title()

                csv_name = f"UKL_{location}{date_str}{suffix}.csv"

                buffer = io.StringIO()
                df.to_csv(buffer, index=False, header=False, quoting=csv.QUOTE_NONE, escapechar='\\', na_rep='')
                zf.writestr(csv_name, buffer.getvalue().encode('utf-8-sig'))

                processed_count += 1
                print(f"SUCCESS: {orig_filename} → {csv_name}")

            except Exception as e:
                error_msg = f"FILE: {orig_filename}\nERROR: {str(e)}\n"
                error_msg += f"TRACEBACK:\n{traceback.format_exc()}\n"
                error_msg += "-" * 60 + "\n"
                errors.append(error_msg)
                print(f"FAILED: {orig_filename} → {e}")

            finally:
                if os.path.exists(filepath):
                    try:
                        os.remove(filepath)
                    except:
                        pass

        # Add error log inside ZIP
        if errors:
            summary = "SAP B1 PROCESSING ERROR REPORT\n"
            summary += f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            summary += f"Total files: {len(files)} | Success: {processed_count} | Failed: {len(errors)}\n\n"
            summary += "\n".join(errors)
            zf.writestr("ERRORS_AND_WARNINGS.txt", summary.encode('utf-8'))

    sessions.pop(session_id, None)

    if processed_count == 0:
        return "All files failed to process. Check ERRORS_AND_WARNINGS.txt in previous downloads.", 400

    zip_buffer.seek(0)
    return send_file(
        zip_buffer,
        as_attachment=True,
        download_name=f"UKL_Processed_{datetime.now().strftime('%Y%m%d_%H%M%S')}{'_with_errors' if errors else ''}.zip",
        mimetype='application/zip'
    )

@app.errorhandler(413)
def too_large(e):
    return "File too large (max 50MB)", 413

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)