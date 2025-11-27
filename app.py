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
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024
app.config['SESSION_TIMEOUT'] = timedelta(hours=2)

os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

sessions = {}

def clean_route_name(value):
    """Remove only invisible Ctrl+J characters from Route Name"""
    if pd.isna(value) or value in ['', '0']:
        return value
    text = str(value)
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
        test = os.path.join(app.config['UPLOAD_FOLDER'], 'health.test')
        with open(test, 'w') as f: f.write('ok')
        os.remove(test)
        return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat()}), 200
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
        if not file or not file.filename: continue
        safe_name = secure_filename(file.filename)
        if not safe_name: continue
        path = os.path.join(app.config['UPLOAD_FOLDER'], f"{session_id}_{safe_name}")
        file.save(path)

        if preview:
            try:
                dfp = pd.read_excel(path, engine='openpyxl', dtype=str, nrows=5)
                previews[safe_name] = dfp.head(5).to_html(classes='table table-sm', index=False, border=0)
            except Exception as e:
                previews[safe_name] = f"<p class='text-danger'>Error: {e}</p>"

    if preview:
        return render_template('preview.html', previews=previews.items(), session_id=session_id,
                               exclude_returns=exclude_returns, exclude_invoices=exclude_invoices)
    return redirect(url_for('process_files', session_id=session_id))

@app.route('/process/<session_id>')
def process_files(session_id):
    cleanup_old_sessions()
    session = sessions.get(session_id)
    if not session:
        return "Session expired", 404

    exclude_returns = session.get('exclude_returns', False)
    exclude_invoices = session.get('exclude_invoices', False)

    pattern = f"{session_id}_"
    files = [f for f in os.listdir(app.config['UPLOAD_FOLDER']) if f.startswith(pattern)]
    if not files:
        return "No files", 404

    zip_buffer = io.BytesIO()
    processed = 0
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
            orig_name = filename[len(pattern):]

            try:
                df = pd.read_excel(filepath, engine='openpyxl', dtype=str, keep_default_na=False)

                # CLEAN ONLY Route Name
                if 'Route Name' in df.columns:
                    df['Route Name'] = df['Route Name'].apply(clean_route_name)

                # Required columns
                missing = [c for c in required_cols if c not in df.columns]
                if missing:
                    raise ValueError(f"Missing columns: {', '.join(missing)}")

                # Filter zero qty
                for c in ['Sale_Qty_Pcs', 'Free_Total_Qty']:
                    df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
                df = df[~((df['Sale_Qty_Pcs'] == 0) & (df['Free_Total_Qty'] == 0))]

                if exclude_returns:
                    df = df[df['Document Type Descrw'] != 'Credit for Returns']
                if exclude_invoices:
                    df = df[df['Document Type Descrw'] != 'Invoice']

                if df.empty:
                    raise ValueError("Empty after filtering")

                # Numeric columns
                for c in ['Pieces per Case', 'List Price per case', 'Gross Sale', 'Net Sale',
                          'Bonus_Discount', 'Trade_Discount', 'Cash_Discount', 'Total Discount']:
                    if c in df.columns:
                        df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)

                if 'Salesrep Code' in df.columns:
                    df['Salesrep Code'] = df['Salesrep Code'].astype(str).str.zfill(3)
                if 'Salesrep Name' in df.columns:
                    df['Salesrep Name'] = df['Salesrep Name'].str.strip().str.replace(r'\s+', ' ', regex=True)

                line_col = 'LineNum' if 'LineNum' in df.columns else (df.columns[7] if len(df.columns)>7 else None)
                if line_col:
                    df[line_col] = df[line_col].astype(str).str.zfill(2)

                # Choose date column
                counts = df['Document Type Descrw'].value_counts()
                use_invoice = counts.get('Invoice', 0) >= counts.get('Credit for Returns', 0)
                date_col = 'Invoice Date' if use_invoice else 'Reference Doc Date'
                suffix = 'Inv' if use_invoice else 'Returns'

                # FINAL DATE FIX — THIS ONE WORKS 100%
                for col in ['Invoice Date', 'Reference Doc Date']:
                    if col not in df.columns:
                        continue

                    # ONLY convert if it's a real Excel serial number (float > 59)
                    def safe_date_convert(val):
                        if pd.isna(val):
                            return '0'
                        s = str(val).strip()
                        if s in ['0', '0.0', '', 'NaT']:
                            return '0'
                        try:
                            num = float(s)
                            if num > 59:  # valid Excel date serial
                                dt = pd.to_datetime(num, unit='D', origin='1899-12-30', errors='coerce')
                                if pd.notna(dt):
                                    return dt.strftime('%d/%m/%Y')
                        except:
                            pass
                        # If not serial → try parsing as text (dayfirst)
                        try:
                            dt = pd.to_datetime(s, dayfirst=True, errors='coerce')
                            if pd.notna(dt):
                                return dt.strftime('%d/%m/%Y')
                        except:
                            pass
                        return '0'  # anything weird → '0'

                    df[col] = df[col].apply(safe_date_convert)

                # Get date for filename
                valid_dates = [d for d in df[date_col] if d != '0']
                if valid_dates:
                    date_str = datetime.strptime(valid_dates[0], '%d/%m/%Y').strftime('%Y%m%d')
                else:
                    date_str = datetime.now().strftime('%Y%m%d')

                loc = df['Storage Location'].dropna().astype(str)
                location = location_map.get(loc.iloc[0] if len(loc)>0 else '', 'Unknown').title()

                csv_name = f"UKL_{location}{date_str}{suffix}.csv"

                buf = io.StringIO()
                df.to_csv(buf, index=False, header=False, quoting=csv.QUOTE_NONE, escapechar='\\', na_rep='')
                zf.writestr(csv_name, buf.getvalue().encode('utf-8-sig'))

                processed += 1
                print(f"SUCCESS: {orig_name} → {csv_name}")

            except Exception as e:
                msg = f"FILE: {orig_name}\nERROR: {e}\nTRACEBACK:\n{traceback.format_exc()}\n{'-'*70}\n"
                errors.append(msg)
                print(f"FAILED: {orig_name}")

            finally:
                try:
                    os.remove(filepath)
                except:
                    pass

        if errors:
            report = f"SAP PROCESSING ERRORS - {datetime.now():%Y-%m-%d %H:%M}\n"
            report += f"Success: {processed} | Failed: {len(errors)}\n\n"
            report += "\n".join(errors)
            zf.writestr("ERRORS_AND_WARNINGS.txt", report.encode('utf-8'))

    sessions.pop(session_id, None)

    if processed == 0:
        return "All files failed", 400

    zip_buffer.seek(0)
    return send_file(
        zip_buffer,
        as_attachment=True,
        download_name=f"UKL_Processed_{datetime.now():%Y%m%d_%H%M%S}{'_errors' if errors else ''}.zip",
        mimetype='application/zip'
    )

@app.errorhandler(413)
def too_large(e):
    return "File too large (max 50MB)", 413

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)