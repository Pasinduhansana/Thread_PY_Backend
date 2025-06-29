from flask import Flask, json, request, jsonify, send_file
import pandas as pd
import os
import re
import requests
from openpyxl import load_workbook
import smtplib
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
# from dotenv import load_dotenv
# load_dotenv()

app = Flask(__name__)

# Allow frontend to communicate with the backend
from flask_cors import CORS
CORS(app)

UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

@app.route("/upload_Dashboard", methods=["POST"])
def upload_file():

    #Load PCD File - if not get the previous PCD File
    pcd_file_path=os.path.join(UPLOAD_FOLDER,"Production Plan.xlsx")
    pcd_df = pd.read_excel(pcd_file_path, engine="openpyxl")
    print("pass data")
    pcd_df.rename(columns={    pcd_df.columns[0]: 'OC',
                                pcd_df.columns[1]: 'PCD',  
                                pcd_df.columns[2]: 'PSD'   
                            }, inplace=True)
    print("pass data 1")
    
    kpi_file = request.files.get("kpi")
    invoice_file = request.files.get("invoice")
    if not kpi_file or not invoice_file:
        return jsonify({"error": "Both files are required"}), 400

    kpi_path = os.path.join(UPLOAD_FOLDER, kpi_file.filename)
    invoice_path = os.path.join(UPLOAD_FOLDER, invoice_file.filename)
    kpi_file.save(kpi_path)
    invoice_file.save(invoice_path)

    
    try:
        
        # Read the Excel file into a DataFrapipme
        columns_to_read = ["OC Number", "Season","Style Code","Style Name","Supplier Name","RMPONo","RMPO Status","Article Sub Category","Article Code","Article Name","Color Code","Color Name","Size Code","PO Qty(Purchase UOM)","Received Qty","Balance to Receive Qty","Ship to Location","PO Value","OCFactory","DS In-House","Ship to Location"]
        df = pd.read_excel(kpi_file, usecols=columns_to_read,nrows=100000, engine="openpyxl")   
        df = df[df['Article Sub Category'] == 'THREAD (DECIMAL)']

        # Debugging: Check if DataFrame is empty
        if df.empty:
            raise ValueError("The Excel file is empty or not read properly.")

        # Define the column name to sort by (Change this to your actual date column)
        Balance_to_Receive_Qty_column = "Balance to Receive Qty" 
        PO_Qty_Column = "PO Qty(Purchase UOM)" 
        Received_qty = "Received Qty"
        sub_category = "Article Sub Category"
        Ship_to_location = "Ship to Location"
        article_code = "Article Code"
        article_name = "Article Name"
        article_colour_code = "Color Code"
        article_colour_name = "Color Name"
        rmpo_no = "RMPONo" 
        
        print("test 0.1")
        # Convert PCD/PSD columns to datetime
        pcd_df["PCD"] = pd.to_datetime(pcd_df["PCD"], errors='coerce')
        pcd_df["PSD"] = pd.to_datetime(pcd_df["PSD"], errors='coerce')
 
        print(pcd_df)
        
        # Find earliest PCD/PSD for each OC (or PO if that's the key)
        earliest_pcd = pcd_df.groupby('OC')['PCD'].min().reset_index().rename(columns={'PCD': 'Earliest PCD'})
        earliest_psd = pcd_df.groupby('OC')['PSD'].min().reset_index().rename(columns={'PSD': 'Earliest PSD'})
        print("test 0.2")

        # Merge with KPI data on OC
        # Need to print earliest PCd list 

        # print("KPI columns:", df.columns.tolist())
        #pcd_df['OC Number'] = pcd_df['OC'] 
        # print("PCD columns:", pcd_df.columns.tolist())
        # print("df OC Number sample:", df)
        # print(df)
        
        df = df.merge(earliest_pcd, left_on="OC Number", right_on='OC', how='left')
        print("test 0.3")
        df = df.merge(earliest_psd, left_on="OC Number", right_on='OC', how='left')
        print("test 0.4")
        # print("KPI columns:", df.columns.tolist())
        # print(df)
#
        # If you want to group by PO, do:
        earliest_pcd_article = df.groupby(['RMPONo', 'Article Code', 'Color Code'])['Earliest PCD'].min().reset_index()
        earliest_psd_article = df.groupby(['RMPONo', 'Article Code', 'Color Code'])['Earliest PSD'].min().reset_index()
        
        df = df.merge(earliest_pcd_article,left_on=['RMPONo', 'Article Code', 'Color Code'],right_on=['RMPONo', 'Article Code', 'Color Code'],how='left')
        df = df.merge(earliest_psd_article,left_on=['RMPONo', 'Article Code', 'Color Code'],right_on=['RMPONo', 'Article Code', 'Color Code'],how='left')
        
        print("test 0.5")
        # print("KPI columns:", df.columns.tolist())
        
        pivot_table = df.pivot_table(
            index=[Ship_to_location,sub_category,rmpo_no, article_code,article_name, article_colour_code,article_colour_name],  
            values=[PO_Qty_Column,Received_qty,Balance_to_Receive_Qty_column],  # Values to aggregate
            aggfunc="sum"  # Aggregation function (sum, mean, count, etc.)
        )
        print("test 0.6")
        # print(pivot_table)
        

        # Reset index to make it tabular
        pivot_table = pivot_table.reset_index()
        
        # Add new column
        pivot_table["Coats Key"] = pivot_table[rmpo_no] + pivot_table[article_name].apply(
            lambda x: x[3:10] if x.lower().startswith("pe") else x[:7]
        ) + "-" + pivot_table[article_colour_code].apply(lambda x: x if "natural" not in x.lower() else "NATRL")
        
        invoice_table = pd.read_excel(invoice_path, engine="openpyxl")
        
        # Add new column to invoice report
        invoice_table["Coats Key"] = invoice_table["Customer PO No."] + invoice_table["Material Code"]

        # Add new Column to add PCD date
        # print(pivot_table.columns.tolist())
        pivot_table = pivot_table.merge(earliest_pcd_article,left_on=['RMPONo', 'Article Code', 'Color Code'],right_on=['RMPONo', 'Article Code', 'Color Code'],how='left')
        pivot_table = pivot_table.merge(earliest_psd_article,left_on=['RMPONo', 'Article Code', 'Color Code'],right_on=['RMPONo', 'Article Code', 'Color Code'],how='left')
        pivot_table['Earliest PCD'] = pivot_table['Earliest PCD'].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else None)
        pivot_table['Earliest PSD'] = pivot_table['Earliest PSD'].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else None)
        pivot_table['Earliest PCD'] = pivot_table['Earliest PCD'].fillna("o")
        pivot_table['Earliest PSD'] = pivot_table['Earliest PSD'].fillna("o")
        
        # print(pivot_table)
        print("test 0.7")
        
        # Merging Invoices into Pivot Table (Left Join)
        merged_table = pivot_table.merge(invoice_table, on="Coats Key", how="left")

        # Grouping by Pivot Table Records to Aggregate Invoices
        invoice_summary = merged_table.groupby(["RMPONo", "Article Code","Color Code"]).agg({
            "Billing Doc. No": lambda x: list(x.dropna()),  # Store all Invoice Nos in a list
            "Qty": "sum"  # Sum of Invoice Quantities
        }).reset_index()

        # Merging back to Pivot Table
        pivot_table = pivot_table.merge(invoice_summary, on=["RMPONo", "Article Code","Color Code"], how="left")
        
        # Fill NaN values (if no invoice found)
        pivot_table["Billing Doc. No"] = pivot_table["Billing Doc. No"].apply(lambda x: x if isinstance(x, list) else [])
        pivot_table["Qty"] = pivot_table["Qty"].fillna(0).astype(int)
        
        # After building pivot_table and before saving
        exmill_path = os.path.join(UPLOAD_FOLDER, "orderbook_exmill.json")
        if os.path.exists(exmill_path):
            with open(exmill_path, "r") as f:
                exmill_map = json.load(f)
            pivot_table["Ex-Mill Date"] = pivot_table["Coats Key"].map(exmill_map).fillna("o")
        else:
            pivot_table["Ex-Mill Date"] = "o"
            
        print(pivot_table)
        
        #add last uploaded date
        pivot_table["Last Uploaded Date"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Save the sorted data to a new Excel file
        processed_file_path = os.path.join(UPLOAD_FOLDER, "processed_data.xlsx")
        pivot_table.to_excel(processed_file_path, index=False)

        # Convert to JSON and send response
        return jsonify(pivot_table.to_dict(orient="records"))

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/fetch_saved_data", methods=["GET"])
def fetch_saved_data():
    try:
        processed_file_path = os.path.join(UPLOAD_FOLDER, "processed_data.xlsx")
        if not os.path.exists(processed_file_path):
            return jsonify({"error": "No saved data found"}), 404

        # Read the saved Excel file
        df = pd.read_excel(processed_file_path, engine="openpyxl")
        print(df)
        last_uploaded_date = df["Last Uploaded Date"].iloc[0] if "Last Uploaded Date" in df.columns else "Unknown"
        return jsonify({"data": df.to_dict(orient="records"), "last_uploaded_date": last_uploaded_date})

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@app.route("/export", methods=["POST"])
def export_data():
    try:
        data = request.json
        df = pd.DataFrame(data)

        output_file = "PO Wise - Thread.xlsx"
        df.to_excel(output_file, index=False)

        return send_file(output_file, as_attachment=True)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

def process_excel_in_chunks(file_path, columns, chunk_size=1000):
    """Process a large Excel file in chunks using skiprows and nrows"""
    xls = pd.ExcelFile(file_path, engine='openpyxl')
    sheet_name = xls.sheet_names[0]  # Assume first sheet

    
    print(f"File found: {file_path}")
    print(f"Reading sheet: {sheet_name}")
     
    # Get total number of rows  
    total_rows = pd.read_excel(file_path, sheet_name=sheet_name, nrows=0, header=None).shape[0]
    
    #print("Total Rows" + total_rows )
    # Process in chunks
    all_data = []
    for i in range(1, total_rows, chunk_size):
        # Skip header row (0) and read only specified columns
        chunk = pd.read_excel(
            file_path, 
            sheet_name=sheet_name,
            usecols=columns,
            skiprows=range(1, i) if i > 1 else 0,  
            nrows=chunk_size,
            engine='openpyxl'
        )
        print("Processing chunk from row {} to {}".format(i, i + chunk_size - 1))
        
        # Filter to include only THREAD records
        filtered_chunk = chunk[chunk['Article Sub Category'] == 'THREAD (DECIMAL)'] 
        print("Filtered Chunk :" + filtered_chunk)
        if not filtered_chunk.empty:
            all_data.append(filtered_chunk)
    
    # Combine all chunks
    return pd.concat(all_data) if all_data else pd.DataFrame(columns=columns)

@app.route("/save_priority_orders", methods=["POST"])
def save_priority_orders():
    try:
        priority_orders = request.json.get("priorityOrders", [])
        if not priority_orders:
            return jsonify({"error": "No priority orders provided"}), 400

        # Save priority orders to a file or database
        priority_file_path = os.path.join(UPLOAD_FOLDER, "priority_orders.json")
        with open(priority_file_path, "w") as f:
            json.dump(priority_orders, f, indent=4)
            
        # --- Update processed_data.xlsx with Is Priority column ---
        processed_file_path = os.path.join(UPLOAD_FOLDER, "processed_data.xlsx")
        if os.path.exists(processed_file_path):
            df = pd.read_excel(processed_file_path, engine="openpyxl")

            # Build a set of priority keys for fast lookup
            # Adjust the key fields as per your priority order structure and processed_data columns
            priority_keys = set()
            for po in priority_orders:
                priority_keys.add(po.get("RMPONo"))

            def is_priority(row):
                return "Yes" if (row["RMPONo"]) in priority_keys else "No"

            df["Is Priority"] = df.apply(is_priority, axis=1)
            df.to_excel(processed_file_path, index=False)            
    
        return jsonify({"message": "Priority orders saved successfully"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@app.route("/fetch_priority_orders", methods=["GET"])
def fetch_priority_orders():
    try:
        priority_file_path = os.path.join(UPLOAD_FOLDER, "priority_orders.json")
        if not os.path.exists(priority_file_path):
            print("Priority orders file does not exist.")
            return jsonify({"error": "No priority orders found"}), 404

        with open(priority_file_path, "r") as f:
            priority_orders = json.load(f)

        return jsonify({"priorityOrders": priority_orders}), 200
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/fetch_shared_data", methods=["GET"])
def fetch_shared_data():
    try:
        print("test passed 0")
        shared_link = os.environ.get("SHARED_LINK")
        print("test passed 1")
        if not shared_link:
            return jsonify({"error": "SHARED_LINK environment variable is not defined"}), 500

        # Fetch the Excel file from the shared link
        response = requests.get(shared_link)
        print("test passed 2")
        if response.status_code != 200:
            return jsonify({"error": f"Failed to fetch file: {response.status_code}"}), 500

        print("test passed 2.1")
        # Save the file locally
        response = requests.get(shared_link)
        content_type = response.headers.get("Content-Type", "")
        if "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" not in content_type:
            print(f"Content-Type: {content_type}")
            raise ValueError(f"Downloaded file is not an Excel file. Content-Type: {content_type}")
           
        print("test passed 2.2")

        file_path = request.args.get("file_path", None)
        print("test passed 2.3")
        
        if not file_path:
            if os.path.exists("selected_file_path.json"):
                with open("selected_file_path.json", "r") as f:
                    file_path = json.load(f).get("file_path", None)
                    
        if not file_path or not os.path.exists(file_path):
            return jsonify([]), 200
                    


        # Read the Excel file
        df = pd.read_excel(file_path, engine="openpyxl")
        print("Read Test Successs !")
        # Convert the data to JSON
        data = df.to_dict(orient="records")
        return jsonify(data), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/upload_pcd", methods=["POST"])
def upload_pcd():
    pcd_file = request.files.get("pcd")
    if not pcd_file:
        return jsonify({"error": "PCD file is required"}), 400

    pcd_path = os.path.join(UPLOAD_FOLDER, "Production Plan.xlsx")
    pcd_file.save(pcd_path)

    # Save last updated time
    last_updated = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(os.path.join(UPLOAD_FOLDER, "pcd_last_updated.txt"), "w") as f:
        f.write(last_updated)

    return jsonify({"message": "PCD file uploaded successfully", "last_updated": last_updated})

@app.route("/get_pcd_last_updated", methods=["GET"])
def get_pcd_last_updated():
    try:
        path = os.path.join(UPLOAD_FOLDER, "pcd_last_updated.txt")
        if not os.path.exists(path):
            return jsonify({"last_updated": ""})
        with open(path, "r") as f:
            last_updated = f.read()
        return jsonify({"last_updated": last_updated})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/upload_orderbook", methods=["POST"])
def upload_orderbook():
    orderbook_file = request.files.get("orderbook")
    if not orderbook_file:
        return jsonify({"error": "Orderbook file is required"}), 400

    orderbook_path = os.path.join(UPLOAD_FOLDER, "Orderbook.xlsx")
    orderbook_json_path = os.path.join(UPLOAD_FOLDER, "orderbook.json")
    orderbook_file.save(orderbook_path)

    # Save last updated time
    last_updated = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(os.path.join(UPLOAD_FOLDER, "orderbook_last_updated.txt"), "w") as f:
        f.write(last_updated)

    # Extract Ex-Mill Date mapping and save full orderbook as JSON
    try:
        df = pd.read_excel(orderbook_path, engine="openpyxl")
        df["Coats Key"] = df["Purchase Order Number"].astype(str) + df["Material"].astype(str)
        exmill_map = dict(zip(df["Coats Key"], df["Delivery Date"].astype(str)))
        with open(os.path.join(UPLOAD_FOLDER, "orderbook_exmill.json"), "w") as f:
            json.dump(exmill_map, f)
            
        # Save full orderbook as JSON
        df.to_json(orderbook_json_path, orient="records", force_ascii=False)
        # Optionally remove the Excel file
        # os.remove(orderbook_path)
    except Exception as e:
        print("Failed to process orderbook:", e)

    return jsonify({"message": "Orderbook file uploaded successfully", "last_updated": last_updated})

@app.route("/get_orderbook_last_updated", methods=["GET"])
def get_orderbook_last_updated():
    try:
        path = os.path.join(UPLOAD_FOLDER, "orderbook_last_updated.txt")
        if not os.path.exists(path):
            return jsonify({"last_updated": ""})
        with open(path, "r") as f:
            last_updated = f.read()
        return jsonify({"last_updated": last_updated})
    except Exception as e:
        return jsonify({"error": str(e)}), 500  
    
@app.route("/supplier_dashboard_data", methods=["GET"])
def supplier_dashboard_data():
    try:
        processed_file_path = os.path.join(UPLOAD_FOLDER, "processed_data.xlsx")
        if not os.path.exists(processed_file_path):
            return jsonify({"error": "No saved data found"}), 404

        # Read the saved Excel file
        df = pd.read_excel(processed_file_path, engine="openpyxl")
        
        # Convert dates to proper format
        date_cols = ["Ex-Mill Date", "Earliest PCD", "Earliest PSD"]
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
        
        today = pd.Timestamp.now()
        
        # Calculate key metrics
        missed_exmill = df[(df["Ex-Mill Date"] < today) & (df["Balance to Receive Qty"] > 0) & (df["Ex-Mill Date"].notna())]
        missed_count = len(missed_exmill)
        missed_percent = round(missed_count / len(df) * 100, 1) if len(df) > 0 else 0
        
        upcoming = df[
            (df["Earliest PCD"] >= today) & 
            (df["Earliest PCD"] <= today + pd.Timedelta(days=10)) & 
            (df["Balance to Receive Qty"] > 0) &
            (df["Earliest PCD"].notna())
        ]
        
        # Group by location
        location_groups = upcoming.groupby("Ship to Location").size().reset_index(name="count")
        location_groups = location_groups.sort_values("count", ascending=False).head(5)
        
        # Format dates back to strings
        for col in date_cols:
            if col in df.columns:
                df[col] = df[col].dt.strftime('%Y-%m-%d')
        
        return jsonify({
            "data": df.to_dict(orient="records"),
            "metrics": {
                "missed_exmill_count": missed_count,
                "missed_exmill_percent": missed_percent,
                "upcoming_count": len(upcoming),
                "top_locations": location_groups.to_dict(orient="records")
            }
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/upload_requirement', methods=['POST'])
def upload_requirement():
    import pandas as pd
    requirement_file = request.files.get("requirement")
    if not requirement_file:
        return jsonify({"error": "Requirement file is required"}), 400

    # Save uploaded file temporarily
    temp_path = os.path.join(UPLOAD_FOLDER, 'temp_requirement.xlsx')
    requirement_file.save(temp_path)
    print("Temp file saved")

    # Read Excel, handle variable header rows
    df = pd.read_excel(temp_path, dtype=str)
    first_row = df.iloc[0]
    print("test 0.1")


    os.remove(temp_path)

    print("Columns in requirement file:", df.columns.tolist())
    df.columns = [col.strip() for col in df.columns]
    print("Columns in requirement file:", df.columns.tolist())
    print("test 0.2")
    df = df[
        (df['Sub Category'].str.strip().str.upper() == "THREAD (DECIMAL)") &
        (df['Body Type'].str.strip().str.upper() == "GENERAL RM") &
        (df['Color Code'].str.contains("C9760", na=False)) &
        (
            df['Article Name'].str.startswith('5722160', na=False) |
            df['Article Name'].str.startswith('2925120', na=False) |
            df['Article Name'].str.startswith('F025160', na=False) |
            df['Article Name'].str.startswith('F025140', na=False) |
            df['Article Name'].str.startswith('57A3140', na=False)
        ) &
        (
            df['OCFactory'].str.contains("INQUBE RANALA SAMPLE ROOM", na=False) |
            df['OCFactory'].str.contains("BRANDIX ATHLEISURE GIRITALE", na=False) |
            df['OCFactory'].str.contains("BRANDIX INTIMATES APPAREL MINUWANGODA", na=False) |
            df['OCFactory'].str.contains("INQUBE PRODUCTION ENGENEERING", na=False) 
        )
    ]
    print("test 0.3")
    # Map PCD/PSD columns from PCD file using OCNum
    pcd_path = os.path.join(UPLOAD_FOLDER, "Production Plan.xlsx")
    if os.path.exists(pcd_path):
        pcd_df = pd.read_excel(pcd_path, engine="openpyxl")
        pcd_df.rename(columns={pcd_df.columns[0]: 'OC', pcd_df.columns[1]: 'PCD', pcd_df.columns[2]: 'PSD'}, inplace=True)
        pcd_df['OC'] = pcd_df['OC'].astype(str)
        df['OCNum'] = df['OCNum'].astype(str)
        df = df.merge(pcd_df[['OC', 'PCD', 'PSD']], left_on='OCNum', right_on='OC', how='left')
        df.drop(columns=['OC'], inplace=True, errors='ignore')
    else:
        df['PCD'] = ""
        df['PSD'] = ""

    print("test 0.4")
    
    # Save filtered data as JSON
    records = df.fillna("o").to_dict(orient='records')
    with open(os.path.join(UPLOAD_FOLDER, 'requirement.json'), 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    print("test 0.5")
    # Save last updated time
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(os.path.join(UPLOAD_FOLDER, 'requirement_last_updated.txt'), 'w') as f:
        f.write(now_str)

    return jsonify({'last_updated': now_str, 'count': len(records)}), 200

@app.route('/upload_inventory', methods=['POST'])
def upload_inventory():
    file = request.files.get('inventory')
    if not file:
        return jsonify({'error': 'No file uploaded'}), 400

    # Save uploaded file temporarily
    temp_path = os.path.join(UPLOAD_FOLDER, 'temp_inventory.xlsx')
    file.save(temp_path)

    # Read Excel, handle variable header rows
    df = pd.read_excel(temp_path, dtype=str)
    # Check if first row is a header or data
    first_row = df.iloc[0]
    # If more than 5 columns in first row are filled with date-like values, treat as data, else remove first 2 rows
    date_like_count = sum(
        pd.to_datetime(str(val), errors='coerce') is not pd.NaT
        for val in first_row[:6]
    )
    if date_like_count > 5:
        # Table starts at top, do nothing
        pass
    else:
        # Remove first 2 rows, reset header
        df = pd.read_excel(temp_path, dtype=str, header=2)

    # Clean up temp file
    os.remove(temp_path)

    # Filtering
    df = df[
        (df['Item Sub Category'].str.strip().str.upper() == "THREAD (DECIMAL)") &
        (df['Color Code'].str.contains("C9760", na=False)) &
        (
            df['Article Name'].str.startswith('5722160', na=False) |
            df['Article Name'].str.startswith('2925120', na=False) |
            df['Article Name'].str.startswith('F025160', na=False) |
            df['Article Name'].str.startswith('F025140', na=False) |
            df['Article Name'].str.startswith('57A3140', na=False)
        )
    ]

    # Save filtered data as JSON
    records = df.fillna("").to_dict(orient='records')
    with open(os.path.join(UPLOAD_FOLDER, 'inventory.json'), 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    # Update last updated time
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(os.path.join(UPLOAD_FOLDER, 'inventory_last_updated.txt'), 'w') as f:
        f.write(now_str)
        
    # Load minimum levels
    min_levels_path = os.path.join(UPLOAD_FOLDER, 'minimum_levels.json')
    if os.path.exists(min_levels_path):
        with open(min_levels_path, 'r', encoding='utf-8') as f:
            minimum_levels = json.load(f)
    else:
        minimum_levels = {}
    
    # Check for low stock and send email
    recipient_email = "pasinduh@inqube.com"  # Set your recipient
    for item in records:
        article_code = item.get("Article Name", "").split("-")[0]
        article_name = item.get("Article Name", "")
        current_qty = float(item.get("Total Qty", 0))
        min_qty = float(minimum_levels.get(article_code, 10))
        #if current_qty < min_qty:
            #send_low_stock_email(article_code, article_name, current_qty, min_qty, recipient_email)

    return jsonify({'last_updated': now_str, 'count': len(records)}), 200

@app.route("/get_inventory_data", methods=["GET"])
def get_inventory_data():
    try:
        inventory_path = os.path.join(UPLOAD_FOLDER, 'inventory.json')
        if not os.path.exists(inventory_path):
            return jsonify({"inventory": []}), 200

        with open(inventory_path, 'r', encoding='utf-8') as f:
            inventory = json.load(f)

        return jsonify({"inventory": inventory}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/get_minimum_levels", methods=["GET"])
def get_minimum_levels():
    try:
        min_levels_path = os.path.join(UPLOAD_FOLDER, 'minimum_levels.json')
        if not os.path.exists(min_levels_path):
            return jsonify({"minimumLevels": {}}), 200

        with open(min_levels_path, 'r', encoding='utf-8') as f:
            minimum_levels = json.load(f)

        return jsonify({"minimumLevels": minimum_levels}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/save_minimum_levels", methods=["POST"])
def save_minimum_levels():
    try:
        data = request.json
        minimum_levels = data.get('minimumLevels', {})

        min_levels_path = os.path.join(UPLOAD_FOLDER, 'minimum_levels.json')
        with open(min_levels_path, 'w', encoding='utf-8') as f:
            json.dump(minimum_levels, f, ensure_ascii=False, indent=2)

        return jsonify({"message": "Minimum levels saved successfully"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/upload_kpi", methods=["POST"])
def upload_kpi():
    kpi_file = request.files.get("kpi")
    if not kpi_file:
        return jsonify({"error": "KPI file is required"}), 400

    # Save the file temporarily
    temp_path = os.path.join(UPLOAD_FOLDER, "temp_kpi.xlsx")
    kpi_file.save(temp_path)

    # Read Excel, handle variable header rows
    df = pd.read_excel(temp_path, dtype=str)
    first_row = df.iloc[0]
    # If more than 5 columns in first row are filled with date-like values, treat as data, else remove first 2 rows
    date_like_count = sum(
        pd.to_datetime(str(val), errors='coerce') is not pd.NaT
        for val in first_row[:6]
    )
    if date_like_count > 5:
        # Table starts at top, do nothing
        pass
    #else:
        # Remove first 2 rows, reset header
        #df = pd.read_excel(temp_path, dtype=str, header=2)

    # Clean up temp file
    os.remove(temp_path)

    # Filtering
    df = df[
        (df['Article Sub Category'].str.strip().str.upper() == "THREAD (DECIMAL)") &
        (df['Color Code'].str.contains("C9760", na=False)) &
        (df['Supplier Name'].str.strip().str.upper() == "COATS THREAD EXPORTS (PVT) LTD") &
        (
            df['Article Name'].str.startswith('5722160', na=False) |
            df['Article Name'].str.startswith('2925120', na=False) |
            df['Article Name'].str.startswith('F025160', na=False) |
            df['Article Name'].str.startswith('F025140', na=False) |
            df['Article Name'].str.startswith('57A3140', na=False)
        )
    ]

    # Save filtered data as JSON
    records = df.fillna("").to_dict(orient='records')
    with open(os.path.join(UPLOAD_FOLDER, 'kpi.json'), 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    # Save last updated time
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(os.path.join(UPLOAD_FOLDER, 'kpi_last_updated.txt'), 'w') as f:
        f.write(now_str)

    return jsonify({'last_updated': now_str, 'count': len(records)}), 200

@app.route("/get_kpi_data", methods=["GET"])
def get_kpi_data():
    try:
        kpi_path = os.path.join(UPLOAD_FOLDER, 'kpi.json')
        if not os.path.exists(kpi_path):
            return jsonify({"kpi": []}), 200

        with open(kpi_path, 'r', encoding='utf-8') as f:
            kpi = json.load(f)

        return jsonify({"kpi": kpi}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/get_requirement_data", methods=["GET"])
def get_requirement_data():
    try:
        requirement_path = os.path.join(UPLOAD_FOLDER, 'requirement.json')
        if not os.path.exists(requirement_path):
            return jsonify({"requirement": []}), 200

        with open(requirement_path, 'r', encoding='utf-8') as f:
            requirement = json.load(f)

        return jsonify({"requirement": requirement}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/get_requirement_last_updated", methods=["GET"])
def get_requirement_last_updated():
    try:
        path = os.path.join(UPLOAD_FOLDER, "requirement_last_updated.txt")
        if not os.path.exists(path):
            return jsonify({"last_updated": ""})
        with open(path, "r") as f:
            last_updated = f.read()
        return jsonify({"last_updated": last_updated})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/get_inventory_last_updated", methods=["GET"])
def get_inventory_last_updated():
    try:
        path = os.path.join(UPLOAD_FOLDER, "inventory_last_updated.txt")
        if not os.path.exists(path):
            return jsonify({"last_updated": ""})
        with open(path, "r") as f:
            last_updated = f.read()
        return jsonify({"last_updated": last_updated})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/get_kpi_last_updated", methods=["GET"])
def get_kpi_last_updated():
    try:
        path = os.path.join(UPLOAD_FOLDER, "kpi_last_updated.txt")
        if not os.path.exists(path):
            return jsonify({"last_updated": ""})
        with open(path, "r") as f:
            last_updated = f.read()
        return jsonify({"last_updated": last_updated})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
        
@app.route("/process_reports", methods=["POST"])
def process_reports():
    try:
        # Add your processing logic here
        # For example, read all three reports and generate combined data
        
        # Sample implementation
        requirement_path = os.path.join(UPLOAD_FOLDER, "Requirement.xlsx")
        inventory_path = os.path.join(UPLOAD_FOLDER, "Inventory.xlsx")
        kpi_path = os.path.join(UPLOAD_FOLDER, "KPI.xlsx")
        
        # Check if files exist
        if not os.path.exists(requirement_path) or not os.path.exists(inventory_path) or not os.path.exists(kpi_path):
            return jsonify({"error": "Some files are missing"}), 400
            
        # Process files
        # df_req = pd.read_excel(requirement_path)
        # df_inv = pd.read_excel(inventory_path)
        # df_kpi = pd.read_excel(kpi_path)
        
        # Your processing logic here...
        
        return jsonify({"message": "Reports processed successfully"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/upload_swrm1", methods=["POST"])
def upload_swrm1():
    
    swrm_file = request.files.get("swrm")
    if not swrm_file:
        return jsonify({"error": "SWRM file is required"}), 400

    print("test 0.1")
    temp_path = os.path.join(UPLOAD_FOLDER, "temp_swrm.xlsx")
    swrm_file.save(temp_path)
    print("test 0.2")

    # Process in chunks
    filtered_chunks = []
    print("test 0.3")
    try:
        for chunk in pd.read_excel(temp_path, dtype=str, chunksize=10000, engine="openpyxl"):
            # Adjust column name as per your file
            if "Sub Category" in chunk.columns:
                filtered = chunk[chunk["Sub Category"].str.strip().str.upper() == "THREAD (DECIMAL)"]
                print("test 0.4")
                if not filtered.empty:
                    filtered_chunks.append(filtered)
                    print("test 0.5")
        if filtered_chunks:
            result_df = pd.concat(filtered_chunks)
            print("test 0.6")
        else:
            result_df = pd.DataFrame()
            print
        os.remove(temp_path)
        print("test 0.7")
    except Exception as e:
        os.remove(temp_path)
        print("Error processing SWRM file:", e)
        return jsonify({"error": str(e)}), 500

    # Save as JSON
    records = result_df.fillna("").to_dict(orient="records")
    with open(os.path.join(UPLOAD_FOLDER, "swrm.json"), "w", encoding="utf-8") as f:
        print("test 0.8")
        json.dump(records, f, ensure_ascii=False, indent=2)

    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print("test 0.9")
    with open(os.path.join(UPLOAD_FOLDER, "swrm_last_updated.txt"), "w") as f:
        f.write(now_str)
        
    print("test 1.0")

    return jsonify({"last_updated": now_str, "count": len(records)}), 200
@app.route("/upload_swrm", methods=["POST"])
def upload_swrm():
    
    swrm_file = request.files.get("swrm")
    if not swrm_file:
        return jsonify({"error": "SWRM file is required"}), 400
    print("test 0.1")
    temp_path = os.path.join(UPLOAD_FOLDER, "temp_swrm.xlsx")
    swrm_file.save(temp_path)
    print("test 0.2")

    try:
        # Read the entire file at once for speed
        df = pd.read_excel(temp_path, dtype=str, engine="openpyxl")
        print("test 0.3")
        # Try both possible column names for robustness
        subcat_col = None
        print("test 0.4")
        for col in df.columns:
            print("test 0.5")
            if col.strip().lower() in ["sub category", "subcategory"]:
                print("test 0.6")
                subcat_col = col
                break
        if not subcat_col:
            os.remove(temp_path)
            print("test 0.7")
            return jsonify({"error": "Sub Category column not found"}), 400


        # Filter for THREAD (DECIMAL)
        print("test 0.8")
        #filtered_df = df[df[subcat_col].str.strip().str.upper() == "THREAD (DECIMAL)"]
        filtered_df = df

        os.remove(temp_path)

    except Exception as e:
        if os.path.exists(temp_path):
            os.remove(temp_path)
        return jsonify({"error": str(e)}), 500

    # Save as JSON
    records = filtered_df.fillna("").to_dict(orient="records")
    with open(os.path.join(UPLOAD_FOLDER, "swrm.json"), "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    
    stylecode_col = stylename_col = ocfactory_col = gmtcolor_col = None
    key_cols = []
    for col in df.columns:
        print(col)
        if col.strip().lower() in ["stylecode"]:
            stylecode_col = col
        if col.strip().lower() in ["stylename", "style name"]:
            stylename_col = col
        if col.strip().lower() in ["ocfactory"]:
            ocfactory_col = col
        if col.strip().lower() in ["gmtcolorname", "gmt color name"]:
            gmtcolor_col = col
    key_cols = [stylecode_col, stylename_col, ocfactory_col, gmtcolor_col]

    all_combos = df[key_cols].drop_duplicates()
    thread_combos = filtered_df[key_cols].drop_duplicates()

    # Find combinations in all_combos that are NOT in thread_combos
    merged = all_combos.merge(thread_combos, on=key_cols, how='left', indicator=True)
    missing_combos = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])

    missing_thread_combinations = missing_combos.fillna("").to_dict(orient="records")

    with open(os.path.join(UPLOAD_FOLDER, "missing_thread_combinations.json"), "w", encoding="utf-8") as f:
        json.dump(missing_thread_combinations, f, ensure_ascii=False, indent=2)

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(os.path.join(UPLOAD_FOLDER, "swrm_last_updated.txt"), "w") as f:
        f.write(now_str)

    return jsonify({
        "last_updated": now_str,
        "count": len(records),
        "missing_thread_combinations": missing_thread_combinations
    }), 200
    
@app.route("/get_swrm_data", methods=["GET"])
def get_swrm_data():
    try:
        swrm_path = os.path.join(UPLOAD_FOLDER, 'swrm.json')
        last_updated_path = os.path.join(UPLOAD_FOLDER, 'swrm_last_updated.txt')
        missing_combos_path = os.path.join(UPLOAD_FOLDER, 'missing_thread_combinations.json')
        
        if not os.path.exists(swrm_path):
            return jsonify({
                "swrm": [],
                "last_updated": "",
                "missing_thread_combinations": []
            }), 200

        with open(swrm_path, 'r', encoding='utf-8') as f:
            swrm = json.load(f)
            
        last_updated = ""
        if os.path.exists(last_updated_path):
            with open(last_updated_path, 'r') as f:
                last_updated = f.read()
                
        # If missing combinations were saved separately
        missing_thread_combinations = []
        if os.path.exists(missing_combos_path):
            with open(missing_combos_path, 'r', encoding='utf-8') as f:
                missing_thread_combinations = json.load(f)

        return jsonify({
            "swrm": swrm,
            "last_updated": last_updated,
            "missing_thread_combinations": missing_thread_combinations
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def send_low_stock_email(article_code, article_name, current_qty, min_qty, recipient_email):
    subject = f"Low Stock Alert: {article_code} - {article_name}"
    body = f"""
    Alert: Stock for article {article_code} - {article_name} is below the minimum level.
    Current Quantity: {current_qty}
    Minimum Required: {min_qty}
    Please take necessary action.
    """

    msg = MIMEMultipart()
    msg['From'] = "pasinduh@inqube.com"
    msg['To'] = recipient_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    # SMTP config (example for Gmail)
    smtp_server = "smtp.gmail.com"
    smtp_port = 587
    smtp_user = "pasinduh@inqube.com"
    smtp_password = "your_app_password"  # Use an app password, not your main password

    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.sendmail(msg['From'], msg['To'], msg.as_string())
        server.quit()
        print(f"Low stock email sent for {article_code}")
    except Exception as e:
        print(f"Failed to send email: {e}")
        
        
        
if __name__ == "__main__":
    app.run(debug=True)

