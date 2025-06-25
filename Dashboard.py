from flask import Flask, json, request, jsonify, send_file
import pandas as pd
import os
import re
import requests
from datetime import datetime
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

                    
        # Get the shared link from the environment variable
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
    
if __name__ == "__main__":
    app.run(debug=True)

