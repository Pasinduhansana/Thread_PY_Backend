from flask import Flask, request, jsonify, send_file
import pandas as pd
import os
import re

app = Flask(__name__)

# Allow frontend to communicate with the backend
from flask_cors import CORS
CORS(app)

UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

@app.route("/upload_Dashboard", methods=["POST"])
def upload_file():

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
        columns_to_read = [ "Season","Style Code","Style Name","Supplier Name","RMPONo","RMPO Status","Article Sub Category","Article Code","Article Name","Color Code","Color Name","Size Code","PO Qty(Purchase UOM)","Received Qty","Balance to Receive Qty","Ship to Location","PO Value","OCFactory","DS In-House","Ship to Location"]
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
        print("Test 3 Passed")
        
        pivot_table = df.pivot_table(
            index=[Ship_to_location,sub_category,rmpo_no, article_code,article_name, article_colour_code,article_colour_name ],  
            values=[PO_Qty_Column,Received_qty,Balance_to_Receive_Qty_column],  # Values to aggregate
            aggfunc="sum"  # Aggregation function (sum, mean, count, etc.)
        )

        # Reset index to make it tabular
        pivot_table = pivot_table.reset_index()
        
        # Add new column
        pivot_table["Coats Key"] = pivot_table[rmpo_no] + pivot_table[article_name].apply(
            lambda x: x[3:10] if x.lower().startswith("pe") else x[:7]
        ) + "-" + pivot_table[article_colour_code].apply(lambda x: x if "natural" not in x.lower() else "NATRL")
        
        invoice_table = pd.read_excel(invoice_path, engine="openpyxl")
        
        # Add new column to invoice report
        invoice_table["Coats Key"] = invoice_table["Customer PO No."] + invoice_table["Material Code"]

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

if __name__ == "__main__":
    app.run(debug=True)
