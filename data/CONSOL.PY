# process_csv.py
import pandas as pd
import glob
import os
import sys

# Directory containing your CSV files.
DATA_DIR = r"C:\Users\nathalie\OneDrive\Documents\product-assignment-server\data"

# Output file path (this will be created in the current working directory).
OUTPUT_FILE = os.path.join(os.getcwd(), "output.csv")

def main():
    # Use glob to get all CSV files in the directory.
    csv_files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    
    if not csv_files:
        print(f"No CSV files found in {DATA_DIR}")
        sys.exit(1)
    
    # Read each CSV file into a DataFrame and collect them in a list.
    df_list = []
    for file in csv_files:
        try:
            df = pd.read_csv(file)
            print(f"Read {len(df)} rows from {os.path.basename(file)}")
            df_list.append(df)
        except Exception as e:
            print(f"Error reading {file}: {e}")
    
    if not df_list:
        print("No data to process.")
        sys.exit(1)
    
    # Append all data into one DataFrame.
    data = pd.concat(df_list, ignore_index=True)
    
    # Standardize column names by stripping whitespace.
    data.columns = [col.strip() for col in data.columns]
    
    # Define expected column names and fallbacks.
    abstract_col = "item.abstract_product_id"
    priority_col = "rule.priority"
    tenant_col = "tenant_id"
    date_candidates = ["sys_created_on", "created_on", "CreatedOn"]
    
    # Ensure the abstract ID column is present.
    if abstract_col not in data.columns:
        print(f"Column '{abstract_col}' not found in the data.")
        sys.exit(1)
    
    # Fallback for rule priority.
    if priority_col not in data.columns:
        if "priority" in data.columns:
            priority_col = "priority"
        else:
            print("No priority column found.")
            sys.exit(1)
    
    # Fallback for tenant ID.
    if tenant_col not in data.columns:
        if "TenantID" in data.columns:
            tenant_col = "TenantID"
        elif "Tenant ID" in data.columns:
            tenant_col = "Tenant ID"
        else:
            print("No tenant ID column found.")
            sys.exit(1)
    
    # Determine the created date column (choose the first available candidate).
    created_col = None
    for col in date_candidates:
        if col in data.columns:
            created_col = col
            break
    if created_col is None:
        print("No created date column found.")
        sys.exit(1)
    
    # Convert the created date column to datetime, coercing errors to NaT.
    data[created_col] = pd.to_datetime(data[created_col], errors='coerce')
    # Drop rows with invalid dates.
    data = data.dropna(subset=[created_col])
    
    # Group by the abstract ID and aggregate:
    # - rule_priority: first value,
    # - tenant_id: first value,
    # - oldest_created_on: minimum of created date,
    # - count: number of rows per group.
    grouped = data.groupby(abstract_col, as_index=False).agg(
        abstract_product_id=(abstract_col, 'first'),
        rule_priority=(priority_col, 'first'),
        tenant_id=(tenant_col, 'first'),
        oldest_created_on=(created_col, 'min'),
        count=(abstract_col, 'size')
    )
    
    # Sort by oldest_created_on ascending (oldest first).
    grouped = grouped.sort_values(by="oldest_created_on", ascending=True)
    
    # Format the date column as "YYYY-MM-DD HH:MM:SS"
    grouped["oldest_created_on"] = grouped["oldest_created_on"].dt.strftime("%Y-%m-%d %H:%M:%S")
    
    # Write the output CSV.
    grouped.to_csv(OUTPUT_FILE, index=False)
    print(f"Output written to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
