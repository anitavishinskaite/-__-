log_processing_code = """
import os
import pandas as pd
from datetime import datetime, timedelta
import sys

def load_logs_for_period(start_date, end_date, input_dir):
    logs = []
    for single_date in pd.date_range(start=start_date, end=end_date):
        file_path = os.path.join(input_dir, f"{single_date.strftime('%Y-%m-%d')}.csv")
        if os.path.isfile(file_path):
            day_logs = pd.read_csv(file_path, names=['email', 'action', 'dt'])
            logs.append(day_logs)
    if logs:
        return pd.concat(logs, ignore_index=True)
    else:
        return pd.DataFrame()  

def compute_weekly_aggregates(target_date_str, input_dir, output_dir):
    target_date = datetime.strptime(target_date_str, '%Y-%m-%d')
    start_date = target_date - timedelta(days=7)
    end_date = target_date - timedelta(days=1)

    logs = load_logs_for_period(start_date, end_date, input_dir)

    if logs.empty:
        print(f"No logs available for the period {start_date.date()} to {end_date.date()}.")
        return

    aggregates = logs.groupby(['email', 'action']).size().unstack(fill_value=0)

    for action in ['CREATE', 'READ', 'UPDATE', 'DELETE']:
        if action not in aggregates.columns:
            aggregates[action] = 0

    aggregates.columns = ['create_count', 'read_count', 'update_count', 'delete_count']
    aggregates.reset_index(inplace=True)

    output_file = os.path.join(output_dir, f"{target_date_str}.csv")
    os.makedirs(output_dir, exist_ok=True)
    aggregates.to_csv(output_file, index=False)
    print(f"Aggregated data saved to {output_file}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <YYYY-mm-dd>")
        sys.exit(1)
    try:
        input_date = datetime.strptime(sys.argv[1], '%Y-%m-%d')
    except ValueError:
        print("Error: the date should be in the following format YYYY-mm-dd.")
        sys.exit(1)

    input_dir = 'input'  
    output_dir = 'output'  

    compute_weekly_aggregates(sys.argv[1], input_dir, output_dir)
"""

# Write the log processing script to a file
with open("process_logs.py", "w") as file:
    file.write(log_processing_code)