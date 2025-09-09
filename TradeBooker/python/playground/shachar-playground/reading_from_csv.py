import csv
import os
import pandas as pd
from datetime import date

# Get the directory this script is in
current_dir = os.path.dirname(__file__)
csv_path = os.path.join(current_dir, "bank_trades.csv")

# Option 1: Read each line as a raw string (no parsing)
with open(csv_path, "r") as f:
    lines = f.read().splitlines()

for line in lines:
    print(line)

# Load the CSV into a dataframe
# df = pd.read_csv(csv_path)
# print(df)