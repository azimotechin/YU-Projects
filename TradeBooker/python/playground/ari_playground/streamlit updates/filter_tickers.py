import os


def is_likely_warrant_or_unit(ticker: str) -> bool:
   return (
       ticker.endswith("W") or
       ticker.endswith("U") or
       ticker.endswith("R") or
       ticker.endswith("WS") or
       ticker.endswith("WT") or
       ticker[-1] in {"W", "U", "R"}
   )


# Resolve paths relative to this script's location
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
input_file = os.path.join(base_dir, "ui", "all_tickers.txt")
output_file = os.path.join(base_dir, "ui", "all_tickers_filtered.txt")


with open(input_file, "r") as infile:
   lines = infile.readlines()


with open(output_file, "w") as outfile:
   for line in lines:
       ticker = line.strip()
       if not ticker:
           continue
       if is_likely_warrant_or_unit(ticker):
           continue
       outfile.write(f"{ticker}\n")


print(f"Filtered tickers written to {output_file}")