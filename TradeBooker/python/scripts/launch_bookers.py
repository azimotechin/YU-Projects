# This code, called by docker compose, spins up any amount of trade_booker instances that can read off of stream
import subprocess
import sys

# Default to 1 if no argument provided
num_instances = int(sys.argv[1]) if len(sys.argv) > 1 else 1

for i in range(1, num_instances + 1):
    log_file = open(f"booker_log_{i}.txt", "w")
    subprocess.Popen(
        ["python", "scripts/trade_booker.py"],
        stdout=log_file,
        stderr=subprocess.STDOUT
    )
