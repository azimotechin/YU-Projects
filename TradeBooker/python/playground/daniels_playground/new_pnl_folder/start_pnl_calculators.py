#!/usr/bin/env python3

import subprocess
import os
import time
import string
from pathlib import Path

# ANSI color codes
class Colors:
    RED = '\033[0;31m'
    GREEN = '\033[0;32m'
    YELLOW = '\033[1;33m'
    BLUE = '\033[0;34m'
    NC = '\033[0m'  # No Color

def print_colored(message, color):
    """Print message with specified color"""
    print(f"{color}{message}{Colors.NC}")

def main():
    SCRIPT_NAME = "pnl_calculator.py"
    LOG_DIR = "logs"
    PID_FILE = "pnl_pids.txt"
    
    print_colored("🚀 Starting 26 PnL Calculator instances...", Colors.BLUE)
    
    # Create logs directory if it doesn't exist
    Path(LOG_DIR).mkdir(exist_ok=True)
    
    # Clear previous PID file
    open(PID_FILE, 'w').close()
    
    # Check if the Python script exists
    if not os.path.isfile(SCRIPT_NAME):
        print_colored(f"❌ Error: {SCRIPT_NAME} not found in current directory", Colors.RED)
        return 1
    
    processes = {}
    
    # Start instances for each letter
    for letter in string.ascii_lowercase:
        print_colored(f"📊 Starting PnL Calculator for accounts starting with '{letter.upper()}'", Colors.YELLOW)
        
        # Prepare log file path
        log_file_path = os.path.join(LOG_DIR, f"pnl_{letter}.log")
        
        try:
            # Open log file for writing
            with open(log_file_path, 'w') as log_file:
                # Start the process with stdout and stderr redirected to log file
                process = subprocess.Popen(
                    ["python", SCRIPT_NAME, letter],
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                    cwd=os.getcwd()
                )
            
            # Store process info
            processes[letter] = process
            
            # Save PID to file for later cleanup
            with open(PID_FILE, 'a') as pid_file:
                pid_file.write(f"{letter}:{process.pid}\n")
            
            print_colored(f"✅ Started instance for letter '{letter}' (PID: {process.pid})", Colors.GREEN)
            
            # Small delay to prevent overwhelming the system
            time.sleep(0.1)
            
        except Exception as e:
            print_colored(f"❌ Failed to start instance for letter '{letter}': {e}", Colors.RED)
    
    print()
    print_colored("🎉 Successfully started 26 PnL Calculator instances!", Colors.GREEN)
    print_colored(f"📝 Process IDs saved to: {PID_FILE}", Colors.BLUE)
    print_colored(f"📋 Logs available in: {LOG_DIR}/pnl_[letter].log", Colors.BLUE)
    print()
    print_colored("To stop all instances, run: python stop_pnl_calculators.py", Colors.YELLOW)
    print_colored(f"To view logs for a specific letter: tail -f {LOG_DIR}/pnl_a.log", Colors.YELLOW)
    print_colored("To check if processes are running: ps aux | grep notifications_pnl.py", Colors.YELLOW)
    
    # Display summary
    print()
    print_colored("📊 Summary:", Colors.BLUE)
    print("Total instances started: 26")
    print(f"Log directory: {LOG_DIR}")
    print(f"PID file: {PID_FILE}")
    print()
    
    # Optional: Display first few lines from logs to verify startup
    print_colored("🔍 Sample startup messages:", Colors.BLUE)
    time.sleep(2)  # Give processes time to start and write to logs
    
    for letter in ['a', 'b', 'c']:
        log_file_path = os.path.join(LOG_DIR, f"pnl_{letter}.log")
        if os.path.isfile(log_file_path):
            print_colored(f"--- Instance {letter} ---", Colors.GREEN)
            try:
                with open(log_file_path, 'r') as f:
                    lines = f.readlines()[:3]  # Read first 3 lines
                    if lines:
                        for line in lines:
                            print(line.strip())
                    else:
                        print("Log not ready yet")
            except Exception:
                print("Log not ready yet")
        print()
    
    return 0

if __name__ == "__main__":
    exit(main())