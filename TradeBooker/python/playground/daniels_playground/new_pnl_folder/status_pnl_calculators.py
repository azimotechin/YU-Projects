#!/usr/bin/env python3

import os
import subprocess
import glob
import time
import string
from pathlib import Path
from datetime import datetime, timedelta

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

def is_process_running(pid):
    """Check if a process with given PID is running"""
    try:
        os.kill(pid, 0)  # Send signal 0 to check if process exists
        return True
    except (OSError, ProcessLookupError):
        return False

def get_last_log_line(log_file_path):
    """Get the last line from a log file"""
    try:
        with open(log_file_path, 'r') as f:
            lines = f.readlines()
            if lines:
                return lines[-1].strip()
    except Exception:
        pass
    return None

def get_running_processes():
    """Get all running pnl_calculator.py processes"""
    try:
        result = subprocess.run(
            ["ps", "aux"],
            capture_output=True,
            text=True,
            check=True
        )
        
        lines = []
        for line in result.stdout.split('\n'):
            if 'pnl_calculator.py' in line and 'grep' not in line:
                lines.append(line)
        return lines
    except Exception:
        return []

def get_memory_usage():
    """Calculate total memory usage of all PnL processes"""
    try:
        result = subprocess.run(
            ["ps", "aux"],
            capture_output=True,
            text=True,
            check=True
        )
        
        total_memory = 0.0
        for line in result.stdout.split('\n'):
            if 'pnl_calculator.py' in line and 'grep' not in line:
                parts = line.split()
                if len(parts) >= 4:
                    try:
                        memory_percent = float(parts[3])
                        total_memory += memory_percent
                    except ValueError:
                        continue
        
        return total_memory
    except Exception:
        return 0.0

def get_directory_size(directory):
    """Get the total size of a directory"""
    try:
        result = subprocess.run(
            ["du", "-sh", directory],
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout.split()[0]
    except Exception:
        return "Unknown"

def get_largest_log_files(log_dir, count=3):
    """Get the largest log files"""
    try:
        result = subprocess.run(
            ["ls", "-lSh"] + glob.glob(f"{log_dir}/pnl_*.log"),
            capture_output=True,
            text=True,
            check=True
        )
        lines = result.stdout.strip().split('\n')
        return lines[:count]
    except Exception:
        return []

def find_recent_errors(log_dir, minutes=5):
    """Find recent errors in log files"""
    errors_found = {}
    cutoff_time = datetime.now() - timedelta(minutes=minutes)
    
    for letter in string.ascii_lowercase:
        log_file = f"{log_dir}/pnl_{letter}.log"
        if os.path.isfile(log_file):
            try:
                # Check if file was modified in the last 'minutes' minutes
                file_mtime = datetime.fromtimestamp(os.path.getmtime(log_file))
                if file_mtime > cutoff_time:
                    # Read last few lines and look for errors
                    with open(log_file, 'r') as f:
                        lines = f.readlines()
                        error_lines = []
                        for line in lines[-10:]:  # Check last 10 lines
                            if 'ERROR' in line or 'CRITICAL' in line:
                                error_lines.append(line.strip())
                        
                        if error_lines:
                            errors_found[letter] = error_lines[-2:]  # Last 2 error lines
            except Exception:
                continue
    
    return errors_found

def main():
    PID_FILE = "pnl_pids.txt"
    LOG_DIR = "logs"
    
    print_colored("📊 PnL Calculator Status Monitor", Colors.BLUE)
    print("================================")
    
    # Check if PID file exists
    if not os.path.isfile(PID_FILE):
        print_colored("⚠️  No PID file found. Checking for any running instances...", Colors.YELLOW)
        
        # Check for any running processes
        running_processes = get_running_processes()
        
        if not running_processes:
            print_colored("❌ No PnL Calculator instances running", Colors.RED)
        else:
            print_colored("✅ Found running instances:", Colors.GREEN)
            for process in running_processes:
                print(process)
        return 0
    
    # Status counters
    running_count = 0
    stopped_count = 0
    
    print_colored("Checking status of all instances...", Colors.BLUE)
    print()
    
    # Check each instance
    try:
        with open(PID_FILE, 'r') as f:
            lines = f.readlines()
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            try:
                letter, pid_str = line.split(':')
                pid = int(pid_str)
                
                # Check if process is running
                if is_process_running(pid):
                    print_colored(f"✅ Letter '{letter}' (PID: {pid}) - RUNNING", Colors.GREEN)
                    running_count += 1
                    
                    # Show recent log activity (last line)
                    log_file_path = f"{LOG_DIR}/pnl_{letter}.log"
                    if os.path.isfile(log_file_path):
                        last_log = get_last_log_line(log_file_path)
                        if last_log:
                            print_colored(f"   Last activity: {last_log}", Colors.BLUE)
                else:
                    print_colored(f"❌ Letter '{letter}' (PID: {pid}) - STOPPED", Colors.RED)
                    stopped_count += 1
                    
            except ValueError:
                print_colored(f"❌ Invalid line format in PID file: {line}", Colors.RED)
                continue
                
    except Exception as e:
        print_colored(f"❌ Error reading PID file: {e}", Colors.RED)
        return 1
    
    print()
    print_colored("📈 Summary:", Colors.BLUE)
    print_colored(f"Running: {running_count} instances", Colors.GREEN)
    print_colored(f"Stopped: {stopped_count} instances", Colors.RED)
    print("Total expected: 26 instances")
    
    # Show overall system resource usage
    print()
    print_colored("🖥️  System Resource Usage:", Colors.BLUE)
    memory_usage = get_memory_usage()
    if memory_usage > 0:
        print(f"Memory usage: {memory_usage:.1f}% total")
    else:
        print("Memory usage: 0% (no processes running)")
    
    # Show log file status
    print()
    print_colored("📁 Log File Status:", Colors.BLUE)
    if os.path.isdir(LOG_DIR):
        total_log_size = get_directory_size(LOG_DIR)
        log_files = glob.glob(f"{LOG_DIR}/pnl_*.log")
        log_count = len(log_files)
        print(f"Log files: {log_count} files, total size: {total_log_size}")
        
        # Show largest log files
        print("Largest log files:")
        largest_files = get_largest_log_files(LOG_DIR)
        for file_info in largest_files:
            if file_info.strip():
                print(f"  {file_info}")
    else:
        print("Log directory not found")
    
    # Show recent errors (if any)
    print()
    print_colored("🚨 Recent Errors (last 5 minutes):", Colors.BLUE)
    recent_errors = find_recent_errors(LOG_DIR)
    
    if recent_errors:
        for letter, error_lines in recent_errors.items():
            print_colored(f"❌ Errors in instance '{letter}':", Colors.RED)
            for error_line in error_lines:
                print(f"  {error_line}")
    else:
        print_colored("✅ No recent errors found", Colors.GREEN)
    
    print()
    print_colored("💡 Useful commands:", Colors.YELLOW)
    print(f"  View logs for letter 'a': tail -f {LOG_DIR}/pnl_a.log")
    print(f"  View all recent activity: tail -f {LOG_DIR}/pnl_*.log")
    print("  Stop all instances: python stop_pnl_calculators.py")
    print("  Restart all instances: python stop_pnl_calculators.py && python start_pnl_calculators.py")
    
    return 0

if __name__ == "__main__":
    exit(main())