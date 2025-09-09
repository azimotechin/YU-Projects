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
    PURPLE = '\033[0;35m'
    NC = '\033[0m'  # No Color


def print_colored(message, color):
    """Print message with specified color"""
    print(f"{color}{message}{Colors.NC}")


def main():
    WORKER_SCRIPT = "pnl_calculator.py"
    LISTENER_SCRIPT = "notification_listener.py"
    LOG_DIR = "logs"
    PID_FILE = "pnl_pids.txt"

    # Create a logs directory if it doesn't exist
    Path(LOG_DIR).mkdir(exist_ok=True)

    # Clear the previous PID file
    open(PID_FILE, 'w').close()

    # Check if scripts exist
    for script in [WORKER_SCRIPT, LISTENER_SCRIPT]:
        if not os.path.isfile(script):
            print_colored(f"❌ Error: {script} not found in current directory", Colors.RED)
            return 1

    # --- 1. Start the Notification Listener ---
    print_colored("🚀 Starting the central Notification Listener...", Colors.PURPLE)
    listener_log_path = os.path.join(LOG_DIR, "notification_listener.log")
    try:
        with open(listener_log_path, 'w') as log_file:
            listener_process = subprocess.Popen(
                ["python", LISTENER_SCRIPT],
                stdout=log_file,
                stderr=subprocess.STDOUT,
                cwd=os.getcwd()
            )
        with open(PID_FILE, 'a') as pid_file:
            pid_file.write(f"listener:{listener_process.pid}\n")
        print_colored(f"✅ Started Notification Listener (PID: {listener_process.pid})", Colors.GREEN)
        print_colored(f"📋 Logs available in: {listener_log_path}", Colors.BLUE)
    except Exception as e:
        print_colored(f"❌ Failed to start the Notification Listener: {e}", Colors.RED)
        return 1

    print()
    time.sleep(1)  # Give the listener a moment to initialize

    # --- 2. Start the 26 PnL Worker instances ---
    print_colored("🚀 Starting 26 PnL Worker instances...", Colors.BLUE)
    processes = {}

    for letter in string.ascii_lowercase:
        print_colored(f"📊 Starting PnL Worker for shard '{letter.upper()}'", Colors.YELLOW)

        log_file_path = os.path.join(LOG_DIR, f"pnl_worker_{letter}.log")

        try:
            with open(log_file_path, 'w') as log_file:
                process = subprocess.Popen(
                    ["python", WORKER_SCRIPT, letter],  # Pass only the single letter
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                    cwd=os.getcwd()
                )

            processes[letter] = process

            with open(PID_FILE, 'a') as pid_file:
                pid_file.write(f"worker_{letter}:{process.pid}\n")

            print_colored(f"✅ Started worker for shard '{letter}' (PID: {process.pid})", Colors.GREEN)
            time.sleep(0.1)

        except Exception as e:
            print_colored(f"❌ Failed to start worker for shard '{letter}': {e}", Colors.RED)

    print()
    print_colored("🎉 Successfully launched 1 Listener and 26 Worker processes!", Colors.GREEN)
    print_colored(f"📝 Process IDs saved to: {PID_FILE}", Colors.BLUE)
    print_colored("To stop all instances, you will need to create a corresponding stop script.", Colors.YELLOW)

    """# This infinite loop prevents the script from exiting, keeping the container alive.
    try:
        while True:
            # Sleep for a long interval to consume minimal CPU.
            # The script's only job now is to exist.
            time.sleep(60)
            # You could add logic here to check if the child processes are still running
    except KeyboardInterrupt:
        print_colored("\nMonitoring stopped.", Colors.BLUE)
    # --- END OF ADDED LOOP ---"""

    return 0


if __name__ == "__main__":
    exit(main())