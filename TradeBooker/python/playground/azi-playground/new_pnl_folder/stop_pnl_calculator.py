import os
import signal
import time

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
    PID_FILE = "pnl_pids.txt"
    
    print_colored("🛑 Stopping PnL Calculator instances...", Colors.BLUE)
    
    # Check if the PID file exists
    if not os.path.isfile(PID_FILE):
        print_colored(f"❌ PID file '{PID_FILE}' not found. No instances to stop or they may have already been stopped.", Colors.RED)
        return 1
    
    stopped_count = 0
    failed_count = 0
    
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
                
                print_colored(f"🔄 Stopping instance for letter '{letter}' (PID: {pid})", Colors.YELLOW)
                
                # Check if a process exists and terminate it
                try:
                    os.kill(pid, signal.SIGTERM)  # Graceful termination
                    time.sleep(0.1)  # Give it a moment to terminate gracefully
                    
                    # Check if it's still running
                    try:
                        os.kill(pid, 0)  # Check if a process exists
                        # If we get here, a process is still running, force kill it
                        print_colored(f"⚠️  Process {pid} didn't terminate gracefully, force killing...", Colors.YELLOW)
                        os.kill(pid, signal.SIGKILL)
                    except ProcessLookupError:
                        # Process terminated successfully
                        pass
                    
                    print_colored(f"✅ Stopped instance for letter '{letter}' (PID: {pid})", Colors.GREEN)
                    stopped_count += 1
                    
                except ProcessLookupError:
                    print_colored(f"⚠️  Process {pid} (letter '{letter}') was not running", Colors.YELLOW)
                    stopped_count += 1
                except PermissionError:
                    print_colored(f"❌ Permission denied to stop process {pid} (letter '{letter}')", Colors.RED)
                    failed_count += 1
                    
            except ValueError:
                print_colored(f"❌ Invalid line format in PID file: {line}", Colors.RED)
                failed_count += 1
                continue
    
    except Exception as e:
        print_colored(f"❌ Error reading PID file: {e}", Colors.RED)
        return 1
    
    print()
    print_colored("📊 Summary:", Colors.BLUE)
    print(f"Instances stopped: {stopped_count}")
    if failed_count > 0:
        print(f"Failed to stop: {failed_count}")
    
    # Clean up the PID file
    try:
        os.remove(PID_FILE)
        print_colored(f"🗑️  Removed PID file: {PID_FILE}", Colors.GREEN)
    except Exception as e:
        print_colored(f"⚠️  Could not remove PID file: {e}", Colors.YELLOW)
    
    if failed_count == 0:
        print_colored("🎉 All PnL Calculator instances stopped successfully!", Colors.GREEN)
        return 0
    else:
        print_colored(f"⚠️  Completed with {failed_count} failures", Colors.YELLOW)
        return 1

if __name__ == "__main__":
    exit(main())