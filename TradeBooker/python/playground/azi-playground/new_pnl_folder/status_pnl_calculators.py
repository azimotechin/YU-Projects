import os
import subprocess


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


def is_process_running(pid):
    """Check if a process with the given PID is running"""
    try:
        os.kill(pid, 0)
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
    return "Could not read log."


def get_process_info():
    """Get all running listener and worker processes"""
    try:
        # Use a regex to find both scripts at once
        result = subprocess.run(
            ["ps", "aux"],
            capture_output=True,
            text=True,
            check=True
        )
        lines = []
        for line in result.stdout.split('\n'):
            if ('pnl_calculator.py' in line or 'notification_listener.py' in line) and 'grep' not in line:
                lines.append(line)
        return lines
    except Exception:
        return []


def get_memory_usage():
    """Calculate total memory usage of all PnL related processes"""
    total_memory = 0.0
    processes = get_process_info()
    for line in processes:
        parts = line.split()
        if len(parts) >= 4:
            try:
                total_memory += float(parts[3])
            except ValueError:
                continue
    return total_memory


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


def main():
    PID_FILE = "pnl_pids.txt"
    LOG_DIR = "logs"

    print_colored("📊 PnL System Status Monitor", Colors.BLUE)
    print("================================")

    if not os.path.isfile(PID_FILE):
        print_colored("⚠️  No PID file found. Checking for any running instances...", Colors.YELLOW)
        running_processes = get_process_info()
        if not running_processes:
            print_colored("❌ No PnL system processes are running.", Colors.RED)
        else:
            print_colored(f"✅ Found {len(running_processes)} running instances:", Colors.GREEN)
            for process in running_processes:
                print(process)
        return 0

    running_count = 0
    stopped_count = 0

    print_colored("Checking status of all processes...", Colors.BLUE)
    print()

    with open(PID_FILE, 'r') as f:
        lines = f.readlines()

    total_expected = len(lines)

    for line in lines:
        line = line.strip()
        if not line:
            total_expected -= 1
            continue

        try:
            name, pid_str = line.split(':')
            pid = int(pid_str)

            # Determine the log file path based on the process name
            if name == "listener":
                log_file_path = f"{LOG_DIR}/notification_listener.log"
                color = Colors.PURPLE
                display_name = "Listener"
            else:  # It's a worker
                letter = name.split('_')[-1]
                log_file_path = f"{LOG_DIR}/pnl_worker_{letter}.log"
                color = Colors.GREEN
                display_name = f"Worker '{letter.upper()}'"

            if is_process_running(pid):
                print_colored(f"✅ {display_name} (PID: {pid}) - RUNNING", color)
                running_count += 1
                last_log = get_last_log_line(log_file_path)
                print_colored(f"   Last activity: {last_log}", Colors.BLUE)
            else:
                print_colored(f"❌ {display_name} (PID: {pid}) - STOPPED", Colors.RED)
                stopped_count += 1

        except (ValueError, IndexError):
            print_colored(f"❌ Invalid line format in PID file: {line}", Colors.RED)
            continue

    print()
    print_colored("📈 Summary:", Colors.BLUE)
    print_colored(f"Running: {running_count} processes",
                  Colors.GREEN if running_count == total_expected else Colors.YELLOW)
    print_colored(f"Stopped: {stopped_count} processes", Colors.RED if stopped_count > 0 else Colors.BLUE)
    print(f"Total expected: {total_expected} processes")

    print()
    print_colored("🖥️  System Resource Usage:", Colors.BLUE)
    memory_usage = get_memory_usage()
    print(f"Memory usage: {memory_usage:.1f}% total")

    print()
    print_colored("💡 Useful commands:", Colors.YELLOW)
    print(f"  View listener logs: tail -f {LOG_DIR}/notification_listener.log")
    print(f"  View worker 'a' logs: tail -f {LOG_DIR}/pnl_worker_a.log")
    print("  Stop all processes: python stop_pnl_calculators.py")

    return 0


if __name__ == "__main__":
    exit(main())