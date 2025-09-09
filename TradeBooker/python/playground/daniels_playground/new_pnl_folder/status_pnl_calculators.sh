#!/bin/bash

# PnL Calculator - Status monitor for all 26 instances
# Usage: ./status_pnl_calculators.sh

PID_FILE="pnl_pids.txt"
LOG_DIR="logs"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}📊 PnL Calculator Status Monitor${NC}"
echo "================================"

# Check if PID file exists
if [ ! -f "$PID_FILE" ]; then
    echo -e "${YELLOW}⚠️  No PID file found. Checking for any running instances...${NC}"
    
    # Check for any running processes
    running_processes=$(ps aux | grep "notifications_pnl.py" | grep -v grep)
    
    if [ -z "$running_processes" ]; then
        echo -e "${RED}❌ No PnL Calculator instances running${NC}"
    else
        echo -e "${GREEN}✅ Found running instances:${NC}"
        echo "$running_processes"
    fi
    exit 0
fi

# Status counters
running_count=0
stopped_count=0

echo -e "${BLUE}Checking status of all instances...${NC}"
echo ""

# Check each instance
while IFS=':' read -r letter pid; do
    if [ ! -z "$pid" ]; then
        # Check if process is running
        if kill -0 $pid 2>/dev/null; then
            echo -e "${GREEN}✅ Letter '$letter' (PID: $pid) - RUNNING${NC}"
            ((running_count++))
            
            # Show recent log activity (last line)
            if [ -f "$LOG_DIR/pnl_${letter}.log" ]; then
                last_log=$(tail -1 "$LOG_DIR/pnl_${letter}.log" 2>/dev/null)
                if [ ! -z "$last_log" ]; then
                    echo -e "   ${BLUE}Last activity: ${last_log}${NC}"
                fi
            fi
        else
            echo -e "${RED}❌ Letter '$letter' (PID: $pid) - STOPPED${NC}"
            ((stopped_count++))
        fi
    fi
done < "$PID_FILE"

echo ""
echo -e "${BLUE}📈 Summary:${NC}"
echo -e "${GREEN}Running: $running_count instances${NC}"
echo -e "${RED}Stopped: $stopped_count instances${NC}"
echo -e "Total expected: 26 instances"

# Show overall system resource usage
echo ""
echo -e "${BLUE}🖥️  System Resource Usage:${NC}"
memory_usage=$(ps aux | grep "notifications_pnl.py" | grep -v grep | awk '{sum += $4} END {printf "%.1f", sum}')
if [ ! -z "$memory_usage" ]; then
    echo "Memory usage: ${memory_usage}% total"
else
    echo "Memory usage: 0% (no processes running)"
fi

# Show log file sizes
echo ""
echo -e "${BLUE}📁 Log File Status:${NC}"
if [ -d "$LOG_DIR" ]; then
    total_log_size=$(du -sh "$LOG_DIR" 2>/dev/null | cut -f1)
    log_count=$(ls -1 "$LOG_DIR"/pnl_*.log 2>/dev/null | wc -l)
    echo "Log files: $log_count files, total size: $total_log_size"
    
    # Show largest log files
    echo "Largest log files:"
    ls -lSh "$LOG_DIR"/pnl_*.log 2>/dev/null | head -3 | while read line; do
        echo "  $line"
    done
else
    echo "Log directory not found"
fi

# Show recent errors (if any)
echo ""
echo -e "${BLUE}🚨 Recent Errors (last 5 minutes):${NC}"
error_found=false
for letter in {a..z}; do
    if [ -f "$LOG_DIR/pnl_${letter}.log" ]; then
        recent_errors=$(find "$LOG_DIR/pnl_${letter}.log" -mmin -5 -exec grep -l "ERROR\|CRITICAL" {} \; 2>/dev/null)
        if [ ! -z "$recent_errors" ]; then
            echo -e "${RED}❌ Errors in instance '$letter':${NC}"
            tail -5 "$LOG_DIR/pnl_${letter}.log" | grep "ERROR\|CRITICAL" | tail -2
            error_found=true
        fi
    fi
done

if [ "$error_found" = false ]; then
    echo -e "${GREEN}✅ No recent errors found${NC}"
fi

echo ""
echo -e "${YELLOW}💡 Useful commands:${NC}"
echo "  View logs for letter 'a': tail -f $LOG_DIR/pnl_a.log"
echo "  View all recent activity: tail -f $LOG_DIR/pnl_*.log"
echo "  Stop all instances: ./stop_pnl_calculators.sh"
echo "  Restart all instances: ./stop_pnl_calculators.sh && ./start_pnl_calculators.sh"