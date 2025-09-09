#!/bin/bash

# PnL Calculator - Stop all 26 instances
# Usage: ./stop_pnl_calculators.sh

PID_FILE="pnl_pids.txt"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🛑 Stopping PnL Calculator instances...${NC}"

# Check if PID file exists
if [ ! -f "$PID_FILE" ]; then
    echo -e "${YELLOW}⚠️  No PID file found. Attempting to kill all notifications_pnl.py processes...${NC}"
    
    # Try to find and kill all notifications_pnl.py processes
    PIDS=$(ps aux | grep "notifications_pnl.py" | grep -v grep | awk '{print $2}')
    
    if [ -z "$PIDS" ]; then
        echo -e "${GREEN}✅ No PnL Calculator processes found running.${NC}"
        exit 0
    else
        echo -e "${YELLOW}Found processes: $PIDS${NC}"
        for pid in $PIDS; do
            kill $pid 2>/dev/null
            if [ $? -eq 0 ]; then
                echo -e "${GREEN}✅ Killed process $pid${NC}"
            else
                echo -e "${RED}❌ Failed to kill process $pid${NC}"
            fi
        done
    fi
    exit 0
fi

# Read PIDs from file and stop each process
stopped_count=0
failed_count=0

while IFS=':' read -r letter pid; do
    if [ ! -z "$pid" ]; then
        echo -e "${YELLOW}🔄 Stopping instance for letter '$letter' (PID: $pid)${NC}"
        
        # Check if process is still running
        if kill -0 $pid 2>/dev/null; then
            # Send SIGTERM first (graceful shutdown)
            kill $pid 2>/dev/null
            
            # Wait a moment for graceful shutdown
            sleep 0.5
            
            # Check if it's still running, if so force kill
            if kill -0 $pid 2>/dev/null; then
                echo -e "${YELLOW}⚠️  Graceful shutdown failed for $letter, forcing kill...${NC}"
                kill -9 $pid 2>/dev/null
            fi
            
            # Verify it's stopped
            if ! kill -0 $pid 2>/dev/null; then
                echo -e "${GREEN}✅ Stopped instance for letter '$letter'${NC}"
                ((stopped_count++))
            else
                echo -e "${RED}❌ Failed to stop instance for letter '$letter'${NC}"
                ((failed_count++))
            fi
        else
            echo -e "${YELLOW}⚠️  Process $pid for letter '$letter' was already stopped${NC}"
            ((stopped_count++))
        fi
    fi
done < "$PID_FILE"

echo ""
echo -e "${BLUE}📊 Summary:${NC}"
echo -e "${GREEN}Stopped: $stopped_count instances${NC}"
if [ $failed_count -gt 0 ]; then
    echo -e "${RED}Failed: $failed_count instances${NC}"
fi

# Clean up PID file
rm -f "$PID_FILE"
echo -e "${GREEN}🗑️  Cleaned up PID file${NC}"

# Optional: Show remaining processes
remaining=$(ps aux | grep "notifications_pnl.py" | grep -v grep | wc -l)
if [ $remaining -gt 0 ]; then
    echo -e "${YELLOW}⚠️  Warning: $remaining PnL Calculator processes may still be running${NC}"
    echo -e "${YELLOW}Run 'ps aux | grep notifications_pnl.py' to check${NC}"
else
    echo -e "${GREEN}✅ All PnL Calculator instances stopped successfully!${NC}"
fi