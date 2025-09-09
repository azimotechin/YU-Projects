#!/bin/bash

# PnL Calculator - Start 26 instances (one for each letter)
# Usage: ./start_pnl_calculators.sh

SCRIPT_NAME="notifications_pnl.py"
LOG_DIR="logs"
PID_FILE="pnl_pids.txt"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🚀 Starting 26 PnL Calculator instances...${NC}"

# Create logs directory if it doesn't exist
mkdir -p $LOG_DIR

# Clear previous PID file
> $PID_FILE

# Check if the Python script exists
if [ ! -f "$SCRIPT_NAME" ]; then
    echo -e "${RED}❌ Error: $SCRIPT_NAME not found in current directory${NC}"
    exit 1
fi

# Start instances for each letter
for letter in {a..z}; do
    echo -e "${YELLOW}📊 Starting PnL Calculator for accounts starting with '${letter^^}'${NC}"
    
    # Start the process in background and redirect output to log file
    python $SCRIPT_NAME $letter > $LOG_DIR/pnl_${letter}.log 2>&1 &
    
    # Capture the process ID
    PID=$!
    
    # Save PID to file for later cleanup
    echo "$letter:$PID" >> $PID_FILE
    
    echo -e "${GREEN}✅ Started instance for letter '$letter' (PID: $PID)${NC}"
    
    # Small delay to prevent overwhelming the system
    sleep 0.1
done

echo ""
echo -e "${GREEN}🎉 Successfully started 26 PnL Calculator instances!${NC}"
echo -e "${BLUE}📝 Process IDs saved to: $PID_FILE${NC}"
echo -e "${BLUE}📋 Logs available in: $LOG_DIR/pnl_[letter].log${NC}"
echo ""
echo -e "${YELLOW}To stop all instances, run: ./stop_pnl_calculators.sh${NC}"
echo -e "${YELLOW}To view logs for a specific letter: tail -f $LOG_DIR/pnl_a.log${NC}"
echo -e "${YELLOW}To check if processes are running: ps aux | grep notifications_pnl.py${NC}"

# Display summary
echo ""
echo -e "${BLUE}📊 Summary:${NC}"
echo "Total instances started: 26"
echo "Log directory: $LOG_DIR"
echo "PID file: $PID_FILE"
echo ""

# Optional: Display first few lines from logs to verify startup
echo -e "${BLUE}🔍 Sample startup messages:${NC}"
sleep 2  # Give processes time to start
for letter in a b c; do
    if [ -f "$LOG_DIR/pnl_${letter}.log" ]; then
        echo -e "${GREEN}--- Instance $letter ---${NC}"
        head -3 "$LOG_DIR/pnl_${letter}.log" 2>/dev/null || echo "Log not ready yet"
    fi
done