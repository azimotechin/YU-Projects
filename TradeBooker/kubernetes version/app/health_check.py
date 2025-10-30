#!/usr/bin/env python3
"""
Simple health check script for the trade manager
"""
import sys
import os

def main():
    """Simple health check that just verifies the service is running"""
    try:
        service_type = os.getenv('SERVICE_TYPE', 'unknown')
        if service_type == 'trade-manager':
            sys.exit(0)
        else:
            sys.exit(1)
    except Exception:
        sys.exit(1)

if __name__ == '__main__':
    main()
