#!/bin/bash
# Trading System - Operations & Testing Script (Kubernetes Edition)
# This script provides operational commands for managing and testing the deployed trading system
# 
# ✨ Features:
# - 📊 System status monitoring and health checks
# - 🧪 Comprehensive failure testing scenarios
# - 🔧 System maintenance and troubleshooting
# - 🧹 Clean shutdown and cleanup capabilities
# - 📋 Log viewing and debugging tools
# - 🔄 Service restart and recovery functions
#
# 🚀 Usage: 
#   ./test.sh                    # Interactive mode
#   ./test.sh status             # Show system status
#   ./test.sh test               # Run failure testing scenarios
#   ./test.sh logs               # Show recent logs
#   ./test.sh cleanup            # Clean shutdown and cleanup
#   ./test.sh restart-ports      # Restart port forwarding
# 
# 🧪 Test Scenarios Available:
# - Primary/Secondary Redis failure simulation
# - Primary/Secondary Streamlit failure simulation  
# - Primary/Secondary Trade Manager failure simulation
# - Market Data service failure simulation
# - Complete service outage testing (both replicas down)
# - Automatic service restoration capabilities

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Configuration
NAMESPACE="trading-system"
HELM_RELEASE="trading-system"
STREAMLIT_PORT=8501
TRADE_MANAGER_PORT=8000
REDIS_PORT=6379
MARKET_DATA_PORT=8080

# Function to print colored output
print_color() {
    printf "${1}${2}${NC}\n"
}

# Function to print section headers
print_header() {
    echo ""
    print_color $CYAN "========================================="
    print_color $CYAN "$1"
    print_color $CYAN "========================================="
}

# Function to check if system is running
check_system_running() {
    # Check if kubectl is available
    if ! command -v kubectl &> /dev/null; then
        print_color $RED "❌ kubectl is not installed or not in PATH"
        print_color $YELLOW "Please install kubectl first"
        exit 1
    fi
    
    # Check if we can connect to Kubernetes cluster
    if ! kubectl cluster-info &> /dev/null; then
        print_color $RED "❌ Cannot connect to Kubernetes cluster"
        print_color $YELLOW "Please ensure your kubeconfig is set up correctly"
        exit 1
    fi
    
    # Check if trading-system namespace exists
    if ! kubectl get namespace $NAMESPACE &> /dev/null; then
        print_color $RED "❌ Trading system namespace '$NAMESPACE' does not exist"
        print_color $YELLOW "Please deploy the system first using Helm"
        exit 1
    fi
    
    # Check if helm release exists
    if ! helm list -n $NAMESPACE | grep -q $HELM_RELEASE; then
        print_color $RED "❌ Helm release '$HELM_RELEASE' not found in namespace '$NAMESPACE'"
        print_color $YELLOW "Please deploy the system first using: helm install $HELM_RELEASE . --namespace $NAMESPACE --create-namespace"
        exit 1
    fi
}

# Function to show system status
show_status() {
    print_header "📊 System Status"
    
    print_color $BLUE "Kubernetes Cluster Information:"
    kubectl cluster-info | head -3
    
    echo ""
    print_color $BLUE "Helm Release Status:"
    helm status $HELM_RELEASE -n $NAMESPACE
    
    echo ""
    print_color $BLUE "StatefulSet Status:"
    kubectl get statefulsets -n $NAMESPACE
    
    echo ""
    print_color $BLUE "Pod Status:"
    kubectl get pods -n $NAMESPACE -o wide
    
    echo ""
    print_color $BLUE "Service Status:"
    kubectl get services -n $NAMESPACE
    
    echo ""
    print_color $BLUE "Persistent Volume Claims:"
    kubectl get pvc -n $NAMESPACE
    
    echo ""
    print_color $BLUE "Pod Health Summary:"
    kubectl get pods -n $NAMESPACE | awk 'NR>1 {
        if ($2 == "1/1" && $3 == "Running") 
            print "✅ " $1 " - Healthy"
        else if ($3 == "Running") 
            print "⚠️  " $1 " - Running but not ready (" $2 ")"
        else 
            print "❌ " $1 " - " $3 " (" $2 ")"
    }'
    
    echo ""
    print_color $BLUE "Port Forwarding Status:"
    if pgrep -f "kubectl port-forward.*$NAMESPACE" > /dev/null; then
        print_color $GREEN "✅ Port forwarding is active"
        print_color $CYAN "   📊 Streamlit UI:    http://localhost:$STREAMLIT_PORT"
        print_color $CYAN "   ⚡ Trade Manager:    http://localhost:$TRADE_MANAGER_PORT"
        print_color $CYAN "   🗄️  Redis (debug):   localhost:$REDIS_PORT"
    else
        print_color $YELLOW "⚠️  Port forwarding is not active"
        print_color $YELLOW "   Run './test.sh restart-ports' to start port forwarding"
    fi
    
    echo ""
    print_color $BLUE "Resource Usage:"
    kubectl top pods -n $NAMESPACE 2>/dev/null || print_color $YELLOW "⚠️  Metrics server not available for resource stats"
}

# Function to show logs
show_logs() {
    print_header "📋 Recent Logs"
    
    print_color $BLUE "Redis logs (redis-0):"
    kubectl logs -n $NAMESPACE redis-0 --tail=10 2>/dev/null || print_color $YELLOW "⚠️  Redis logs not available"
    
    echo ""
    print_color $BLUE "Market Data logs (market-data-0):"
    kubectl logs -n $NAMESPACE market-data-0 --tail=10 2>/dev/null || print_color $YELLOW "⚠️  Market Data logs not available"
    
    echo ""
    print_color $BLUE "Streamlit logs (streamlit-0):"
    kubectl logs -n $NAMESPACE streamlit-0 --tail=10 2>/dev/null || print_color $YELLOW "⚠️  Streamlit logs not available"
    
    echo ""
    print_color $BLUE "Trade Manager logs (trade-manager-0):"
    kubectl logs -n $NAMESPACE trade-manager-0 --tail=10 2>/dev/null || print_color $YELLOW "⚠️  Trade Manager logs not available"
    
    echo ""
    print_color $BLUE "Recent Kubernetes Events:"
    kubectl get events -n $NAMESPACE --sort-by='.lastTimestamp' | tail -5
}

# Function to restart port forwarding
restart_port_forwarding() {
    print_header "🌐 Restarting Port Forwarding"
    
    print_color $BLUE "Stopping existing port forwarding..."
    pkill -f "kubectl port-forward" 2>/dev/null || true
    sleep 2
    
    print_color $BLUE "Setting up port forwarding to services..."
    
    # Port forward to services (they load balance to healthy pods)
    nohup kubectl port-forward -n $NAMESPACE svc/streamlit $STREAMLIT_PORT:8501 > /dev/null 2>&1 &
    nohup kubectl port-forward -n $NAMESPACE svc/trade-manager $TRADE_MANAGER_PORT:8000 > /dev/null 2>&1 &
    nohup kubectl port-forward -n $NAMESPACE svc/redis-primary $REDIS_PORT:6379 > /dev/null 2>&1 &
    
    # Wait a moment for port forwards to establish
    sleep 3
    
    print_color $GREEN "✅ Port forwarding restarted successfully!"
    print_color $CYAN "🌐 Access URLs:"
    print_color $CYAN "   📊 Streamlit UI:    http://localhost:$STREAMLIT_PORT"
    print_color $CYAN "   ⚡ Trade Manager:    http://localhost:$TRADE_MANAGER_PORT"  
    print_color $CYAN "   🗄️  Redis (debug):   localhost:$REDIS_PORT"
    print_color $YELLOW "💡 Port forwarding runs in background. Use 'pkill -f kubectl' to stop."
}

# Function to test market data functionality
test_market_data() {
    print_header "📊 Testing Market Data Service"
    
    print_color $BLUE "Testing market data functions..."
    
    # Test if market-data pod is running
    if kubectl get pod market-data-0 -n $NAMESPACE &> /dev/null; then
        print_color $GREEN "✅ Market Data pod is running"
        
        # Test market data functionality
        kubectl exec -it market-data-0 -n $NAMESPACE -- python -c "
from market_data import get_price, get_historical_price
import sys
try:
    print('📈 Testing AAPL current price...')
    price = get_price('AAPL', 1)
    print(f'✅ Current AAPL price: \${price}')
    
    print('📊 Testing historical data...')  
    hist_price = get_historical_price('AAPL', '1d')
    print(f'✅ AAPL price 1 day ago: \${hist_price}')
    
    print('🔍 Testing GOOGL price...')
    googl_price = get_price('GOOGL', 1)
    print(f'✅ Current GOOGL price: \${googl_price}')
    
    print('🎉 Market data service is working perfectly!')
except Exception as e:
    print(f'❌ Market data error: {e}')
    sys.exit(1)
" 2>/dev/null || print_color $RED "❌ Market data test failed"
    else
        print_color $RED "❌ Market Data pod not found"
    fi
}

# Function to test Redis connectivity
test_redis() {
    print_header "🗄️ Testing Redis Service"
    
    print_color $BLUE "Testing Redis connectivity..."
    
    if kubectl get pod redis-0 -n $NAMESPACE &> /dev/null; then
        print_color $GREEN "✅ Redis pod is running"
        
        # Test Redis functionality
        kubectl exec -it redis-0 -n $NAMESPACE -- redis-cli ping 2>/dev/null && print_color $GREEN "✅ Redis PING successful" || print_color $RED "❌ Redis PING failed"
        
        # Check cached data
        print_color $BLUE "Checking cached market data..."
        cached_keys=$(kubectl exec -it redis-0 -n $NAMESPACE -- redis-cli keys "*" 2>/dev/null | wc -l)
        print_color $CYAN "📊 Total cached keys: $cached_keys"
        
        # Show sample keys
        kubectl exec -it redis-0 -n $NAMESPACE -- redis-cli keys "*AAPL*" 2>/dev/null | head -5 | while read key; do
            if [ -n "$key" ]; then
                print_color $CYAN "🔑 Sample key: $key"
            fi
        done
    else
        print_color $RED "❌ Redis pod not found"
    fi
}

# Function to test the complete system end-to-end
test_system_health() {
    print_header "🔍 Complete System Health Check"
    
    print_color $BLUE "Running comprehensive system tests..."
    
    # Test all components
    test_redis
    test_market_data
    
    print_color $BLUE "Testing service connectivity..."
    
    # Test if services are accessible via port forwarding
    if pgrep -f "kubectl port-forward.*$NAMESPACE.*$STREAMLIT_PORT" > /dev/null; then
        if curl -s -o /dev/null -w "%{http_code}" http://localhost:$STREAMLIT_PORT | grep -q "200"; then
            print_color $GREEN "✅ Streamlit UI is accessible"
        else
            print_color $YELLOW "⚠️  Streamlit UI may not be fully ready"
        fi
    else
        print_color $YELLOW "⚠️  Streamlit port forwarding not active"
    fi
    
    print_color $GREEN "🎉 System health check completed!"
}

# Function to show detailed pod information  
show_pod_details() {
    print_header "🔍 Detailed Pod Information"
    
    print_color $BLUE "Pod resource usage:"
    kubectl top pods -n $NAMESPACE 2>/dev/null || print_color $YELLOW "⚠️  Metrics server not available"
    
    echo ""
    print_color $BLUE "Pod descriptions (events and status):"
    kubectl describe pods -n $NAMESPACE | grep -E "(Name:|Status:|Ready:|Restarts:|Image:|Events:)" | head -20
    
    echo ""
    print_color $BLUE "Recent events:"
    kubectl get events -n $NAMESPACE --sort-by='.lastTimestamp' | tail -10
}

# Function to cleanup everything
cleanup() {
    print_header "🧹 Shutting Down and Cleaning Up"
    
    print_color $YELLOW "⚠️  This will completely remove the trading system!"
    print_color $YELLOW "   • Stops all port forwarding"
    print_color $YELLOW "   • Uninstalls Helm release '$HELM_RELEASE'"
    print_color $YELLOW "   • Deletes namespace '$NAMESPACE'"
    print_color $YELLOW "   • Removes all pods, services, and data"
    print_color $YELLOW ""
    print_color $YELLOW "Are you sure you want to proceed? [y] Yes, delete everything | [N] No, cancel"
    read -r confirm
    
    if [[ $confirm == "y" || $confirm == "Y" ]]; then
        print_color $YELLOW "Stopping port forwarding..."
        pkill -f "kubectl port-forward" 2>/dev/null || true
        
        print_color $YELLOW "Uninstalling Helm release..."
        helm uninstall $HELM_RELEASE -n $NAMESPACE 2>/dev/null || true
        
        print_color $YELLOW "Deleting namespace..."
        kubectl delete namespace $NAMESPACE 2>/dev/null || true
        
        print_color $GREEN "✅ Complete cleanup finished!"
    else
        print_color $CYAN "Cleanup cancelled. System remains running."
    fi
}

# Function to test failure scenarios
testing_mode() {
    print_header "🧪 Failure Testing Mode"
    
    print_color $CYAN "Available test scenarios:"
    print_color $CYAN "  1) Primary Redis goes down (redis-0)"
    print_color $CYAN "  2) Secondary Redis goes down (redis-1)" 
    print_color $CYAN "  3) Both Redis instances go down"
    print_color $CYAN "  4) Primary Market Data goes down (market-data-0)"
    print_color $CYAN "  5) Secondary Market Data goes down (market-data-1)"
    print_color $CYAN "  6) Both Market Data instances go down"
    print_color $CYAN "  7) Primary Streamlit goes down (streamlit-0)"
    print_color $CYAN "  8) Secondary Streamlit goes down (streamlit-1)"
    print_color $CYAN "  9) Both Streamlit instances go down"
    print_color $CYAN "  10) Primary Trade Manager goes down (trade-manager-0)"
    print_color $CYAN "  11) Secondary Trade Manager goes down (trade-manager-1)"
    print_color $CYAN "  12) Both Trade Manager instances go down"
    print_color $CYAN "  13) TOTAL SYSTEM FAILURE - Delete all pods"
    print_color $CYAN "  14) Shut down all PRIMARY pods (failover test)"
    print_color $CYAN "  r) Restore all services to full capacity"
    print_color $CYAN "  h) Run complete system health check"
    print_color $CYAN "  s) Show system status"
    print_color $CYAN "  b) Back to main menu"
    print_color $CYAN ""
    
    while true; do
        print_color $YELLOW "Choose test scenario:"
        print_color $YELLOW "  [1] Redis Primary | [2] Redis Secondary | [3] Both Redis | [4] Market Data Primary | [5] Market Data Secondary"
        print_color $YELLOW "  [6] Both Market Data | [7] Streamlit Primary | [8] Streamlit Secondary | [9] Both Streamlit"
        print_color $YELLOW "  [10] Trade Mgr Primary | [11] Trade Mgr Secondary | [12] Both Trade Mgr | [13] TOTAL FAILURE | [14] All PRIMARY pods down"
        print_color $YELLOW "  [r] Restore All | [h] Health Check | [s] Status | [b] Back to Main"
        print_color $BLUE "Enter choice:"
        read -r choice
        
        case $choice in
            1) simulate_pod_failure "redis" 0 ;;
            2) simulate_pod_failure "redis" 1 ;;
            3) simulate_service_failure "redis" ;;
            4) simulate_pod_failure "market-data" 0 ;;
            5) simulate_pod_failure "market-data" 1 ;;
            6) simulate_service_failure "market-data" ;;
            7) simulate_pod_failure "streamlit" 0 ;;
            8) simulate_pod_failure "streamlit" 1 ;;
            9) simulate_service_failure "streamlit" ;;
            10) simulate_pod_failure "trade-manager" 0 ;;
            11) simulate_pod_failure "trade-manager" 1 ;;
            12) simulate_service_failure "trade-manager" ;;
            13) simulate_total_system_failure ;;
            14) shutdown_primary_pods ;;
            r|R) restore_all_services ;;
            h|H) test_system_health ;;
            s|S) show_status ;;
            b|B) break ;;
            *) 
                print_color $RED "Invalid choice. Available options:"
                print_color $YELLOW "  Numbers 1-14 for failure scenarios, or r/h/s/b for other actions"
                ;;
        esac
        
        if [[ $choice != "b" && $choice != "B" && $choice != "s" && $choice != "S" && $choice != "h" && $choice != "H" ]]; then
            print_color $CYAN ""
            print_color $YELLOW "Test completed. Monitor system behavior and recovery."
            print_color $YELLOW "Press Enter to continue..."
            read
        fi
    done
}


# Interactive mode
interactive_mode() {
    print_header "🎮 Trading System - Operations Center"
    
    print_color $GREEN "🚀 Kubernetes Trading System Operations Center"
    print_color $CYAN ""
    print_color $CYAN "📱 Quick Access Commands:"
    print_color $CYAN "   kubectl get pods -n $NAMESPACE"
    print_color $CYAN "   kubectl logs -f market-data-0 -n $NAMESPACE"
    print_color $YELLOW "Available Commands:"
    print_color $YELLOW "  's' → Show system status (pods, services, health)"
    print_color $YELLOW "  'l' → Show recent logs from all services"
    print_color $YELLOW "  'p' → Show detailed pod information and events"
    print_color $YELLOW "  'r' → Start/restart port forwarding"
    print_color $YELLOW "  'h' → Run complete system health check"
    print_color $YELLOW "  't' → Enter testing mode for failure scenarios"
    print_color $YELLOW "  'c' → Cleanup and remove everything"
    print_color $YELLOW "  'q' → Quit (leave system running)"
    print_color $CYAN ""
    
    while true; do
        print_color $YELLOW "Operations Center Commands:"
        print_color $YELLOW "  [s] Show Status | [l] View Logs | [p] Pod Details | [r] Port Forward"
        print_color $YELLOW "  [h] Health Check | [t] Failure Testing | [c] Cleanup All | [q] Quit"
        print_color $BLUE "Enter command:"
        read -r input
        case $input in
            q|Q)
                print_color $GREEN "👋 Exiting operations center. System remains running."
                break
                ;;
            s|S) show_status ;;
            l|L) show_logs ;;
            r|R) restart_port_forwarding ;;
            p|P) show_pod_details ;;
            h|H) test_system_health ;;
            t|T) testing_mode ;;
            c|C) cleanup ;;
            *)
                print_color $RED "Invalid command. Available options:"
                print_color $YELLOW "  [s] Show Status | [l] View Logs | [p] Pod Details | [r] Port Forward"
                print_color $YELLOW "  [h] Health Check | [t] Failure Testing | [c] Cleanup All | [q] Quit"
                ;;
        esac
        echo ""
    done
}

# Helper function to simulate pod failure
simulate_pod_failure() {
    local service_name=$1
    local pod_index=$2
    local pod_name="${service_name}-${pod_index}"
    
    print_color $RED "💥 Simulating failure: Deleting pod $pod_name"
    
    if kubectl get pod "$pod_name" -n $NAMESPACE &> /dev/null; then
        kubectl delete pod "$pod_name" -n $NAMESPACE --grace-period=0
        print_color $YELLOW "⏳ Pod $pod_name deleted. StatefulSet will recreate it..."
        
        sleep 3
        
        print_color $BLUE "Current pod status for $service_name:"
        kubectl get pods -n $NAMESPACE -l app=$service_name
        
        print_color $CYAN "💡 Recovery will happen automatically:"
        print_color $CYAN "   🔄 Pod lifecycle: Terminating → Pending → ContainerCreating → Running → Ready"
        print_color $CYAN "   ⏱️  Typical recovery time: 30-90 seconds"
        print_color $CYAN "   📊 Monitor recovery with 'show status'"
    else
        print_color $RED "❌ Pod $pod_name not found"
    fi
}

# Helper function to simulate complete service failure
simulate_service_failure() {
    local service_name=$1
    
    print_color $RED "🔥 Simulating COMPLETE $service_name service failure"
    print_color $RED "   This will delete ALL pods for $service_name!"
    
    kubectl delete pods -l app=$service_name -n $NAMESPACE --grace-period=0
    
    print_color $YELLOW "⏳ All $service_name pods deleted. StatefulSet will recreate them..."
    sleep 3
    
    print_color $BLUE "Current status:"
    kubectl get pods -n $NAMESPACE -l app=$service_name
    
    print_color $CYAN "💡 Full service recovery in progress..."
}

# Function to simulate total system failure
simulate_total_system_failure() {
    print_color $RED " TOTAL SYSTEM FAILURE SIMULATION"
    print_color $RED "   This will delete ALL pods in the trading system!"
    print_color $YELLOW "Are you sure? This is destructive! (y/N)"
    read -r confirm
    
    if [[ $confirm == "y" || $confirm == "Y" ]]; then
        print_color $RED "Initiating total system failure..."
        kubectl delete pods --all -n $NAMESPACE --grace-period=0
        
        print_color $RED "TOTAL SYSTEM FAILURE COMPLETE!"
        print_color $YELLOW "⏳ All StatefulSets will attempt to recreate their pods..."
        print_color $CYAN "   Monitor recovery with 'show status' - this may take 2-5 minutes"
        
        sleep 5
        print_color $BLUE "Current system status after total failure:"
        kubectl get pods -n $NAMESPACE
    else
        print_color $CYAN "Total system failure cancelled."
    fi
}

# Function to restore all services
restore_all_services() {
    print_header "🔄 Restoring All Services"
    
    print_color $GREEN "Ensuring all StatefulSets are at full capacity..."
    
    # Get expected replica counts from values.yaml or set defaults
    kubectl scale statefulset redis --replicas=2 -n $NAMESPACE 2>/dev/null || true
    kubectl scale statefulset market-data --replicas=2 -n $NAMESPACE 2>/dev/null || true  
    kubectl scale statefulset streamlit --replicas=2 -n $NAMESPACE 2>/dev/null || true
    kubectl scale statefulset trade-manager --replicas=2 -n $NAMESPACE 2>/dev/null || true
    
    print_color $YELLOW "⏳ Waiting for all services to become ready..."
    
    # Wait for all pods to be ready (with timeout)
    kubectl wait --for=condition=Ready pod -l app=redis -n $NAMESPACE --timeout=180s 2>/dev/null || print_color $YELLOW "⚠️  Redis still starting..."
    kubectl wait --for=condition=Ready pod -l app=market-data -n $NAMESPACE --timeout=180s 2>/dev/null || print_color $YELLOW "⚠️  Market Data still starting..."
    kubectl wait --for=condition=Ready pod -l app=streamlit -n $NAMESPACE --timeout=180s 2>/dev/null || print_color $YELLOW "⚠️  Streamlit still starting..."
    kubectl wait --for=condition=Ready pod -l app=trade-manager -n $NAMESPACE --timeout=180s 2>/dev/null || print_color $YELLOW "⚠️  Trade Manager still starting..."
    
    print_color $GREEN "✅ Service restoration completed!"
    
    # Show final status
    show_status
}

# Interactive mode
interactive_mode() {
    print_header "🎮 Trading System - Operations Center"
    
    print_color $GREEN "🚀 Kubernetes Trading System Operations Center"
    print_color $CYAN ""
    print_color $YELLOW "Available Commands:"
    print_color $YELLOW "  's' → Show system status (pods, services, health)"
    print_color $YELLOW "  't' → Enter testing mode for failure scenarios"
    print_color $YELLOW "  'c' → Cleanup and remove everything"
    print_color $YELLOW "  'q' → Quit (leave system running)"
    print_color $CYAN ""
    
    while true; do
        print_color $YELLOW "Operations Center:"
        print_color $YELLOW "  [s] Show Status | [t] Failure Testing | [c] Cleanup All | [q] Quit"
        print_color $BLUE "Enter command:"
        read -r input
        case $input in
            q|Q)
                print_color $GREEN "👋 Exiting operations center. System remains running."
                break
                ;;
            s|S) show_status ;;
            t|T) testing_mode ;;
            c|C) cleanup ;;
            *)
                print_color $RED "Invalid command. Available options:"
                print_color $YELLOW "  [s] Show Status | [t] Failure Testing | [c] Cleanup All | [q] Quit"
                ;;
        esac
        echo ""
    done
}


# Function to shut down the primary pod of each StatefulSet
shutdown_primary_pods() {
    print_header "🛑 Shutting Down Primary Pods (Failover Test)"
    for svc in redis market-data streamlit trade-manager; do
        local pod_name="${svc}-0"
        print_color $RED "💥 Deleting primary pod: $pod_name"
        if kubectl get pod "$pod_name" -n $NAMESPACE &> /dev/null; then
            kubectl delete pod "$pod_name" -n $NAMESPACE --grace-period=0
            print_color $YELLOW "⏳ $pod_name deleted. StatefulSet will recreate it."
        else
            print_color $YELLOW "⚠️  $pod_name not found (may already be down)"
        fi
        echo ""
    done
    print_color $CYAN "💡 Monitor system status with './test.sh status' to observe failover and backup pods."
}

# Main function
main() {
    print_color $PURPLE "🎯 Trading System - Kubernetes Operations & Testing Script"
    print_color $PURPLE "=========================================================="
    
    # Check if system is running
    check_system_running
    
    # Handle command line arguments
    case "${1:-}" in
        "status") show_status ;;
        "test") testing_mode ;;
        "logs") show_logs ;;
        "cleanup") cleanup ;;
        "restart-ports") restart_port_forwarding ;;
        "pod-details") show_pod_details ;;
        "health") test_system_health ;;
        "primary-down") shutdown_primary_pods ;;
        "")
            # No arguments, enter interactive mode
            interactive_mode
            ;;
        *)
            print_color $RED "❌ Unknown command: $1"
            print_color $YELLOW ""
            print_color $YELLOW "Usage:"
            print_color $YELLOW "  ./test.sh                    # Interactive mode"
            print_color $YELLOW "  ./test.sh status             # Show system status"
            print_color $YELLOW "  ./test.sh test               # Run failure testing scenarios"
            print_color $YELLOW "  ./test.sh logs               # Show recent logs"
            print_color $YELLOW "  ./test.sh health             # Run system health check"
            print_color $YELLOW "  ./test.sh cleanup            # Clean shutdown and cleanup"
            print_color $YELLOW "  ./test.sh restart-ports      # Restart port forwarding"
            print_color $YELLOW "  ./test.sh pod-details        # Show detailed pod information"
            print_color $YELLOW "  ./test.sh primary-down       # Shut down all primary pods (failover test)"
            exit 1
            ;;
    esac
    
    print_color $GREEN "👋 Operations completed!"
}

# Handle Ctrl+C gracefully
trap 'print_color $YELLOW "\n⚠️  Operation interrupted. System is still running."' INT

# Run main function
main "$@"