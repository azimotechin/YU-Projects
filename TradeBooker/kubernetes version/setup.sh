#!/bin/bash

# A script to forcefully clean up and reset the trading-system deployment.
# This will completely delete the namespace and all its contents.

set -e # Exit immediately if a command exits with a non-zero status.

NAMESPACE="trading-system"
RELEASE_NAME="trading-system"

echo "--- Step 1: Uninstalling Helm release '$RELEASE_NAME' ---"
# This might fail if the release doesn't exist, so we ignore errors with '|| true'
helm uninstall "$RELEASE_NAME" --namespace "$NAMESPACE" || true

echo "--- Step 2: Deleting namespace '$NAMESPACE' to remove all orphaned resources ---"
kubectl delete namespace "$NAMESPACE" --ignore-not-found=true

echo "--- Step 3: Waiting for namespace '$NAMESPACE' to be fully terminated ---"
# This loop ensures we don't continue until the namespace is actually gone.
while kubectl get namespace "$NAMESPACE" > /dev/null 2>&1; do
  echo "Namespace '$NAMESPACE' still terminating, waiting 5 seconds..."
  sleep 5
done
echo "Namespace '$NAMESPACE' has been deleted."

echo "--- Step 4: Re-creating namespace '$NAMESPACE' ---"
kubectl create namespace "$NAMESPACE"

helm install trading-system . --namespace trading-system

echo ""
echo "✅ Cleanup complete. The cluster is ready for a fresh install."
echo "Now, run the following commands:"
echo ""
echo "1. helm install trading-system . --namespace trading-system"
echo "2. helm upgrade trading-system . --namespace trading-system"
echo ""
echo "3. Check if pods are running:"
echo "   kubectl get pods -n trading-system"
echo ""
echo "4. Get the current external IP address:"
echo "   kubectl get service streamlit -n trading-system"
echo ""
echo "💡 The external IP may change after each deployment!"
echo "   Always check 'kubectl get services -n trading-system' for the current IP"