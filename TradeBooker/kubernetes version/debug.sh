#!/bin/bash

# Build new image with updated tag from the app directory

cd app
docker buildx build --platform linux/amd64,linux/arm64 -t shacharyk/trading-system:v3.2.9 --push .
cd ..

# Upgrade with new image
helm upgrade trading-system . --namespace trading-system

# Force restart all StatefulSets to pull new image
kubectl rollout restart statefulset/streamlit -n trading-system
kubectl rollout restart statefulset/trade-manager -n trading-system
kubectl rollout restart statefulset/market-data -n trading-system

# Watch pods restart with new image
kubectl get pods -n trading-system

kubectl logs streamlit-0 -n trading-system

kubectl get services -n trading-system