#!/bin/bash
kubectl delete deployment pnr-resequencer-service-deployment
git pull
mvn clean package
docker build . -t gcr.io/sab-order-service-sbx-7006/pnr-resequencer-service
docker push gcr.io/sab-order-service-sbx-7006/pnr-resequencer-service:latest
kubectl apply -f deployment.yaml