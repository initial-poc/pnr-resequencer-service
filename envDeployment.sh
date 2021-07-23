#!/bin/bash
git pull
mvn clean package
docker build . -t gcr.io/sab-ors-poc-sbx-01-9096/pnr-resequencer-service
docker push gcr.io/sab-ors-poc-sbx-01-9096/pnr-resequencer-service:latest
kubectl apply -f deployment.yaml