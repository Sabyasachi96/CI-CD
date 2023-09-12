# Databricks notebook source
name: Manual workflow - Three Env
on: 
  workflow_dispatch:

 

jobs:
  DEV:
    runs-on: ubuntu-latest
    environment: DEV
    steps:
      - uses: actions/checkout@v2

 

      - name: Run a script
        run: |
          echo "I amrunning a job in the DEV environment"

 

  QA:
    runs-on: ubuntu-latest
    environment: QA
    needs: DEV
    steps:
      - uses: actions/checkout@v2

 

      - name: Run a script
        run: |
          echo "I amrunning a job in the QA environment"

 

  PROD:
    runs-on: ubuntu-latest
    environment: PROD
    needs: QA
    steps:
      - uses: actions/checkout@v2

 

      - name: Run a script
        run: |
          echo "I amrunning a job in the Prod environment"

# COMMAND ----------


