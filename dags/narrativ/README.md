# Narrativ

## Data Overview

### Description

Narrativ is a data partner providing affiliate link insights. Affiliate links are links from Conde sites to third party stores and Conde receives a commission
from the sale.

### Data We receive

We receive daily `.csv` reports from narrativ into `s3://cn-data-vendor/affiliate/narrativ/...`. This includes a `clicks` and `orders` report.

More data on the data we receive here: https://drive.google.com/file/d/1HLalCzRZTjsNF13u_RcEuqvfr3mRUGqB/view

## About This Dag

Our goal is to ingest the data daily, soon after it hits. Data is received around `23:00:00 UTC` to `24:00:00 UTC` every day. This dag is scheduled to run
with `@Daily` which airflow will run at midnight, shortly after data is received.

A Databricks notebook will be called for each report type. These notebooks utilize
https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html to determine what files have been added since the last run. This takes care of
most of the headache of backfilling as well as determining what new files to process.

Separate autoloaders, notebooks and tasks are used, one for each report type. This is for a few reasons:

- each report uses a separate schema, and a single autoloader can only be provided a one schema
- if there is a failure in processing one report, it shouldn't have to affect processing another.