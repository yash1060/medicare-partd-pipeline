# Medicare Part D — Prescriber Intelligence Pipeline

A production-grade Data Engineering portfolio project processing 25M+ rows of real US pharma prescription data through a Medallion architecture pipeline.

## What It Does
Ingests CMS Medicare Part D public data from AWS S3, cleanses and transforms it through Bronze → Silver → Gold Delta Lake layers, and produces territory-level prescriber scorecards showing which doctors prescribe which drugs the most — the kind of intelligence pharma sales teams pay millions for.

## Architecture
## Stack
- **Compute**: Databricks on AWS
- **Storage**: AWS S3
- **Table Format**: Delta Lake
- **Transformation**: PySpark
- **CI/CD**: GitHub Actions
- **Testing**: pytest

## Key Numbers
- 25,231,862 rows ingested into Bronze
- 1,381,824 duplicates removed in Silver
- 23,850,038 clean rows in Gold with prescriber rankings
- Full pipeline runs in under 6 minutes
- 4 pytest unit tests — all passing ✅

## Pipeline Notebooks
| Notebook | Purpose |
|---|---|
| 01_data_profiling | Explore raw data, null analysis |
| 02_bronze_ingest | Load raw CSV to Delta via S3 |
| 03_silver_transform | Clean, cast types, deduplicate, DQ checks |
| 04_gold_scorecard | Aggregate and rank prescribers |
| 05_pipeline_runner | Run all 3 layers end to end |
| 06_optimise_tables | Delta time travel and file analysis |

## How to Run
1. Clone this repo
2. Set up AWS S3 bucket and IAM user with S3 access
3. Create a Databricks workspace and shared config notebook with credentials
4. Download CMS Part D data from data.cms.gov and upload to S3
5. Run notebooks 02 → 03 → 04 in order, or use 05_pipeline_runner

## Dataset
CMS Medicare Part D Prescribers by Provider and Drug — public data, free download from data.cms.gov
