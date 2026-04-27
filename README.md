# Smart JSON Bronze Ingestion (Databricks)

## Problem
Spark performance depends on JSON structure:
- Nested array → poor parallelism
- Array of objects → efficient processing

## Solution
This project builds a smart ingestion utility that:
- Detects JSON structure
- Routes processing automatically
- Outputs a clean Spark DataFrame

## Features
- Schema detection
- Smart flattening using Spark explode
- Pandas fallback
- Bronze layer ingestion (Delta)

## Tech Stack
- PySpark
- Delta Lake
- Databricks Community Edition
