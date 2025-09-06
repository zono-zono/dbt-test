# Staging Layer

The staging layer contains models that standardize and clean raw source data.

## Purpose
- Standardize column names and data types
- Apply basic transformations and calculations
- Clean and validate data
- Create consistent interfaces for downstream models

## Naming Convention
- Format: `stg_[source]__[entity]s.sql`
- Example: `stg_jaffle_shop__customers.sql`

## Materialization
- Default: `view` (for performance and freshness)
- Can be overridden to `table` for large datasets

## Organization
- Group by source system (e.g., `jaffle_shop/`, `stripe/`)
- Each source system gets its own subdirectory
