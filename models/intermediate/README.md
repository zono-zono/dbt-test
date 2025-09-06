# Intermediate Layer

The intermediate layer contains models that perform complex business logic and aggregations.

## Purpose
- Combine data from multiple staging models
- Perform complex calculations and transformations
- Create reusable business logic components
- Simplify marts layer models

## Naming Convention
- Format: `int_[entity]s_[action].sql`
- Example: `int_customers_aggregated.sql`

## Materialization
- Default: `ephemeral` (not materialized, embedded in downstream models)
- Can be overridden to `view` or `table` for performance

## Organization
- Group by business domain (e.g., `finance/`, `marketing/`)
- Each domain gets its own subdirectory
