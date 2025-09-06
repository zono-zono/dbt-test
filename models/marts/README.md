# Marts Layer

The marts layer contains final, business-ready models for end users.

## Purpose
- Create final data marts for specific business use cases
- Provide clean, documented datasets for analytics and reporting
- Optimize for query performance and user experience
- Serve as the interface between data team and business users

## Naming Convention
- Use descriptive business names
- Example: `customers.sql`, `daily_sales.sql`

## Materialization
- Default: `table` (for performance and reliability)
- Can use `incremental` for large, frequently updated datasets

## Organization
- Group by business function (e.g., `marketing/`, `sales/`, `finance/`)
- Each function gets its own subdirectory
