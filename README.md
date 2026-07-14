# Data-Validation-Tool-TD-vs-BQ
Enterprise Data Migration &amp; Validation

The Data Validation Tool is an open sourced Python CLI tool based on the Ibis framework that compares data source tables with multi-leveled validation functions.

Data validation is a crucial step in a data warehouse, database, or data lake migration project where data from both the source and the target tables are compared to ensure they are matched and correct after each migration step (e.g. data and schema migration, SQL script translation, ETL migration, etc.). The Data Validation Tool (DVT) provides an automated and repeatable solution to perform this task.

DVT supports the following validations:

- Column validation (count check(*), sum, avg, min, max, stddev, group by)
- Row validation using Hash keys.
- Schema validation TD vs BQ
- Custom Query validation
- Ad hoc SQL exploration
