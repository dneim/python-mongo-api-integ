# Automation Scripts for Canonical and Origin Field Mapping

## Overview

This project provides Python scripts to automate the process of auditing, mapping, and updating canonical and origin field data for various datasets. It integrates data from multiple sources, checks metadata in Elasticsearch, and generates SQL insert and update statements for database operations. The project also generates Excel audit reports for manual review.

The scripts are designed to support a robust ETL (Extract, Transform, Load) workflow with a focus on:

- Canonical field mapping
- Origin field mapping
- Elasticsearch metadata validation
- Automated generation of SQL statements
- Excel audit reporting

---

## Features

### Database Connection Pooling
- Uses `psycopg2.pool.SimpleConnectionPool` to manage PostgreSQL connections efficiently.
- Provides reusable `get_connection()` and `create_pool()` functions for connection handling.

### Data Collection
- `get_src_info(cursor, src_list, dl_type)`: Retrieves dataset source information.
- `get_field_info(cursor, fields, dl_type)`: Retrieves canonical field information.

### Mapping Audit and Proposed Field Handling
- `mapping_audit(cursor, tup_list)`: Audits each dataset-field combination and checks active status in the database.
- `append_proposed_fields(audit_data, field_mapping_definitions)`: Adds proposed long names and transformations for unmapped canonical fields.
- Integrates with `field_mapping_definitions` for predefined field transformations.

### Elasticsearch Metadata Validation
- `get_metadata_elastic_search(...)`: Queries OpenSearch/Elasticsearch to validate metadata for proposed fields.
- `elasticsearch_check_from_df(df, auth_url)`: Adds `es_Pass` and `Proposed Fields Long Name` columns to audit DataFrame.
- Supports dynamic resource handling based on download type and protocol.

### Transformation Handling
- `add_finalized_transformation(df)`: Generates finalized transformations for canonical fields based on ES metadata results.

### Excel Reporting
- `write_updated_audit_to_excel(headers, rows, file_path)`: Writes audit results to Excel with tables, formatting, and column sizing.

### SQL Statement Generators
- `canonical_inserts_from_df(df, conn, download_type)`: Generates canonical field INSERT statements.
- `origin_inserts_from_df(df, conn)`: Generates origin field INSERT statements.
- `canonical_updates_from_df(df, conn)`: Generates canonical field UPDATE statements.
- `origin_updates_from_df(df, conn)`: Generates origin field UPDATE statements.
- All update functions now include `updates_executed` boolean logic to provide feedback on whether any changes were applied.

---

## Main Execution (`main()`)

The `main()` function orchestrates the workflow:

1. Sets source list, download type, canonical fields, and output path.
2. Connects to the database via connection pool.
3. Retrieves source and field information.
4. Performs mapping audit and appends proposed field transformations.
5. Executes Elasticsearch metadata checks.
6. Adds finalized transformations.
7. Writes audit results to Excel and prompts user for review.
8. Generates SQL inserts and updates based on audit results:
    - Canonical inserts for unmapped fields with valid metadata.
    - Origin inserts for unmapped fields.
    - Canonical and origin updates for deactivated fields with valid metadata.

---

## Testing

Unit tests are implemented using `unittest` and `unittest.mock`. Key testing areas include:

- **Query construction**: Verifies SQL SELECT statements for correctness.
- **Canonical and Origin Inserts/Updates**: Ensures correct behavior when:
    - Database entries exist
    - Database entries do not exist
    - Transformations match or differ
- **Fetchone / Fetchall behavior**: Tests proper handling of `cursor.fetchone()` and `cursor.fetchall()` results.
- **Print outputs**: Validates informative feedback messages like `"No updates executed"` or `"Canonical Updates Created"`.

Tests mock database connections, cursors, and Elasticsearch API calls to isolate logic without touching production resources.

---

## Requirements

- Python 3.11+
- `pandas`
- `openpyxl`
- `requests`
- `psycopg2`
- PostgreSQL database

---

## Usage

1. Configure `db_creds.py` with database credentials.
2. Adjust `field_mapping_definitions` as needed.
3. Set sources, download type, and output paths in `main()`.
4. Run the script:

```bash
python main.py
```

5. Review the generated Excel audit spreadsheet before proceeding.
6. Confirm insert/update operations are executed by the script.

---

## Notes

- All database operations are committed at the end of each insert/update function.
- The scripts assume a standardized schema for `table_mapping`, `table_origin_field`, and related datasets.
- Elasticsearch/OpenSearch endpoint must be accessible with the provided `auth_url`.
- User interaction is required to confirm Excel audit review before inserts/updates are executed.

---

## License

This project is proprietary. Use and modification are restricted to internal purposes unless otherwise authorized.
