# coldctl

Cold data lifecycle tooling for operational databases.

`coldctl` helps you identify, archive, and query cold database data.

## Status

coldctl is currently under active development.

## Usage

```bash
coldctl analyze
```

Future releases will support archiving cold PostgreSQL data to
object storage using open formats such as Apache Parquet.

### Goals
- Analyze cold database data
- Archive data to object storage
- Verify archived data
- Query archived data
- Safely enforce retention policies

### License
Apache-2.0