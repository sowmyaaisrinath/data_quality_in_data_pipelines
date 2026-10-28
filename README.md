# CDP Data Quality Starter

A small, configuration-driven Python project for checking data quality in a
Customer Data Platform (CDP) pipeline. It runs locally against CSV, TSV, JSON,
or Parquet files with DuckDB and keeps metric history so it can detect both
sudden changes and slow degradation.

The included example models three common CDP entities:

- **Customers**: unified profiles, consent, and completeness
- **Identities**: email/device identifiers linked to profiles
- **Events**: behavioral activity and ingestion timestamps

## Quick start

Python 3.10 or newer is required.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
cdp-quality --config config/checks.yml
```

The first run reports `WARN` for historical checks because it is recording a
baseline. Run it again to see run-over-run comparisons:

```bash
cdp-quality --config config/checks.yml --run-id run-2
```

Write a report suitable for a pipeline artifact:

```bash
cdp-quality --config config/checks.yml --json reports/quality.json
```

The command exits with status `1` when any check has status `FAIL`.
Informational and warning checks do not fail the command. It exits with
status `2`, without running any checks, when the configuration is invalid (see
[Configuration validation](#configuration-validation)).

## Data quality check catalog

Every check below is configured with a `type:` value in YAML. Full parameters,
defaults, and edge cases for each type are in the
[Check reference](#check-reference).

### 1. Schema and field validity

| Check | `type` | Why it matters | Example |
| --- | --- | --- | --- |
| Required columns | [`required_columns`](#required_columns) | Detects upstream schema loss | `customer_id` and `updated_at` must exist |
| Data type | [`data_type`](#data_type) | Detects incompatible schema changes | `customer_id` must be `VARCHAR` |
| NULL rate | [`null_rate`](#null_rate) | Tracks completeness | At most 1% of customer IDs may be NULL |
| Uniqueness | [`unique`](#unique) | Protects entity keys | One row per `customer_id` |
| Accepted values | [`accepted_values`](#accepted_values) | Controls enumerations | Consent is `opted_in`, `opted_out`, or `unknown` |
| Regex format | [`regex`](#regex) | Validates structured strings | Email resembles `name@domain.tld` |
| Numeric/date range | [`range`](#range) | Detects impossible values | Completeness score is from 0 to 1 |
| SQL expression | [`sql_expression`](#sql_expression) | Enforces cross-field rules | Opted-out profiles must not retain an email |

### 2. CDP entity and relationship integrity

Some of these are patterns built from the general-purpose types above rather
than dedicated check types.

| Check | `type` | Why it matters | Example |
| --- | --- | --- | --- |
| Referential integrity | [`referential_integrity`](#referential_integrity) | Finds orphaned events/identities | Every event customer exists in profiles |
| Composite uniqueness | [`unique`](#unique) with `columns` | Prevents identity collisions | `(identity_type, identity_value)` is unique |
| Event ordering | [`sql_expression`](#sql_expression) | Detects broken timestamps | `ingested_at >= event_at` |
| Consent rules | [`sql_expression`](#sql_expression) | Prevents unsafe activation | Only opted-in, contactable profiles qualify |
| Audience rules | [`sql_expression`](#sql_expression) | Checks segment eligibility | Profile score meets campaign minimum |

### 3. Pipeline health

| Check | `type` | Why it matters | Example |
| --- | --- | --- | --- |
| Freshness | [`freshness`](#freshness) | Detects stale jobs | Latest profile update is under four hours old |
| Row count bounds | [`row_count`](#row_count) | Detects empty/explosive loads | Event count is between expected limits |
| Late arrival | [`late_arriving`](#late_arriving) | Measures event latency | Under 1% of events arrive over one hour late |
| Partition completeness | [`partition_completeness`](#partition_completeness) | Detects missing batches | At least the expected number of daily partitions are present |
| Duplicate ingestion | [`duplicate_ingestion`](#duplicate_ingestion) | Finds replayed records | No repeated `event_id` |

### 4. Changes over time

These checks compare the current run with metrics stored in
`.quality_history.duckdb` by earlier runs.

| Check | `type` | Detection method | Useful for |
| --- | --- | --- | --- |
| Run comparison | [`run_comparison`](#run_comparison) | Percentage change from the previous run | Sudden volume or NULL-rate jumps |
| Rolling baseline | [`rolling_baseline`](#rolling_baseline) | Deviation from the mean of the last N runs | Steady-state stability monitoring |
| Category drift | [`category_drift`](#category_drift) | Total variation distance from the previous distribution | Source/channel/country mix changes |
| Trend | [`trend`](#trend) | Linear slope across N runs | Slow completeness or uniqueness degradation |

Try the slow-degradation simulation:

```bash
python examples/simulate_degradation.py
```

## Configuration

`config/checks.yml` contains an example for every check family. A
configuration has two sections: `tables` and `checks`.

### Tables

Each entry maps a table name, used by checks, to a data file. Relative paths
are resolved from the configuration file's directory. Table names must be
plain SQL identifiers (letters, digits, and underscores, not starting with a
digit).

```yaml
tables:
  customers: ../examples/data/customers.csv
  events: /data/landing/events.parquet
```

| File extension | Read as |
| --- | --- |
| `.csv` | Comma-separated with a header row |
| `.tsv` | Tab-separated with a header row |
| `.parquet` | Parquet |
| `.json`, `.jsonl`, `.ndjson` | JSON or newline-delimited JSON |

Column types are inferred by DuckDB. In CSV and TSV files, empty fields are
read as NULL. In JSON and Parquet, an empty string `""` is a value, not NULL.

If a table file is missing or has an unsupported extension, the run stops with
a configuration error before any check executes.

### Common check fields

Every check accepts these fields. All other keys are type-specific parameters.

| Field | Required | Description |
| --- | --- | --- |
| `name` | Yes | Label shown in reports. Keep names unique. |
| `type` | Yes | One of the check types in the [Check reference](#check-reference). |
| `table` | Yes | A table name from the `tables` section. |
| `column` | Depends on type | Column to check, for single-column checks. |
| `severity` | No | `fail` (default), `warn`, or `information`. Status to report when the check does not pass. |

### Result statuses

| Status | When it is reported | Affects exit code |
| --- | --- | --- |
| `PASS` | The check's condition holds. | No |
| `FAIL` | The check did not pass and has `severity: fail`, **or** the check could not execute. | Yes |
| `WARN` | The check did not pass and has `severity: warn`, **or** a historical check is still building its baseline. | No |
| `INFORMATION` | The check did not pass and has `severity: information`. | No |

A check that passes validation but cannot execute against the data (for
example a column that does not exist in the file, or invalid SQL in an
`sql_expression`) is always reported as `FAIL` with the error in its message,
regardless of `severity`. The remaining checks still run.

### Configuration validation

Every check is validated when the configuration loads, before any data is
read. These mistakes are rejected:

- a missing `name`, `type`, or `table`
- an unknown `type`
- a `table` or `parent_table` that is not defined in `tables`
- a `severity` other than `fail`, `warn`, or `information`
- an unknown parameter for the check's type, such as a misspelling
- a missing required parameter, or a missing bound such as a `range` with
  neither `min` nor `max`
- a `trend` with a `bad_direction` other than `up` or `down`

All problems are reported together, with a suggestion when a name looks like a
typo, and the command exits with status `2`:

```text
Configuration error: Invalid configuration:
  - check #1 ('a'): unknown parameter 'max_rate' (did you mean 'max'?)
  - check #2 ('b'): requires at least one of 'min', 'max'
```

### Things to know before writing checks

- **Most row-level checks ignore NULLs.** `accepted_values`, `regex`, `range`,
  `referential_integrity`, and `late_arriving` skip NULL values. Pair them with
  a `null_rate` check when NULLs also matter.
- **Some checks have no tolerance.** `accepted_values`, `regex`, `range`, and
  `sql_expression` fail on a single bad row. Checks with a rate or count
  threshold are noted in the reference.
- **Rates are fractions**, not percentages: `0.05` means 5%.
- **Column and table names** must be plain SQL identifiers and are
  case-sensitive.

## Check reference

Each entry lists the check's parameters, when it passes, what appears in the
report's `observed` field, and which metric (if any) it saves to history.

### Schema and field validity

#### `required_columns`

Verifies that named columns exist in the table. Extra columns are allowed.

| Parameter | Required | Default | Description |
| --- | --- | --- | --- |
| `columns` | Yes | | List of column names that must exist. |

- **Passes when** every listed column exists. Names are case-sensitive.
- **Observed:** list of missing columns (empty when passing).
- **Metric saved:** none.

```yaml
- name: customer schema
  type: required_columns
  table: customers
  columns: [customer_id, email, consent_status, updated_at]
```

#### `data_type`

Verifies the DuckDB type of a column.

| Parameter | Required | Default | Description |
| --- | --- | --- | --- |
| `column` | Yes | | Column to inspect. |
| `allowed` | Yes | | List of accepted DuckDB type names, such as `VARCHAR`, `BIGINT`, `DOUBLE`, `DATE`, or `TIMESTAMP`. Case-insensitive. |

- **Passes when** the column's type is in `allowed`.
- **Observed:** the actual type name, or `UNKNOWN` when the table is empty
  (which fails the check).
- **Metric saved:** none.
- **Note:** for CSV, TSV, and JSON, types are inferred from the data. A column
  of whole numbers becomes `BIGINT`, and a column with one non-numeric value
  becomes `VARCHAR`. Allow every type your source can legitimately produce.

```yaml
- name: customer id type
  type: data_type
  table: customers
  column: customer_id
  allowed: [VARCHAR]
```

#### `null_rate`

Measures the share of rows where a column is NULL.

| Parameter | Required | Default | Description |
| --- | --- | --- | --- |
| `column` | Yes | | Column to measure. |
| `max` | No | `0` | Highest acceptable NULL fraction, from 0 to 1. |

- **Passes when** NULL rows / all rows ≤ `max`. An empty table has a rate of 0.
- **Observed:** the NULL rate.
- **Metric saved:** `null_rate:<column>`.

```yaml
- name: email completeness
  type: null_rate
  table: customers
  column: email
  max: 0.05
  severity: warn
```

#### `unique`

Counts duplicate rows for a single column or a combination of columns.

| Parameter | Required | Default | Description |
| --- | --- | --- | --- |
| `column` | One of `column` or `columns` | | Single key column. |
| `columns` | One of `column` or `columns` | | List of columns forming a composite key. Takes precedence over `column`. |
| `max_duplicates` | No | `0` | Highest acceptable number of duplicate rows. |

- **Passes when** the duplicate count ≤ `max_duplicates`. Duplicates are rows
  beyond the first for each key: three rows sharing a key count as two
  duplicates.
- **Observed:** the number of duplicate rows.
- **Metric saved:** `duplicate_count:<columns>`.
- **Note:** NULL keys are treated as equal, so two rows with a NULL key count
  as a duplicate. Add a `null_rate` check with `max: 0` on key columns.

```yaml
- name: identity values are unique by type
  type: unique
  table: identities
  columns: [identity_type, identity_value]
```

#### `accepted_values`

Verifies that a column contains only values from an allowed list.

| Parameter | Required | Default | Description |
| --- | --- | --- | --- |
| `column` | Yes | | Column to check. |
| `values` | Yes | | List of allowed values. |

- **Passes when** no non-NULL value falls outside `values`. Matching is exact
  and case-sensitive. NULLs are ignored.
- **Observed:** the number of rows with a value not in the list.
- **Metric saved:** none.
- **Note:** list values must match the column type. For a column inferred as
  `BIGINT`, use numbers (`[1, 2, 3]`), not strings.

```yaml
- name: valid consent status
  type: accepted_values
  table: customers
  column: consent_status
  values: [opted_in, opted_out, unknown]
```

#### `regex`

Verifies that values match a regular expression.

| Parameter | Required | Default | Description |
| --- | --- | --- | --- |
| `column` | Yes | | Column to check. Non-text values are converted to text first. |
| `pattern` | Yes | | Regular expression in RE2 syntax. |

- **Passes when** every non-NULL value matches. NULLs are ignored.
- **Observed:** the number of non-matching rows.
- **Metric saved:** none.
- **Note:** the pattern matches **anywhere** in the value. Without `^` and `$`,
  `[^@]+@[^@]+` also accepts `"see a@b.co for details"`. Anchor patterns when
  the whole value must match, and quote them with single quotes in YAML so
  backslashes are kept.

```yaml
- name: email format
  type: regex
  table: customers
  column: email
  pattern: '^[^@\s]+@[^@\s]+\.[^@\s]+$'
```

#### `range`

Verifies that values fall within inclusive bounds. Works for numbers, dates,
and timestamps.

| Parameter | Required | Default | Description |
| --- | --- | --- | --- |
| `column` | Yes | | Column to check. |
| `min` | At least one of `min` or `max` | No lower bound | Smallest allowed value (inclusive). |
| `max` | At least one of `min` or `max` | No upper bound | Largest allowed value (inclusive). |

- **Passes when** no non-NULL value is below `min` or above `max`. NULLs are
  ignored.
- **Observed:** the number of out-of-range rows.
- **Metric saved:** none.
- **Note:** for date or timestamp columns, write bounds as strings such as
  `"2020-01-01"`. A check with neither `min` nor `max` is rejected as a
  configuration error.

```yaml
- name: profile score range
  type: range
  table: customers
  column: profile_completeness
  min: 0
  max: 1
```

#### `sql_expression`

Evaluates a DuckDB SQL boolean expression for every row. Use it for
cross-field business rules that no other type covers.

| Parameter | Required | Default | Description |
| --- | --- | --- | --- |
| `expression` | Yes | | SQL expression that must be true for each row of `table`. |

- **Passes when** the expression is true for every row. Rows where the
  expression is false **or NULL** count as failures, so handle NULLs
  explicitly (for example `email IS NULL OR ...`).
- **Observed:** the number of failing rows.
- **Metric saved:** none.
- **Note:** the expression may reference other configured tables in
  subqueries, for example `customer_id IN (SELECT customer_id FROM customers)`.
  The expression is executed as written. Treat configuration as trusted code
  and never build it from untrusted input.

```yaml
- name: consent business rule
  type: sql_expression
  table: customers
  expression: "consent_status <> 'opted_out' OR email IS NULL"
```

### Relationship integrity

#### `referential_integrity`

Finds child rows whose key has no matching row in a parent table.

| Parameter | Required | Default | Description |
| --- | --- | --- | --- |
| `column` | Yes | | Foreign-key column in `table` (the child). |
| `parent_table` | Yes | | Table that must contain the key. |
| `parent_column` | Yes | | Key column in the parent table. |
| `max_orphan_rate` | No | `0` | Highest acceptable fraction of orphaned child rows. |

- **Passes when** orphan rows / all child rows ≤ `max_orphan_rate`. Child rows
  with a NULL key are not orphans but are included in the denominator.
- **Observed:** the orphan rate.
- **Metric saved:** `orphan_rate:<column>`.

```yaml
- name: events resolve to customers
  type: referential_integrity
  table: events
  column: customer_id
  parent_table: customers
  parent_column: customer_id
  max_orphan_rate: 0.01
```

### Pipeline health

#### `freshness`

Measures how old the newest record is.

| Parameter | Required | Default | Description |
| --- | --- | --- | --- |
| `column` | Yes | | `TIMESTAMP` or `DATE` column holding the record time. |
| `max_age_hours` | Yes | | Highest acceptable age of the newest record, in hours. |
| `reference_time` | No | Current time | Time to measure age against, such as `"2026-10-27 16:00:00"`. Useful for deterministic tests and backfills. |

- **Passes when** `reference_time` minus the latest value in `column` ≤
  `max_age_hours`. An empty table or an all-NULL column fails.
- **Observed:** age of the newest record, in hours.
- **Metric saved:** `freshness_hours:<column>`.
- **Notes:**
  - Timestamps without a time zone are compared with the current time in the
    local time zone of the machine running the check. Run the checker in UTC,
    or store timezone-aware timestamps, to avoid an offset of several hours.
  - Records dated in the future produce a negative age and pass. Use a `range`
    or `sql_expression` check to catch future timestamps.
  - A text column that wasn't inferred as a timestamp reports a check error.

```yaml
- name: customer data freshness
  type: freshness
  table: customers
  column: updated_at
  max_age_hours: 4
```

#### `row_count`

Verifies the table's row count is within inclusive bounds.

| Parameter | Required | Default | Description |
| --- | --- | --- | --- |
| `min` | No | `0` | Smallest acceptable row count. |
| `max` | No | No limit | Largest acceptable row count. |

- **Passes when** `min` ≤ row count ≤ `max`.
- **Observed:** the row count.
- **Metric saved:** `row_count`.

```yaml
- name: event volume bounds
  type: row_count
  table: events
  min: 5
  max: 1000000
```

#### `late_arriving`

Measures the share of rows ingested long after the event happened.

| Parameter | Required | Default | Description |
| --- | --- | --- | --- |
| `event_time_column` | Yes | | Column holding when the event occurred. |
| `ingestion_time_column` | Yes | | Column holding when the row was ingested. |
| `max_lag_hours` | Yes | | A row is late when ingestion time minus event time exceeds this many hours. |
| `max_late_rate` | No | `0` | Highest acceptable fraction of late rows. |

- **Passes when** late rows / all rows ≤ `max_late_rate`.
- **Observed:** the late-row rate.
- **Metric saved:** `late_arrival_rate`.
- **Notes:** rows with a NULL in either timestamp are not counted as late but
  are included in the denominator. Rows ingested *before* their event time are
  not flagged; use an `sql_expression` such as `ingested_at >= event_at`.

```yaml
- name: event arrival lag
  type: late_arriving
  table: events
  event_time_column: event_at
  ingestion_time_column: ingested_at
  max_lag_hours: 1
  max_late_rate: 0.01
```

#### `partition_completeness`

Counts the distinct calendar dates present in a timestamp or date column.

| Parameter | Required | Default | Description |
| --- | --- | --- | --- |
| `column` | Yes | | Timestamp or date column. Timestamps are truncated to their date. |
| `expected_partitions` | Yes | | Minimum number of distinct dates. |

- **Passes when** the number of distinct dates ≥ `expected_partitions`.
- **Observed:** the number of distinct dates.
- **Metric saved:** `partition_count:<column>`.
- **Note:** this checks how many dates are present, not *which* dates. Data
  for October 1, 5, and 9 passes `expected_partitions: 3` despite the gaps. To
  require specific consecutive dates, combine it with `range` bounds on the
  same column.

```yaml
- name: daily event partitions
  type: partition_completeness
  table: events
  column: event_at
  expected_partitions: 2
```

#### `duplicate_ingestion`

Finds records that were loaded more than once, such as replayed events. It
behaves exactly like [`unique`](#unique) and accepts the same parameters
(`column` or `columns`, and `max_duplicates`). Use it on record identifiers to
make the intent clear in reports.

- **Observed:** the number of duplicate rows.
- **Metric saved:** `duplicate_count:<columns>`.

```yaml
- name: duplicate event ingestion
  type: duplicate_ingestion
  table: events
  columns: [event_id]
  max_duplicates: 0
```

### Changes over time

Historical checks compare the current run with metrics saved by earlier runs
in the history database (see [Metric history](#metric-history)).

`run_comparison`, `rolling_baseline`, and `trend` take a `metric` parameter
with one of these values:

| `metric` value | Measures |
| --- | --- |
| `row_count` | Number of rows in the table |
| `null_rate:<column>` | Fraction of rows where the column is NULL |
| `distinct_count:<column>` | Number of distinct non-NULL values in the column |

While there is not enough history, these checks report `WARN` with a
"baseline" message regardless of `severity`, and save the current value so the
next run can compare against it. History from the current `--run-id` is never
used as a baseline.

#### `run_comparison`

Compares a metric with its value from the previous run.

| Parameter | Required | Default | Description |
| --- | --- | --- | --- |
| `metric` | Yes | | Metric to compare (see table above). |
| `max_change_pct` | Yes | | Highest acceptable change as a fraction of the previous value. `0.30` allows ±30%. |

- **Passes when** |current − previous| / |previous| ≤ `max_change_pct`.
  Increases and decreases are treated the same. If the previous value was 0
  and the current value is not, the change is infinite and the check fails.
- **Observed:** the signed change, for example `-0.42` for a 42% drop.
- **Baseline:** needs one previous run.

```yaml
- name: event count run over run
  type: run_comparison
  table: events
  metric: row_count
  max_change_pct: 0.30
```

#### `rolling_baseline`

Compares a metric with the average of recent runs, which is less sensitive to
a single unusual previous run than `run_comparison`.

| Parameter | Required | Default | Description |
| --- | --- | --- | --- |
| `metric` | Yes | | Metric to compare (see table above). |
| `max_deviation_pct` | Yes | | Highest acceptable deviation from the baseline, as a fraction of the baseline. |
| `window` | No | `7` | Number of most recent previous values to average. |
| `min_history` | No | `3` | Previous values required before the check is evaluated. |

- **Passes when** |current − mean| / |mean| ≤ `max_deviation_pct`, where
  `mean` is the plain average of up to `window` previous values.
- **Observed:** the deviation fraction.
- **Notes:**
  - If the baseline mean is 0, the deviation is treated as 0 and the check
    always passes. For metrics that are normally 0, such as the NULL rate of a
    key column, use a fixed `null_rate` threshold instead.
  - The baseline does not account for weekly or seasonal patterns, so set the
    tolerance wide enough to absorb normal weekday/weekend swings.

```yaml
- name: rolling customer volume
  type: rolling_baseline
  table: customers
  metric: row_count
  window: 7
  min_history: 3
  max_deviation_pct: 0.20
```

#### `category_drift`

Detects a change in how rows are distributed across a column's values, such as
the mix of event sources or countries.

| Parameter | Required | Default | Description |
| --- | --- | --- | --- |
| `column` | Yes | | Categorical column to profile. |
| `max_total_variation` | Yes | | Highest acceptable total variation distance, from 0 (identical) to 1 (no overlap). |

- **Passes when** the total variation distance between the current
  distribution and the previous run's distribution ≤ `max_total_variation`.
  Total variation distance is half the sum of absolute differences in each
  category's share. It equals the fraction of rows that would have to change
  category to make the distributions match, so `0.15` means "up to 15% of the
  mix may shift".
- **Observed:** the total variation distance.
- **Baseline:** needs one previous run. Compares only with the most recent
  previous run, not an average.
- **Metric saved:** one value per category, named
  `distribution:<column>:<value>`. NULL is recorded as the category `<NULL>`.
- **Note:** use only low-cardinality columns. Profiling an ID or email column
  saves one history row per distinct value on every run.

```yaml
- name: event source distribution drift
  type: category_drift
  table: events
  column: source
  max_total_variation: 0.15
  severity: warn
```

#### `trend`

Detects slow, steady degradation that never trips a fixed threshold in a
single run, by fitting a straight line through recent values.

| Parameter | Required | Default | Description |
| --- | --- | --- | --- |
| `metric` | Yes | | Metric to track (see table above). |
| `max_bad_slope_pct` | Yes | | Highest acceptable slope in the bad direction, per run, as a fraction of the window's mean. |
| `bad_direction` | No | `down` | `down` when decreasing values are bad (such as row counts), `up` when increasing values are bad (such as NULL rates). |
| `window` | No | `7` | Number of values to fit, **including** the current run. |
| `min_history` | No | `4` | Values required, **including** the current run, before the check is evaluated. |

- **Passes when** the least-squares slope divided by the window's mean, in the
  bad direction, ≤ `max_bad_slope_pct`. Improvement in the good direction
  always passes. If the window's mean is 0 the slope is treated as 0.
- **Observed:** the signed slope per run as a fraction of the mean.

```yaml
- name: slowly degrading email completeness
  type: trend
  table: customers
  metric: "null_rate:email"
  window: 7
  min_history: 4
  bad_direction: up
  max_bad_slope_pct: 0.10
  severity: warn
```

Here, an upward NULL-rate slope greater than 10% of the rolling mean per run
warns even if each individual run still passes its fixed NULL threshold.

## Metric history

Checks that save a metric write it to the `quality_metrics` table in the
history database (`--history`, default `.quality_history.duckdb`):

| Column | Description |
| --- | --- |
| `run_id` | The run's `--run-id` |
| `recorded_at` | UTC time the value was saved |
| `table_name` | Table the metric describes |
| `metric_name` | For example `row_count` or `null_rate:email` |
| `value` | Metric value |

History holds one value per run, table, and metric name. Checks that produce
the same metric for the same table, such as a `null_rate` check on `email` and
a `trend` on `null_rate:email`, share that value, so `window` and
`min_history` always count runs. Re-running with the same `--run-id`, for
example when an orchestrator retries a task, replaces that run's values
instead of adding new ones.

Metrics such as `orphan_rate`, `freshness_hours`, and `late_arrival_rate` are
saved for later analysis but cannot be used as a historical check's `metric`.

## Reports and command-line options

| Option | Default | Description |
| --- | --- | --- |
| `--config` | `config/checks.yml` | YAML check configuration |
| `--history` | `.quality_history.duckdb` | DuckDB metric history file |
| `--run-id` | Current UTC timestamp | Unique identifier for this run |
| `--json` | Not written | Path to write a JSON report |

The console prints one line per check with its status, name, table, and a
message, followed by a summary count per status.

The JSON report has this shape:

```json
{
  "run_id": "run-2",
  "summary": {"pass": 21, "warn": 2},
  "results": [
    {
      "name": "email completeness",
      "check_type": "null_rate",
      "table": "customers",
      "status": "pass",
      "observed": 0.2,
      "expected": "null rate <= 0.25",
      "message": "20.00% NULL",
      "metric_name": "null_rate:email",
      "metric_value": 0.2
    }
  ]
}
```

`summary` includes only statuses that occurred. `metric_name` and
`metric_value` are `null` for checks that don't save a metric.

## Using it in a pipeline

Use a durable path for `--history`, keyed to the same environment and dataset,
and pass the orchestrator's run ID:

```bash
cdp-quality \
  --config config/checks.yml \
  --history /pipeline-state/production-quality.duckdb \
  --run-id "$PIPELINE_RUN_ID" \
  --json reports/quality.json
```

For parallel distributed jobs, place metric history in a central warehouse
instead of a local DuckDB file. `MetricStore` is deliberately small so it can
be replaced with an adapter for Snowflake, BigQuery, Databricks, or Postgres.

## Development

```bash
pip install -e ".[dev]"
ruff check .
pytest
```

Main modules:

- `config.py`: YAML parsing
- `models.py`: check specification, result, and status types
- `checks.py`: validation and historical algorithms
- `store.py`: DuckDB metric history
- `runner.py`: source registration and check execution
- `cli.py`: console and JSON reporting

## Publish to GitHub

After creating an empty repository on GitHub:

```bash
git init
git add .
git commit -m "Initial CDP data quality starter"
git branch -M main
git remote add origin https://github.com/YOUR_USER/YOUR_REPOSITORY.git
git push -u origin main
```

## License

MIT
