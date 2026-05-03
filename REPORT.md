# Report

## 1. CDC Correctness

The CDC pipeline captures changes from PostgreSQL using Debezium and materializes them into Iceberg tables.

### Row count comparison

PostgreSQL source vs Iceberg silver tables:

📸 Screenshots:

* `scrn/cnts1.png`
* `scrn/cnts2.png`
### Delete propagation

Deletes in PostgreSQL are correctly reflected in the silver table.
Example:

```sql
SELECT * FROM lakehouse.cdc.silver_drivers WHERE id = 17;
```

This shows:

* `is_deleted = true`
* `last_op = 'd'`

📸 Screenshot:

* `scrn/cnts1.png`

### Idempotency

The pipeline is idempotent because:

* Bronze CDC is append-only.
* Silver CDC uses `ROW_NUMBER` to select the latest event per key.
* `MERGE INTO` ensures deterministic updates.

Running the pipeline multiple times without new CDC events produces the same silver state.



## 2. Lakehouse Design

### Bronze CDC

* Stores raw Debezium events
* Fields include:

  * `op`, `before`, `after`, `ts_ms`
  * Kafka metadata (offset, partition, timestamp)
* Append-only (no updates or deletes)

### Silver CDC

* Current-state table (one row per primary key)
* Built using:

  * Deduplication (`ROW_NUMBER OVER ts_ms DESC`)
  * `MERGE INTO` logic:

    * `d` → delete
    * `c`, `u`, `r` → insert/update

### Taxi Tables

* **bronze_taxi**: raw Kafka events
* **silver_taxi**: cleaned and enriched data
* **gold_taxi**: aggregated metrics (hourly, borough-level)


### Iceberg Snapshot History

```sql
SELECT * FROM lakehouse.cdc.silver_drivers.snapshots;
```

📸 Screenshot:

* `scrn/snapshots.png`



### Time Travel

use snapshot ids, reset the table to previous snapshot ID


## 3. Orchestration Design TODO

### DAG Structure

The Airflow DAG is designed as:

```
health_check → [bronze_cdc, bronze_taxi]
             → [silver_cdc, silver_taxi]
             → gold_taxi
             → validation
```



## 4. Streaming Pipeline (Taxi)

The taxi pipeline follows a medallion architecture:

* Bronze → raw Kafka events
* Silver → cleaned and enriched data
* Gold → aggregated metrics

### Gold Output

Example query:

```sql
SELECT *
FROM lakehouse.taxi.gold_hourly_zone
LIMIT 20;
```

📸 Screenshot:

* `scrn/gold_output.png`


### Row Counts

📸 Screenshot:

* `scrn/taxi_counts.png`



### Improvements over Project 2

1. **Broadcast join**

   * Reduced shuffle during joins
   * Improved performance

2. **Disabled Adaptive Query Execution**

   * Ensures deterministic execution plans
   * Makes debugging and performance analysis easier


## 5. Custom Scenario

The custom scenario (incremental MERGE-based gold table) is not finished..
Initial attempt used `MERGE INTO` for incremental updates. got errors and issues I did not manage to resolve

Final implementation uses:

```python
createOrReplace()
```

The intended design:

* Maintain historical records
* Use MERGE-based updates
* Support soft deletes and incremental refresh


