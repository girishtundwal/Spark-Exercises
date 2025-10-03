# PySpark Tutorial - Comprehensive Guide

## Overview

This repository contains comprehensive PySpark tutorials covering data ingestion, transformation, analysis, and writing operations. The materials are designed for both beginners and intermediate users working with Apache Spark and Databricks.

## Contents

### 1. Tutorial Notebook (`1_Tutorial.ipynb`)
Core PySpark concepts and operations demonstrated on the BigMart Sales dataset.

### 2. Exercise Notebook (`Spark_Exercise.ipynb`)
Practical exercises using e-commerce order and seller datasets.

## Prerequisites

- Apache Spark 3.x or Databricks Runtime
- Python 3.7+
- Basic understanding of Python and SQL
- Datasets:
  - `BigMart_Sales.csv`
  - `order_items_dataset.csv`
  - `sellers_dataset.csv`
  - `orders_json_data.json`
  - `drivers.json`

## Topics Covered

### Data Reading
- **CSV Files**: With and without schema inference
- **JSON Files**: Nested and multiline JSON
- **Schema Definition**: 
  - DDL string format
  - StructType/StructField programmatic definition
- **File System Operations**: Using `dbutils.fs`

### Data Transformations

#### Basic Operations
- `select()` - Column selection with aliasing
- `filter()` / `where()` - Conditional filtering
- `withColumn()` - Adding/modifying columns
- `withColumnRenamed()` - Renaming columns
- `drop()` - Removing columns
- `dropDuplicates()` / `distinct()` - Removing duplicates
- `sort()` / `orderBy()` - Sorting data
- `limit()` - Limiting result sets

#### Advanced Operations
- **String Functions**: `upper()`, `lower()`, `initcap()`, `regexp_replace()`
- **Date Functions**: 
  - `current_date()`
  - `date_add()` / `date_sub()`
  - `datediff()`
  - `date_format()`
  - `year()` / `month()`
- **Type Casting**: Converting data types
- **Null Handling**:
  - `dropna()` - Dropping null values
  - `fillna()` - Filling null values

#### Array and Struct Operations
- `split()` - Splitting strings into arrays
- Array indexing
- `explode()` - Expanding arrays into rows
- `array_contains()` - Checking array membership
- Working with nested JSON structures

### Aggregations and Analytics

#### GroupBy Operations
- Single and multiple column grouping
- Aggregate functions: `sum()`, `avg()`, `min()`, `max()`, `count()`
- `collect_list()` - Collecting values into arrays
- `pivot()` - Pivoting data

#### Window Functions
- `row_number()` - Sequential numbering
- `rank()` / `dense_rank()` - Ranking with gaps/without gaps
- Running aggregations (cumulative sums)
- `Window.partitionBy()` and `Window.orderBy()`
- Frame specifications: `rowsBetween()`, `unboundedPreceding`, `unboundedFollowing`

### Conditional Logic
- `when()` / `otherwise()` - SQL-like CASE statements
- Multiple condition chaining

### Joins
- **Join Types**: `inner`, `left`, `right`, `anti`
- Column-based joins
- Broadcast joins for optimization
- Working with alias names to avoid ambiguity

### Advanced Features

#### User Defined Functions (UDF)
```python
# Define function
def my_func(x):
    return x * x

# Register as UDF
my_udf = udf(my_func)

# Apply to DataFrame
df.withColumn('new_col', my_udf('existing_col'))
```

#### Union Operations
- `union()` - Position-based union
- `unionByName()` - Name-based union

#### Accumulator Variables
- Distributed counters for aggregations
- Driver-only access pattern

### Spark SQL
- Creating temporary views: `createTempView()`
- Writing SQL queries: `spark.sql()`
- Mixing SQL and DataFrame API

### Data Writing

#### Output Formats
- CSV with headers and delimiters
- Parquet (columnar format)
- Writing to Hive tables

#### Write Modes
- `overwrite` - Replace existing data
- `append` - Add to existing data
- `error` - Fail if data exists (default)
- `ignore` - Skip if data exists

#### Partitioning
- `partitionBy()` - Creating partitioned datasets
- `coalesce()` - Controlling output file count

### Performance Optimization

#### Partitioning
- Checking default parallelism
- `repartition()` - Increasing partitions
- `coalesce()` - Reducing partitions efficiently
- Partition size configuration

#### Join Optimization
- Broadcast joins for small dimension tables
- Using `broadcast()` hint

## Key Code Examples

### Reading with Explicit Schema
```python
from pyspark.sql.types import *

schema = StructType([
    StructField("Product", StringType(), True),
    StructField("ID", IntegerType(), True),
    StructField("Date", DateType(), True),
    StructField("Price", FloatType(), True)
])

df = spark.read.format('csv') \
    .schema(schema) \
    .option('header', 'true') \
    .load('/path/to/file.csv')
```

### Complex Filtering
```python
df.filter(
    (col('price') < 50) & 
    (col('freight_value') < 10) & 
    (col('year').isin([2017, 2018]))
).show()
```

### Window Function with Running Sum
```python
from pyspark.sql.window import Window

windowSpec = Window.partitionBy('year') \
    .orderBy('shipping_limit_datetime') \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

df.withColumn('running_sum', sum('price').over(windowSpec))
```

### Exploding Nested JSON
```python
# Explode array field
df_exploded = df.select(
    "user_id",
    explode(col("purchases")).alias("purchase")
)

# Access nested fields
df_final = df_exploded.select(
    "user_id",
    col("purchase.order_id").alias("order_id"),
    explode(col("purchase.items")).alias("item")
)
```

## Best Practices

1. **Schema Definition**: Always define schemas explicitly for production code to avoid inference overhead
2. **Partition Management**: Monitor and optimize partition counts based on data size
3. **Broadcast Joins**: Use for small lookup tables (< 10MB)
4. **Column Pruning**: Select only required columns early in transformation chain
5. **Predicate Pushdown**: Filter data as early as possible
6. **Caching**: Use `cache()` or `persist()` for frequently accessed DataFrames
7. **Avoid UDFs**: Use built-in functions when possible for better performance
8. **Write Partitioning**: Partition output data by commonly filtered columns

## Common Patterns

### Data Quality Checks
```python
# Check for nulls
df.filter(col('column_name').isNull()).count()

# Check duplicates
df.groupBy('key_column').count().filter(col('count') > 1)
```

### Incremental Processing
```python
# Read with partition filter
df = spark.read.parquet('/path/to/data') \
    .filter(col('date') >= '2024-01-01')
```

### Handling Multiple File Formats
```python
# Read multiple CSV files
df = spark.read.csv('/path/to/files/*.csv')

# Read with date partition in path
df = spark.read.parquet('/data/year=2024/month=01/*')
```

## Troubleshooting

### Common Issues

**Out of Memory Errors**
- Reduce partition size
- Increase executor memory
- Use `coalesce()` before writing

**Slow Joins**
- Check data skew
- Use broadcast joins for small tables
- Repartition on join keys

**Schema Mismatch**
- Verify schema compatibility when using `union()`
- Use `unionByName()` for different column orders



## Contributing

Feel free to submit issues or pull requests to improve these tutorials.

## License

This tutorial is provided for educational purposes.