# Spark Shuffle Experience

A hands-on project to explore and analyze Spark shuffle behavior using NYC Taxi dataset, comparing different shuffle implementations.

## Overview
This repository demonstrates various Spark shuffle patterns using real-world NYC Yellow Taxi trip data, allowing you to:
- Test different shuffle operations (groupBy, join, repartition)
- Analyze shuffle performance by comparing:
    - Spark default shuffle
    - Comet shuffle plugin
    - Gluten shuffle plugin
- Understand data distribution patterns and performance implications

## Getting Started

1. Setup Spark Environment:

```base
make setup
```

Run setup command to setup spark environment including 1 spark master, 1 worker and 1 history server.

2. Download NYC Taxi Data:

```base
make download-data
```

You may need to fix the dataset schema before starting the analysis. Reason: the original parquet file having the 
inconsistent schema across files.

```base
make fix-schema
```


## Shuffle Test Examples

Use the `make run ...` command to submit a spark job to test shuffle behavior.
The run command supports the following options:
- plugin: `none`, `comet`, `gluten`
- test_type: `groupby`, `join`, `window`, `repartition`, `sort`

Example commands:

```bash
## Test groupby with default shuffle
make run plugin=none test_type=groupby

## Test join with Comet plugin
make run plugin=comet test_type=join

```

## Performance Analysis

The project includes tools to analyze shuffle performance:

- Memory usage monitoring
- Shuffle read/write metrics
- Task distribution visualization
- Network I/O statistics

Results are stored in the `metrics/` directory for comparison.

## Benchmark Results

Performance benchmarks comparing different shuffle implementations:

| Operation | Default | Comet | Gluten |
|-----------|---------|-------|--------|
| GroupBy   | ...     | ...   | ...    |
| Join      | ...     | ...   | ...    |
| Window    | ...     | ...   | ...    |

Detailed analysis and visualizations available in the `benchmarks/` directory.