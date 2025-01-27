# Spark Shuffle Experience

A hands-on project to explore and analyze Spark join strategies, comparing SortMergeJoin vs ShuffleHashJoin implementations across different engines.

## Overview
This repository demonstrates join performance patterns using simulated transaction data, allowing you to:
- Test different join strategies
- Analyze performance by comparing:
  - Spark default (SortMergeJoin)
  - Comet plugin (ShuffleHashJoin)
  - Photon runtime (PhotonShuffledHashJoin)
- Understand data characteristics and performance implications

## Getting Started

1. Install Comet Plugin:

- Follow installation guide: https://datafusion.apache.org/comet/user-guide/installation.html
- Get version compatible with Spark 3.5.0 and Scala 2.12, download and put comet jar into `extra-jars` folder.

2. For Photon Tests:
- Requires access to Databricks workspace
- Permission to create and run jobs
- Workspace supporting Photon runtime and Spark 3.5 (> DBR 15.4 LTS)

3. Setup Spark Environment:

```base
make setup
```

This sets up a Spark environment with 1 master, 1 worker and 1 history server.

4. Prepare Data:

```bash

## Generate test data using Iceberg format
make prepare_dataset table_format=iceberg

## Generate test data using Delta format
make prepare_dataset table_format=delta

## By default, generates data in pure parquet format
make prepare_dataset table_format=parquet
```

## Shuffle Test Examples

Use the `make run ...` command to submit a spark job to test shuffle behavior.
The run command supports the following options:
- plugin: `none`, `comet`
- test_type: `join`, `aggregate`
- table_format: `delta`, `iceberg`, `parquet`

Example commands:

```bash
## Test join with default shuffle
make run plugin=none test_type=join table_format=delta

## Test join with Comet plugin
make run plugin=comet test_type=join table_format=iceberg

```

## Performance Analysis

Here's a performance comparison table across the three engines:

| Metric | Default Spark (SortMergeJoin) | Comet (CometHashJoin) | Photon (PhotonShuffleHashJoin) |
|--------|------------------------------|----------------------|------------------------------|
| Total Duration | 4.4m | 1.8m | 1.8m |
| Most Stage Duration | 3.2m | 54s | 44s |
| Most Shuffle Read | 1939.9 MiB | 1498.9 MiB | 1466.1 MiB |
| Most Shuffle Write | 1939.9 MiB | 1498.9 MiB | 1466.1 MiB |
| Most Spill (Memory) | 10.8 GiB | 0 | 0 |
| Most Spill (Disk) | 1634.3 MiB | 0 | 0 |
| Most Join Duration (Min/Med/Max) | 6.3m (15.6s/16.6s/2.1m) | 1.6m (7.6s/9.0s/13.5s) | 43.2s (1.1s/2.2s/4.1s) |

Detailed analysis and visualizations available in the [blog](https://huongvuong.substack.com/p/lessons-learned-sortmergejoin-vs)