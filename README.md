# ⚡ Data Engine Benchmark

A high-performance benchmark suite comparing modern data processing engines and query engines on analytical **TPC-H** workloads against columnar **Parquet** datasets.

This project evaluates and compares query execution paradigms across **Rust** and **Python** ecosystems:
- 🦀 **Polars (Rust)** — Native multi-threaded DataFrame engine with lazy query planning and vectorization.
- 🏹 **Apache Arrow DataFusion (Rust)** — Extensible SQL query engine and in-memory execution framework built natively on Apache Arrow.
- 🐼 **Pandas (Python)** — Standard vectorized in-memory data manipulation library.
- ⚡ **Apache PySpark (Python / JVM)** — Distributed DataFrame and SQL query engine evaluated in local multi-threaded mode.

---

## 📑 Table of Contents

- [Overview](#-overview)
- [Engines Compared](#-engines-compared)
- [Project Structure](#-project-structure)
- [Dataset Specifications](#-dataset-specifications)
- [Benchmark Queries (TPC-H)](#-benchmark-queries-tpc-h)
- [Standardized Output Format](#-standardized-output-format)
- [Prerequisites](#-prerequisites)
- [Getting Started & Usage](#-getting-started--usage)
  - [1. Running Rust Benchmarks (Polars & DataFusion)](#1-running-rust-benchmarks-polars--datafusion)
  - [2. Running Python Benchmarks (Pandas & PySpark)](#2-running-python-benchmarks-pandas--pyspark)
- [Benchmark Results & Output Verification](#-benchmark-results--output-verification)

---

## 🔭 Overview

Analytical data engines differ significantly in memory model, execution planning (e.g., vectorized physical operators, lazy execution, expression pushdowns), and execution language runtime overhead. 

This repository provides an apples-to-apples performance and correctness comparison across engines running 5 complex analytical queries involving multi-table joins, columnar scans, filter/projection pushdowns, aggregations, and sorting.

---

## 🏎 Engines Compared

| Engine | Language / Runtime | Execution Paradigm | Primary Query Interface |
| :--- | :--- | :--- | :--- |
| **Polars** | Rust | Multi-threaded Vectorized LazyFrame | Rust Lazy DSL / Expressions |
| **DataFusion** | Rust (Apache Arrow) | Vectorized Volcano / Pipeline Engine | ANSI SQL / Arrow RecordBatches |
| **Pandas** | Python (C-Extensions) | In-memory Eager DataFrame | Python DataFrame API |
| **PySpark** | Python / JVM | Catalyst Optimizer + Tungsten Engine | PySpark DataFrame API / SQL |

---

## 📁 Project Structure

```plaintext
data-engine-benchmark/
├── Cargo.toml                  # Rust dependencies (polars, datafusion, tokio, csv)
├── src/
│   ├── main.rs                 # Rust benchmark orchestrator
│   ├── polars.rs               # Polars LazyFrame implementation (Q1–Q5)
│   └── datafusion.rs           # Apache DataFusion SQL implementation (Q1–Q5)
├── benchmark/
│   ├── pandas_benchmark.py     # Pandas benchmark runner & query definitions
│   └── pyspark_benchmark.py    # PySpark benchmark runner & session configuration
├── data/                       # TPC-H dataset stored as columnar Parquet
│   ├── customer.parquet
│   ├── lineitem.parquet
│   ├── nation.parquet
│   ├── orders.parquet
│   ├── part.parquet
│   ├── partsupp.parquet
│   ├── region.parquet
│   └── supplier.parquet
└── results/                    # Standardized CSV benchmark query outputs
    ├── datafusion.csv
    ├── pandas.csv
    ├── polars.csv
    └── pyspark.csv
```

---

## 💾 Dataset Specifications

The benchmark uses standard TPC-H relational schema tables stored in columnar `.parquet` format within the `data/` directory:

| Table | Approximate Size | Description | Key Attributes |
| :--- | :--- | :--- | :--- |
| `lineitem.parquet` | ~149 MB (~6M rows) | Core transaction line item records | `l_orderkey`, `l_suppkey`, `l_extendedprice`, `l_discount` |
| `orders.parquet` | ~36.5 MB (~1.5M rows) | Customer orders master records | `o_orderkey`, `o_custkey`, `o_totalprice` |
| `customer.parquet` | ~8.0 MB (~150K rows) | Customer master records | `c_custkey`, `c_name`, `c_nationkey` |
| `supplier.parquet` | ~526 KB (~10K rows) | Supplier master records | `s_suppkey`, `s_name` |
| `nation.parquet` | ~2 KB (25 rows) | Nation lookup table | `n_nationkey`, `n_name`, `n_regionkey` |
| `region.parquet` | ~1 KB (5 rows) | Region lookup table | `r_regionkey`, `r_name` |
| `part.parquet` | ~4.1 MB | Part catalog records | `p_partkey`, `p_retailprice` |
| `partsupp.parquet` | ~28.8 MB | Part supplier mapping | `ps_partkey`, `ps_suppkey`, `ps_supplycost` |

---

## 📊 Benchmark Queries (TPC-H)

All engines implement the exact same analytical queries:

### 🔹 Q1: Total Revenue
- **Goal**: Computes the grand total discounted revenue across all line items.
- **Formula**: `SUM(l_extendedprice * (1.0 - l_discount))`
- **Target Table**: `lineitem`

### 🔹 Q2: Top 10 Customers by Revenue
- **Goal**: Identifies the highest-spending customers by joining line items, orders, and customer master records.
- **Joins**: `lineitem` ⨝ `orders` on `l_orderkey = o_orderkey` ⨝ `customer` on `o_custkey = c_custkey`
- **Output**: Top 10 customers sorted by aggregated revenue descending.

### 🔹 Q3: Revenue by Nation
- **Goal**: Calculates total aggregated revenue grouped across all 25 nations.
- **Joins**: `lineitem` ⨝ `orders` ⨝ `customer` ⨝ `nation` on `c_nationkey = n_nationkey`
- **Output**: Revenue per nation, ordered by value descending.

### 🔹 Q4: Revenue by Region
- **Goal**: Computes aggregated revenue across global geographic regions.
- **Joins**: `lineitem` ⨝ `orders` ⨝ `customer` ⨝ `nation` ⨝ `region` on `n_regionkey = r_regionkey`
- **Output**: Revenue per region (e.g. `EUROPE`, `ASIA`, `AMERICA`, `AFRICA`, `MIDDLE EAST`), ordered by value descending.

### 🔹 Q5: Top 10 Suppliers by Revenue
- **Goal**: Finds the top 10 suppliers generating the most revenue.
- **Joins**: `lineitem` ⨝ `supplier` on `l_suppkey = s_suppkey`
- **Output**: Top 10 suppliers by revenue, sorted descending.

---

## 📐 Standardized Output Format

To verify correctness and numerical consistency across engines, all benchmarks output their results using a uniform 5-column schema saved into `results/<engine>.csv`:

```csv
query,category,key,value,metric
q1,total_revenue,all,218102223885.0001,total_revenue
q2,customer,Customer#000143500,6757566.0218,revenue
q3,nation,FRANCE,8960205391.8314,revenue
q4,region,EUROPE,44032702326.2956,revenue
q5,supplier,Supplier#000005994,28698575.6313,revenue
```

| Field | Description | Example |
| :--- | :--- | :--- |
| `query` | Identifier of the benchmark query | `q1`, `q2`, `q3`, `q4`, `q5` |
| `category` | High-level grouping dimension | `total_revenue`, `customer`, `nation`, `region`, `supplier` |
| `key` | Entity identifier / group key | `Customer#000143500`, `FRANCE`, `EUROPE` |
| `value` | Computed metric value | `218102223885.0001` |
| `metric` | Name of the calculated metric | `total_revenue`, `revenue` |

---

## 🛠 Prerequisites

### For Rust (Polars & DataFusion):
- **Rust Toolchain**: Rust 1.85+ (Edition 2024 support)
- **Cargo**: Included with Rust installation

### For Python (Pandas & PySpark):
- **Python**: Python 3.10+
- **Python Libraries**:
  ```bash
  pip install pandas pyarrow pyspark
  ```
- **Java Runtime** (for PySpark): Java 8/11/17 installed and `JAVA_HOME` configured.

---

## 🚀 Getting Started & Usage

### 1. Running Rust Benchmarks (Polars & DataFusion)

Run both the Polars and DataFusion benchmarks in release mode for optimal performance:

```bash
# Run in release mode with maximum compiler optimizations
cargo run --release
```

This will execute:
1. **Polars Suite**: Executes queries using lazy scans, multi-threaded aggregation, and writes `results/polars.csv`.
2. **DataFusion Suite**: Initializes Arrow context, executes SQL queries asynchronously via Tokio runtime, and writes `results/datafusion.csv`.

---

### 2. Running Python Benchmarks (Pandas & PySpark)

#### Run Pandas Benchmark:
```bash
python benchmark/pandas_benchmark.py
```
Outputs execution timings and exports `results/pandas.csv`.

#### Run PySpark Benchmark:
```bash
python benchmark/pyspark_benchmark.py
```
Initializes local Spark session with optimized shuffle partitions and exports `results/pyspark.csv`.

---

## 📈 Benchmark Results & Output Verification

After executing the benchmarks, the generated CSV files in `results/` can be compared to validate that all four engines produce consistent results:

```bash
# Compare result files (e.g. using diff / git diff)
diff results/polars.csv results/datafusion.csv
diff results/polars.csv results/pandas.csv
```

Each benchmark prints individual query execution durations and total elapsed time to standard output for direct performance evaluation.
