from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import time
import os
import csv
from pathlib import Path


DATA_DIR = Path("data")
RESULTS_DIR = Path("results")


# ============================================================
# CREATE SPARK SESSION
# ============================================================

def create_spark():

    spark = (
        SparkSession.builder
        .appName("TPC-H PySpark Benchmark")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# ============================================================
# LOAD DATA
# ============================================================

def load_data(spark):

    print("\nLoading Parquet files...")

    start = time.perf_counter()

    lineitem = spark.read.parquet(
        str(DATA_DIR / "lineitem.parquet")
    )

    orders = spark.read.parquet(
        str(DATA_DIR / "orders.parquet")
    )

    customer = spark.read.parquet(
        str(DATA_DIR / "customer.parquet")
    )

    nation = spark.read.parquet(
        str(DATA_DIR / "nation.parquet")
    )

    region = spark.read.parquet(
        str(DATA_DIR / "region.parquet")
    )

    supplier = spark.read.parquet(
        str(DATA_DIR / "supplier.parquet")
    )

    # --------------------------------------------------------
    # Convert Decimal columns to Double
    # --------------------------------------------------------

    lineitem = (
        lineitem
        .withColumn("l_quantity", F.col("l_quantity").cast("double"))
        .withColumn("l_extendedprice", F.col("l_extendedprice").cast("double"))
        .withColumn("l_discount", F.col("l_discount").cast("double"))
        .withColumn("l_tax", F.col("l_tax").cast("double"))
    )

    orders = (
        orders
        .withColumn("o_totalprice", F.col("o_totalprice").cast("double"))
    )

    customer = (
        customer
        .withColumn("c_acctbal", F.col("c_acctbal").cast("double"))
    )

    supplier = (
        supplier
        .withColumn("s_acctbal", F.col("s_acctbal").cast("double"))
    )

    # --------------------------------------------------------
    # Materialize data
    #
    # Spark is lazy, so count() forces Parquet reading.
    # --------------------------------------------------------

    lineitem = lineitem.cache()
    orders = orders.cache()
    customer = customer.cache()
    nation = nation.cache()
    region = region.cache()
    supplier = supplier.cache()

    lineitem_count = lineitem.count()
    orders_count = orders.count()
    customer_count = customer.count()
    nation_count = nation.count()
    region_count = region.count()
    supplier_count = supplier.count()

    elapsed = time.perf_counter() - start

    print(f"\nData loaded in: {elapsed:.4f} seconds")

    print("\nDataset sizes:")
    print(f"Lineitem : {lineitem_count:,} rows")
    print(f"Orders   : {orders_count:,} rows")
    print(f"Customer : {customer_count:,} rows")
    print(f"Nation   : {nation_count:,} rows")
    print(f"Region   : {region_count:,} rows")
    print(f"Supplier : {supplier_count:,} rows")

    return (
        lineitem,
        orders,
        customer,
        nation,
        region,
        supplier,
        elapsed
    )


# ============================================================
# Q1 - TOTAL REVENUE
# ============================================================

def query_1(lineitem):

    print("\n--- Q1: Total Revenue ---")

    start = time.perf_counter()

    result = (
        lineitem
        .select(
            (
                F.col("l_extendedprice")
                * (1.0 - F.col("l_discount"))
            ).alias("revenue")
        )
        .agg(
            F.sum("revenue").alias("total_revenue")
        )
        .collect()[0]["total_revenue"]
    )

    elapsed = time.perf_counter() - start

    print(f"Total Revenue: {result}")
    print(f"Time: {elapsed:.4f}s")

    results = [
        [
            "q1",
            "total_revenue",
            "all",
            result,
            "total_revenue"
        ]
    ]

    return results, elapsed


# ============================================================
# Q2 - TOP 10 CUSTOMERS
# ============================================================

def query_2(lineitem, orders, customer):

    print("\n" + "=" * 50)
    print("Q2 - TOP 10 CUSTOMERS")
    print("=" * 50)

    start = time.perf_counter()

    df = (
        lineitem
        .select(
            "l_orderkey",
            "l_extendedprice",
            "l_discount"
        )
        .join(
            orders.select(
                "o_orderkey",
                "o_custkey"
            ),
            F.col("l_orderkey") == F.col("o_orderkey")
        )
        .join(
            customer.select(
                "c_custkey",
                "c_name"
            ),
            F.col("o_custkey") == F.col("c_custkey")
        )
    )

    result = (
        df
        .withColumn(
            "revenue",
            F.col("l_extendedprice")
            * (1.0 - F.col("l_discount"))
        )
        .groupBy("c_name")
        .agg(
            F.sum("revenue").alias("revenue")
        )
        .orderBy(
            F.col("revenue").desc()
        )
        .limit(10)
        .collect()
    )

    elapsed = time.perf_counter() - start

    for row in result:
        print(
            f"{row['c_name']} : {row['revenue']}"
        )

    print(f"\nExecution time: {elapsed:.4f} seconds")

    results = []

    for row in result:

        results.append(
            [
                "q2",
                "customer",
                row["c_name"],
                row["revenue"],
                "revenue"
            ]
        )

    return results, elapsed


# ============================================================
# Q3 - REVENUE BY NATION
# ============================================================

def query_3(
    lineitem,
    orders,
    customer,
    nation
):

    print("\n" + "=" * 50)
    print("Q3 - REVENUE BY NATION")
    print("=" * 50)

    start = time.perf_counter()

    df = (
        lineitem
        .select(
            "l_orderkey",
            "l_extendedprice",
            "l_discount"
        )
        .join(
            orders.select(
                "o_orderkey",
                "o_custkey"
            ),
            F.col("l_orderkey") == F.col("o_orderkey")
        )
        .join(
            customer.select(
                "c_custkey",
                "c_nationkey"
            ),
            F.col("o_custkey") == F.col("c_custkey")
        )
        .join(
            nation.select(
                "n_nationkey",
                "n_name"
            ),
            F.col("c_nationkey") == F.col("n_nationkey")
        )
    )

    result = (
        df
        .withColumn(
            "revenue",
            F.col("l_extendedprice")
            * (1.0 - F.col("l_discount"))
        )
        .groupBy("n_name")
        .agg(
            F.sum("revenue").alias("revenue")
        )
        .orderBy(
            F.col("revenue").desc()
        )
        .collect()
    )

    elapsed = time.perf_counter() - start

    for row in result:
        print(
            f"{row['n_name']} : {row['revenue']}"
        )

    print(f"\nExecution time: {elapsed:.4f} seconds")

    results = []

    for row in result:

        results.append(
            [
                "q3",
                "nation",
                row["n_name"],
                row["revenue"],
                "revenue"
            ]
        )

    return results, elapsed


# ============================================================
# Q4 - REVENUE BY REGION
# ============================================================

def query_4(
    lineitem,
    orders,
    customer,
    nation,
    region
):

    print("\n" + "=" * 50)
    print("Q4 - REVENUE BY REGION")
    print("=" * 50)

    start = time.perf_counter()

    df = (
        lineitem
        .select(
            "l_orderkey",
            "l_extendedprice",
            "l_discount"
        )
        .join(
            orders.select(
                "o_orderkey",
                "o_custkey"
            ),
            F.col("l_orderkey") == F.col("o_orderkey")
        )
        .join(
            customer.select(
                "c_custkey",
                "c_nationkey"
            ),
            F.col("o_custkey") == F.col("c_custkey")
        )
        .join(
            nation.select(
                "n_nationkey",
                "n_regionkey"
            ),
            F.col("c_nationkey") == F.col("n_nationkey")
        )
        .join(
            region.select(
                "r_regionkey",
                "r_name"
            ),
            F.col("n_regionkey") == F.col("r_regionkey")
        )
    )

    result = (
        df
        .withColumn(
            "revenue",
            F.col("l_extendedprice")
            * (1.0 - F.col("l_discount"))
        )
        .groupBy("r_name")
        .agg(
            F.sum("revenue").alias("revenue")
        )
        .orderBy(
            F.col("revenue").desc()
        )
        .collect()
    )

    elapsed = time.perf_counter() - start

    for row in result:
        print(
            f"{row['r_name']} : {row['revenue']}"
        )

    print(f"\nExecution time: {elapsed:.4f} seconds")

    results = []

    for row in result:

        results.append(
            [
                "q4",
                "region",
                row["r_name"],
                row["revenue"],
                "revenue"
            ]
        )

    return results, elapsed


# ============================================================
# Q5 - TOP 10 SUPPLIERS
# ============================================================

def query_5(lineitem, supplier):

    print("\n" + "=" * 50)
    print("Q5 - TOP 10 SUPPLIERS")
    print("=" * 50)

    start = time.perf_counter()

    df = (
        lineitem
        .select(
            "l_suppkey",
            "l_extendedprice",
            "l_discount"
        )
        .join(
            supplier.select(
                "s_suppkey",
                "s_name"
            ),
            F.col("l_suppkey") == F.col("s_suppkey")
        )
    )

    result = (
        df
        .withColumn(
            "revenue",
            F.col("l_extendedprice")
            * (1.0 - F.col("l_discount"))
        )
        .groupBy("s_name")
        .agg(
            F.sum("revenue").alias("revenue")
        )
        .orderBy(
            F.col("revenue").desc()
        )
        .limit(10)
        .collect()
    )

    elapsed = time.perf_counter() - start

    for row in result:
        print(
            f"{row['s_name']} : {row['revenue']}"
        )

    print(f"\nExecution time: {elapsed:.4f} seconds")

    results = []

    for row in result:

        results.append(
            [
                "q5",
                "supplier",
                row["s_name"],
                row["revenue"],
                "revenue"
            ]
        )

    return results, elapsed


# ============================================================
# SAVE RESULTS
# ============================================================

def save_results(results):

    os.makedirs(RESULTS_DIR, exist_ok=True)

    output_path = RESULTS_DIR / "pyspark.csv"

    with open(
        output_path,
        "w",
        newline="",
        encoding="utf-8"
    ) as file:

        writer = csv.writer(file)

        writer.writerow([
            "query",
            "category",
            "key",
            "value",
            "metric"
        ])

        writer.writerows(results)

    print("\n" + "=" * 50)
    print("RESULTS SAVED")
    print("=" * 50)

    print(f"File: {output_path}")
    print(f"Rows: {len(results)}")


# ============================================================
# MAIN
# ============================================================

def main():

    print("=" * 60)
    print("             PYSPARK TPC-H BENCHMARK")
    print("=" * 60)

    total_start = time.perf_counter()

    spark = create_spark()

    (
        lineitem,
        orders,
        customer,
        nation,
        region,
        supplier,
        load_time
    ) = load_data(spark)

    all_results = []
    timings = []

    # --------------------------------------------------------
    # Q1
    # --------------------------------------------------------

    results, time_taken = query_1(
        lineitem
    )

    all_results.extend(results)

    timings.append(
        ["Q1", time_taken]
    )

    # --------------------------------------------------------
    # Q2
    # --------------------------------------------------------

    results, time_taken = query_2(
        lineitem,
        orders,
        customer
    )

    all_results.extend(results)

    timings.append(
        ["Q2", time_taken]
    )

    # --------------------------------------------------------
    # Q3
    # --------------------------------------------------------

    results, time_taken = query_3(
        lineitem,
        orders,
        customer,
        nation
    )

    all_results.extend(results)

    timings.append(
        ["Q3", time_taken]
    )

    # --------------------------------------------------------
    # Q4
    # --------------------------------------------------------

    results, time_taken = query_4(
        lineitem,
        orders,
        customer,
        nation,
        region
    )

    all_results.extend(results)

    timings.append(
        ["Q4", time_taken]
    )

    # --------------------------------------------------------
    # Q5
    # --------------------------------------------------------

    results, time_taken = query_5(
        lineitem,
        supplier
    )

    all_results.extend(results)

    timings.append(
        ["Q5", time_taken]
    )

    save_results(all_results)

    total_time = time.perf_counter() - total_start

    print("BENCHMARK SUMMARY")

    print(
        f"\nData loading time : {load_time:.4f} seconds"
    )

    print("\nQuery execution times:")

    for query, query_time in timings:

        print(
            f"{query}: {query_time:.4f} seconds"
        )

    print(
        f"\nTotal program time : {total_time:.4f} seconds"
    )

    spark.stop()


if __name__ == "__main__":
    main()