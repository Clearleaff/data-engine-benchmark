import pandas as pd
import os
import time
from pathlib import Path

DATA_DIR = Path("data")
RESULTS_DIR = Path("results")

def convert_numeric_columns(df, columns):
    for col in columns:
        if col in df.columns:
            df[col] = df[col].astype("float64")
    return df

def load_data():

    print("\nLoading Parquet files...")

    start = time.perf_counter()

    lineitem = pd.read_parquet(
        DATA_DIR / "lineitem.parquet"
    )

    orders = pd.read_parquet(
        DATA_DIR / "orders.parquet"
    )

    customer = pd.read_parquet(
        DATA_DIR / "customer.parquet"
    )

    nation = pd.read_parquet(
        DATA_DIR / "nation.parquet"
    )

    region = pd.read_parquet(
        DATA_DIR / "region.parquet"
    )

    supplier = pd.read_parquet(
        DATA_DIR / "supplier.parquet"
    )

    # --------------------------------------------------------
    # Convert TPC-H DECIMAL columns to float64
    # --------------------------------------------------------

    lineitem = convert_numeric_columns(
        lineitem,
        [
            "l_quantity",
            "l_extendedprice",
            "l_discount",
            "l_tax"
        ]
    )

    orders = convert_numeric_columns(
        orders,
        [
            "o_totalprice"
        ]
    )

    customer = convert_numeric_columns(
        customer,
        [
            "c_acctbal"
        ]
    )

    supplier = convert_numeric_columns(
        supplier,
        [
            "s_acctbal"
        ]
    )

    # If you later load these tables:
    #
    # part:
    # p_retailprice
    #
    # partsupp:
    # ps_supplycost

    elapsed = time.perf_counter() - start

    print(f"Data loaded in: {elapsed:.4f} seconds")

    print("\nDataset sizes:")
    print(f"Lineitem : {len(lineitem):,} rows")
    print(f"Orders   : {len(orders):,} rows")
    print(f"Customer : {len(customer):,} rows")
    print(f"Nation   : {len(nation):,} rows")
    print(f"Region   : {len(region):,} rows")
    print(f"Supplier : {len(supplier):,} rows")

    return (
        lineitem,
        orders,
        customer,
        nation,
        region,
        supplier,
        elapsed
    )


def read_parquet(filename, columns=None):
    df = pd.read_parquet(DATA_DIR / filename, columns=columns)

    # Convert Decimal/object numeric columns to float
    for col in df.columns:
        if df[col].dtype == "object":
            try:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            except Exception:
                pass

    return df

def query_1(lineitem):
    print("\n--- Q1: Total Revenue ---")

    start = time.perf_counter()

    lineitem = read_parquet(
        "lineitem.parquet",
        columns=["l_extendedprice", "l_discount"]
    )

    lineitem["revenue"] = (
        lineitem["l_extendedprice"].astype(float)
        * (1.0 - lineitem["l_discount"].astype(float))
    )

    total_revenue = lineitem["revenue"].sum()

    result = [
      [
        "q1",
        "total_revenue",
        "all",
        total_revenue,
        "total_revenue"
       ]
   ]

    elapsed = time.perf_counter() - start

    print(f"Total Revenue: {total_revenue}")
    print(f"Time: {elapsed:.4f}s")

    return result, elapsed


def query_2(lineitem, orders, customer):

    print("\n" + "=" * 50)
    print("Q2 - TOP 10 CUSTOMERS")
    print("=" * 50)

    start = time.perf_counter()

    # lineitem -> orders
    df = lineitem[
        [
            "l_orderkey",
            "l_extendedprice",
            "l_discount"
        ]
    ].merge(
        orders[
            [
                "o_orderkey",
                "o_custkey"
            ]
        ],
        left_on="l_orderkey",
        right_on="o_orderkey"
    )

    # orders -> customer
    df = df.merge(
        customer[
            [
                "c_custkey",
                "c_name"
            ]
        ],
        left_on="o_custkey",
        right_on="c_custkey"
    )

    # Revenue
    df["revenue"] = (
        df["l_extendedprice"]
        * (1.0 - df["l_discount"])
    )

    # Group customers
    result = (
        df.groupby("c_name")["revenue"]
        .sum()
        .sort_values(ascending=False)
        .head(10)
    )

    elapsed = time.perf_counter() - start

    print(result)
    print(f"\nExecution time: {elapsed:.4f} seconds")

    results = []

    for name, revenue in result.items():

        results.append(
            [
                "q2",
                "customer",
                name,
                revenue,
                "revenue"
            ]
        )

    return results, elapsed

def query_3(lineitem, orders, customer, nation):

    print("\n" + "=" * 50)
    print("Q3 - REVENUE BY NATION")
    print("=" * 50)

    start = time.perf_counter()

    # lineitem -> orders
    df = lineitem[
        [
            "l_orderkey",
            "l_extendedprice",
            "l_discount"
        ]
    ].merge(
        orders[
            [
                "o_orderkey",
                "o_custkey"
            ]
        ],
        left_on="l_orderkey",
        right_on="o_orderkey"
    )

    # orders -> customer
    df = df.merge(
        customer[
            [
                "c_custkey",
                "c_nationkey"
            ]
        ],
        left_on="o_custkey",
        right_on="c_custkey"
    )

    # customer -> nation
    df = df.merge(
        nation[
            [
                "n_nationkey",
                "n_name"
            ]
        ],
        left_on="c_nationkey",
        right_on="n_nationkey"
    )

    # Revenue
    df["revenue"] = (
        df["l_extendedprice"]
        * (1.0 - df["l_discount"])
    )

    # Revenue by nation
    result = (
        df.groupby("n_name")["revenue"]
        .sum()
        .sort_values(ascending=False)
    )

    elapsed = time.perf_counter() - start

    print(result)
    print(f"\nExecution time: {elapsed:.4f} seconds")

    results = []

    for name, revenue in result.items():

        results.append(
            [
                "q3",
                "nation",
                name,
                revenue,
                "revenue"
            ]
        )

    return results, elapsed

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

    # lineitem -> orders
    df = lineitem[
        [
            "l_orderkey",
            "l_extendedprice",
            "l_discount"
        ]
    ].merge(
        orders[
            [
                "o_orderkey",
                "o_custkey"
            ]
        ],
        left_on="l_orderkey",
        right_on="o_orderkey"
    )

    # orders -> customer
    df = df.merge(
        customer[
            [
                "c_custkey",
                "c_nationkey"
            ]
        ],
        left_on="o_custkey",
        right_on="c_custkey"
    )

    # customer -> nation
    df = df.merge(
        nation[
            [
                "n_nationkey",
                "n_regionkey"
            ]
        ],
        left_on="c_nationkey",
        right_on="n_nationkey"
    )

    # nation -> region
    df = df.merge(
        region[
            [
                "r_regionkey",
                "r_name"
            ]
        ],
        left_on="n_regionkey",
        right_on="r_regionkey"
    )

    # Revenue
    df["revenue"] = (
        df["l_extendedprice"]
        * (1.0 - df["l_discount"])
    )

    # Revenue by region
    result = (
        df.groupby("r_name")["revenue"]
        .sum()
        .sort_values(ascending=False)
    )

    elapsed = time.perf_counter() - start

    print(result)
    print(f"\nExecution time: {elapsed:.4f} seconds")

    results = []

    for name, revenue in result.items():

        results.append(
            [
                "q4",
                "region",
                name,
                revenue,
                "revenue"
            ]
        )

    return results, elapsed

def query_5(lineitem, supplier):

    print("\n" + "=" * 50)
    print("Q5 - TOP 10 SUPPLIERS")
    print("=" * 50)

    start = time.perf_counter()

    # lineitem -> supplier
    df = lineitem[
        [
            "l_suppkey",
            "l_extendedprice",
            "l_discount"
        ]
    ].merge(
        supplier[
            [
                "s_suppkey",
                "s_name"
            ]
        ],
        left_on="l_suppkey",
        right_on="s_suppkey"
    )

    # Revenue
    df["revenue"] = (
        df["l_extendedprice"]
        * (1.0 - df["l_discount"])
    )

    # Top suppliers
    result = (
        df.groupby("s_name")["revenue"]
        .sum()
        .sort_values(ascending=False)
        .head(10)
    )

    elapsed = time.perf_counter() - start

    print(result)
    print(f"\nExecution time: {elapsed:.4f} seconds")

    results = []

    for name, revenue in result.items():

        results.append(
            [
                "q5",
                "supplier",
                name,
                revenue,
                "revenue"
            ]
        )

    return results, elapsed

def save_results(results):

    os.makedirs(RESULTS_DIR, exist_ok=True)

    output_path = RESULTS_DIR / "pandas.csv"

    df = pd.DataFrame(
        results,
        columns=[
            "query",
            "category",
            "key",
            "value",
            "metric"
        ]
    )

    df.to_csv(
        output_path,
        index=False
    )
    print(f"Rows: {len(df)}")


def main():

    print("=" * 60)
    print("              PANDAS TPC-H BENCHMARK")
    print("=" * 60)

    total_start = time.perf_counter()
    (
        lineitem,
        orders,
        customer,
        nation,
        region,
        supplier,
        load_time
    ) = load_data()

    all_results = []
    timings = []

    results, time_taken = query_1(
        lineitem
    )

    all_results.extend(results)

    timings.append(
        ["Q1", time_taken]
    )

    results, time_taken = query_2(
        lineitem,
        orders,
        customer
    )

    all_results.extend(results)

    timings.append(
        ["Q2", time_taken]
    )


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

if __name__ == "__main__":
    main()