import dlt
from pyspark.sql.functions import *

# ── Base Path ──────────────────────────────────
base_path = "abfss://sapdata@adlsloki.dfs.core.windows.net"
bronze_path = f"{base_path}/Bronze"

# ── DLT Bronze — VBAK ─────────────────────────
@dlt.table(
    name="bronze_vbak",
    comment="Raw SAP Sales Order data from ADLS Bronze layer",
    table_properties={"quality": "bronze"}
)
def bronze_vbak():
    return (
        spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(f"{bronze_path}/vbak.csv")
            .withColumn("_ingestion_ts", current_timestamp())
            .withColumn("_source_system", lit("SAP"))
            .withColumn("_layer", lit("bronze"))
    )

# ── DLT Bronze — KNA1 ─────────────────────────
@dlt.table(
    name="bronze_kna1",
    comment="Raw SAP Customer Master data",
    table_properties={"quality": "bronze"}
)
def bronze_kna1():
    return (
        spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(f"{bronze_path}/kna1.csv")
            .withColumn("_ingestion_ts", current_timestamp())
            .withColumn("_source_system", lit("SAP"))
            .withColumn("_layer", lit("bronze"))
    )

# ── DLT Bronze — MARA ─────────────────────────
@dlt.table(
    name="bronze_mara",
    comment="Raw SAP Material Master data",
    table_properties={"quality": "bronze"}
)
def bronze_mara():
    return (
        spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(f"{bronze_path}/mara.csv")
            .withColumn("_ingestion_ts", current_timestamp())
            .withColumn("_source_system", lit("SAP"))
            .withColumn("_layer", lit("bronze"))
    )

# ── DLT Bronze — BKPF ─────────────────────────
@dlt.table(
    name="bronze_bkpf",
    comment="Raw SAP Accounting Header data",
    table_properties={"quality": "bronze"}
)
def bronze_bkpf():
    return (
        spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(f"{bronze_path}/bkpf.csv")
            .withColumn("_ingestion_ts", current_timestamp())
            .withColumn("_source_system", lit("SAP"))
            .withColumn("_layer", lit("bronze"))
    )

# ── DLT Bronze — BSEG ─────────────────────────
@dlt.table(
    name="bronze_bseg",
    comment="Raw SAP Accounting Line Items",
    table_properties={"quality": "bronze"}
)
def bronze_bseg():
    return (
        spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(f"{bronze_path}/bseg.csv")
            .withColumn("_ingestion_ts", current_timestamp())
            .withColumn("_source_system", lit("SAP"))
            .withColumn("_layer", lit("bronze"))
    )
# vbap table
@dlt.table(
    name="bronze_vbap",
    comment="Raw SAP Sales Order Item data",
    table_properties={"quality": "bronze"}
)
def bronze_vbap():
    return (
        spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(f"{bronze_path}/vbap.csv")
            .withColumn("_ingestion_ts", current_timestamp())
            .withColumn("_source_system", lit("SAP"))
            .withColumn("_layer", lit("bronze"))
    )

# ───────────────────────────────────────────────
# SILVER LAYER - VBAK
# ───────────────────────────────────────────────

@dlt.table(
    name="silver_vbak",
    comment="Cleaned Sales Order Header",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_order", "sales_order_number IS NOT NULL")
@dlt.expect_or_drop("valid_customer", "customer_number IS NOT NULL")
def silver_vbak():

    return (

        dlt.read("bronze_vbak")

        .dropDuplicates(["vbeln"])

        .filter(col("vbeln").isNotNull())

        .withColumnRenamed('vbeln', "sales_order_number")

        .withColumnRenamed("kunnr", "customer_number")

        .withColumnRenamed("erdat", "order_date")

        .withColumn("_silver_load_time", current_timestamp())

    )
@dlt.table(
    name="silver_vbap",
    comment="cleaned Sales Order Items",
    table_properties={"quality":"silver"}
)
def silver_vbap():
    return (
        dlt.read("bronze_vbap")
        .dropDuplicates(["vbeln","matnr"])
        .filter(col("vbeln").isNotNull())
        .withColumnRenamed("vbeln","sales_order_number")
        .withColumnRenamed("matnr","material_number")
        .withColumnRenamed("netwr","net_value")
        .withColumnRenamed("netpr","net_price")
        .withColumn("_silver_load_time", current_timestamp())
    )

@dlt.table(
    name="silver_kna1",
    comment="Cleaned customer data",
    table_properties={"quality":"silver"}
)
def silver_kna1():
    return(
        dlt.read("bronze_kna1")
        .dropDuplicates(["kunnr"])
        .filter(col("kunnr").isNotNull())
        .withColumnRenamed("kunnr","customer_number")
        .withColumnRenamed("name1","customer_name")
        .withColumn("_silver_load_time", current_timestamp())
    )
@dlt.table(
    name="silver_mara",
    comment="Cleaned Material data",
    table_properties={"quality":"silver"}
)
def silver_mara():
    return(
        dlt.read("bronze_mara")
        .dropDuplicates(["matnr"])
        .filter(col("matnr").isNotNull())
        .withColumnRenamed("matnr","material_number")
        .withColumn("mtart", upper(col("mtart")))
        .withColumnRenamed("mtart","material_type")
        .withColumnRenamed("matkl","material_group")
        .withColumn("_silver_load_time", current_timestamp())
    )
@dlt.table(
    name="silver_bkpf",
    comment="Cleaned Accounting Document Header",
    table_properties={"quality": "silver"}
)  
def silver_bkpf():
    return (

        dlt.read("bronze_bkpf")

        .dropDuplicates(["belnr","gjahr"])

        .filter(col("belnr").isNotNull())

        .withColumnRenamed("belnr","accounting_document")

        .withColumnRenamed("bukrs","company_code")

        .withColumnRenamed("gjahr","fiscal_year")

        .withColumnRenamed("monat","posting_period")

        .withColumn("_silver_load_time", current_timestamp())
     )
@dlt.table(
    name="silver_bseg",
    comment="Cleaned Accounting Line Items",
    table_properties={"quality": "silver"}
)
def silver_bseg():

    return (

        dlt.read("bronze_bseg")

        .dropDuplicates(["belnr","buzei"])

        .filter(col("belnr").isNotNull())

        .withColumnRenamed("belnr","accounting_document")

        .withColumnRenamed("buzei","line_item")

        .withColumnRenamed("hkont","gl_account")

        .withColumnRenamed("dmbtr","amount")

        .withColumn("_silver_load_time", current_timestamp())

    )

# ───────────────────────────────────────────────
# GOLD LAYER 
# ───────────────────────────────────────────────
# Sales_orders
@dlt.table(
    name="gold_sales_orders",
    comment="Sales Orders with Customer Details",
    table_properties={"quality":"gold"}
)
def gold_sales_orders():

    vbak_df = dlt.read("silver_vbak")
    kna1_df = dlt.read("silver_kna1")

    gold_df = (
        vbak_df
        .join(
            kna1_df,
            on="customer_number",
            how="left"
        )
        .select(
            "sales_order_number",
            "customer_number",
            "customer_name",
            "order_date"
        )
        .withColumn("_gold_load_time", current_timestamp())
    )
    return gold_df

# Product Performance

@dlt.table(
    name="gold_product_performance",
    comment="Product Performance",
    table_properties={"quality":"gold"}
)
def gold_product_performance():

    vbap_df = dlt.read("silver_vbap")
    mara_df = dlt.read("silver_mara")

    gold_df=(vbap_df.join(mara_df,on="material_number",how="left")

    .select("material_number","material_type","material_group","sales_order_number","net_value","net_price")
    .withColumn("_gold_load_time", current_timestamp()))
    return gold_df

# Finance Summary

@dlt.table(
    name="gold_Finance_summary",
    comment="Finance Summary",
    table_properties={"quality":"gold"}
)
def gold_Finance_summary():

    bkpf_df = dlt.read("silver_bkpf")
    bseg_df = dlt.read("silver_bseg")

    gold_df=(bkpf_df.join(bseg_df,on="accounting_document",how="inner")

    .select("accounting_document","company_code","fiscal_year","posting_period","gl_account","amount")
    .withColumn("_gold_load_time", current_timestamp()))
    return gold_df