import dlt
from pyspark.sql.functions import col, current_timestamp, regexp_extract, to_date

RAW_PATH = (
    "abfss://raw@dlmsfnex5038regeeupdev.dfs.core.windows.net"
    "/ms/sl/dp/crm/"
)
PARQUET_GLOB = "Product_Dim_2*.parquet"


@dlt.view(
    name="stg_crm_product_dim",
    comment="Streaming ingestion of CRM Product_Dim Parquet snapshots via Auto Loader."
)
def stg_crm_product_dim():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("recursiveFileLookup", "true")
            .option("pathGlobFilter", PARQUET_GLOB)
            .option("mergeSchema", "true")
            .load(RAW_PATH)
            .withColumn("_source_file", col("_metadata.file_path"))
            .withColumn("_load_ts", current_timestamp())
            .withColumn(
                "_snapshot_date",
                to_date(
                    regexp_extract(col("_metadata.file_name"),
                                   r"Product_Dim_(\d{8})\.parquet", 1),
                    "yyyyMMdd"
                )
            )
    )


dlt.create_streaming_table(
    name="crm_product_dim",
    comment="SCD2 history of CRM Product_Dim. Keyed by _SK_PRODUCT.",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "quality": "silver"
    }
)


dlt.apply_changes(
    target="crm_product_dim",
    source="stg_crm_product_dim",
    keys=["_SK_PRODUCT"],
    sequence_by=col("_TF_LAST_UPDATE"),
    except_column_list=[
        "_TF_ETL",
        "_TF_LAST_UPDATE",
        "_source_file",
        "_load_ts",
        "_snapshot_date",
    ],
    stored_as_scd_type=2
)