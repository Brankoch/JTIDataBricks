import dlt
from pyspark.sql.functions import col

ENRICHED_TABLE = "`ldlp-ms-sl-dp-default-dev`.enriched.crm_product_dim"


@dlt.table(
    name="crm_product_dim",
    comment="Current version of CRM Product dim — gold layer. "
            "Filtered to active SCD2 rows where Level = 'Product'.",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "quality": "gold"
    }
)
def crm_product_dim():
    return (
        spark.read.table(ENRICHED_TABLE)
            .filter(col("__END_AT").isNull())
            .filter(col("Level") == "Product")
            .drop("__START_AT", "__END_AT",
                  "_source_file", "_load_ts", "_snapshot_date")
    )