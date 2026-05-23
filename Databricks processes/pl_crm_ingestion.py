# Databricks notebook source
# MAGIC %md
# MAGIC # pl_crm_to_raw
# MAGIC DLT pipeline — incremental load of CRM Synapse views into raw Delta tables.
# MAGIC
# MAGIC **Strategies:**
# MAGIC - `DELTA`     — views with `_TF_LAST_UPDATE`: append rows newer than watermark (~126 views)
# MAGIC - `ROW_COUNT` — `Date_Dim`, `Month_Dim`: full refresh only if source row count grew
# MAGIC - `FULL`      — `Sales_Dim`, `Sales_Fact_Month`, `SurveyAnswerCount_Aux`: always overwrite
# MAGIC
# MAGIC **Watermark table:** `raw.crm_watermark` — managed as a DLT table, written after all views.
# MAGIC **Schedule:** set in DLT pipeline settings — not hardcoded here.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType
from datetime import datetime, timezone

# ── Config ────────────────────────────────────────────────────────────────────
SECRET_SCOPE  = "ldlp-ms-sl-dp"
STORAGE_ACCT  = "dlmsfnex5038regeeupdev"
CRM_SCHEMA    = "MS_GLB_ENT_TMECS14_SK"
CATALOG       = "ldlp-ms-sl-dp-default-dev"
UC_SCHEMA     = "raw"

# ── Special view routing — everything not listed here uses DELTA ──────────────
ROW_COUNT_VIEWS = {"Date_Dim", "Month_Dim"}
FULL_VIEWS      = {"Sales_Dim", "Sales_Fact_Month", "SurveyAnswerCount_Aux"}

# COMMAND ----------

# ── JDBC connection ───────────────────────────────────────────────────────────
jdbc_url      = dbutils.secrets.get(SECRET_SCOPE, "crm-synapse-jdbc-url-dev")
jdbc_user     = dbutils.secrets.get(SECRET_SCOPE, "crm-synapse-jdbc-user-dev")
jdbc_password = dbutils.secrets.get(SECRET_SCOPE, "crm-synapse-jdbc-password-dev")
jdbc_database = dbutils.secrets.get(SECRET_SCOPE, "crm-synapse-jdbc-database-dev")

jdbc_full_url = (
    f"jdbc:sqlserver://{jdbc_url};"
    f"database={jdbc_database};"
    "encrypt=true;"
    "trustServerCertificate=false;"
    "hostNameInCertificate=*.sql.azuresynapse.net;"
    "loginTimeout=30;"
)

jdbc_opts = {
    "url":      jdbc_full_url,
    "user":     jdbc_user,
    "password": jdbc_password,
    "driver":   "com.microsoft.sqlserver.jdbc.SQLServerDriver",
}

# COMMAND ----------

# ── Helpers ───────────────────────────────────────────────────────────────────

def jdbc_read(query: str):
    """Execute a JDBC query and return a DataFrame."""
    return (
        spark.read.format("jdbc")
        .options(**jdbc_opts)
        .option("query", query)
        .load()
    )


def jdbc_read_table(view_name: str):
    """Read a full view via dbtable (more efficient than query= for full reads)."""
    return (
        spark.read.format("jdbc")
        .options(**jdbc_opts)
        .option("dbtable", f"[{CRM_SCHEMA}].[{view_name}]")
        .load()
    )


def read_watermarks() -> dict:
    """
    Read crm_watermark DLT table from the previous pipeline run.
    Returns dict keyed by view_name. Returns empty dict on first run.
    """
    try:
        rows = spark.table(f"`{CATALOG}`.`{UC_SCHEMA}`.`crm_watermark`").collect()
        return {r["view_name"]: r.asDict() for r in rows}
    except Exception:
        return {}


def get_strategy(view_name: str) -> str:
    if view_name in FULL_VIEWS:
        return "FULL"
    if view_name in ROW_COUNT_VIEWS:
        return "ROW_COUNT"
    return "DELTA"

# COMMAND ----------

# ── Discover views ────────────────────────────────────────────────────────────
all_views_df = jdbc_read(f"""
    SELECT TABLE_NAME
    FROM   INFORMATION_SCHEMA.VIEWS
    WHERE  TABLE_SCHEMA = '{CRM_SCHEMA}'
""")

ALL_VIEWS = sorted([row["TABLE_NAME"] for row in all_views_df.collect()])
print(f"Total views in schema: {len(ALL_VIEWS)}")
for s in ["DELTA", "ROW_COUNT", "FULL"]:
    count = sum(1 for v in ALL_VIEWS if get_strategy(v) == s)
    print(f"  {s:12s}: {count}")

# COMMAND ----------

# ── Read current watermarks ───────────────────────────────────────────────────
watermarks = read_watermarks()
print(f"Watermark entries found: {len(watermarks)}")

# COMMAND ----------

# ── DLT table factory ─────────────────────────────────────────────────────────

def make_delta_loader(view_name: str):
    def _load():
        wm      = watermarks.get(view_name, {})
        last_wm = wm.get("last_watermark")

        if last_wm:
            last_wm_str = last_wm.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            df = jdbc_read(
                f"SELECT * FROM [{CRM_SCHEMA}].[{view_name}] "
                f"WHERE _TF_LAST_UPDATE > '{last_wm_str}'"
            )
        else:
            # First run — full extract
            df = jdbc_read_table(view_name)

        return df
    return _load


def make_row_count_loader(view_name: str):
    def _load():
        wm           = watermarks.get(view_name, {})
        stored_count = wm.get("last_row_count") or 0

        source_count = int(
            jdbc_read(f"SELECT COUNT(*) AS cnt FROM [{CRM_SCHEMA}].[{view_name}]")
            .collect()[0]["cnt"]
        )

        if source_count <= stored_count:
            print(f"  {view_name}: row count unchanged ({source_count}) — returning existing table")
            return spark.table(f"`{CATALOG}`.`{UC_SCHEMA}`.`crm_{view_name.lower()}`")

        print(f"  {view_name}: row count {stored_count} → {source_count}, full refresh")
        return jdbc_read_table(view_name)
    return _load


def make_full_loader(view_name: str):
    def _load():
        return jdbc_read_table(view_name)
    return _load


# ── Register one DLT table per view ──────────────────────────────────────────
for view_name in ALL_VIEWS:
    strategy = get_strategy(view_name)
    tbl_name = f"crm_{view_name.lower()}"

    if strategy == "DELTA":
        dlt.table(
            name    = tbl_name,
            comment = f"CRM view {view_name} — DELTA load via _TF_LAST_UPDATE watermark",
        )(make_delta_loader(view_name))

    elif strategy == "ROW_COUNT":
        dlt.table(
            name    = tbl_name,
            comment = f"CRM view {view_name} — ROW_COUNT load, full refresh on growth",
        )(make_row_count_loader(view_name))

    elif strategy == "FULL":
        dlt.table(
            name    = tbl_name,
            comment = f"CRM view {view_name} — FULL overwrite on every run",
        )(make_full_loader(view_name))

# COMMAND ----------

# ── Watermark DLT table ───────────────────────────────────────────────────────
# Uses dlt.read() to declare dependencies — DLT ensures all source tables
# are written before this table is computed.

_WATERMARK_SCHEMA = StructType([
    StructField("view_name",      StringType(),    nullable=False),
    StructField("last_watermark", TimestampType(), nullable=True),
    StructField("last_row_count", LongType(),      nullable=True),
    StructField("last_run_ts",    TimestampType(), nullable=False),
    StructField("last_status",    StringType(),    nullable=False),
])

@dlt.table(
    name    = "crm_watermark",
    comment = "Watermark snapshot — written at end of every pipeline run.",
)
def crm_watermark():
    now  = datetime.now(timezone.utc)
    rows = []

    for view_name in ALL_VIEWS:
        strategy = get_strategy(view_name)
        tbl_name = f"crm_{view_name.lower()}"

        if strategy == "DELTA":
            max_wm = (
                dlt.read(tbl_name)
                .agg(F.max("_TF_LAST_UPDATE"))
                .collect()[0][0]
            )
            rows.append((view_name, max_wm, None, now, "SUCCESS"))

        elif strategy == "ROW_COUNT":
            cnt = dlt.read(tbl_name).count()
            rows.append((view_name, None, int(cnt), now, "SUCCESS"))

        elif strategy == "FULL":
            cnt = dlt.read(tbl_name).count()
            rows.append((view_name, None, int(cnt), now, "SUCCESS"))

    return spark.createDataFrame(rows, _WATERMARK_SCHEMA)