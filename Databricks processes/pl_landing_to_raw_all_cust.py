# =============================================================================
# LDLP | DLT Pipeline - Customer Monthly Data | Raw Schema
# =============================================================================
#
# Sources:
#   abfss://raw@dlmsfnex5038regeeupdev.dfs.core.windows.net
#         /ms/sl/dp/customer/<SOURCE>/<YYYY>/<MM>/<SOURCE>_<YYYYMM>.<ext>
#
# Target:
#   Catalog : ldlp-ms-sl-dp-default-<env>
#   Schema  : raw
#   Tables  : customer_data_load   - append-only file registry, ALL sources
#             customer_tesco       - xlsx, header row 1
#             customer_billa       - xlsx, header row 1 (generic column names)
#             customer_ggt         - xlsx, header row 8
#             customer_mikros      - xls,  header row 1
#             customer_m_m         - xml,  attributes from <row> elements
#             ... one table per source, generated from SOURCE_DEFINITIONS
#
# Load behaviour:
#   Both tables are plain append-only streams - no apply_changes, no _src tables.
#   Raw keeps full history of all versions (original + corrections).
#   "Latest per period" logic is handled in the staging pipeline.
#
# Adding a new source:
#   1. Add one entry to SOURCE_DEFINITIONS below
#   2. Run Full Refresh in DLT pipeline UI
#   No other code changes needed.
#
# Table naming convention:
#   customer_<source_folder_lowercase>  (spaces and + replaced with _)
#   e.g. customer/Tesco/  -> customer_tesco
#        customer/M+M/    -> customer_m_m
#
# Column naming convention:
#   raw schema     : exact source system names  e.g. "Local TPN item TPN"
#   staging schema : snake_cased                e.g. "local_tpn_item_tpn"
#
# DLT pipeline configuration:
#   pipeline.env = dev | qa | prd
#   Target catalog : ldlp-ms-sl-dp-default-<env>
#   Target schema  : raw
#   Notifications  : pipeline settings -> Notifications -> Pipeline failure
# =============================================================================

import dlt
import io
import re
import openpyxl
import xlrd
import xml.etree.ElementTree as ET

from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, MapType, StringType

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BF      = "ms"
GEO     = "sl"
DPNAME  = "dp"

ENV = spark.conf.get("pipeline.env", "dev")

STORAGE_ACCOUNT = "dlmsfnex5038regeeupdev"
CONTAINER       = "raw"

SOURCE_BASE_PATH = (
    f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"
    f"/{BF}/{GEO}/{DPNAME}/customer/"
)

# ---------------------------------------------------------------------------
# SOURCE DEFINITIONS
# One entry per customer data source.
#
# Keys:
#   folder          : subfolder name under customer/ - used in table name and path
#   file_format     : "xlsx" | "xls" | "xml"
#   header_row      : 1-based row number of the header (default 1)
#                     e.g. GGT has headers in row 8 so header_row=8
#   expected_columns: exact column names as they appear in the source header
#   numeric_columns : columns that must be numeric (expect_or_fail)
#   not_null_columns: columns that must not be null (expect_or_fail)
#   skip_col_count  : skip column count check (default False)
#                     use for XML where rows can have missing attributes
#   skip_has_col    : skip per-column presence check (default False)
#                     use for XML where rows can have missing attributes
#
# Table generated: customer_<folder.lower() with spaces/+ replaced by _>
# ---------------------------------------------------------------------------
SOURCE_DEFINITIONS = [
    {
        "folder":           "Tesco",
        "file_format":      "xlsx",
        "header_row":       1,
        "expected_columns": [
            "Local TPN item TPN",
            "Local TPN item TPNB",
            "Local TPN item Short description",
            "Calendar month",
            "Store code",
            "Store name",
            "Sold units",
        ],
        "numeric_columns":  ["Sold units"],
        "not_null_columns": ["Sold units"],
    },
    {
        "folder":           "Billa",
        "file_format":      "xlsx",
        "header_row":       1,
        "expected_columns": [
            "Column1",
            "Column2",
            "Column3",
            "Column4",
            "Column5",
            "Column6",
            "Column7",
            "Attribute",
            "Value",
        ],
        "numeric_columns":  ["Value"],
        "not_null_columns": ["Column1", "Column4", "Value"],
    },
    {
        "folder":           "GGT",
        "file_format":      "xlsx",
        "header_row":       8,      # GGT header is in row 8, rows 1-7 are metadata
        "expected_columns": [
            "Rok",
            "Mesiac",
            "Mandant",
            "Číslo skladu",
            "Dodávateľ produktu",
            "Kód produktu",
            "Názov produktu",
            "Množstvo",
            "Množstvo SD",
        ],
        "numeric_columns":  ["Množstvo", "Množstvo SD"],
        "not_null_columns": ["Rok", "Mesiac"],
    },
    {
        "folder":           "Mikros",
        "file_format":      "xls",
        "header_row":       1,
        "expected_columns": [
            "evc",
            "ean",
            "nazov",
            "ico",
            "dic",
            "firma",
            "mesto",
            "ulica",
            "psc",
            "datum",
            "mn",
            "kategoria",
            "dealer",
            "dod2",
            "fid",
            "miestourc",
            "kodzar",
            "nazovzar",
        ],
        "numeric_columns":  ["mn"],
        "not_null_columns": ["evc", "mn"],
    },
    {
        "folder":           "M+M",
        "file_format":      "xml",
        "header_row":       1,
        "expected_columns": [
            "idzbo",
            "nazev",
            "ean",
            "idodber",
            "ico",
            "nazev_odb",
            "ulice",
            "psc",
            "mesto",
            "fulice",
            "fpsc",
            "mesto_fm",
            "datum",
            "mnozstvi",
            "mj",
            "a1",
            "kod_kategorie",
            "dlcomptypz",
            "dlcomptypo",
            "dealer",
            "stredisko",
            "EOID",
            "FID",
        ],
        "numeric_columns":  ["mnozstvi"],
        "not_null_columns": ["idodber", "mnozstvi"],
        # XML rows can have missing attributes (e.g. psc missing in some rows)
        # so skip structural checks - data is still valid
        "skip_col_count":   True,
        "skip_has_col":     True,
    },
]


# ---------------------------------------------------------------------------
# UDF - parse XLSX binary -> list of rows as {column_name: value} maps
#
# header_row: 1-based row index of the header (default 1)
#             rows before the header are skipped (metadata rows e.g. GGT)
# ---------------------------------------------------------------------------
def make_parse_xlsx(header_row: int = 1):
    @F.udf(returnType=ArrayType(MapType(StringType(), StringType())))
    def parse_xlsx(content: bytes):
        if content is None:
            return []

        wb = openpyxl.load_workbook(
            filename=io.BytesIO(content),
            read_only=True,
            data_only=True,
        )
        ws   = wb.worksheets[0]
        rows = list(ws.iter_rows(values_only=True))

        if len(rows) < header_row:
            wb.close()
            return []

        # header_row is 1-based - convert to 0-based index
        header_idx  = header_row - 1
        raw_headers = rows[header_idx]

        # Filter ghost columns
        valid_indices = [
            i for i, h in enumerate(raw_headers)
            if h is not None and str(h).strip() != ""
        ]
        headers = [str(raw_headers[i]).strip() for i in valid_indices]

        # Data rows start after the header row
        result = [
            {
                **{headers[j]: (str(row[i]).strip() if row[i] is not None else None)
                   for j, i in enumerate(valid_indices)},
                "_row_number": str(row_idx),
            }
            for row_idx, row in enumerate(rows[header_idx + 1:])
        ]
        wb.close()
        return result

    return parse_xlsx


# ---------------------------------------------------------------------------
# UDF - parse XLS binary -> list of rows as {column_name: value} maps
# ---------------------------------------------------------------------------
@F.udf(returnType=ArrayType(MapType(StringType(), StringType())))
def parse_xls(content: bytes):
    if content is None:
        return []

    wb  = xlrd.open_workbook(file_contents=content)
    ws  = wb.sheet_by_index(0)

    if ws.nrows < 1:
        return []

    # Filter ghost columns
    raw_headers   = ws.row_values(0)
    valid_indices = [
        i for i, h in enumerate(raw_headers)
        if h is not None and str(h).strip() != ""
    ]
    headers = [str(raw_headers[i]).strip() for i in valid_indices]

    result = [
        {
            **{headers[j]: (str(ws.cell_value(row_idx, i)).strip()
                            if ws.cell_value(row_idx, i) is not None else None)
               for j, i in enumerate(valid_indices)},
            "_row_number": str(row_idx - 1),
        }
        for row_idx in range(1, ws.nrows)
    ]
    return result


# ---------------------------------------------------------------------------
# UDF - parse a single XML <row .../> attribute string into a map
# Input:  a string like  idzbo="v1" nazev="v2" ... />
# Output: {"idzbo": "v1", "nazev": "v2", ..., "_row_number": "0"}
#
# This tiny per-row UDF replaces the previous whole-file parse_xml UDF
# that exceeded the serverless 1 GB UDF memory limit for large M+M files.
# The heavy lifting (decode + split + explode) is done in Spark SQL / JVM.
# ---------------------------------------------------------------------------
@F.udf(returnType=MapType(StringType(), StringType()))
def parse_xml_row(row_str: str, row_number: int):
    if row_str is None:
        return None
    from html import unescape
    attrs = {k: unescape(v) for k, v in re.findall(r'(\w+)="([^"]*)"', row_str)}
    attrs["_row_number"] = str(row_number)
    return attrs


# ---------------------------------------------------------------------------
# Helper - derive audit columns from file path
# ---------------------------------------------------------------------------
def with_audit_cols(df, source_name: str):
    return (
        df
        .withColumn("source",
            F.lit(source_name))
        .withColumn("_load_timestamp",
            F.current_timestamp())
        .withColumn("_source_file_path",
            F.col("path"))
        .withColumn("_source_file_modified_at",
            F.col("modificationTime"))
        .withColumn("_report_year",
            F.regexp_extract("path", r"/(\d{4})/\d{2}/", 1))
        .withColumn("_report_month",
            F.regexp_extract("path", r"/\d{4}/(\d{2})/", 1))
        .withColumn("_date_key",
            F.concat(
                F.regexp_extract("path", r"/(\d{4})/\d{2}/", 1),
                F.lit("m"),
                F.regexp_extract("path", r"/\d{4}/(\d{2})/", 1),
            ))
        .withColumn("_frequency",   F.lit("monthly"))
        .withColumn("_env",         F.lit(ENV))
        .withColumn("_dataproject", F.lit(f"{BF}/{GEO}/{DPNAME}"))
    )


# ---------------------------------------------------------------------------
# Helper - file extension per format
# ---------------------------------------------------------------------------
FORMAT_GLOB = {
    "xlsx": "*.xlsx",
    "xls":  "*.xls",
    "xml":  "*.xml",
}


# ---------------------------------------------------------------------------
# Table 1 : customer_data_load
# Append-only delta load registry for ALL sources under customer/.
# One row appended per newly arrived file per pipeline run.
# Corrected files produce a new row - old row kept as history.
#
# Picks up all supported file formats across all source subfolders.
# ---------------------------------------------------------------------------
@dlt.table(
    name="customer_data_load",
    comment=(
        "Raw schema - Append-only delta load registry for ALL customer monthly sources. "
        "One row per newly arrived file per pipeline run. "
        f"Covers all subfolders under: {SOURCE_BASE_PATH}"
    ),
    table_properties={
        "quality":                              "raw",
        "ldlp.layer":                           "raw",
        "ldlp.bf":                              BF,
        "ldlp.geo":                             GEO,
        "ldlp.dpname":                          DPNAME,
        "ldlp.frequency":                       "monthly",
        "delta.autoOptimize.optimizeWrite":     "true",
        "delta.autoOptimize.autoCompact":       "true",
    },
)
def customer_data_load():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("cloudFiles.useNotifications", "false")
        # Match all supported formats across all source subfolders
        .option("pathGlobFilter", "*.{xlsx,xls,xml}")
        .option("recursiveFileLookup", "true")
        .load(SOURCE_BASE_PATH)
        .select(
            F.regexp_extract("path", r"/customer/([^/]+)/\d{4}/", 1)  .alias("source"),
            F.current_timestamp()                                       .alias("_load_timestamp"),
            F.col("path")                                              .alias("_source_file_path"),
            F.col("modificationTime")                                  .alias("_source_file_modified_at"),
            F.col("length")                                            .alias("_source_file_size_bytes"),
            F.regexp_extract("path", r"/(\d{4})/\d{2}/", 1)           .alias("_report_year"),
            F.regexp_extract("path", r"/\d{4}/(\d{2})/", 1)           .alias("_report_month"),
            F.concat(
                F.regexp_extract("path", r"/(\d{4})/\d{2}/", 1),
                F.lit("m"),
                F.regexp_extract("path", r"/\d{4}/(\d{2})/", 1),
            )                                                           .alias("_date_key"),
            F.lit("monthly")                                            .alias("_frequency"),
            F.lit(ENV)                                                  .alias("_env"),
            F.lit(f"{BF}/{GEO}/{DPNAME}")                              .alias("_dataproject"),
        )
    )


# ---------------------------------------------------------------------------
# Factory - generate one parsed DLT table per source definition
# ---------------------------------------------------------------------------
def make_source_table(source_def: dict):
    """
    Generates and registers one DLT table (one row per file) for a given
    source definition. Called once per entry in SOURCE_DEFINITIONS at
    module load time.
    """
    folder          = source_def["folder"]
    file_format     = source_def.get("file_format", "xlsx")
    header_row      = source_def.get("header_row", 1)
    expected_cols   = source_def["expected_columns"]
    numeric_cols    = source_def.get("numeric_columns", [])
    not_null_cols   = source_def.get("not_null_columns", [])
    skip_col_count  = source_def.get("skip_col_count", False)
    skip_has_col    = source_def.get("skip_has_col", False)
    col_count       = len(expected_cols)
    source_path     = SOURCE_BASE_PATH + folder + "/"
    glob_filter     = FORMAT_GLOB.get(file_format, "*.xlsx")

    # e.g. Tesco -> customer_tesco, M+M -> customer_m_m
    table_name = f"customer_{folder.lower().replace(' ', '_').replace('+', '_')}"

    # Expectation key helper
    def to_key(col_name: str) -> str:
        return col_name.lower().replace(" ", "_").replace("/", "_").replace("+", "_")

    # ------------------------------------------------------------------
    # Build expectation dict dynamically
    # skip_col_count and skip_has_col allow bypassing structural checks
    # for sources like XML where rows can legitimately have missing attrs
    # ------------------------------------------------------------------
    expectations = {}

    if not skip_col_count:
        # _row_number is added by UDF so subtract 1 from map key count
        expectations["col_count_exact"] = (
            f"cardinality(map_keys(_file_data[0])) - 1 = {col_count}"
        )

    if not skip_has_col:
        for col in expected_cols:
            expectations[f"has_col_{to_key(col)}"] = (
                f"map_contains_key(_file_data[0], '{col}')"
            )

    for col in not_null_cols:
        expectations[f"not_null_{to_key(col)}"] = (
            f"exists(_file_data, r -> r['{col}'] IS NOT NULL)"
        )
    for col in numeric_cols:
        expectations[f"is_numeric_{to_key(col)}"] = (
            f"forall(_file_data, r -> r['{col}'] IS NULL OR r['{col}'] RLIKE '^-?[0-9]+(\\.[0-9]+)?$')"
        )

    # ------------------------------------------------------------------
    # Table function
    # ------------------------------------------------------------------
    def table_fn():
        base = (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "binaryFile")
            .option("cloudFiles.includeExistingFiles", "true")
            .option("cloudFiles.useNotifications", "false")
            .option("pathGlobFilter", glob_filter)
            .option("recursiveFileLookup", "true")
            .load(source_path)
        )

        if file_format == "xlsx":
            parse_fn = make_parse_xlsx(header_row)
            df = base.withColumn("_file_data", parse_fn(F.col("content")))
        elif file_format == "xls":
            df = base.withColumn("_file_data", parse_xls(F.col("content")))
        elif file_format == "xml":
            df = (
            base
            .withColumn("_xml_str",
                F.decode(F.col("content"), "windows-1250"))
            .drop("content")
            .withColumn("_row_elements",
                F.split(F.col("_xml_str"), "<row "))
            .drop("_xml_str")
            .select("*",
                F.posexplode("_row_elements").alias("_pos", "_row_str"))
            .drop("_row_elements")
            .filter(F.col("_pos") > 0)
            .withColumn("_row_data",
                parse_xml_row(F.col("_row_str"), F.col("_pos") - 1))
            .drop("_row_str", "_pos")
            # ── Regroup back to one row per file ──────────────────────────
            # collect_list gathers all parsed row maps back into one array
            # grouped by file path, restoring the one-row-per-file structure
            # consistent with xlsx/xls sources
            .groupBy(
                "path",
                "modificationTime",
                "length",
            )
            .agg(
                F.collect_list("_row_data").alias("_file_data")
            )
        )
        else:
            raise ValueError(
                f"Unsupported file_format '{file_format}' for source '{folder}'. "
                f"Supported: xlsx, xls, xml."
            )

        df = df.drop("content")
        df = with_audit_cols(df, folder)

        return df.select(
            "_file_data",
            "source",
            "_load_timestamp",
            "_source_file_path",
            "_report_year",
            "_report_month",
            "_date_key",
            "_frequency",
            "_source_file_modified_at",
            F.col("length").alias("_source_file_size_bytes"),
            "_env",
            "_dataproject",
        )

    # Apply expect_or_fail in reverse order (decorators apply bottom-up)
    fn = table_fn
    for name, constraint in reversed(list(expectations.items())):
        fn = dlt.expect_or_fail(name, constraint)(fn)

    dlt.table(
        name=table_name,
        comment=(
            f"Raw schema - {folder} customer MONTHLY data. "
            f"One row per file. All data stored in _file_data array<map<string,string>>. "
            f"Append-only, full history. Exploded and typed in staging. "
            f"Source: {source_path}"
        ),
        table_properties={
            "quality":                          "raw",
            "ldlp.layer":                       "raw",
            "ldlp.bf":                          BF,
            "ldlp.geo":                         GEO,
            "ldlp.dpname":                      DPNAME,
            "ldlp.source":                      folder.lower(),
            "ldlp.frequency":                   "monthly",
            "delta.autoOptimize.optimizeWrite": "true",
            "delta.autoOptimize.autoCompact":   "true",
        },
    )(fn)


# ---------------------------------------------------------------------------
# Register all parsed tables from SOURCE_DEFINITIONS
# ---------------------------------------------------------------------------
for source_def in SOURCE_DEFINITIONS:
    make_source_table(source_def)