# LDLP Data Platform — Project Handoff Summary

_Context document to seed a new chat. Captures the state of the LDLP (Lakeflow
Data Platform) build as of this session._

---

## 1. Project overview

- **What it is:** a retail/POS sales analytics platform on **Azure Data Factory (ADF) + Azure Databricks (DLT) + Unity Catalog**, for JTI. Ingests sales data from multiple sources, harmonises it against CRM master data, and will serve a star schema to Power BI. Tracks **competitor** sales alongside own-brand (own-vs-competitor / market-share analytics).
- **Roles:** *Peter* is the developer (learning data engineering on the job). A **project leader** owns architectural decisions; several open questions are deferred to him (see §10).
- **Three source tracks:** CRM (Synapse), customer vendor files (uploaded to landing), OpenData API (Slovak Statistics Office).

### Working preferences (please follow)
- Work **incrementally** — resolve one thing at a time, don't tackle many at once.
- **Write code only when explicitly asked** — don't proactively offer code.
- Keep replies **concise, minimal formatting**.
- Peter is learning — **explain processes / good practices** when something is more advanced.

---

## 2. Layer naming — IMPORTANT (this changed mid-project)

**Medallion mapping (current, correct):**

```
raw  →  enriched (SILVER)  →  curated (GOLD)
```

- These are the actual **Unity Catalog schemas**: `raw`, `enriched`, `curated`.
- **Landing** is a **storage container** where customers upload files — NOT a UC schema.
- Earlier in the project the layers were described as `Landing → Raw → Curated → Cleaned` (curated=silver, cleaned=gold). **That is obsolete.** The real UC schemas are `raw / enriched / curated`. If older files/notes say "cleaned" or "curated=silver", treat as stale.
- **Convention adopted:** code talks in **medallion roles** (raw / silver / gold); the **literal schema names** (`enriched`, `curated`) live only in pipeline configuration and comments. This makes code immune to future schema renames (a rename already bit us once — the old `ldlp.curated_schema` key became a lie when curated moved from silver to gold).

---

## 3. Infrastructure identifiers (dev)

- **Storage account:** `dlmsfnex5038regeeupdev`
- **Unity Catalog:** `ldlp-ms-sl-dp-default-dev` (hyphens → must be backtick-quoted in SQL)
- **Containers:** `landing` (ADF-only; **Databricks cannot access landing**), `raw`
- **Path root:** `ms/sl/dp/...` (e.g. `ms/sl/dp/crm/`, `ms/sl/dp/customer/`)
- **Secret scope (from history):** `ldlp-ms-sl-dp`
- **Developer AD group:** `Cloud-ldlp-ms-sl-dp-developers` (roles inherited at resource-group scope)
- Environment is expected to change (dev → test → prod); the storage account name is the env-specific part. All env-specific values must come from **pipeline configuration**, never hardcoded.

---

## 4. CRM track — BUILT end to end (raw → enriched → curated)

**Source:** Azure Synapse Analytics, schema `MS_GLB_ENT_TMECS14_SK`, ~two dozen views.

### 4a. ADF ingestion (Synapse → raw) — BUILT
- Pipeline **`pl_ingest_CRM_to_ADLS2`** copies each view to raw as parquet.
- **Incremental** via watermark column `_TF_LAST_UPDATE`, using `WHERE _TF_LAST_UPDATE BETWEEN <prev> AND <max>` (inclusive both ends — boundary row re-pulled each run, harmless for SCD2).
- First load: watermark lookup fails → init to `1900-01-01` → full `SELECT *`.
- Output: `raw/ms/sl/dp/crm/{view}/{yyyy}/{MM}/{dd}/{view}_{maxwatermark}.parquet` (filename carries the **new max** watermark; empty `TOP 0` files on no-change runs carry the **previous** watermark → `1900` in name means the view was empty).
- Watermarks: `raw/ms/sl/dp/crm/_watermarks/{view}/latest.json`.
- **Trigger:** daily, timezone set to **UTC** (folder uses `utcNow()`, so UTC trigger keeps folder date aligned).
- Widened from 2 test views (`Product_Dim`, `Account_Dim`) to **all views**.
- Three views lack `_TF_LAST_UPDATE` → full-load: **`Date_Dim`, `Month_Dim`, `Sales_Dim`**.
- Datasets: `DS_ADLS_CRM_Parquet`, `DS_ADLS_CRM_Watermark`, `DS_CRM_Synapse_View`. Linked services: `AzureDataLakeStorage_DEV`, `CRM_AzureSynapseAnalytics`.

### 4b. CRM delete behaviour — investigated, CRM soft-deletes (three patterns)
- **`Deleted` (+ `Deleted_DRP`)** flag — ~90 transactional/attachment views → candidate for `apply_as_deletes`.
- **`Status` + `Inactivated_since` + `Reason_for_inactivation`** — Account_Dim (no `Deleted` col). Business state → track as a normal attribute, NOT a delete.
- **`Active` + `Start_date` / `End_date`** — Product_Dim (no `Deleted` col). Business state.
- Technical/audit columns to exclude: the `_TF_*` family (`_TF_CHECKSUM`, `_TF_ETL`, `_TF_LAST_UPDATE`, `_TF_*_S_ID` source IDs).
- Keys: `_SK_<entity>` (e.g. `_SK_ACCOUNT`, `_SK_PRODUCT`). Other `_SK_*` cols are FK references to keep.

### 4c. raw → enriched (silver, SCD2) — BUILT
- DLT pipeline **`pl_crm_raw_to_enriched.py`**, config-driven via **`crm_scd2.yaml`**.
- Auto Loader (`cloudFiles`, parquet) staging view per entity → **`create_auto_cdc_flow`** (SCD2). (`apply_changes` is the old name; `create_auto_cdc_flow` is current.)
- SCD2 manages `__START_AT` / `__END_AT`. **No default `is_current`** — current = `__END_AT IS NULL`.
- Lineage cols `_source_file` / `_load_ts` stamped in staging; kept in target but NOT in `track_history_column_list`, so they update in place without minting versions.
- Destination schema (`enriched`) set in **pipeline config**, not code.

### 4d. enriched → curated (gold, current-only) — BUILT
- DLT pipeline **`pl_crm_enriched_to_curated.py`** — separate pipeline (one DLT pipeline = one target schema).
- Reads silver tables from `enriched`, filters `__END_AT IS NULL`, drops bookkeeping + lineage, writes to `curated`.
- Batch **materialized view** (recomputes current snapshot each run) — vs the silver **streaming table** (must accumulate history incrementally).
- Optional per-entity **`filter`** (e.g. Product carries `filter: "Level = 'Product'"` to keep leaf products only, excluding category/brand hierarchy rows — carried from an old placeholder, **needs PL confirmation**).

### `crm_scd2.yaml` fields
`entity`, `target`, `keys`, `sequence_by`, `scd_type`, `except_column_list`, `track_history_column_list`, `comment`; optional `delete_when` (→ apply_as_deletes), `filter` (gold), `gold_target`, `load_type` (reserved for full-load views). Currently defines **Account_Dim** and **Product_Dim** with starter `track_history_column_list`s (to be reviewed with PL).

---

## 5. Customer track — BUILT through raw

**Source:** vendors upload monthly POS files (xlsx / csv / xml; **xls retired**, vendors now send xlsx) to the `landing` container under `ms/sl/dp/customer/<vendor>/<yyyy>/<MM>/`.

### 5a. ADF landing → raw copy — BUILT
- Pipeline **`pl_landing_to_raw_all_cust`** (ADF): **storage-event-triggered**, binary copy (bytes untouched) landing → raw.
- **Three triggers** (one suffix each): `.xlsx`, `.xml`, `.csv`, all → same pipeline, scoped to `ms/sl/dp/customer/`.
- **Trigger parameter mapping** (critical fix):
  - `sourceFolder` → `@replace(triggerBody().folderPath, 'landing/', '')` (trigger passes the container-prefixed path; must strip the leading `landing/` or you get `landing/landing/...` PathNotFound).
  - `sourceFile` → `@triggerBody().fileName`.
- **Filename** in raw: `{originalname}_{yyyyMMdd}_{HHmmss}.{ext}` — timestamp inserted before the extension via an ADF expression that splits on the last dot (preserves the source extension; contrast CRM which hardcodes `.parquet` because ADF generates it). `utcNow()`, UTC.
- Audit is derived downstream by Databricks from the **path** + `_metadata` (no columns stamped inside the file). ADF just lands bytes in a dated path with a timestamped name.

### 5b. Databricks parser (raw files → raw tables) — BUILT
- DLT pipeline (also named **`pl_landing_to_raw_all_cust`** in Databricks; file **`pl_customer_landing_to_raw.py`**), config-driven via **`customer_sources_config.yaml`**.
- Reads raw files via **binaryFile Auto Loader**, parses per format, lands **one-row-per-file** raw tables: all cells in **`_file_data: array<map<string,string>>`** + audit columns. Uniform schema across all vendors.
- Parsers: **xlsx** (openpyxl, configurable header row), **xml** (per-row UDF + `collect_list` regroup — memory-safe for large files), **csv** (new UDF: decode by encoding, split on delimiter, honours `skip_quotes`).
- Dynamic `expect_or_fail` validation built per-vendor (col-count, has-col, not-null, numeric — numeric regex allows negatives).
- `customer_data_load` = append-only file registry across all vendors.
- Also creates empty tables for configured vendors with no files yet (expected — tables defined by pipeline structure, not by data presence).

### Why one-row-per-file `_file_data`
Uniform schema across heterogeneous vendors (one generic pipeline, add-vendor = config change), atomic file-level validation (whole file accept/reject), faithful raw (no typing decisions baked in). Must be **exploded at the enriched layer**.

### `customer_sources_config.yaml` fields
`folder`, `table`, `file_format`, `header_row`, `encoding`, `delimiter`, `expected_columns`, `numeric_columns`, `not_null_columns`, `skip_col_count`, `skip_has_col`, `skip_quotes`.

### The 7 vendors (all parsing into raw, verified)
| Vendor | Format | Notes |
|---|---|---|
| **Tesco** | xlsx | header row 1 |
| **Billa** | xlsx | header row 1; generic `Column1..7` names |
| **GGT** | xlsx | **header row 8** (rows 1–7 metadata) |
| **Mikros** | xlsx | formerly .xls |
| **M+M** | xml | **windows-1250**; ~200 MB, **475,250 rows**; `skip_col_count`/`skip_has_col`. `idzbo`/`ean` sometimes hold catch-all text ("Ostatné", "BTV ostatny") not real product IDs. **OOM on `collect_list`** — currently masked by serverless autoscale-on-retry (see Option B, §9). |
| **BECICA** | csv | **folder is ALL CAPS "BECICA"**; `;` delimiter; **cp1250** (verified clean); `skip_col_count`; `skip_quotes` (stray unbalanced `"` in product names); negatives = returns; `POS1001` sentinel/dummy rows present |
| **CBA Verex** | xlsx | **header row 2** (row 1 is a title); GGT-branded (CBA Verex is a retail chain selling GGT-supplied products — expected); **two sheets**, use sheet 1 (per-product-per-store); `Celková hodnota` **subtotal rows** interleaved (filter at enriched, not raw); `Množstvo` format `1.000`/`376.000` = whole units with `.000` padding (decimal, NOT thousands) |

---

## 6. Config / environment pattern (both tracks)

- All env-specific values in **DLT pipeline configuration keys**: `ldlp.raw_base`, `ldlp.silver_schema`, `ldlp.config_path`, `ldlp.env`, `ldlp.dataproject`. No hardcoded storage accounts/paths in code.
- **Target schema set in the pipeline's destination**, not in code (so table names stay unqualified). One DLT pipeline publishes to one schema (why silver and gold are separate pipelines).
- Gold pipeline reads silver via `ldlp.silver_schema` = `` `ldlp-ms-sl-dp-default-dev`.enriched `` (backticks for hyphens).

---

## 7. Key technical gotchas established

- **Databricks cannot access the `landing` container** — ADF-only. This is why ADF hops files landing→raw for customers (and Synapse→raw for CRM); Databricks continues from raw.
- Use `_metadata.file_path` (or binaryFile's `path` column), **not** `input_file_name()` (unsupported in UC).
- `schema=` on `@dlt.table` unsupported; `MERGE INTO` via `spark.sql()` unsupported inside DLT.
- **One DLT pipeline = one target schema** → silver/gold are separate pipelines.
- `recursiveFileLookup` is **redundant with Auto Loader** (cloudFiles recurses by default); it WAS needed for a batch `spark.read.parquet` over `yyyy/MM/dd` subfolders.
- **Blob storage has no real folders** — they're name prefixes. Empty folders need a `.keep` placeholder blob. On ADLS Gen2 (hierarchical namespace) directories persist after deleting the last blob.
- Storage **event triggers don't fire retroactively** — only for blobs created after the trigger exists.
- Serverless **autoscales resources on OOM retry** (first attempt fails, retry gets more heap).
- `create_auto_cdc_flow` SCD2 → `__START_AT`/`__END_AT`, current = `__END_AT IS NULL`.
- ADF publish quirks: Git mode needs publishing from the collaboration branch; "invalid reference / recreate in Git mode" resolved via **Overwrite live mode** (Manage → Git configuration).

---

## 8. Permissions (all now granted)

Via group `Cloud-ldlp-ms-sl-dp-developers` (resource-group scope):
- **Monitoring Contributor** — unblocks Azure Monitor failure alerts (`Microsoft.Insights/metricalerts/write`).
- **EventGrid EventSubscription Contributor** — unblocks the storage-event trigger (`Microsoft.EventGrid/EventSubscriptions/Write`). Trigger now works.
- Also present: Storage Blob Data Contributor, Log Analytics Contributor, Reader, Cost Management Reader.

---

## 9. Outstanding work (roughly in priority order)

1. **Customer enriched layer** (biggest next build): explode `_file_data` → transaction rows; map each vendor's columns to canonical fields (**product / account / quantity / date**); apply the deferred cleanups (CBA Verex `Celková hodnota` subtotal filter; M+M "Ostatné" catch-all rows; CBA Verex `.000` decimal cast); **MDM bridge matching** with quarantine.
2. **MDM bridge matching** sub-system: map vendor free-text product/account names → CRM keys; quarantine unmatched; Bridge Excel out to a business user, filled and re-ingested; previously-unmatched resolve on next run.
3. **Customer curated (star schema):** unified sales fact table + account/product/competitor dimensions; own-vs-competitor split.
4. **OpenData track** — entirely unbuilt: **ADF** ingestion (API → raw) → enriched → curated (market context).
5. **M+M Option B:** switch M+M to arrive **pre-exploded** (skip the `collect_list` regroup) to eliminate the OOM — naturally slots in at the enriched build.
6. **Serving:** Databricks SQL Warehouse (Unity Catalog) → Power BI.
7. **Failure alerting:** permission is now granted; the Azure Monitor alert rule on "Failed pipeline runs" (email action group) just needs building.
8. Possible future parser field: **`sheet`** (index/name) for multi-sheet xlsx vendors whose wanted sheet isn't the first.

---

## 10. Project-leader question list (carried, unresolved)

- CRM **`track_history_column_list`** review — including whether to **drop `Category`** from Product versioning (churns often, creates many versions).
- Gold **`End_date` / active-product** filter — keep-and-flag vs. physically drop discontinued products (affects whether historical sales still join).
- **`stg_*` views intent** — ephemeral views (current) vs. persisted tables (queryable for forensic debugging); and whether to add `@dlt.expect` validation to them.
- **`Deleted_DRP`** meaning vs `Deleted`.
- **Vendor upload access model** — do vendors upload manually? If so, how is per-vendor write access scoped (per-vendor SAS / managed identity)?
- **Negative quantities = returns** across vendors — confirm keep, not reject.
- **M+M "Ostatné" / "BTV ostatny"** catch-all product rows — quarantine as unmapped, or a business rule?
- **`POS1001`** sentinel/dummy rows in Becica.
- **CBA Verex subtotal rows** — confirm filtering at enriched (identifiable: product-code col contains "Celková hodnota", store col empty).
- The **`Level = 'Product'`** filter on the gold product dim — confirm it should stay.

---

## 11. Files produced this session

Code/config (deploy to the Databricks workspace / repo):
- **`crm_scd2.yaml`** — CRM SCD2 + gold config (Account_Dim, Product_Dim).
- **`pl_crm_raw_to_enriched.py`** — CRM silver (SCD2) DLT pipeline.
- **`pl_crm_enriched_to_curated.py`** — CRM gold (current-only) DLT pipeline.
- **`customer_sources_config.yaml`** — customer parser config (7 vendors).
- **`pl_customer_landing_to_raw.py`** — customer raw parser DLT pipeline.
- **`ldlp_architecture_current.html`** — interactive architecture diagram (for the project leader).

ADF objects (built in ADF Studio, not files): `pl_ingest_CRM_to_ADLS2` (CRM ingest), `pl_landing_to_raw_all_cust` (customer landing→raw copy + 3 event triggers).

_Note: config filenames referenced in the pipeline code are `crm_scd2.yaml` and `customer_sources_config.yaml`; ensure `ldlp.config_path` points at the deployed locations._
