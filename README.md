# KGR Sync: PostgreSQL to iDAI.field

**Script**: `various_scripts/kgr_sync_pg_to_field.py`
**Config**: `config/kgr_sync_config.json`
**Version**: February 2026

---

## 1. Overview

This script performs a one-way initial sync of archaeological building damage assessment data from a PostgreSQL database (KGR schema) into an iDAI.field project (CouchDB). It maps PostgreSQL table data into iDAI.field documents with proper relations, categories, and field mappings.

### What it does

1. **Reads** data from 5 PostgreSQL table groups: `operational_area`, `building`, `floor0-3`/`floor-1`, `findings`, `damage_floor0-3`/`damage_floor-1`
2. **Adds custom field definitions** (`kgr_chru_datamodel:*`) to the project's existing KGR configuration
3. **Creates iDAI.field documents** with correct hierarchy and relations
4. **Maps values** from English PG data to German KGR valuelist entries where possible
5. **Creates subcategory documents** (Kgr:WallOutside, Kgr:RoofOutside) from building component data

### Document hierarchy created

```
Place: Einsatzort
  +-- Survey: OA-xx          (from operational_area, with geometry)
  |     +-- Find: ...         (from findings, linked to Survey)
  +-- Building: B-xx          (from building, with geometry)
  |     +-- Kgr:WallOutside   (facade data extracted from building row)
  |     +-- Kgr:RoofOutside   (roof data extracted from building row)
  |     +-- Level: L-xx       (from floor tables)
  |     +-- Kgr:Damage: ...   (from damage_floor tables)
  +-- Survey: Unzugeordnet    (catch-all for orphan Finds)
```

---

## 2. Prerequisites

- **Python environment**: `conda run -n gisfield` with packages: `requests`, `pandas`, `geopandas`, `sqlalchemy`, `shapely`
- **iDAI.field Desktop** running locally (default: `http://localhost:3001`)
- **PostgreSQL** access to the KGR database at `sql.dainst.org`
- **KGR configuration imported**: The target project **MUST** have the KGR configuration imported via Field Desktop **before** running the script. The script will abort if no Kgr: categories are found.

### Setup steps

1. Create a new project in Field Desktop
2. **Import the KGR configuration file** (Field Desktop -> Project -> Settings -> Import Configuration -> select `kgr.configuration` file). This step is mandatory — the script will refuse to run without it.
3. Edit `config/kgr_sync_config.json` — set the `database` name to match your project
4. Run: `conda run -n gisfield python scripts/kgr_sync_pg_to_field.py`

---

## 3. Configuration File

**`config/kgr_sync_config.json`**:

| Key | Description |
|-----|-------------|
| `postgresql.url` | PostgreSQL connection string |
| `postgresql.schema` | Data schema (e.g., `kgr_2024_dai`) |
| `postgresql.listen_schema` | Valuelist reference schema (e.g., `listen`) |
| `idaifield.url` | Field Desktop URL (default: `http://localhost:3001`) |
| `idaifield.database` | Target project/database name |
| `idaifield.username` / `password` | CouchDB auth credentials |
| `prefix` | Custom field prefix (default: `kgr_chru_datamodel`) |
| `dry_run` | Set `true` to preview without writing |
| `place_identifier` | Top-level Place name (default: `Einsatzort`) |
| `orphan_survey_identifier` | Catch-all for unlinked Finds (default: `Unzugeordnet`) |

---

## 4. Field Mapping Strategy

The script uses a **two-tier mapping** approach:

### Tier 1: KGR field mapping (semantic)

Where PG columns correspond to existing KGR fields, values are translated and written to the proper KGR field names. This includes English-to-German valuelist translation.

| PG Source | KGR Target | Example |
|-----------|------------|---------|
| `building.construction_materials` | `buildingMaterialKGR` | "Brick" -> "Ziegel" |
| `building.facade_condition_class` | `buildingCondition` | "CC2" -> "CC2" |
| `building.facade_*` columns | `Kgr:WallOutside` document | Material, damages, condition |
| `building.roof_*` columns | `Kgr:RoofOutside` document | Type, material, damages |
| `damage.damage_type` | `damages-kgr-str` | "Crack" -> "riss1" |
| `damage.condition_class` | `damage-level` | "CC3" -> "4-Schwer" |

### Tier 2: Custom fields (unmapped PG columns)

All PG columns without a KGR equivalent are stored as `kgr_chru_datamodel:*` custom fields. These appear in a "pgData" group in the form layout. The script automatically adds field definitions and embedded valuelists for these fields to the project configuration.

### Built-in field mappings

These PG columns are mapped to standard iDAI.field built-in fields:

| PG Column | Built-in Field | Categories |
|-----------|---------------|------------|
| `processor` | `resource.processor` | Survey, Building, Level, Kgr:Damage |
| `name` / `building_name` / `object_name` | `resource.shortDescription` | Survey, Building, Find |
| `site_description` / `building_notes` / `notes` | `resource.description` | All |
| `date` | `resource.date` | Survey, Building, Level, Kgr:Damage |

---

## 5. Sync Process (Step by Step)

| Step | Function | Description |
|------|----------|-------------|
| 0 | `ensure_project_and_config()` | Verifies Project and Configuration documents exist |
| A | `apply_kgr_config()` | Adds custom PG field definitions to the imported KGR config |
| C | Processor normalization | Collects all processor names, builds abbreviation map (e.g., "BF" -> "Bernhard Fritsch"), updates Project staff list |
| 1 | Surveys | Creates Survey documents from `operational_area` (with PostGIS geometry) |
| 2 | Buildings | Creates Building docs + Kgr:WallOutside + Kgr:RoofOutside subcategories |
| 3 | Levels | Creates Level documents from `floor-1` through `floor3` tables |
| 4 | Finds | Creates Find documents from `findings` table |
| 5 | Damage | Creates Kgr:Damage documents from `damage_floor-1` through `damage_floor3` |
| Save | `save_docs()` | Bulk-writes all documents to CouchDB |

The script is **idempotent for identifiers** — existing documents (matched by identifier) are skipped, not overwritten.

---

## 6. Known Limitations

- **One-way sync**: PG -> iDAI.field only. Changes in Field Desktop are not synced back.
- **Initial sync only**: The script skips existing identifiers. To re-sync, delete the project and start fresh.
- **Unmatched fields**: Many PG columns (foundation_*, construction_*, detailed damage measures) have no direct KGR equivalent and remain as custom `kgr_chru_datamodel:*` fields.
- **Valuelist gaps**: Some PG values (e.g., "Bitumen Coating") have no exact KGR match and are mapped to "Sonstige" (Other) or dropped.
- **Orphan Finds**: Finds without a matching building_id or op_area_id are assigned to the "Unzugeordnet" Survey.
- **WallOutside/RoofOutside**: Only created if mapped facade/roof columns contain non-empty values that match KGR valuelists.

---

## 7. File Reference

### Files to deliver

| File | Purpose |
|------|---------|
| `various_scripts/kgr_sync_pg_to_field.py` | Main sync script |
| `config/kgr_sync_config.json` | Configuration (users must edit `database` field) |
| `output/kgr_improved_config.json` | Embedded valuelist definitions for custom PG fields |
| This documentation | User handbook |

### Reference files (not needed for running)

| File | Purpose |
|------|---------|
| `output/kgr_test7_config.json` | Saved copy of the developer's KGR configuration |
| `output/Config-KGR-v362.json` | Obsolete KGR config (do not use) |
| `output/kgr_config_comparison.md` | Field comparison report |
| `output/kgr_field_mapping_assessment.md` | Built-in vs custom field analysis |
| `output/valuelists_v362.json` | KGR valuelist values reference |
