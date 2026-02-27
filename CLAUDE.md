# KGRfield - Claude Code Instructions

## Project Overview

Python sync tool that transfers KGR (Kulturgutrettung / Cultural Heritage Rescue) building damage assessment data from PostgreSQL to iDAI.field (CouchDB-based archaeological documentation system).

**Main script**: `scripts/kgr_sync_pg_to_field.py`
**Config**: `config/kgr_sync_config.json`
**Documentation**: `KGR_Sync_Documentation.md`

---

## PostgreSQL Database Architecture

### Connection
```
Host: sql.dainst.org:5432
Database: kgr_demo
Schema (data): kgr_2024_dai
Schema (valuelists): listen
Auth: see config/kgr_sync_config.json
```

### Data Tables (schema: `kgr_2024_dai`)

| Table | Content | Key Columns | Geometry |
|-------|---------|-------------|----------|
| `operational_area` | Survey/operational areas | operational_area_id, name, address, city, hazard_event | PostGIS (EPSG varies, reprojected to WGS84) |
| `building` | Buildings assessed | building_id, building_nr, building_name, foundation_*, construction_*, facade_*, roof_* | PostGIS |
| `floor-1` to `floor3` | Floors/rooms per building | building_id, floor_nr, room_nr, damage, decorative, movable_heritage | PostGIS |
| `findings` | Mobile heritage finds | finding_id, building_name, floor_nummer, object_name, qr_code, reg_*, cl_*, dry_* | PostGIS |
| `damage_floor-1` to `damage_floor3` | Per-element damage records | building_id, floor_nummer, room_nr, damage_type, condition_class, urgency_class | PostGIS |

### Building Table Structure (60+ columns)

The building table has **per-component flat columns**:
- `foundation_*`: type, materials, damage, condition_class, urgency_class, notes (9 cols)
- `construction_*`: type, materials, damage, condition_class, urgency_class, notes (9 cols)
- `facade_*`: load_bearing, material, damage, condition_class, urgency_class, notes (10 cols)
- `roof_*`: type, material, material_construction, damage, condition_class, urgency_class, notes (12 cols)
- Plus general: building_use, historic_use, accessibility, thw_status, building_area, etc.

### Valuelist Tables (schema: `listen`)

22 reference tables with English dropdown/checkbox values:
- `construction_materials`: Brick, Stone, Clay, Timber, Iron/Steel, Concrete, Other
- `material_facade`: Brick, Stone, Clay, Timber, Plaster, Other
- `material_roofing`: Tiles, Concrete, Bitumen Coating, Clay, Straw, Metal, Timber, Other
- `damage`: Loss of Surface, Crack, Partial Collapse, Collapse, Contamination, etc. (13 values)
- `risk_assessment`: Potential Loss of Historical Surface/Substance, Secondary Damage, Impact on Safety
- `foundation`: Point/Strip/Plate Foundation
- `type_roof`: Flat Roof, Gable-end, Hipped-end, Monopitch, Mansard, Jerkinhead, Other
- `processor`: Reference list of team members (canonical full names)
- Plus: stabilization, protection, salvage, evacuation, emergency, documentation_assessment, etc.

### Data Patterns
- Condition classes: CC0 (no symptoms) to CC4 (collapse)
- Urgency classes: UC1 (long-term) to UC4 (immediate)
- PG arrays: Stored as `{"value1","value2"}` text literals, parsed with `parse_pg_array()`
- Dates: ISO format (2025-10-16) or datetime (2025-10-23T14:47:14.970)
- Processors: Abbreviated forms exist (BF, MZ, Sarah) — normalized to full names

---

## iDAI.field (CouchDB) Architecture

### Connection
```
URL: http://localhost:3001
Auth: ('username', 'password')  -- per-project password
Each project = one CouchDB database
```

### Document Structure

Every resource document follows this structure:
```json
{
  "_id": "uuid-string",
  "_rev": "auto-managed-by-couchdb",
  "resource": {
    "id": "same-as-_id",
    "identifier": "human-readable-name",
    "category": "Building",
    "shortDescription": "display name",
    "description": "long text",
    "processor": "Person Name",
    "date": {"value": "16.10.2025 00:00"},
    "relations": {
      "liesWithin": ["parent-resource-id"],
      "isRecordedIn": ["operation-resource-id"]
    },
    "geometry": {"type": "Polygon", "coordinates": [...]},
    "customPrefix:fieldName": "value"
  },
  "created": {"user": "username", "date": "2025-10-16T00:00:00.000Z"},
  "modified": [{"user": "username", "date": "2025-10-16T00:00:00.000Z"}]
}
```

### Category Hierarchy & Relations Rules

```
Project (top-level, _id MUST be "project")
  +-- Operation (Place, Survey, Building, Trench, Excavation)
  |     relations: {} or liesWithin: [Place]
  +-- BuildingPart (Level, Damage, Kgr:WallOutside, Kgr:Damage, etc.)
  |     relations: isRecordedIn: [Operation-id]
  +-- Find (Find, Pottery, Bone, etc.)
  |     relations: isRecordedIn: [Operation-id], optionally liesWithin: [parent]
  +-- Image (Photo, Drawing)
        relations: depicts: [resource-id] -- NEVER liesWithin or isRecordedIn!
```

**Critical relation rules:**
- Place is top-level: `relations: {}`
- Operations (Survey, Building) inside Place: `liesWithin: [place_id]`, NO isRecordedIn
- Direct children of Operation: `isRecordedIn: [operation_id]`, NO liesWithin
- Deeper children: `liesWithin: [parent_id]` + `isRecordedIn: [operation_id]`
- Place can ONLY contain Operations (not Finds directly)
- BuildingParts (Damage, Level, WallOutside) MUST be under a Building (isRecordedIn)
- Photos/Drawings have ONLY `depicts` -- NEVER liesWithin or isRecordedIn

### Configuration Document

Every project has a special document with `_id: "configuration"`:

```json
{
  "_id": "configuration",
  "resource": {
    "forms": { ... },
    "order": [ ... ],
    "valuelists": { ... },
    "languages": { "de": {} }
  }
}
```

**CRITICAL rules for the configuration document:**

1. **Config is a DELTA/OVERLAY** on built-in definitions, NOT a full replacement. Built-in categories (Building, Survey, Find, etc.) exist in the app source code. The config only adds/modifies.

2. **Every category in `forms` MUST be in `order`** -- categories missing from order crash the app on startup.

3. **Custom fields MUST use a prefix** (e.g., `kgr_chru_datamodel:fieldName` or `Kgr:CategoryName`). Never use unprefixed names for custom fields.

4. **Field definitions** require at minimum `inputType`:
   ```json
   "prefix:fieldName": {"inputType": "checkboxes"}
   ```

5. **Valid inputTypes**: `input` (text), `dropdown` (single select), `checkboxes` (multi-select), `boolean`, `date` (archaeological dating with periods -- NOT calendar dates!), `unsignedInt`, `float`, `multiInput` (tags), `text` (multiline), `dimension`, `literature`, `composite`, `radio`

6. **Do NOT put built-in groups** (stem, hierarchy, workflow) in the config -- only add custom groups.

7. **Valuelists** can be:
   - **Built-in**: Shipped with Field Desktop (906 valuelists in v3.6.2), referenced by name
   - **Config-embedded**: Defined in `resource.valuelists` dict, format: `{"valueName": {"label": {"en": "Label"}}}`
   - **Standalone CouchDB docs**: Separate documents with `_id: "valuelist-name"`

8. **Hidden fields**: Use `"hidden": ["fieldName1", "fieldName2"]` in a form to hide built-in fields that conflict with custom ones.

### KGR-Specific Configuration (Kgr: prefix)

The KGR project configuration (imported by users before sync) adds:
- **17 new categories** with `Kgr:` prefix: WallOutside, RoofOutside, WallInside, FloorInside, CeilingInside, Stairs, Window, Door, Damage, Equipment, FindRecording, OrganicAnimal/Vegetable/Human/Synthetic, AnorganicMineral/Metal
- **Building:default** with 34 KGR fields (buildingCondition, buildingMaterialKGR, buildingDamage-str/-mat, etc.)
- All Kgr: categories have `parent: BuildingPart` or `parent: Find`
- References built-in KGR valuelists (German values): Building-material-kgr (59 materials), Building-technique-kgr (33 techniques), damages-kgr-str (14 structural), damages-kgr-mat (18 material), Building-condition-kgr (CC0-CC4), priority-kgr (UC values), etc.

### KGR Valuelists (Built-in to Field Desktop)

Source: https://github.com/dainst/idai-field/tree/master/core/config/Library/Valuelists

| Valuelist | Field | Values (examples) |
|-----------|-------|-------------------|
| Building-material-kgr | buildingMaterialKGR | Naturstein, Ziegel, Beton, Holz, Metall, Glas, Mörtel, Stuck, Fliesen, Sonstige (59 total) |
| Building-technique-kgr | buildingTechniqueKGR | Massivbau, Skelettbau, Fachwerkbau, Bruchsteinmauerwerk, Ziegelmauerwerk, etc. (33 total) |
| Building-construction-kgr | buildingConstructionKGR | Fundament, Außenmauer, Innenmauer, Decke, Gewölbe, Treppe, Tür, Fenster, etc. (43 total) |
| Building-condition-kgr | buildingCondition | CCO, CC1, CC2, CC3, CC4 (note: CC0 is stored as "CCO" with letter O) |
| damages-kgr-str | buildingDamage-str | totalCollapse, partialCollapse, fractures, riss1-4, cavities, delamination, etc. (14 total) |
| damages-kgr-mat | buildingDamage-mat | exfoliation, spalling, erosion, moisture, corrosion, fireDamage, etc. (18 total) |
| damages-kgr | wallOutsideDamages | Combined structural+material damages (31 total) |
| priority-kgr | buildingPriority | UC-, UC0, UC1, UC2, UC4 |
| level-kgr | damage-level | 1-SehrGering, 2-Gering, 3-Mittel, 4-Schwer, 5-SehrSchwer |
| Monument-risks-kgr | buildingRisks | fallHazard, dissolution, fallDownBricks, waterSanitary, etc. (10 total) |
| firstAid-kgr | monumentFirstAid | KeineMaßnahme, Abriss, HorizontalStützen, Reparatur, etc. (9 total) |
| roofing-kgr | roofOutsideMaterial | Beton, Blei, Dachstein, Dachziegel, Erde, Holz, Schiefer, Sonstige |
| BuildingType-kgr | buildingTypeKGR | Altar, Basilika, Kirche, Moschee, Palast, Tempel, Theater, Wohnbau, etc. (34 total) |

---

## Sync Script Architecture

### Data Flow
```
PostgreSQL (kgr_2024_dai) --[read]--> Python --[write]--> CouchDB (iDAI.field)
```

### Two-Tier Field Mapping

1. **KGR fields** (semantic mapping): PG columns mapped to KGR field names with English-to-German valuelist translation. Defined in `BUILDING_KGR_MAP`, `FACADE_KGR_MAP`, `ROOF_KGR_MAP`, `DAMAGE_KGR_MAP`.

2. **Custom fields** (`kgr_chru_datamodel:*`): Unmapped PG columns stored with project prefix. Defined in `BUILDING_CUSTOM_MAP`, `DAMAGE_CUSTOM_MAP`, `SURVEY_MAP`, `LEVEL_MAP`, `FIND_MAP`.

### Key Functions

| Function | Purpose |
|----------|---------|
| `apply_kgr_config()` | Adds custom PG field definitions to the imported KGR config |
| `add_custom_pg_fields()` | Injects field defs, groups, and valuelists into config |
| `map_row(row, col_map)` | Maps PG row to `prefix:fieldName` custom fields |
| `map_kgr_fields(row, kgr_map)` | Maps PG row to KGR field names with VL translation |
| `make_doc()` | Creates full iDAI.field document with built-in field mappings |
| `make_wall_outside_doc()` | Creates Kgr:WallOutside from facade_* columns |
| `make_roof_outside_doc()` | Creates Kgr:RoofOutside from roof_* columns |
| `normalize_processor()` | Resolves abbreviations (BF -> Bernhard Fritsch) |

### Built-in Field Mappings (in `make_doc()`)

These PG columns go to standard iDAI.field resource fields (not prefixed):
- `processor` -> `resource.processor` (Survey, Building, Level, Kgr:Damage)
- `name`/`building_name`/`object_name` -> `resource.shortDescription`
- `site_description`/`building_notes`/`notes` -> `resource.description`
- `date` -> `resource.date` as `{"value": "DD.MM.YYYY HH:MM"}`

---

## Common Pitfalls & Lessons Learned

1. **Config-KGR.json is OBSOLETE** -- do not use `output/Config-KGR-v362.json`. The developer provides importable configuration files separately.

2. **Never REPLACE `resource.forms`/`resource.order`** in the config document -- only ADD to them. Replacing deletes built-in categories and crashes the app.

3. **`date` inputType is for archaeological dating** (periods like "Ubaid", "Uruk"), NOT calendar dates. Use `input` for workflow dates like "23.10.2025".

4. **Building-condition-kgr has typo**: CC0 is stored as "CCO" (letter O, not zero) in the valuelist.

5. **PG array literals** look like `{"Floor","Wall"}` -- must be parsed, not treated as strings.

6. **Processor normalization**: PG data has abbreviations (BF, MZ), first names only (Sarah), and case variants (Mz vs MZ). The script auto-resolves these using the `listen.processor` reference table.

7. **Conda on Windows**: `conda run -n env python -c "multiline..."` fails. Must write to a temp .py file for multiline scripts.

8. **Windows console encoding**: Always use `sys.stdout.reconfigure(encoding='utf-8')` at script start.

9. **`_id: "project"`**: The Project document MUST have `_id` set to `"project"` (literal string), not a UUID. Field Desktop expects this.

10. **Identifiers are case-insensitive** in iDAI.field matching.

11. **NEVER embed valuelists in the config document for custom fields**. KGR valuelists are built-in to Field Desktop (source: https://github.com/dainst/idai-field/tree/master/core/config/Library/Valuelists). Adding embedded valuelists with wrong format (e.g., extra fields like `createdBy`, `creationDate`) crashes the app. Custom `kgr_chru_datamodel:*` fields must always use `inputType: "input"` (free text) with no valuelist references. Only KGR-prefixed fields from the imported config may reference the built-in KGR valuelists.

12. **Fields referencing non-existent valuelists crash the app**. If a form's `valuelists` section references a valuelist name that doesn't exist (not built-in and not in `resource.valuelists`), Field Desktop will fail to load the project.

---

## Running the Script

```bash
# Prerequisites: conda environment with gisfield packages
conda run -n gisfield python scripts/kgr_sync_pg_to_field.py
```

### User Workflow
1. Create project in Field Desktop
2. Import KGR configuration (`kgr.configuration` file)
3. Edit `config/kgr_sync_config.json` (set database name + password)
4. Run sync script
5. Open project in Field Desktop to verify
