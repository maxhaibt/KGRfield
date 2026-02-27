# -*- coding: utf-8 -*-
"""
KGR Sync: PostgreSQL -> iDAI.field (initial full sync)
Builds the complete iDAI.field project from the kgr_2024_dai PostgreSQL schema.

iDAI.field hierarchy (confirmed from test DB + GUI):

  Place: Einsatzort                -> relations: {}  (top-level)
    +-- Survey: OA-xx              -> liesWithin: [Einsatzort]
    |     +-- Find: ...            -> isRecordedIn: [OA-xx]
    +-- Building: B-xx             -> liesWithin: [Einsatzort]
    |     +-- Level: ...           -> isRecordedIn: [B-xx]
    |     +-- Damage: ...          -> isRecordedIn: [B-xx]
    +-- Survey: Unzugeordnet       -> liesWithin: [Einsatzort]  (orphan catch-all)
          +-- Find/Damage: ...     -> isRecordedIn: [Unzugeordnet]

  Rules:
    - Place is top-level: relations = {}
    - Operations inside Place: liesWithin: [place_id], NO isRecordedIn
    - Direct children of Operation: isRecordedIn: [operation_id], NO liesWithin
    - Deeper children: liesWithin: [parent] + isRecordedIn: [operation_id]
    - Place can ONLY contain Operations (not Finds/Damage directly)
    - Damage can ONLY live under Building (not Survey)

Uses an improved configuration (output/kgr_improved_config.json) that includes
valuelist definitions from the PostgreSQL 'listen' schema.

Usage:
    conda run -n gisfield python various_scripts/kgr_sync_pg_to_field.py
"""
import json
import os
import uuid
import math
import sys
sys.stdout.reconfigure(encoding='utf-8')

import requests
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine
from shapely.geometry import mapping

# ============================================================
# Load user configuration
# ============================================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)
CONFIG_PATH = os.path.join(PROJECT_DIR, 'config', 'kgr_sync_config.json')

with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
    USER_CONFIG = json.load(f)

PG_URL = USER_CONFIG['postgresql']['url']
PG_SCHEMA = USER_CONFIG['postgresql']['schema']
PG_LISTEN_SCHEMA = USER_CONFIG['postgresql'].get('listen_schema', 'listen')

FIELD_URL = USER_CONFIG['idaifield']['url']
FIELD_DB = USER_CONFIG['idaifield']['database']
FIELD_AUTH = (USER_CONFIG['idaifield']['username'], USER_CONFIG['idaifield']['password'])

PREFIX = USER_CONFIG.get('prefix', 'kgr_chru_datamodel')
DRY_RUN = USER_CONFIG.get('dry_run', False)

IMPROVED_CONFIG_PATH = os.path.join(PROJECT_DIR, USER_CONFIG['improved_config_path'])
PLACE_IDENTIFIER = USER_CONFIG.get('place_identifier', 'Einsatzort')
ORPHAN_SURVEY_IDENTIFIER = USER_CONFIG.get('orphan_survey_identifier', 'Unzugeordnet')
DATE_DEFAULT_TIME = USER_CONFIG.get('date_default_time', '00:00')

print(f"Config loaded from: {CONFIG_PATH}")
print(f"  PG: {PG_SCHEMA} @ ...{PG_URL[-30:]}")
print(f"  iDAI.field: {FIELD_URL}/{FIELD_DB}")
print(f"  Improved config: {IMPROVED_CONFIG_PATH}")

# ============================================================
# Column mappings: PG snake_case -> iDAI.field camelCase
# Only columns that have a matching field in the iDAI.field config.
# Format: { pg_column: field_name_without_prefix }
# ============================================================

SURVEY_MAP = {
    'operational_area_id': 'operationalAreaId',
    # 'date' and 'processor' mapped to built-in fields (see BUILTIN_FIELD_MAP)
    'name': 'name',
    'address': 'address',
    'city': 'city',
    'region': 'region',
    'country': 'country',
    'hazard_event': 'hazardEvent',
    'hazard_event_other': 'hazardEventOther',
    'gross_op': 'grossOp',
    'site_name': 'siteName',
    'monuments_amount': 'monumentsAmount',
    'monuments_category': 'monumentsCategory',
    'monument_category_other': 'monumentCategoryOther',
    'site_context': 'siteContext',
    'area_utilization': 'areaUtilization',
    'area_utilization_other': 'areaUtilizationOther',
    'use_before_event': 'useBeforeEvent',
    'historic_use': 'historicUse',
    'construction_year_known_period': 'consructionYearKnownPeriod',  # typo in datamodel
    'construction_year_unknown_period': 'constructionYearUnknownPeriod',
    'site_description': 'siteDescription',
    'protection_level': 'protectionLevel',
    'protection_level_other': 'protectionLevelOther',
    'authority': 'authority',
    'heritage_inventory_number': 'heritageInventoryNumber',
    'intangible_heritage': 'intangibleHeritage',
    'intangible_heritage_note': 'intangibleHeritageNote',
}

BUILDING_MAP = {
    # 'date' and 'processor' mapped to built-in fields (see BUILTIN_FIELD_MAP)
    'op_area_id': 'opAreaId',
    'building_id': 'buildingId',
    'building_nr': 'buildingNr',
    'building_name': 'buildingName',
    'building_use': 'buildingUse',
    'historic_use': 'historicUse',
    'accessibility': 'accessibility',
    'thw_status': 'thwStatus',
    'remote_assessment': 'remoteAsessment',  # typo in datamodel
    'outside_assessment': 'outsideAssessment',
    'interior_inspection': 'interiorInspection',
    'number_of_floor_above': 'numberOfFloorAbove',
    'known_floors_below': 'knownFloorsBelow',
    'number_of_floor_below': 'numberOfFloorBelow',
    'building_area': 'buildingArea',
    'foundation_type': 'foundationType',
    'foundation_materials': 'foundationMaterials',
    'foundation_materials_other': 'foundationMaterialsOther',
    'foundation_damage': 'foundationDamage',
    'foundation_damage_other': 'foundationDamageOther',
    'foundation_damage_extend': 'foundationDamageExtend',
    'foundation_condition_class': 'foundationConditionClass',
    'foundation_risk_assessment': 'foundationRiskAssessment',
    'foundation_urgency_class': 'foundationUrgencyClass',
    'fundation_notes': 'foundationNotes',  # typo in PG
    'construction_type': 'constructionType',
    'construction_materials': 'constructionMaterials',
    'construction_materials_other': 'constructionMaterialsOther',
    'construction_damage': 'constructionDamage',
    'construction_damage_other': 'constructionDamageOther',
    'construction_damage_extend': 'constructionDamageExtend',
    'construction_condition_class': 'consructionConditionClass',  # typo in datamodel
    'construction_risk_assessment': 'constructionRiskAssessment',
    'construction_urgency_class': 'constructionUrgencyClass',
    'construction_notes': 'constructionNotes',
    'facade_load_bearing': 'facadeLoadBearing',
    'facade_material': 'facadeMaterial',
    'facade_material_other': 'facadeMaterialOther',
    'facade_damage': 'facadeDamage',
    'facade_damage_other': 'facadeDamageOther',
    'facade_damage_extend': 'facadeDamageExtent',  # PG: extend, field: Extent
    'facade_condition_class': 'facadeConditionClass',
    'facade_risk_assessment': 'facadeRiskAssessment',
    'facade_urgency_class': 'facadeUrgencyClass',
    'facade_notes': 'facadeNotes',
    'roof_type': 'roofType',
    'roof_type_other': 'roofTypeOther',
    'roof_material_construction': 'roofMaterialConstruction',
    'roof_material': 'roofMaterial',
    'roof_material_other': 'roofMaterialOther',
    'roof_damage': 'roofDamage',
    'roof_damage_other': 'roofDamageOther',
    'roof_damage_extend': 'roofDamageExtend',
    'roof_condition_class': 'roofConditionClass',
    'roof_risk_assessment': 'roofRiskAssessment',
    'roof_urgency_class': 'roofUrgencyClass',
    'roof_notes': 'roofNotes',
    'recommendation_class': 'recommendationClass',
    'building_notes': 'buildingNotes',
    'uuid': 'uuid',
}

LEVEL_MAP = {
    # 'date' and 'processor' mapped to built-in fields (see BUILTIN_FIELD_MAP)
    'building_id': 'buildingId',
    'floor_nr': 'floorNr',
    'room_nr': 'roomNr',
    'room_designation': 'roomDesignation',
    'accessibility': 'accessibility',
    'room_area': 'roomArea',
    'damage': 'damage',
    'damage_notes': 'damageNotes',
    'rubble': 'rubble',
    'rubble_notes': 'rubbleNotes',
    'decorative': 'decorative',
    'decorative_notes': 'decorativeNotes',
    'movable_heritage': 'movableHeritage',
    'movable_heritage_notes': 'movableHeritageNotes',
    'priority': 'priority',
    'basis_documentation': 'basisDocumentation',
    'heritage_assessment': 'heritageAssessment',
    'm_heritage_acquisition': 'mHeritageAcquisition',
    'notes_dec': 'notesDec',
}

FIND_MAP = {
    'op_area_id': 'opAreaId',
    'building_name': 'buildingName',
    'floor_nummer': 'floorNummer',
    'mov_date': 'movDate',
    'mov_processor': 'movProcessor',
    'finding_id': 'findingId',
    'object_name': 'objectName',
    'object_type': 'objectType',
    'qr_code': 'qrCode',
    'qr_code_cluster': 'qrCodeCluster',
    'qr_code_cluster2cluster': 'qrCodeCluster2Cluster',
    'room': 'room',
    'reg_risk_assessment_condition': 'regRiskAssessmentCondition',
    'reg_condition_complete': 'regConditionComplete',
    'evac_stay': 'evacStay',
    'reg_salvage_transport': 'regSalvageTransport',
    'notes': 'notes',
    'reg_date': 'regDate',
    'reg_processor': 'regProcessor',
    'reg_contamination': 'regContamination',
    'reg_contamination_note': 'regContaminationNote',
    'reg_cleaning': 'regCleaning',
    'reg_photo_docu': 'regPhotoDocu',
    'reg_priority': 'regPriority',
    'reg_acquisition_completed': 'regAcquisitionCompleted',
    'reg_notes': 'regNotes',
    'cl_new_code': 'clNewCode',
    'cl_date': 'clDate',
    'cl_processor': 'clProcessor',
    'cl_type': 'clType',
    'cl_wet': 'clWet',
    'cl_dry': 'clDry',
    'cl_status': 'clStatus',
    'cl_notes': 'clNotes',
    'dry_new_code': 'dryNewCode',
    'dry_processor': 'dryProcessor',
    'dry_depot': 'dryDepot',
    'dry_date': 'dryDate',
    'dry_technique': 'dryTechnique',
    'dry_humidity': 'dryHumidity',
    'dry_temperature': 'dryTemperature',
    'dry_start_time': 'dryStartTime',
    'dry_status': 'dryStatus',
    'dry_depot_status': 'dryDepotStatus',
    'dry_notes': 'dryNotes',
}

DAMAGE_MAP = {
    # 'date' and 'processor' mapped to built-in fields (see BUILTIN_FIELD_MAP)
    'operational_area_id': 'operationalAreaId',
    'building_id': 'buildingId',
    'floor_nummer': 'floorNummer',
    'room_nr': 'roomNr',
    'damage': 'damage',
    'constructional_element_primary': 'constructionalElementPrimary',
    'constructional_element_secondary': 'constructionalElementSecondary',
    'constructional_element_secondary_other': 'constructionalElementSecondaryOther',
    'load_bearing_component': 'loadBearingComponent',
    'damage_type': 'damageType',
    'other': 'other',
    # 'condition_class' mapped to built-in 'condition' field (see BUILTIN_CONDITION_COLUMNS)
    'risk_assessment': 'riskAssessment',
    'urgency_class': 'urgencyClass',
    'documentation_assessment': 'documentationAssessment',
    'documentation_assessment_other': 'documentationAssessmentOther',
    'further_investigation': 'furtherInvestigation',
    'further_investigation_other': 'furtherInvestigationOther',
    'clearing_cleaning': 'clearingCleaning',
    'clearing_cleaning_other': 'clearingCleaningOther',
    'stabilization': 'stabilization',
    'stabilization_other': 'stabilizationOther',
    'protection': 'protection',
    'protection_other': 'protectionOther',
    'salvage_dismantling': 'salvageDismantling',
    'salvage_dismantling_other': 'salvageDismantlingOther',
    'emergency_conservation': 'emergencyConservation',
    'emergency_conservation_other': 'emergencyConservationOther',
    'evacuation_storing': 'evacuationStoring',
    'evacuation_storing_other': 'evacuationStoringOther',
    'priority': 'priority',
    'measures_completed': 'measuresCompleted',
    'notes': 'notes',
}


# ============================================================
# Field type sets: control how map_row() converts values
# ============================================================

# Dropdown fields: single string value
DROPDOWN_FIELDS = {
    # Survey
    'areaUtilization', 'intangibleHeritage', 'protectionLevel',
    'siteContext', 'siteName',
    # Building
    'accessibility', 'consructionConditionClass', 'constructionDamageExtend',
    'constructionType', 'constructionUrgencyClass', 'facadeConditionClass',
    'facadeDamageExtent', 'facadeLoadBearing', 'facadeUrgencyClass',
    'foundationConditionClass', 'foundationDamageExtend', 'foundationUrgencyClass',
    'knownFloorsBelow', 'recommendationClass', 'roofConditionClass',
    'roofDamageExtend', 'roofUrgencyClass', 'thwStatus',
    'foundationType', 'roofType', 'roofMaterialConstruction',  # upgraded from listen
    # Level
    'basisDocumentation', 'damage', 'decorative', 'heritageAssessment',
    'mHeritageAcquisition', 'movableHeritage', 'priority', 'rubble',
    # Find
    'clDry', 'clNewCode', 'clStatus', 'clType', 'clWet',
    'dryDepotStatus', 'dryNewCode', 'dryStatus', 'dryTechnique',
    'evacStay', 'floorNummer', 'objectType', 'regAcquisitionCompleted',
    'regCleaning', 'regConditionComplete', 'regContamination',
    'regPhotoDocu', 'regPriority', 'regRiskAssessmentCondition',
    # Damage (single-select)
    'loadBearingComponent', 'measuresCompleted',
    'urgencyClass', 'constructionalElementPrimary',  # upgraded from listen
}

# Checkboxes fields: array of strings
CHECKBOXES_FIELDS = {
    # Survey
    'hazardEvent', 'monumentsCategory',
    # Building - upgraded from listen
    'constructionMaterials', 'foundationMaterials', 'facadeMaterial', 'roofMaterial',
    'constructionRiskAssessment', 'facadeRiskAssessment',
    'foundationRiskAssessment', 'roofRiskAssessment',
    # Damage - upgraded from listen
    'constructionalElementSecondary', 'damageType', 'riskAssessment',
    'documentationAssessment', 'furtherInvestigation',
    'clearingCleaning', 'stabilization', 'protection',
    'salvageDismantling', 'emergencyConservation', 'evacuationStoring',
}

MULTI_INPUT_FIELDS = {
    'operationalAreaId',  # Damage:default
}

# Value normalization: PG abbreviations/typos -> config valuelist keys
VALUE_NORMALIZE = {
    'priority': {'med': 'medium'},
}

# Fields where all values must be strings (inputType=input)
INPUT_TEXT_FIELDS = {
    'floorNr', 'buildingId', 'roomNr', 'roomDesignation',
    'buildingNr', 'opAreaId', 'numberOfFloorAbove', 'numberOfFloorBelow',
}

# Standalone valuelists that are referenced by forms but NOT embedded in config
STANDALONE_VALUELISTS = {
    'Building-condition-kgr': {
        'CC0': {'label': {'en': 'CC0'}, 'description': {'en': 'CC0 = No Symptoms'}},
        'CC1': {'label': {'en': 'CC1'}, 'description': {'en': 'CC1 = Minor Symptoms'}},
        'CC2': {'label': {'en': 'CC2'}, 'description': {'en': 'CC2 = Moderate Symptoms'}},
        'CC3': {'label': {'en': 'CC3'}, 'description': {'en': 'CC3 = Major Symptoms'}},
        'CC4': {'label': {'en': 'CC4'}, 'description': {'en': 'CC4 = Partial Collapse/Collapse/Total Loss'}},
    },
}

# ============================================================
# PG → KGR Valuelist Mapping Tables
# Maps English PG values to KGR valuelist value IDs (German/coded)
# ============================================================

# PG construction_materials / foundation_materials → Building-material-kgr
PG_TO_KGR_MATERIAL = {
    'Brick': 'Ziegel',
    'Stone': 'Naturstein',
    'Clay': 'Erde / Lehm',
    'Timber': 'Holz',
    'Iron/Steel': 'Stahl',
    'Concrete': 'Beton',
    'Other': 'Sonstige',
}

# PG material_facade → Building-material-kgr
PG_TO_KGR_FACADE_MATERIAL = {
    'Brick': 'Ziegel',
    'Stone': 'Naturstein',
    'Clay': 'Erde / Lehm',
    'Timber': 'Holz',
    'Plaster': 'Putz',
    'Other': 'Sonstige',
}

# PG material_roofing → roofing-kgr
PG_TO_KGR_ROOF_MATERIAL = {
    'Tiles': 'Dachziegel',
    'Concrete': 'Beton',
    'Bitumen Coating': 'Sonstige',
    'Clay': 'Erde',
    'Straw': 'Sonstige',
    'Metal': 'Blei',  # closest match
    'Timber': 'Holz',
    'Other': 'Sonstige',
}

# PG type_roof → free text (roofOutsideType is inputType: input)
PG_TO_KGR_ROOF_TYPE = {
    'Flat Roof': 'Flachdach',
    'Gable-end Roof': 'Satteldach',
    'Hipped-end Roof': 'Walmdach',
    'Monopitch': 'Pultdach',
    'Mansard Roof': 'Mansarddach',
    'Jerkinhead Roof': 'Krüppelwalmdach',
    'Other': 'Sonstige',
}

# PG roof_construction → free text (roofConstruction is inputType: input)
PG_TO_KGR_ROOF_CONSTRUCTION = {
    'Timber': 'Holzkonstruktion',
    'Metal Construction': 'Metallkonstruktion',
}

# PG damage types → damages-kgr (combined structural + material)
PG_TO_KGR_DAMAGE = {
    'Loss of Surface': 'exfoliation',
    'Loss of Structure': 'fragmentation',
    'Damage of load bearing Capacity': 'fractures',
    'Crack': 'riss1',
    'Deformation': 'deformationSwelling',
    'Partial Collapse': 'partialCollapse',
    'Collapse': 'totalCollapse',
    'Contamination': 'contamination',
    'Change in Material and Properties': 'weathering',
    'Missing Building Components': 'missingconst',
    'Wet': 'moisture',
    'Burned': 'fireDamage',
    'Other': None,  # goes to -other field
}

# PG condition_class → Building-condition-kgr (CC0-CC4)
# Note: KGR has typo 'CCO' instead of 'CC0' in valuelist
PG_TO_KGR_CONDITION = {
    'CC0': 'CCO', 'CC1': 'CC1', 'CC2': 'CC2', 'CC3': 'CC3', 'CC4': 'CC4',
    # Also handle lowercase and numeric
    'cc0': 'CCO', 'cc1': 'CC1', 'cc2': 'CC2', 'cc3': 'CC3', 'cc4': 'CC4',
    '0': 'CCO', '1': 'CC1', '2': 'CC2', '3': 'CC3', '4': 'CC4',
}

# PG condition_class → level-kgr (for Kgr:Damage)
PG_TO_KGR_DAMAGE_LEVEL = {
    'CC0': '1-SehrGering', 'CC1': '2-Gering', 'CC2': '3-Mittel',
    'CC3': '4-Schwer', 'CC4': '5-SehrSchwer',
    'cc0': '1-SehrGering', 'cc1': '2-Gering', 'cc2': '3-Mittel',
    'cc3': '4-Schwer', 'cc4': '5-SehrSchwer',
}

# PG priority → priority-kgr (UC values)
PG_TO_KGR_PRIORITY = {
    'low': 'UC0', 'medium': 'UC1', 'high': 'UC2', 'urgent': 'UC4',
    'Low': 'UC0', 'Medium': 'UC1', 'High': 'UC2', 'Urgent': 'UC4',
    'med': 'UC1',
}

# PG risk_assessment → Monument-risks-kgr
PG_TO_KGR_RISK = {
    'Potential Loss of Historical Surface': 'dissolution',
    'Secondary Damage': 'moisturedamage',
    'Potential Loss of Historical Substance': 'fallDownParts',
    'Impact on Safety': 'riskofinjury',
}

# PG component_primary → used for Kgr:Damage 'location' field (free text)
PG_TO_KGR_LOCATION = {
    'Facade': 'Fassade',
    'Wall': 'Wand',
    'Floor': 'Fußboden',
    'Ceiling': 'Decke',
    ' Pillar': 'Pfeiler',
    'Foundation': 'Fundament',
    'Structural roof component': 'Dachkonstruktion',
}

# ============================================================
# Building fields split: KGR-mapped vs custom-only
# PG columns that map to KGR Building:default fields
# ============================================================

# PG building columns that map DIRECTLY to KGR Building:default fields
BUILDING_KGR_MAP = {
    # PG column -> (KGR field name, VL mapping table or None)
    'construction_materials': ('buildingMaterialKGR', PG_TO_KGR_MATERIAL),
    'facade_condition_class': ('buildingCondition', PG_TO_KGR_CONDITION),
    'recommendation_class': ('buildingPriority', PG_TO_KGR_PRIORITY),
}

# PG facade columns that map to Kgr:WallOutside fields
FACADE_KGR_MAP = {
    'facade_material': ('wallOutsideMaterial', PG_TO_KGR_FACADE_MATERIAL),
    'facade_damage': ('wallOutsideDamages', PG_TO_KGR_DAMAGE),
    'facade_condition_class': ('wallOutsideCondition', PG_TO_KGR_CONDITION),
    'facade_notes': ('wallOutsideNotes', None),  # direct text
}

# PG roof columns that map to Kgr:RoofOutside fields
ROOF_KGR_MAP = {
    'roof_type': ('roofOutsideType', PG_TO_KGR_ROOF_TYPE),
    'roof_material_construction': ('roofConstruction', PG_TO_KGR_ROOF_CONSTRUCTION),
    'roof_material': ('roofOutsideMaterial', PG_TO_KGR_ROOF_MATERIAL),
    'roof_damage': ('roofOutsideDamages', PG_TO_KGR_DAMAGE),
    'roof_condition_class': ('roofOutsideCondition', PG_TO_KGR_CONDITION),
    'roof_notes': ('roofOutsideNotes', None),  # direct text
}

# PG damage columns that map to Kgr:Damage fields
DAMAGE_KGR_MAP = {
    'damage_type': ('damages-kgr-str', PG_TO_KGR_DAMAGE),
    'condition_class': ('damage-level', PG_TO_KGR_DAMAGE_LEVEL),
    'risk_assessment': ('buildingRisks', PG_TO_KGR_RISK),
    'priority': ('buildingPriority', PG_TO_KGR_PRIORITY),
    'notes': ('damage-notes', None),  # direct text
    'constructional_element_primary': ('location', PG_TO_KGR_LOCATION),
    'constructional_element_secondary': ('affectedAreas', None),  # direct text
}

# PG building columns that should NOT go to custom fields (mapped to KGR or subcategories)
BUILDING_KGR_HANDLED_COLS = set(BUILDING_KGR_MAP.keys()) | set(FACADE_KGR_MAP.keys()) | set(ROOF_KGR_MAP.keys())

# PG damage columns that are handled by KGR mapping (don't duplicate as custom)
DAMAGE_KGR_HANDLED_COLS = set(DAMAGE_KGR_MAP.keys())

# Building columns that stay as custom kgr_chru_datamodel:* fields
BUILDING_CUSTOM_MAP = {
    k: v for k, v in BUILDING_MAP.items()
    if k not in BUILDING_KGR_HANDLED_COLS
}

# Damage columns that stay as custom kgr_chru_datamodel:* fields
DAMAGE_CUSTOM_MAP = {
    k: v for k, v in DAMAGE_MAP.items()
    if k not in DAMAGE_KGR_HANDLED_COLS
}

# ============================================================
# Built-in field mappings: PG columns -> iDAI.field built-in resource fields
# These are NOT prefixed with kgr_chru_datamodel:, they go directly to resource.*
# ============================================================

# PG columns that map to the built-in 'date' field (per category)
# Format: {"value": "DD.MM.YYYY HH:MM"}
BUILTIN_DATE_COLUMNS = {
    'Survey': 'date',
    'Building': 'date',
    'Level': 'date',
    'Kgr:Damage': 'date',
    # Find has no general 'date' column - only workflow-specific dates
}

# PG columns that map to the built-in 'processor' field
BUILTIN_PROCESSOR_COLUMNS = {
    'Survey': 'processor',
    'Building': 'processor',
    'Level': 'processor',
    'Kgr:Damage': 'processor',
    # Find has no general processor - only workflow-specific processors
}

# PG columns that map to 'shortDescription'
BUILTIN_SHORT_DESC_COLUMNS = {
    'Survey': 'name',
    'Building': 'building_name',
    'Find': 'object_name',
}

# PG columns that map to 'description'
BUILTIN_DESC_COLUMNS = {
    'Survey': 'site_description',
    'Building': 'building_notes',
    'Find': 'notes',
    'Kgr:Damage': 'notes',
    'Level': 'notes_dec',
}

# PG columns that map to the built-in 'condition' field (checkboxes = array)
# Disabled for Kgr:Damage — condition_class is mapped to KGR 'damage-level' instead
BUILTIN_CONDITION_COLUMNS = {
    # 'Kgr:Damage': 'condition_class',  # Now mapped via DAMAGE_KGR_MAP
}

# Custom fields that use the iDAI.field date format {"value": "DD.MM.YYYY HH:MM"}
# These stay as kgr_chru_datamodel:* but need date format conversion
CUSTOM_DATE_FIELDS = {
    'movDate', 'regDate', 'clDate', 'dryDate',
}


# Runtime normalization map: raw processor value -> normalized form
# Built by build_processor_normalization_map() during sync startup
PROCESSOR_NORM_MAP = {}


def discover_processor_columns(engine):
    """Auto-discover all columns containing 'processor' in their name across the PG schema.

    Returns list of (table_name, column_name) tuples.
    """
    df = pd.read_sql(
        "SELECT table_name, column_name FROM information_schema.columns "
        "WHERE table_schema = %s AND lower(column_name) LIKE '%%processor%%' "
        "ORDER BY table_name, column_name",
        engine,
        params=(PG_SCHEMA,),
    )
    sources = [(row['table_name'], row['column_name']) for _, row in df.iterrows()]
    return sources


def collect_all_processors(engine):
    """Collect all unique processor names from all auto-discovered PG columns
    AND the listen.processor reference table.

    The listen.processor table contains the official/canonical full names and is
    included as a primary source for the normalization map (full names help
    resolve abbreviations and first-name-only entries from the data tables).

    Returns a sorted list of unique, trimmed, non-empty processor strings.
    """
    # 1. Auto-discover processor columns in the data schema
    sources = discover_processor_columns(engine)
    print(f"  Auto-discovered {len(sources)} processor columns in {PG_SCHEMA}:")
    for table, col in sources:
        print(f"    {table}.{col}")

    all_processors = set()
    for table, col in sources:
        try:
            df = pd.read_sql(
                f'SELECT DISTINCT "{col}" FROM {PG_SCHEMA}."{table}" WHERE "{col}" IS NOT NULL',
                engine,
            )
            for val in df.iloc[:, 0].dropna().unique():
                s = str(val).strip()
                if s:
                    all_processors.add(s)
        except Exception:
            pass  # table may not exist or column missing

    # 2. Include listen.processor reference table (canonical full names)
    listen_processors = set()
    try:
        df = pd.read_sql(
            f'SELECT DISTINCT processor FROM {PG_LISTEN_SCHEMA}.processor '
            f'WHERE processor IS NOT NULL',
            engine,
        )
        for val in df['processor'].dropna().unique():
            s = str(val).strip()
            if s:
                listen_processors.add(s)
                all_processors.add(s)
        print(f"  Reference list ({PG_LISTEN_SCHEMA}.processor): {sorted(listen_processors)}")
    except Exception as e:
        print(f"  No reference list found ({PG_LISTEN_SCHEMA}.processor): {e}")

    return sorted(all_processors)


def build_processor_normalization_map(raw_processors):
    """Build a normalization map: raw value -> canonical form.

    Applies these rules (only when the match is unambiguous):
    1. Trim whitespace
    2. Initials -> full name: 'BF' -> 'Bernhard Fritsch' (if exactly one match)
    3. First name -> full name: 'Sarah' -> 'Sarah Giering' (if exactly one match)
    4. Case dedup: 'MZ' and 'Mz' -> prefer the most common or uppercase form

    If a mapping is ambiguous (multiple candidates), the value is kept as-is.
    Returns dict {raw_value: normalized_value} only for values that change.
    """
    norm_map = {}
    trimmed_set = set()
    for v in raw_processors:
        t = v.strip()
        if t != v:
            norm_map[v] = t  # whitespace normalization
        trimmed_set.add(t)

    # Separate full names (contain space) from short forms
    full_names = sorted(fn for fn in trimmed_set if ' ' in fn)
    short_forms = sorted(sf for sf in trimmed_set if ' ' not in sf)

    # Build initials index: "BF" -> ["Bernhard Fritsch"]
    initials_index = {}
    for fn in full_names:
        parts = fn.split()
        initials = ''.join(p[0].upper() for p in parts if p)
        initials_index.setdefault(initials, []).append(fn)

    # Try to resolve each short form
    for sf in short_forms:
        sf_upper = sf.upper()

        # Rule 2: Initials match (e.g. "BF" -> "Bernhard Fritsch")
        if sf_upper in initials_index and len(initials_index[sf_upper]) == 1:
            norm_map[sf] = initials_index[sf_upper][0]
            continue

        # Rule 3: First name match (e.g. "Sarah" -> "Sarah Giering")
        first_name_matches = [fn for fn in full_names
                              if fn.split()[0].lower() == sf.lower()]
        if len(first_name_matches) == 1:
            norm_map[sf] = first_name_matches[0]
            continue

    # Rule 4: Case-insensitive deduplication for remaining unmapped values
    # Group all values (including full names) by lowercase
    lower_groups = {}
    for v in trimmed_set:
        canonical = norm_map.get(v, v)  # use already-mapped target
        lower_groups.setdefault(canonical.lower(), []).append(v)

    for lower_key, variants in lower_groups.items():
        if len(variants) <= 1:
            continue
        # All variants that map to different targets -> pick the best target
        targets = set()
        for v in variants:
            targets.add(norm_map.get(v, v))
        if len(targets) <= 1:
            continue  # already normalized to same value
        # Among the targets, prefer the longest (most complete) form
        best = sorted(targets, key=lambda t: (-len(t), t))[0]
        for v in variants:
            current_target = norm_map.get(v, v)
            if current_target != best:
                norm_map[v] = best

    return norm_map


def normalize_processor(value):
    """Normalize a single processor value using the global PROCESSOR_NORM_MAP.

    Returns the normalized value (or original if no mapping exists).
    """
    if not value:
        return value
    s = str(value).strip()
    return PROCESSOR_NORM_MAP.get(s, s)


def update_project_staff(processors):
    """Update the Project document's staff list with normalized processors.

    Merges new processors into the existing staff list without removing
    entries that were added manually via the GUI.
    """
    print("\n--- Update Project staff list ---")

    # Find the Project document
    resp = requests.get(f'{FIELD_URL}/{FIELD_DB}/_all_docs', auth=FIELD_AUTH,
                        params={'include_docs': True, 'limit': 200})
    rows = resp.json().get('rows', [])
    project_doc = None
    for r in rows:
        doc = r.get('doc', {})
        res = doc.get('resource', {})
        if res.get('category') == 'Project':
            project_doc = doc
            break

    if not project_doc:
        print("  WARNING: No Project document found, cannot update staff.")
        return False

    resource = project_doc['resource']
    existing_staff = resource.get('staff', [])
    existing_names = {entry['value'] for entry in existing_staff}
    print(f"  Existing staff: {sorted(existing_names)}")

    # Use normalized unique processor names for the staff list
    normalized_unique = sorted(set(normalize_processor(p) for p in processors))

    # Add new processors not already in staff
    added = []
    for name in normalized_unique:
        if name not in existing_names:
            existing_staff.append({'value': name, 'selectable': True})
            added.append(name)

    if not added:
        print("  No new processors to add (all already in staff).")
        return True

    resource['staff'] = existing_staff

    # Update modified timestamp
    now = pd.Timestamp.now().strftime('%Y-%m-%dT%H:%M:%S.000Z')
    project_doc.setdefault('modified', []).append({'user': 'pg_sync', 'date': now})

    if DRY_RUN:
        print(f"  DRY RUN: Would add {len(added)} processors: {added}")
        return True

    resp = requests.put(
        f'{FIELD_URL}/{FIELD_DB}/{project_doc["_id"]}',
        auth=FIELD_AUTH,
        json=project_doc,
    )
    if resp.status_code in (200, 201):
        print(f"  Added {len(added)} processors to staff: {added}")
        print(f"  Total staff now: {len(existing_staff)}")
        return True
    else:
        print(f"  ERROR updating Project: {resp.status_code} {resp.text[:300]}")
        return False


def format_date_string(raw_value):
    """Convert a PG date value to formatted string: "DD.MM.YYYY HH:MM".

    Handles:
    - ISO date strings: '2025-10-16' -> '16.10.2025 00:00'
    - ISO datetime strings: '2025-10-23T14:47:14.970' -> '23.10.2025 14:47'
    - Python datetime.date objects: date(2025, 10, 17) -> '17.10.2025 00:00'

    Returns plain string (for custom fields with inputType: input) or None.
    """
    import datetime

    if raw_value is None or (isinstance(raw_value, float) and pd.isna(raw_value)):
        return None

    if isinstance(raw_value, datetime.date) and not isinstance(raw_value, datetime.datetime):
        return raw_value.strftime(f'%d.%m.%Y {DATE_DEFAULT_TIME}')

    if isinstance(raw_value, datetime.datetime):
        return raw_value.strftime('%d.%m.%Y %H:%M')

    s = str(raw_value).strip()
    if not s:
        return None

    # Try ISO datetime: '2025-10-23T14:47:14.970'
    if 'T' in s:
        try:
            dt = datetime.datetime.fromisoformat(s)
            return dt.strftime('%d.%m.%Y %H:%M')
        except ValueError:
            pass

    # Try ISO date: '2025-10-16'
    try:
        dt = datetime.datetime.strptime(s, '%Y-%m-%d')
        return dt.strftime(f'%d.%m.%Y {DATE_DEFAULT_TIME}')
    except ValueError:
        pass

    # Try German format already: '16.10.2025'
    try:
        dt = datetime.datetime.strptime(s, '%d.%m.%Y')
        return dt.strftime(f'%d.%m.%Y {DATE_DEFAULT_TIME}')
    except ValueError:
        pass

    # Fallback: store as-is with default time
    return f'{s} {DATE_DEFAULT_TIME}'


def format_field_date(raw_value):
    """Convert a PG date value to iDAI.field built-in date format: {"value": "DD.MM.YYYY HH:MM"}.

    Used ONLY for the built-in resource.date field (which expects the {"value": "..."} wrapper).
    For custom fields with inputType: input, use format_date_string() instead.
    """
    s = format_date_string(raw_value)
    if s:
        return {'value': s}
    return None


# ============================================================
# Pre-sync: Apply improved configuration
# ============================================================

def ensure_project_and_config():
    """Step 0: Ensure the database has a Project and Configuration document."""
    print("\n--- Step 0: Ensure Project & Configuration exist ---")

    # Check if database exists
    resp = requests.get(f'{FIELD_URL}/{FIELD_DB}', auth=FIELD_AUTH)
    if resp.status_code == 404:
        print(f"  Creating database {FIELD_DB}...")
        resp = requests.put(f'{FIELD_URL}/{FIELD_DB}', auth=FIELD_AUTH)
        if resp.status_code not in (201, 200):
            print(f"  ERROR creating DB: {resp.status_code} {resp.text[:200]}")
            return False
        print(f"  Database created!")

    # Check for Project document
    resp = requests.get(f'{FIELD_URL}/{FIELD_DB}/_all_docs', auth=FIELD_AUTH,
                       params={'include_docs': True, 'limit': 100})
    rows = resp.json().get('rows', [])
    has_project = False
    has_config = False
    for r in rows:
        doc = r.get('doc', {})
        if doc.get('_id') == 'configuration':
            has_config = True
        res = doc.get('resource', {})
        if res.get('category') == 'Project':
            has_project = True

    if not has_project:
        print("  Creating Project document...")
        now = pd.Timestamp.now().strftime('%Y-%m-%dT%H:%M:%S.000Z')
        project_doc = {
            '_id': 'project',
            'resource': {
                'id': 'project',
                'identifier': FIELD_DB,
                'category': 'Project',
                'relations': {},
            },
            'created': {'user': 'pg_sync', 'date': now},
            'modified': [{'user': 'pg_sync', 'date': now}],
        }
        resp = requests.put(f'{FIELD_URL}/{FIELD_DB}/project', auth=FIELD_AUTH,
                           json=project_doc)
        if resp.status_code in (201, 200):
            print(f"  Project created (id=project)")
        else:
            print(f"  ERROR: {resp.status_code} {resp.text[:200]}")
            return False

    if not has_config:
        print("  Creating initial Configuration document...")
        config_doc = {
            '_id': 'configuration',
            'resource': {
                'forms': {},
                'order': [],
                'valuelists': {},
            }
        }
        resp = requests.put(f'{FIELD_URL}/{FIELD_DB}/configuration', auth=FIELD_AUTH,
                           json=config_doc)
        if resp.status_code in (201, 200):
            print(f"  Configuration created (rev: {resp.json().get('rev', '?')})")
        else:
            print(f"  ERROR: {resp.status_code} {resp.text[:200]}")
            return False

    print("  Project & Configuration OK")
    return True


def apply_kgr_config():
    """Step A: Add custom PG fields to the existing KGR configuration.

    Assumes the KGR configuration has already been imported by the user
    via Field Desktop's configuration import feature. This function only
    adds kgr_chru_datamodel:* field definitions for PG columns that have
    no KGR field equivalent, plus ensures Level is in the order.
    """
    print("\n--- Step A: Add custom PG fields to existing KGR config ---")

    # 1. Read current config from CouchDB
    resp = requests.get(f'{FIELD_URL}/{FIELD_DB}/configuration', auth=FIELD_AUTH)
    config_doc = resp.json()
    existing_forms = config_doc.get('resource', {}).get('forms', {})
    existing_order = config_doc.get('resource', {}).get('order', [])
    print(f"  Existing config: {len(existing_forms)} forms, {len(existing_order)} order items")
    print(f"  Config rev: {config_doc.get('_rev', '?')}")

    # Verify KGR config is present (check for Kgr: categories)
    kgr_cats = [k for k in existing_forms if k.startswith('Kgr:')]
    if not kgr_cats:
        print("  ERROR: No Kgr: categories found in project configuration!")
        print("  The KGR configuration MUST be imported in Field Desktop BEFORE running this script.")
        print("  Steps: Field Desktop -> Project -> Settings -> Import Configuration -> select kgr.configuration file")
        print("\n  ABORTING. Please import the KGR configuration and try again.")
        return False
    else:
        print(f"  KGR categories found: {len(kgr_cats)} ({', '.join(kgr_cats[:5])}...)")

    # 2. Add custom kgr_chru_datamodel:* fields for unmapped PG columns
    add_custom_pg_fields(config_doc)

    # 3. PUT updated configuration back to CouchDB
    resp = requests.put(f'{FIELD_URL}/{FIELD_DB}/configuration',
                        auth=FIELD_AUTH, json=config_doc)
    if resp.status_code == 201:
        new_rev = resp.json().get('rev', '?')
        total_forms = len(config_doc['resource']['forms'])
        print(f"  Config updated! (rev: {new_rev})")
        print(f"  Total forms: {total_forms}, order: {len(config_doc['resource']['order'])}")
        return True
    else:
        print(f"  ERROR: {resp.status_code} {resp.text[:300]}")
        return False


def add_custom_pg_fields(config_doc):
    """Add kgr_chru_datamodel:* field definitions for PG columns not mapped to KGR fields.

    Modifies config_doc in place: adds fields, groups, and valuelists to the
    relevant form definitions.
    """
    forms = config_doc['resource']['forms']
    valuelists = config_doc['resource'].setdefault('valuelists', {})

    # Define unmapped custom fields per category
    # Each entry: (field_suffix, inputType, valuelist_name_or_None)
    custom_defs = {
        'Survey:default': [
            (fs, _get_input_type(fs), _get_valuelist(fs, 'Survey'))
            for fs in SURVEY_MAP.values()
        ],
        'Building:default': [
            (fs, _get_input_type(fs), _get_valuelist(fs, 'Building'))
            for fs in BUILDING_CUSTOM_MAP.values()
        ],
        'Level:default': [
            (fs, _get_input_type(fs), _get_valuelist(fs, 'Level'))
            for fs in LEVEL_MAP.values()
        ],
        'Find:default': [
            (fs, _get_input_type(fs), _get_valuelist(fs, 'Find'))
            for fs in FIND_MAP.values()
        ],
        'Kgr:Damage': [
            (fs, _get_input_type(fs), _get_valuelist(fs, 'Damage'))
            for fs in DAMAGE_CUSTOM_MAP.values()
        ],
    }

    for form_key, field_list in custom_defs.items():
        if form_key not in forms:
            # Create minimal form definition if it doesn't exist
            if form_key.endswith(':default'):
                forms[form_key] = {'fields': {}, 'groups': [], 'valuelists': {}}
            else:
                continue  # Skip non-default forms that don't exist

        form = forms[form_key]
        form_fields = form.setdefault('fields', {})
        custom_field_names = []

        for field_suffix, input_type, vl_name in field_list:
            prefixed = f'{PREFIX}:{field_suffix}'
            if prefixed in form_fields:
                continue  # Already defined (e.g., by KGR config)

            form_fields[prefixed] = {'inputType': input_type}
            custom_field_names.append(prefixed)

        # Add custom fields to a 'pgData' group
        if custom_field_names:
            groups = form.setdefault('groups', [])
            # Check if pgData group already exists
            pg_group = None
            for g in groups:
                if g.get('name') == 'pgData':
                    pg_group = g
                    break
            if pg_group is None:
                pg_group = {'name': 'pgData', 'fields': []}
                groups.append(pg_group)
            pg_group['fields'].extend(custom_field_names)

    # NOTE: We do NOT add embedded valuelists to the config document.
    # KGR valuelists are built-in to Field Desktop. Custom fields use
    # inputType 'input' (free text) and don't need valuelists.

    # Ensure all categories we sync to are in the order list
    order = config_doc['resource'].setdefault('order', [])
    order_set = set(order)
    needed_in_order = ['Level']  # Level is not in KGR config but we sync floor data
    for cat in needed_in_order:
        if cat not in order_set:
            # Insert after Building
            try:
                idx = order.index('Building') + 1
            except ValueError:
                idx = len(order)
            order.insert(idx, cat)
            print(f"  Added '{cat}' to order at position {idx}")

    # Count what we added
    total_custom = sum(
        sum(1 for f in forms.get(fk, {}).get('fields', {})
            if f.startswith(f'{PREFIX}:'))
        for fk in custom_defs
    )
    print(f"  Added {total_custom} custom {PREFIX}:* fields across {len(custom_defs)} forms")


def _get_input_type(field_suffix):
    """Determine the inputType for a custom field.

    All custom kgr_chru_datamodel:* fields use 'input' (free text).
    KGR valuelists are built-in to Field Desktop and only work with
    KGR-prefixed fields from the imported config — embedding valuelists
    for custom fields in the config document crashes the app.
    """
    return 'input'


def _get_valuelist(field_suffix, category):
    """Get the valuelist name for a custom field, if it needs one.

    Returns None for all custom fields — we do NOT embed valuelists
    in the config document. KGR valuelists are built-in to Field Desktop.
    """
    return None




def create_standalone_valuelists():
    """Step B: Create standalone valuelist documents not embedded in the config."""
    print("\n--- Step B: Create standalone valuelist documents ---")

    created = 0
    skipped = 0

    for vl_id, values in STANDALONE_VALUELISTS.items():
        resp = requests.head(f'{FIELD_URL}/{FIELD_DB}/{vl_id}', auth=FIELD_AUTH)
        if resp.status_code == 200:
            skipped += 1
            print(f"  EXISTS: {vl_id}")
            continue

        doc = {'_id': vl_id, 'values': values}
        resp = requests.put(f'{FIELD_URL}/{FIELD_DB}/{vl_id}', auth=FIELD_AUTH, json=doc)
        if resp.status_code in (201, 200):
            created += 1
            print(f"  CREATED: {vl_id}")
        else:
            print(f"  ERROR: {vl_id} -> {resp.status_code} {resp.text[:200]}")
            return False

    print(f"  Created: {created}, Skipped (already exist): {skipped}")
    return True


# ============================================================
# Helpers
# ============================================================

def make_id():
    """Generate a new UUID for iDAI.field document."""
    return str(uuid.uuid4())


def pg_to_geojson(row):
    """Convert a row's geometry to iDAI.field GeoJSON format."""
    geom = None
    if hasattr(row, 'geometry') and hasattr(row.geometry, 'is_empty'):
        geom = row.geometry
    elif isinstance(row, pd.Series) and 'geom' in row.index:
        val = row['geom']
        if hasattr(val, 'is_empty'):
            geom = val
    if geom is None:
        return None
    try:
        if geom.is_empty:
            return None
        return mapping(geom)
    except Exception:
        return None


def read_geo_table(query, engine, geom_col='geom'):
    """Read a PostGIS table as GeoDataFrame with reprojection to WGS84."""
    try:
        gdf = gpd.read_postgis(query, engine, geom_col=geom_col)
        if gdf.crs and gdf.crs.to_epsg() != 4326:
            gdf = gdf.to_crs(epsg=4326)
        return gdf
    except Exception as e:
        print(f"  WARNING: PostGIS read failed ({e}), falling back to plain SQL")
        return pd.read_sql(query, engine)


def parse_pg_array(val):
    """Parse PostgreSQL array literal like '{"Floor","Wall"}' into a list."""
    if isinstance(val, str) and val.startswith('{') and val.endswith('}'):
        inner = val[1:-1]
        if not inner:
            return []
        items = []
        for item in inner.split(','):
            item = item.strip().strip('"')
            if item:
                items.append(item)
        return items
    return None


def map_row(row, col_map):
    """Map a PG row to iDAI.field resource fields using column mapping.

    Handles type conversions:
    - dropdown fields: ensure string values (not int/float)
    - checkboxes fields: ensure array of strings
    - multiInput fields: wrap in array
    - PG array literals: parse into lists or join as string
    - dates: keep as plain strings (config uses inputType: input)
    """
    fields = {}
    for pg_col, field_suffix in col_map.items():
        if pg_col in row.index:
            val = row[pg_col]
            if pd.notna(val) and str(val).strip():
                full_field = f'{PREFIX}:{field_suffix}'

                # Parse PG array literals
                pg_arr = parse_pg_array(val) if isinstance(val, str) else None

                # Normalize known abbreviations/typos
                if field_suffix in VALUE_NORMALIZE:
                    nmap = VALUE_NORMALIZE[field_suffix]
                    if pg_arr is not None:
                        pg_arr = [nmap.get(v, v) for v in pg_arr]
                    elif isinstance(val, str):
                        val = nmap.get(val, val)

                # Date fields: format as plain string "DD.MM.YYYY HH:MM"
                # (custom fields use inputType: input, so no {"value": ...} wrapper)
                if field_suffix in CUSTOM_DATE_FIELDS:
                    date_str = format_date_string(val)
                    if date_str:
                        fields[full_field] = date_str
                    continue

                # Processor fields: normalize via PROCESSOR_NORM_MAP
                if 'processor' in field_suffix.lower():
                    val = normalize_processor(val)
                    fields[full_field] = val
                    continue

                # Checkboxes fields: must be array of strings
                if field_suffix in CHECKBOXES_FIELDS:
                    if pg_arr is not None:
                        val = [str(v) for v in pg_arr]
                    elif isinstance(val, list):
                        val = [str(v) for v in val]
                    else:
                        # Single value -> wrap in array
                        if isinstance(val, float) and val == int(val):
                            val = [str(int(val))]
                        else:
                            val = [str(val)]

                # Dropdown fields: must be string
                elif field_suffix in DROPDOWN_FIELDS:
                    if pg_arr is not None:
                        val = str(pg_arr[0]) if pg_arr else None
                        if not val:
                            continue
                    else:
                        val = str(int(val)) if isinstance(val, float) and val == int(val) else str(val)

                # multiInput fields: must be array
                elif field_suffix in MULTI_INPUT_FIELDS:
                    if pg_arr is not None:
                        val = pg_arr
                    elif not isinstance(val, list):
                        val = [str(val)]

                # Input text fields: must be string
                elif field_suffix in INPUT_TEXT_FIELDS:
                    if pg_arr is not None:
                        val = ', '.join(str(v) for v in pg_arr)
                    elif isinstance(val, (int, float)):
                        val = str(int(val)) if isinstance(val, float) and val == int(val) else str(val)

                # General: join PG arrays as text, convert floats
                else:
                    if pg_arr is not None:
                        val = ', '.join(str(v) for v in pg_arr) if pg_arr else None
                        if not val:
                            continue
                    elif isinstance(val, float) and val == int(val):
                        val = int(val)

                fields[full_field] = val
    return fields


def map_kgr_value(raw_val, vl_map):
    """Map a PG value to a KGR valuelist value using a mapping table.

    Handles single values and PG array literals.
    Returns the mapped value (string for dropdowns, list for checkboxes), or None.
    """
    if raw_val is None or (isinstance(raw_val, float) and pd.isna(raw_val)):
        return None

    raw_str = str(raw_val).strip()
    if not raw_str:
        return None

    # Try PG array literal
    pg_arr = parse_pg_array(raw_str) if raw_str.startswith('{') else None

    if pg_arr is not None:
        # Array: map each element, return list of mapped values
        mapped = []
        for item in pg_arr:
            m = vl_map.get(item.strip())
            if m:
                mapped.append(m)
        return mapped if mapped else None
    else:
        # Single value
        return vl_map.get(raw_str)


def map_kgr_fields(row, kgr_map):
    """Map PG columns to KGR field names using the KGR mapping tables.

    kgr_map: dict of {pg_col: (kgr_field_name, vl_map_or_None)}
    Returns dict of {kgr_field_name: mapped_value}
    """
    fields = {}
    for pg_col, (kgr_field, vl_map) in kgr_map.items():
        if pg_col not in row.index:
            continue
        val = row[pg_col]
        if pd.isna(val) or not str(val).strip():
            continue

        if vl_map is not None:
            mapped = map_kgr_value(val, vl_map)
            if mapped is not None:
                fields[kgr_field] = mapped
        else:
            # Direct text mapping
            fields[kgr_field] = str(val)
    return fields


def make_wall_outside_doc(building_row, building_identifier, building_rid):
    """Create a Kgr:WallOutside document from PG facade_* columns.

    Returns (doc, rid) or (None, None) if no facade data.
    """
    kgr_fields = map_kgr_fields(building_row, FACADE_KGR_MAP)

    # Also collect unmapped facade columns as custom fields
    facade_custom_cols = {
        'facade_load_bearing': 'facadeLoadBearing',
        'facade_material_other': 'facadeMaterialOther',
        'facade_damage_other': 'facadeDamageOther',
        'facade_damage_extend': 'facadeDamageExtent',
        'facade_risk_assessment': 'facadeRiskAssessment',
        'facade_urgency_class': 'facadeUrgencyClass',
    }
    custom_fields = {}
    for pg_col, field_suffix in facade_custom_cols.items():
        if pg_col in building_row.index:
            val = building_row[pg_col]
            if pd.notna(val) and str(val).strip():
                custom_fields[f'{PREFIX}:{field_suffix}'] = str(val)

    all_fields = {**kgr_fields, **custom_fields}
    if not all_fields:
        return None, None

    rid = make_id()
    identifier = f"{building_identifier}-WallOutside"
    relations = {'isRecordedIn': [building_rid]}

    now = pd.Timestamp.now().strftime('%Y-%m-%dT%H:%M:%S.000Z')
    resource = {
        'id': rid,
        'identifier': identifier,
        'category': 'Kgr:WallOutside',
        'relations': relations,
    }
    resource.update(all_fields)

    doc = {
        '_id': rid,
        'resource': resource,
        'created': {'user': 'pg_sync', 'date': now},
        'modified': [{'user': 'pg_sync', 'date': now}],
    }
    return doc, rid


def make_roof_outside_doc(building_row, building_identifier, building_rid):
    """Create a Kgr:RoofOutside document from PG roof_* columns.

    Returns (doc, rid) or (None, None) if no roof data.
    """
    kgr_fields = map_kgr_fields(building_row, ROOF_KGR_MAP)

    # Also collect unmapped roof columns as custom fields
    roof_custom_cols = {
        'roof_type_other': 'roofTypeOther',
        'roof_material_other': 'roofMaterialOther',
        'roof_damage_other': 'roofDamageOther',
        'roof_damage_extend': 'roofDamageExtend',
        'roof_risk_assessment': 'roofRiskAssessment',
        'roof_urgency_class': 'roofUrgencyClass',
    }
    custom_fields = {}
    for pg_col, field_suffix in roof_custom_cols.items():
        if pg_col in building_row.index:
            val = building_row[pg_col]
            if pd.notna(val) and str(val).strip():
                custom_fields[f'{PREFIX}:{field_suffix}'] = str(val)

    all_fields = {**kgr_fields, **custom_fields}
    if not all_fields:
        return None, None

    rid = make_id()
    identifier = f"{building_identifier}-RoofOutside"
    relations = {'isRecordedIn': [building_rid]}

    now = pd.Timestamp.now().strftime('%Y-%m-%dT%H:%M:%S.000Z')
    resource = {
        'id': rid,
        'identifier': identifier,
        'category': 'Kgr:RoofOutside',
        'relations': relations,
    }
    resource.update(all_fields)

    doc = {
        '_id': rid,
        'resource': resource,
        'created': {'user': 'pg_sync', 'date': now},
        'modified': [{'user': 'pg_sync', 'date': now}],
    }
    return doc, rid


def make_doc(resource_id, identifier, category, fields, relations, geometry=None,
             row=None):
    """Create a full iDAI.field document.

    Handles built-in field mappings:
    - date: PG date column -> resource.date (formatted as {"value": "DD.MM.YYYY HH:MM"})
    - processor: PG processor column -> resource.processor
    - shortDescription: PG name/buildingName/objectName -> resource.shortDescription
    - description: PG notes/siteDescription/buildingNotes -> resource.description
    - condition: PG condition_class -> resource.condition (array, uses Damage-condition-default)
    """
    now = pd.Timestamp.now().strftime('%Y-%m-%dT%H:%M:%S.000Z')
    resource = {
        'id': resource_id,
        'identifier': str(identifier),
        'category': category,
        'relations': relations,
    }

    # Map built-in fields from PG row
    if row is not None:
        # Built-in date
        date_col = BUILTIN_DATE_COLUMNS.get(category)
        if date_col and date_col in row.index:
            raw = row[date_col]
            if pd.notna(raw) and str(raw).strip():
                date_obj = format_field_date(raw)
                if date_obj:
                    resource['date'] = date_obj

        # Built-in processor (normalized)
        proc_col = BUILTIN_PROCESSOR_COLUMNS.get(category)
        if proc_col and proc_col in row.index:
            raw = row[proc_col]
            if pd.notna(raw) and str(raw).strip():
                resource['processor'] = normalize_processor(raw)

        # Built-in shortDescription
        sd_col = BUILTIN_SHORT_DESC_COLUMNS.get(category)
        if sd_col and sd_col in row.index:
            raw = row[sd_col]
            if pd.notna(raw) and str(raw).strip():
                resource['shortDescription'] = str(raw)

        # Built-in description
        desc_col = BUILTIN_DESC_COLUMNS.get(category)
        if desc_col and desc_col in row.index:
            raw = row[desc_col]
            if pd.notna(raw) and str(raw).strip():
                resource['description'] = str(raw)

        # Built-in condition (checkboxes = array of strings)
        cond_col = BUILTIN_CONDITION_COLUMNS.get(category)
        if cond_col and cond_col in row.index:
            raw = row[cond_col]
            if pd.notna(raw) and str(raw).strip():
                resource['condition'] = [str(raw).strip()]

    # Add custom fields
    resource.update(fields)
    if geometry:
        resource['geometry'] = geometry

    return {
        '_id': resource_id,
        'resource': resource,
        'created': {'user': 'pg_sync', 'date': now},
        'modified': [{'user': 'pg_sync', 'date': now}],
    }


def get_existing_docs(field_url, field_db, field_auth):
    """Load all existing docs from iDAI.field to check for duplicates."""
    response = requests.get(f'{field_url}/{field_db}', auth=field_auth)
    result = response.json()
    all_docs = []
    doc_count = result.get('doc_count', 0)
    limit = max(1, math.ceil(doc_count / 10000))
    for i in range(limit):
        response = requests.get(f'{field_url}/{field_db}/_all_docs', auth=field_auth,
                              params={'limit': 10000, 'include_docs': True, 'skip': i * 10000})
        rows = response.json().get('rows', [])
        all_docs.extend([r['doc'] for r in rows if 'doc' in r])
    return all_docs


def save_docs(docs, field_url, field_db, field_auth, dry_run=False):
    """Save documents to iDAI.field via _bulk_docs."""
    if not docs:
        print("  No documents to save.")
        return
    if dry_run:
        print(f"  DRY RUN: Would save {len(docs)} documents.")
        return

    chunks = [docs[i:i+200] for i in range(0, len(docs), 200)]
    total_ok = 0
    total_err = 0
    for chunk in chunks:
        response = requests.post(f'{field_url}/{field_db}/_bulk_docs', auth=field_auth,
                               json={'docs': chunk})
        result = response.json()
        ok = sum(1 for r in result if r.get('ok'))
        err = sum(1 for r in result if 'error' in r)
        total_ok += ok
        total_err += err
        if err:
            for r in result:
                if 'error' in r:
                    print(f"    Error: {r}")
    print(f"  Saved: {total_ok} ok, {total_err} errors")


# ============================================================
# Main sync
# ============================================================

def main():
    print("=" * 60)
    print("KGR Sync: PostgreSQL -> iDAI.field (initial full sync)")
    print("=" * 60)

    # ===========================================================
    # Pre-sync: Initialization + Configuration setup (Steps 0, A, B)
    # ===========================================================
    if not ensure_project_and_config():
        print("ABORT: DB initialization failed!")
        return
    if not apply_kgr_config():
        print("ABORT: KGR config application failed!")
        return
    # Standalone valuelists NOT needed — KGR valuelists are built-in to Field Desktop

    print("\n" + "=" * 60)
    print("Pre-sync complete. Starting data sync...")
    print("=" * 60)

    engine = create_engine(PG_URL)

    # ===========================================================
    # Step C: Collect processors, build normalization map, update staff
    # ===========================================================
    global PROCESSOR_NORM_MAP
    all_processors = collect_all_processors(engine)
    print(f"\n  Unique raw processors from PG: {len(all_processors)} -> {all_processors}")

    PROCESSOR_NORM_MAP = build_processor_normalization_map(all_processors)
    if PROCESSOR_NORM_MAP:
        print(f"\n  Processor normalization map ({len(PROCESSOR_NORM_MAP)} mappings):")
        for raw, norm in sorted(PROCESSOR_NORM_MAP.items()):
            print(f"    '{raw}' -> '{norm}'")
    else:
        print("\n  No processor normalization needed (all values unambiguous).")

    normalized_unique = sorted(set(normalize_processor(p) for p in all_processors))
    print(f"  Normalized unique processors: {len(normalized_unique)} -> {normalized_unique}")

    update_project_staff(all_processors)

    # Load existing iDAI.field docs to find project ID and avoid duplicates
    print("\nLoading existing iDAI.field documents...")
    existing = get_existing_docs(FIELD_URL, FIELD_DB, FIELD_AUTH)
    existing_identifiers = {}
    project_id = None
    for doc in existing:
        res = doc.get('resource', {})
        ident = res.get('identifier', '')
        existing_identifiers[ident] = res.get('id', doc['_id'])
        if res.get('category') == 'Project':
            project_id = res.get('id', doc['_id'])

    print(f"  Found {len(existing)} docs, project_id={project_id}")
    print(f"  Existing identifiers: {list(existing_identifiers.keys())[:10]}...")

    # ID lookup tables for building relations
    survey_id_map = {}   # operational_area_id -> field resource id
    building_id_map = {} # pg building_id -> field resource id
    level_id_map = {}    # "building_id_floorNr" -> field resource id

    new_docs = []

    # ----------------------------------------------------------
    # 0. PLACE (top-level container for all Operations)
    # ----------------------------------------------------------
    print(f"\n--- Place: {PLACE_IDENTIFIER} ---")
    if PLACE_IDENTIFIER in existing_identifiers:
        einsatzort_id = existing_identifiers[PLACE_IDENTIFIER]
        print(f"  EXISTS: {PLACE_IDENTIFIER} (id={einsatzort_id})")
    else:
        einsatzort_id = make_id()
        doc = make_doc(einsatzort_id, PLACE_IDENTIFIER, 'Place', {}, {})
        new_docs.append(doc)
        existing_identifiers[PLACE_IDENTIFIER] = einsatzort_id
        print(f"  NEW: {PLACE_IDENTIFIER} (id={einsatzort_id})")

    # ----------------------------------------------------------
    # 1. SURVEYS (operational_area) -> liesWithin: [Place]
    # ----------------------------------------------------------
    print("\n--- Surveys (operational_area) ---")
    gdf = read_geo_table(
        f'SELECT * FROM {PG_SCHEMA}.operational_area', engine
    )
    print(f"  PG rows: {len(gdf)}")

    for _, row in gdf.iterrows():
        identifier = f"OA-{row['operational_area_id']}"
        if identifier in existing_identifiers:
            survey_id_map[row['operational_area_id']] = existing_identifiers[identifier]
            print(f"  SKIP (exists): {identifier}")
            continue

        rid = make_id()
        survey_id_map[row['operational_area_id']] = rid
        fields = map_row(row, SURVEY_MAP)
        geometry = pg_to_geojson(row)
        relations = {'liesWithin': [einsatzort_id]}

        doc = make_doc(rid, identifier, 'Survey', fields, relations, geometry, row=row)
        new_docs.append(doc)
        print(f"  NEW: {identifier} ({row.get('name', '?')})")

    # ----------------------------------------------------------
    # 2. BUILDINGS -> liesWithin: [Place]
    # ----------------------------------------------------------
    print("\n--- Buildings ---")
    gdf = read_geo_table(
        f'SELECT * FROM {PG_SCHEMA}.building', engine
    )
    print(f"  PG rows: {len(gdf)}")

    for _, row in gdf.iterrows():
        bname = row.get('building_name', row['building_id'])
        identifier = f"B-{row.get('building_nr', row['building_id'])}"
        if identifier in existing_identifiers:
            building_id_map[row['building_id']] = existing_identifiers[identifier]
            print(f"  SKIP (exists): {identifier}")
            continue

        rid = make_id()
        building_id_map[row['building_id']] = rid

        # Map custom PG fields (unmapped columns stay as kgr_chru_datamodel:*)
        fields = map_row(row, BUILDING_CUSTOM_MAP)

        # Map PG columns to KGR Building:default fields
        kgr_fields = map_kgr_fields(row, BUILDING_KGR_MAP)
        fields.update(kgr_fields)

        geometry = pg_to_geojson(row)
        relations = {'liesWithin': [einsatzort_id]}

        doc = make_doc(rid, identifier, 'Building', fields, relations, geometry, row=row)
        new_docs.append(doc)

        # Create Kgr:WallOutside subcategory document from facade_* columns
        wall_doc, wall_rid = make_wall_outside_doc(row, identifier, rid)
        if wall_doc:
            new_docs.append(wall_doc)
            print(f"  NEW: {identifier} ({bname}) + WallOutside")
        else:
            print(f"  NEW: {identifier} ({bname})")

        # Create Kgr:RoofOutside subcategory document from roof_* columns
        roof_doc, roof_rid = make_roof_outside_doc(row, identifier, rid)
        if roof_doc:
            new_docs.append(roof_doc)
            print(f"       + RoofOutside")

    # ----------------------------------------------------------
    # 2b. SURVEY "Unzugeordnet" (catch-all for orphan Finds)
    # ----------------------------------------------------------
    print(f"\n--- Survey: {ORPHAN_SURVEY_IDENTIFIER} (orphan catch-all) ---")
    if ORPHAN_SURVEY_IDENTIFIER in existing_identifiers:
        unzugeordnet_id = existing_identifiers[ORPHAN_SURVEY_IDENTIFIER]
        print(f"  EXISTS: {ORPHAN_SURVEY_IDENTIFIER} (id={unzugeordnet_id})")
    else:
        unzugeordnet_id = make_id()
        relations = {'liesWithin': [einsatzort_id]}
        doc = make_doc(unzugeordnet_id, ORPHAN_SURVEY_IDENTIFIER, 'Survey', {}, relations)
        new_docs.append(doc)
        existing_identifiers[ORPHAN_SURVEY_IDENTIFIER] = unzugeordnet_id
        print(f"  NEW: {ORPHAN_SURVEY_IDENTIFIER} (id={unzugeordnet_id})")

    # First Building ID as fallback for orphan Damage (Damage can't go under Survey)
    first_building_id = next(iter(building_id_map.values()), None)

    # ----------------------------------------------------------
    # 3. LEVELS (floors/rooms)
    # ----------------------------------------------------------
    print("\n--- Levels (floors) ---")
    floor_tables = ['floor-1', 'floor0', 'floor1', 'floor2', 'floor3']
    for ftable in floor_tables:
        try:
            gdf = read_geo_table(
                f'SELECT * FROM {PG_SCHEMA}."{ftable}"', engine
            )
        except Exception:
            continue
        if len(gdf) == 0:
            continue

        print(f"  Table {ftable}: {len(gdf)} rows")
        for _, row in gdf.iterrows():
            frid = row.get('floor_room_id', '')
            identifier = f"L-{frid}"
            bid = row.get('building_id', '')
            fnr = row.get('floor_nr', '')
            level_key = f"{bid}_{fnr}" if bid and pd.notna(fnr) else frid

            if identifier in existing_identifiers:
                level_id_map[level_key] = existing_identifiers[identifier]
                print(f"    SKIP (exists): {identifier}")
                continue

            rid = make_id()
            level_id_map[level_key] = rid
            fields = map_row(row, LEVEL_MAP)
            geometry = pg_to_geojson(row)

            # Relation: isRecordedIn -> Building (the Operation)
            building_rid = building_id_map.get(row.get('building_id'))
            relations = {}
            if building_rid:
                relations['isRecordedIn'] = [building_rid]

            doc = make_doc(rid, identifier, 'Level', fields, relations, geometry, row=row)
            new_docs.append(doc)
            print(f"    NEW: {identifier}")

    # ----------------------------------------------------------
    # 4. FINDINGS -> Find
    # ----------------------------------------------------------
    print("\n--- Findings -> Find ---")
    gdf = read_geo_table(
        f'SELECT * FROM {PG_SCHEMA}.findings', engine
    )
    print(f"  PG rows: {len(gdf)}")

    for _, row in gdf.iterrows():
        fid = row['finding_id']
        identifier = fid  # Use finding_id as identifier directly
        if identifier in existing_identifiers:
            print(f"  SKIP (exists): {identifier}")
            continue

        rid = make_id()
        fields = map_row(row, FIND_MAP)
        geometry = pg_to_geojson(row)

        # Relation: isRecordedIn -> Operation (Building or Survey)
        relations = {}
        pg_building_id = row.get('building_name')  # confusingly named column
        floor_nr = row.get('floor_nummer')
        op_area_id = row.get('op_area_id')

        if pd.notna(pg_building_id) and pg_building_id:
            building_rid = building_id_map.get(pg_building_id)
            if building_rid:
                # Try to nest in Level first (more specific)
                floor_str = str(int(floor_nr)) if pd.notna(floor_nr) else None
                level_rid = level_id_map.get(f'{pg_building_id}_{floor_str}') if floor_str else None

                if level_rid:
                    relations['liesWithin'] = [level_rid]
                # isRecordedIn always points to the Building (Operation)
                relations['isRecordedIn'] = [building_rid]

        # Fallback: link to Survey via op_area_id
        if 'isRecordedIn' not in relations and pd.notna(op_area_id) and op_area_id:
            survey_rid = survey_id_map.get(op_area_id)
            if survey_rid:
                relations['isRecordedIn'] = [survey_rid]

        # Last resort: assign to Unzugeordnet
        if not relations:
            relations['isRecordedIn'] = [unzugeordnet_id]
            print(f"  ORPHAN -> {ORPHAN_SURVEY_IDENTIFIER}: {identifier}")

        doc = make_doc(rid, identifier, 'Find', fields, relations, geometry, row=row)
        new_docs.append(doc)

    print(f"  Created {sum(1 for d in new_docs if d['resource']['category'] == 'Find')} Find docs")

    # ----------------------------------------------------------
    # 5. DAMAGE
    # ----------------------------------------------------------
    print("\n--- Damage ---")
    damage_tables = ['damage_floor-1', 'damage_floor0', 'damage_floor1', 'damage_floor2', 'damage_floor3']
    damage_count = 0
    for dtable in damage_tables:
        try:
            gdf = read_geo_table(
                f'SELECT * FROM {PG_SCHEMA}."{dtable}"', engine
            )
        except Exception:
            continue
        if len(gdf) == 0:
            continue

        print(f"  Table {dtable}: {len(gdf)} rows")
        for _, row in gdf.iterrows():
            did = row.get('damage_id', '')
            identifier = did
            if identifier in existing_identifiers:
                print(f"    SKIP (exists): {identifier}")
                continue

            rid = make_id()

            # Map custom PG fields (unmapped columns stay as kgr_chru_datamodel:*)
            fields = map_row(row, DAMAGE_CUSTOM_MAP)

            # Map PG columns to Kgr:Damage KGR fields
            kgr_fields = map_kgr_fields(row, DAMAGE_KGR_MAP)
            fields.update(kgr_fields)

            geometry = pg_to_geojson(row)

            # Relation: isRecordedIn -> Building (Kgr:Damage is a BuildingPart)
            relations = {}
            building_rid = building_id_map.get(row.get('building_id'))
            if building_rid:
                relations['isRecordedIn'] = [building_rid]
            elif first_building_id:
                relations['isRecordedIn'] = [first_building_id]
                print(f"    ORPHAN DAMAGE -> first Building: {identifier}")
            else:
                print(f"    WARNING: {identifier} has no building and no fallback!")

            # Use Kgr:Damage category (developer's KGR subcategory, not built-in Damage)
            doc = make_doc(rid, identifier, 'Kgr:Damage', fields, relations, geometry, row=row)
            new_docs.append(doc)
            damage_count += 1

    print(f"  Created {damage_count} Kgr:Damage docs")

    # ----------------------------------------------------------
    # SAVE
    # ----------------------------------------------------------
    print(f"\n{'=' * 60}")
    print(f"Total new documents: {len(new_docs)}")
    by_cat = {}
    for d in new_docs:
        cat = d['resource']['category']
        by_cat[cat] = by_cat.get(cat, 0) + 1
    for cat, count in sorted(by_cat.items()):
        print(f"  {cat}: {count}")

    print(f"\nSaving to {FIELD_URL}/{FIELD_DB}...")
    save_docs(new_docs, FIELD_URL, FIELD_DB, FIELD_AUTH, dry_run=DRY_RUN)

    print("\nDone!")


if __name__ == '__main__':
    main()
