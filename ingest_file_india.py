import sys
import json
import hashlib
import pandas as pd
from datetime import datetime, timedelta
import re
import os
import psycopg2

# --- INDIA-SPECIFIC CONFIGURATION ---

COL_INDIVIDUAL_ID = 'Individual ID'
COL_FIRST_NAME = 'First Name'
COL_MIDDLE_NAME = 'Middle Name'
COL_LAST_NAME = 'Last Name'
COL_SEX = 'Sex'
COL_MOBILE = 'Mobile #'
COL_AGE = 'Age'
# Facility hierarchy columns
COL_REGION = 'Region'
COL_DISTRICT = 'District'
COL_PHC = 'PHC'
COL_SHC = 'SHC'
# HTN columns
COL_HTN_TREATMENT_START = 'HTN_Treatment_Start_Date'
COL_HTN_LAST_FOLLOWUP = 'HTN_LastFollowup_Completed_Date'
COL_HTN_TREATMENT_STATUS = 'HTN_Treatment_Status'
COL_HYPERTENSION_CONTROL = 'Hypertension_Control'
# DM columns
COL_DM_TREATMENT_START = 'Diabetes_Treatment_Start_Date'
COL_DM_LAST_FOLLOWUP = 'Diabetes_LastFollowup_Completed_Date'
COL_DM_STATUS = 'DM_Treatment_Status'
COL_DM_CONTROL = 'Diabetes_Control'

CSV_DATE_FORMATS = ["%Y-%m-%d", "%d-%m-%Y", "%d/%m/%y"]
DATE_FORMAT_OUT = "%Y-%m-%d"

# --- DATABASE CONNECTION DETAILS ---
DB_CONNECTION_PARAMS = {
    'host': os.getenv('POSTGRES_HOST', 'postgres'),
    'database': os.getenv('POSTGRES_DB', 'metrics_db'),
    'user': os.getenv('POSTGRES_USER', 'grafana_user'),
    'password': os.getenv('POSTGRES_PASSWORD', 'your_db_password'),
}
SP_REGION_VALUE = 'India'

NEWLY_DIAGNOSED_MONTHS = 3
CONTROLLED_BP = (120, 80)
# Using 141/91 to satisfy database view logic (systolic > 140 OR diastolic > 90)
UNCONTROLLED_BP = (141, 91)
CONTROLLED_SUGAR = 130
UNCONTROLLED_SUGAR = 150
DEFAULT_SUGAR_TYPE = 'random'

# --- HELPER FUNCTIONS ---

def uuid_to_int_hash(uuid_str):
    if pd.isna(uuid_str) or not uuid_str:
        return None
    digest = hashlib.sha256(str(uuid_str).strip().encode('utf-8')).hexdigest()
    return int(digest[:15], 16) % (2**63)

def semantic_normalize(text):
    if pd.isna(text) or text is None:
        return ''
    s = str(text).strip().lower()
    s = re.sub(r'[^\w\s]', '', s)
    s = re.sub(r'[_\-\s]+', '', s)
    return s

def calculate_birth_date(age, reference_date=None):
    if reference_date is None:
        reference_date = datetime.now()

    if pd.isna(age) or age is None:
        return None

    try:
        age_int = int(float(str(age).strip()))
        if age_int < 0 or age_int > 150:
            return None
        birth_date = reference_date - timedelta(days=int(age_int * 365.25))
        return birth_date
    except (ValueError, TypeError):
        return None

def parse_india_date(date_str):
    if pd.isna(date_str) or date_str is None or str(date_str).strip() == '':
        return None

    date_str = str(date_str).strip()
    for fmt in CSV_DATE_FORMATS:
        try:
            return datetime.strptime(date_str, fmt)
        except (ValueError, TypeError):
            continue
    return None

def is_newly_diagnosed(start_date, reference_date=None):
    if start_date is None:
        return False

    if reference_date is None:
        reference_date = datetime.now()

    months_diff = (reference_date.year - start_date.year) * 12 + (reference_date.month - start_date.month)
    return months_diff <= NEWLY_DIAGNOSED_MONTHS

def synthesize_bp_from_status(htn_status, control_status):
    htn_norm = semantic_normalize(htn_status)
    control_norm = semantic_normalize(control_status)

    if not htn_norm or htn_norm == '':
        return None, None, 'not_under_htn_care'

    if 'control' in control_norm and 'uncontrol' not in control_norm:
        return CONTROLLED_BP[0], CONTROLLED_BP[1], None

    if 'uncontrol' in control_norm:
        return UNCONTROLLED_BP[0], UNCONTROLLED_BP[1], None

    if 'undetermin' in control_norm or control_norm == '':
        if 'miss' in htn_norm or 'follow' in htn_norm:
            return None, None, 'missed_followup'
        return None, None, 'undetermined_exclude'

    return None, None, 'status_unclear'

def synthesize_diabetes(dm_status, control_status):
    dm_norm = semantic_normalize(dm_status)
    control_norm = semantic_normalize(control_status)

    if not dm_norm:
        return None, None, 'not_under_dm_care'

    # Newly diagnosed: insert patient only, no blood sugar record
    if 'newlydiagnosed' in dm_norm or 'newly' in dm_norm:
        return None, None, 'newly_diagnosed'

    # Missed follow-up: no blood sugar regardless of control
    if 'miss' in dm_norm:
        return None, None, 'missed_followup'

    # Adhering to follow-up: use control status
    if 'control' in control_norm and 'uncontrol' not in control_norm:
        return CONTROLLED_SUGAR, DEFAULT_SUGAR_TYPE, None

    if 'uncontrol' in control_norm:
        return UNCONTROLLED_SUGAR, DEFAULT_SUGAR_TYPE, None

    # Undetermined or blank control for adhering patients
    return None, None, 'undetermined_exclude'

# --- SQL HELPER FUNCTIONS ---

def safe_str(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    return str(value)

def to_sql_literal(value, target_type=None):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        if target_type == 'bigint':
            return 'NULL::BIGINT'
        elif target_type == 'DATE':
            return 'NULL::DATE'
        elif target_type == 'TIMESTAMP':
            return 'NULL::TIMESTAMP'
        elif target_type == 'NUMERIC':
            return 'NULL::NUMERIC'
        else:
            return 'NULL::VARCHAR'

    if target_type == 'bigint':
        val_str = str(value).strip()
        if val_str.endswith('.0'):
            val_str = val_str[:-2]
        if not val_str.isdigit():
            return 'NULL::BIGINT'
        return f"CAST('{val_str}' AS bigint)"

    if target_type == 'DATE' and isinstance(value, (datetime, pd.Timestamp)):
        return f"'{value.strftime('%Y-%m-%d')}'::DATE"

    if target_type == 'TIMESTAMP' and isinstance(value, (datetime, pd.Timestamp)):
        return f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'::timestamp"

    if isinstance(value, str):
        return f"'{value.replace(chr(39), chr(39)+chr(39))}'::VARCHAR"

    if isinstance(value, (int, float)):
        return str(value)

    return f"'{str(value).replace(chr(39), chr(39)+chr(39))}'::VARCHAR"

# --- DATABASE EXECUTION FUNCTIONS (matching reference pattern) ---

def execute_upsert_org_unit_chain(cur, hierarchy):
    """Upsert org_unit hierarchy chain and return leaf org_unit_id.
    hierarchy: list of (name, level) tuples from top to bottom.
    Skips entries with None names.
    """
    names = [h[0] for h in hierarchy if h[0] is not None]
    levels = [h[1] for h in hierarchy if h[0] is not None]
    if not names:
        return None
    names_literal = "ARRAY[" + ",".join(to_sql_literal(n) for n in names) + "]"
    levels_literal = "ARRAY[" + ",".join(str(l) for l in levels) + "]"
    sql = f"SELECT upsert_org_unit_chain({names_literal}::VARCHAR[], {levels_literal}::INTEGER[]);"
    cur.execute(sql)
    return cur.fetchone()[0]

def execute_upsert_patient(cur, patient_id_sql, patient_name, gender, phone_number, registration_date, birth_date, org_unit_id):
    """Insert new patient or update registration_date if earlier."""
    sql = f"""
INSERT INTO patients (patient_id, patient_name, gender, phone_number, patient_status, registration_date, birth_date, org_unit_id)
VALUES (
    {patient_id_sql},
    {to_sql_literal(patient_name)},
    {to_sql_literal(gender)},
    {to_sql_literal(phone_number)},
    'ALIVE'::VARCHAR,
    {to_sql_literal(registration_date, target_type='TIMESTAMP')},
    {to_sql_literal(birth_date, target_type='DATE')},
    {org_unit_id}
)
ON CONFLICT (patient_id) DO UPDATE SET
    registration_date = LEAST(patients.registration_date, EXCLUDED.registration_date);
"""
    cur.execute(sql)

def execute_insert_encounter(cur, patient_id_sql, encounter_datetime, org_unit_id):
    """Create encounter (or get existing). Returns encounter_id."""
    sql = f"""
INSERT INTO encounters (patient_id, encounter_date, org_unit_id)
VALUES ({patient_id_sql}, {to_sql_literal(encounter_datetime, target_type='TIMESTAMP')}, {org_unit_id})
ON CONFLICT (patient_id, encounter_date)
DO UPDATE SET org_unit_id = EXCLUDED.org_unit_id
RETURNING id;
"""
    cur.execute(sql)
    return cur.fetchone()[0]

def execute_insert_bp(cur, encounter_id, systolic, diastolic):
    """Insert blood pressure for an encounter."""
    if systolic is None and diastolic is None:
        return
    sql = f"""
INSERT INTO blood_pressures (encounter_id, systolic_bp, diastolic_bp)
VALUES ({encounter_id}, {to_sql_literal(systolic, target_type='NUMERIC')}, {to_sql_literal(diastolic, target_type='NUMERIC')})
ON CONFLICT (encounter_id) DO UPDATE SET
    systolic_bp = EXCLUDED.systolic_bp, diastolic_bp = EXCLUDED.diastolic_bp;
"""
    cur.execute(sql)

def execute_insert_bs(cur, encounter_id, blood_sugar_type, blood_sugar_value):
    """Insert blood sugar for an encounter."""
    if blood_sugar_value is None:
        return
    sql = f"""
INSERT INTO blood_sugars (encounter_id, blood_sugar_type, blood_sugar_value)
VALUES ({encounter_id}, {to_sql_literal(safe_str(blood_sugar_type))}, {to_sql_literal(blood_sugar_value, target_type='NUMERIC')})
ON CONFLICT (encounter_id) DO UPDATE SET
    blood_sugar_type = EXCLUDED.blood_sugar_type, blood_sugar_value = EXCLUDED.blood_sugar_value;
"""
    cur.execute(sql)

# --- MAIN INGESTION AND EXECUTION FUNCTION ---

def ingest_and_execute(file_path: str) -> None:
    """
    Reads an Excel file, synthesizes BP/BS from status fields, and inserts
    into the database using direct SQL (matching reference hierarchy pattern).

    Facility hierarchy: Region → District → PHC → SHC

    Encounter date priority:
      HTN followup → DM followup → Registration date

    When BOTH followup dates are present with BP + BS data,
    two separate encounters are created with their respective dates.
    """

    DTYPE_MAPPING = {COL_INDIVIDUAL_ID: str, COL_MOBILE: str}

    stats = {
        'total_rows': 0,
        'unique_patients': set(),
        'under_htn_care': 0,
        'controlled_count': 0,
        'uncontrolled_count': 0,
        'missed_followup_count': 0,
        'newly_diagnosed_excluded': 0,
        'not_under_care_excluded': 0,
        'invalid_registration_date': 0,
        'processed_records': 0
    }

    try:
        if file_path.lower().endswith('.csv'):
            df_data = pd.read_csv(file_path, dtype=DTYPE_MAPPING)
        else:
            df_data = pd.read_excel(
                file_path,
                sheet_name=0,
                header=0,
                dtype=DTYPE_MAPPING,
                engine='openpyxl'
            )
    except Exception as e:
        print(f"Error loading file: {e}", file=sys.stderr)
        return

    stats['total_rows'] = len(df_data)

    print(f"Columns found: {list(df_data.columns)}", file=sys.stderr)
    print(f"Total rows: {len(df_data)}", file=sys.stderr)

    if df_data.empty:
        print("Error: No data rows found in Excel", file=sys.stderr)
        return

    conn = None
    cur = None

    try:
        conn = psycopg2.connect(**DB_CONNECTION_PARAMS)
        conn.autocommit = True
        cur = conn.cursor()

        for idx, row in df_data.iterrows():
            if pd.isna(row.get(COL_INDIVIDUAL_ID)) or str(row.get(COL_INDIVIDUAL_ID)).strip() == '':
                continue

            htn_start = parse_india_date(row.get(COL_HTN_TREATMENT_START))
            dm_start = parse_india_date(row.get(COL_DM_TREATMENT_START))

            # Registration date: earliest of HTN or DM start date
            if htn_start and dm_start:
                registration_date = min(htn_start, dm_start)
            elif htn_start:
                registration_date = htn_start
            elif dm_start:
                registration_date = dm_start
            else:
                stats['invalid_registration_date'] += 1
                print(f"Row {idx + 2}: Skipping - no HTN or DM treatment start date", file=sys.stderr)
                continue

            htn_status = row.get(COL_HTN_TREATMENT_STATUS)
            control_status = row.get(COL_HYPERTENSION_CONTROL)

            systolic, diastolic, exclusion_reason = synthesize_bp_from_status(htn_status, control_status)

            sugar_value, sugar_type, dm_reason = synthesize_diabetes(
                row.get(COL_DM_STATUS),
                row.get(COL_DM_CONTROL)
            )

            if dm_reason in ('not_under_dm_care', 'undetermined_exclude', 'status_unclear', 'newly_diagnosed', 'missed_followup'):
                sugar_value = None
                sugar_type = None

            # Parse DM followup date for encounter splitting
            dm_followup_date = parse_india_date(row.get(COL_DM_LAST_FOLLOWUP))

            if exclusion_reason == 'not_under_htn_care':
                stats['not_under_care_excluded'] += 1
                print(f"Row {idx + 2}: Excluding - not under HTN care (blank status)", file=sys.stderr)
                continue

            stats['under_htn_care'] += 1
            patient_id = uuid_to_int_hash(row.get(COL_INDIVIDUAL_ID))
            stats['unique_patients'].add(patient_id)

            is_newly = is_newly_diagnosed(registration_date)
            if is_newly:
                stats['newly_diagnosed_excluded'] += 1
                print(f"Row {idx + 2}: Newly diagnosed (within {NEWLY_DIAGNOSED_MONTHS} months) - will create patient record but exclude from outcomes", file=sys.stderr)

            # Determine HTN encounter datetime
            if is_newly:
                encounter_datetime = registration_date
            elif exclusion_reason == 'missed_followup':
                stats['missed_followup_count'] += 1
                followup_date = parse_india_date(row.get(COL_HTN_LAST_FOLLOWUP))
                if followup_date is not None and followup_date != registration_date:
                    encounter_datetime = followup_date
                else:
                    encounter_datetime = registration_date
            elif exclusion_reason == 'undetermined_exclude':
                print(f"Row {idx + 2}: Excluding - undetermined status (exclude from outcomes)", file=sys.stderr)
                continue
            else:
                followup_date = parse_india_date(row.get(COL_HTN_LAST_FOLLOWUP))
                if followup_date is not None and followup_date != registration_date:
                    encounter_datetime = followup_date
                else:
                    encounter_datetime = registration_date

            if systolic == CONTROLLED_BP[0] and diastolic == CONTROLLED_BP[1]:
                stats['controlled_count'] += 1
            elif systolic == UNCONTROLLED_BP[0] and diastolic == UNCONTROLLED_BP[1]:
                stats['uncontrolled_count'] += 1

            # Build patient fields
            first_name = str(row.get(COL_FIRST_NAME, '')).strip() if not pd.isna(row.get(COL_FIRST_NAME)) else ''
            middle_name = str(row.get(COL_MIDDLE_NAME, '')).strip() if not pd.isna(row.get(COL_MIDDLE_NAME)) else ''
            last_name = str(row.get(COL_LAST_NAME, '')).strip() if not pd.isna(row.get(COL_LAST_NAME)) else ''
            patient_name = ' '.join(filter(None, [first_name, middle_name, last_name])).strip()
            if not patient_name:
                patient_name = None

            gender = safe_str(row.get(COL_SEX)) if not pd.isna(row.get(COL_SEX)) else None

            phone_raw = row.get(COL_MOBILE)
            if pd.isna(phone_raw):
                phone_number = None
            else:
                phone_str = str(phone_raw).strip()
                if phone_str.endswith('.0'):
                    phone_str = phone_str[:-2]
                if phone_str.lower() == 'nan' or phone_str == '':
                    phone_number = None
                else:
                    phone_number = phone_str

            birth_date = calculate_birth_date(row.get(COL_AGE))

            # Facility hierarchy (Region → District → PHC → SHC)
            region = safe_str(row.get(COL_REGION)) or SP_REGION_VALUE
            district = safe_str(row.get(COL_DISTRICT))
            phc = safe_str(row.get(COL_PHC)) or 'UNKNOWN'
            shc = safe_str(row.get(COL_SHC))

            # Build hierarchy: (name, level) tuples from top to bottom
            # Level 1=Region, 2=District, 3=PHC, 4=SHC
            hierarchy = [
                (region, 1),
                (district, 2),
                (phc, 3),
                (shc, 4),
            ]

            has_bp = systolic is not None
            has_bs = sugar_value is not None

            # Log the record
            log_record = {
                'patient_id': patient_id,
                'patient_name': patient_name,
                'facility': phc,
                'registration_date': registration_date.strftime(DATE_FORMAT_OUT) if registration_date else None,
                'encounter_datetime': encounter_datetime.strftime('%Y-%m-%d %H:%M:%S') if encounter_datetime else None,
                'systolic_bp': systolic,
                'diastolic_bp': diastolic,
                'htn_status': str(htn_status) if not pd.isna(htn_status) else None,
                'control_status': str(control_status) if not pd.isna(control_status) else None
            }
            print(json.dumps(log_record, ensure_ascii=False, default=str))

            # --- Per-Row Insertion ---
            try:
                patient_id_sql = to_sql_literal(patient_id, target_type='bigint')

                if patient_id_sql == 'NULL::BIGINT':
                    print(f"Row {idx + 2}: Skipping - NULL patient_id", file=sys.stderr)
                    continue

                # 0. Upsert org_unit hierarchy chain (returns leaf org_unit_id)
                org_unit_id = execute_upsert_org_unit_chain(cur, hierarchy)

                # 1. Upsert patient
                execute_upsert_patient(cur, patient_id_sql, patient_name, gender, phone_number, registration_date, birth_date, org_unit_id)

                # 2. Create encounter(s) and insert clinical data
                htn_followup_date = parse_india_date(row.get(COL_HTN_LAST_FOLLOWUP))

                if is_newly:
                    # Newly diagnosed: patient + encounter, no clinical data
                    execute_insert_encounter(cur, patient_id_sql, encounter_datetime, org_unit_id)
                    stats['processed_records'] += 1
                elif htn_followup_date and dm_followup_date and htn_followup_date != dm_followup_date and has_bp and has_bs:
                    # Both dates present, different → 2 encounters
                    bp_enc_id = execute_insert_encounter(cur, patient_id_sql, htn_followup_date, org_unit_id)
                    execute_insert_bp(cur, bp_enc_id, systolic, diastolic)

                    bs_enc_id = execute_insert_encounter(cur, patient_id_sql, dm_followup_date, org_unit_id)
                    execute_insert_bs(cur, bs_enc_id, sugar_type, sugar_value)

                    stats['processed_records'] += 1
                elif htn_followup_date and dm_followup_date and htn_followup_date == dm_followup_date:
                    # Both dates present, same → 1 encounter with BP + BS
                    enc_id = execute_insert_encounter(cur, patient_id_sql, htn_followup_date, org_unit_id)
                    execute_insert_bp(cur, enc_id, systolic, diastolic)
                    execute_insert_bs(cur, enc_id, sugar_type, sugar_value)

                    stats['processed_records'] += 1
                elif htn_followup_date and not dm_followup_date:
                    # HTN only → encounter at HTN date with BP only
                    bp_enc_id = execute_insert_encounter(cur, patient_id_sql, htn_followup_date, org_unit_id)
                    execute_insert_bp(cur, bp_enc_id, systolic, diastolic)

                    stats['processed_records'] += 1
                elif dm_followup_date and not htn_followup_date:
                    # DM only → encounter at DM date with BS only
                    bs_enc_id = execute_insert_encounter(cur, patient_id_sql, dm_followup_date, org_unit_id)
                    execute_insert_bs(cur, bs_enc_id, sugar_type, sugar_value)

                    stats['processed_records'] += 1
                else:
                    # No followup dates → encounter at registration_date
                    enc_id = execute_insert_encounter(cur, patient_id_sql, registration_date, org_unit_id)
                    execute_insert_bp(cur, enc_id, systolic, diastolic)
                    execute_insert_bs(cur, enc_id, sugar_type, sugar_value)

                    stats['processed_records'] += 1

            except psycopg2.Error as e:
                print(f"\n--- RECORD FAILURE ---", file=sys.stderr)
                print(f"Error processing row {idx + 2}. Skipping. Details: {e}", file=sys.stderr)

        print(f"\n--- EXECUTION SUMMARY ---", file=sys.stderr)
        print(f"Total rows in Excel: {stats['total_rows']}", file=sys.stderr)
        print(f"Unique patients: {len(stats['unique_patients'])}", file=sys.stderr)
        print(f"Under HTN care: {stats['under_htn_care']}", file=sys.stderr)
        print(f"Controlled count: {stats['controlled_count']}", file=sys.stderr)
        print(f"Uncontrolled count: {stats['uncontrolled_count']}", file=sys.stderr)
        print(f"Missed follow-up count: {stats['missed_followup_count']}", file=sys.stderr)
        print(f"Newly diagnosed excluded: {stats['newly_diagnosed_excluded']}", file=sys.stderr)
        print(f"Not under care excluded: {stats['not_under_care_excluded']}", file=sys.stderr)
        print(f"Invalid registration date excluded: {stats['invalid_registration_date']}", file=sys.stderr)
        print(f"Successfully processed records: {stats['processed_records']}", file=sys.stderr)

    except psycopg2.Error as e:
        print(f"\n--- CONNECTION ERROR ---", file=sys.stderr)
        print(f"PostgreSQL Connection Error: {e}", file=sys.stderr)

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python ingest_file_india.py <xlsx_file_path>", file=sys.stderr)
        sys.exit(1)
    else:
        ingest_and_execute(sys.argv[1])
