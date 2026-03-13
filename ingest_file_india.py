import sys
import json
import hashlib
import pandas as pd
from datetime import datetime, timedelta
import re
import os
import psycopg2
from typing import Dict, Any, Optional, Tuple

# --- INDIA-SPECIFIC CONFIGURATION ---

COL_INDIVIDUAL_ID = 'Individual ID'
COL_FIRST_NAME = 'First Name'
COL_MIDDLE_NAME = 'Middle Name'
COL_LAST_NAME = 'Last Name'
COL_SEX = 'Sex'
COL_MOBILE = 'Mobile #'
COL_AGE = 'Age'
COL_PHC = 'PHC'
COL_HTN_TREATMENT_START = 'HTN_Treatment_Start_Date'
COL_HTN_LAST_FOLLOWUP = 'HTN_LastFollowup_Completed_Date'
COL_HTN_TREATMENT_STATUS = 'HTN_Treatment_Status'
COL_HYPERTENSION_CONTROL = 'Hypertension_Control'

CSV_DATE_FORMAT = "%d/%m/%y"
DATE_FORMAT_OUT = "%Y-%m-%d"

REGION_VALUE = 'India'

NEWLY_DIAGNOSED_MONTHS = 3
CONTROLLED_BP = (120, 80)
# Using 141/91 to satisfy database view logic (systolic > 140 OR diastolic > 90)
UNCONTROLLED_BP = (141, 91)

DB_CONNECTION_PARAMS = {
    'host': os.getenv('POSTGRES_HOST', 'postgres'),
    'database': os.getenv('POSTGRES_DB', 'metrics_db'),
    'user': os.getenv('POSTGRES_USER', 'grafana_user'),
    'password': os.getenv('POSTGRES_PASSWORD', 'your_db_password'),
}

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

    try:
        return datetime.strptime(str(date_str).strip(), CSV_DATE_FORMAT)
    except (ValueError, TypeError):
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

def generate_sql_insert_statement(record: Dict[str, Any], facility: str, region: str) -> str:
    def to_sql_literal(value, target_type=None):
        if value is None or pd.isna(value):
            return 'NULL'
        
        if target_type == 'bigint':
            if isinstance(value, int):
                return str(value)
            val_str = str(value).strip()
            if val_str.endswith('.0'):
                val_str = val_str[:-2]
            if not val_str.isdigit():
                return 'NULL'
            return f"CAST('{val_str}' AS bigint)"
            
        if target_type == 'DATE' and isinstance(value, (datetime, pd.Timestamp)):
            return f"'{value.strftime('%Y-%m-%d')}'::DATE"
            
        if target_type == 'TIMESTAMP' and isinstance(value, (datetime, pd.Timestamp)):
            return f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'::timestamp"

        if isinstance(value, str):
            escaped = value.replace("'", "''")
            return f"'{escaped}'"
        
        if isinstance(value, (int, float)):
            return str(value)
        
        escaped = str(value).replace("'", "''")
        return f"'{escaped}'"

    patient_id_sql = to_sql_literal(record.get('patient_id'), target_type='bigint')
    birth_date_sql = to_sql_literal(record.get('birth_date'), target_type='DATE')
    encounter_datetime_sql = to_sql_literal(record.get('encounter_datetime'), target_type='TIMESTAMP')

    sql_call = f"""
SELECT insert_heart360_data(
    p_patient_id => {patient_id_sql},
    p_patient_name => {to_sql_literal(record.get('patient_name'))},
    p_gender => {to_sql_literal(record.get('gender'))},
    p_phone_number => {to_sql_literal(str(record.get('phone_number')))},
    p_birth_date => {birth_date_sql},
    p_facility => {to_sql_literal(facility)},
    p_region => {to_sql_literal(region)},
    p_encounter_datetime => {encounter_datetime_sql},
    p_diastolic_bp => {to_sql_literal(record.get('diastolic_bp'))},
    p_systolic_bp => {to_sql_literal(record.get('systolic_bp'))}
);
"""
    return sql_call.strip()

# --- MAIN INGESTION AND EXECUTION FUNCTION ---

def ingest_and_execute(file_path: str) -> None:
    """
    Main function to read CSV data, generate SQL, and execute against the DB 
    using row-by-row commit (autocommit).
    """
    
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
        df_data = pd.read_csv(file_path, dtype={COL_INDIVIDUAL_ID: str})
    except Exception as e:
        print(f"Error loading CSV file: {e}", file=sys.stderr)
        return

    stats['total_rows'] = len(df_data)
    
    required_cols = [
        COL_INDIVIDUAL_ID, COL_FIRST_NAME, COL_LAST_NAME, COL_SEX,
        COL_AGE, COL_PHC, COL_HTN_TREATMENT_START, COL_HTN_TREATMENT_STATUS,
        COL_HYPERTENSION_CONTROL
    ]
    missing_cols = [col for col in required_cols if col not in df_data.columns]
    if missing_cols:
        print(f"Error: Missing required columns: {missing_cols}", file=sys.stderr)
        return
    
    print(f"Detected columns: {list(df_data.columns)}", file=sys.stderr)
    
    if df_data.empty:
        print("Error: No data rows found in CSV", file=sys.stderr)
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
            
            registration_date = parse_india_date(row.get(COL_HTN_TREATMENT_START))
            if registration_date is None:
                stats['invalid_registration_date'] += 1
                print(f"Row {idx + 2}: Skipping - invalid or missing HTN_Treatment_Start_Date", file=sys.stderr)
                continue
            
            htn_status = row.get(COL_HTN_TREATMENT_STATUS)
            control_status = row.get(COL_HYPERTENSION_CONTROL)
            
            systolic, diastolic, exclusion_reason = synthesize_bp_from_status(htn_status, control_status)
            
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

            # Two-step insert ensures registration_date (HTN_Treatment_Start_Date) and 
            # encounter_date (HTN_LastFollowup_Completed_Date) are both correct
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
            
            first_name = str(row.get(COL_FIRST_NAME, '')).strip() if not pd.isna(row.get(COL_FIRST_NAME)) else ''
            middle_name = str(row.get(COL_MIDDLE_NAME, '')).strip() if not pd.isna(row.get(COL_MIDDLE_NAME)) else ''
            last_name = str(row.get(COL_LAST_NAME, '')).strip() if not pd.isna(row.get(COL_LAST_NAME)) else ''
            patient_name = ' '.join(filter(None, [first_name, middle_name, last_name])).strip()
            if not patient_name:
                patient_name = None
            
            gender = row.get(COL_SEX) if not pd.isna(row.get(COL_SEX)) else None
            
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
            facility = str(row.get(COL_PHC, 'UNKNOWN')).strip() if not pd.isna(row.get(COL_PHC)) else 'UNKNOWN'
            
            record = {
                'patient_id': patient_id,
                'patient_name': patient_name,
                'gender': gender,
                'phone_number': phone_number,
                'birth_date': birth_date,
                'encounter_datetime': encounter_datetime,
                'systolic_bp': systolic,
                'diastolic_bp': diastolic
            }
            
            log_record = {
                'patient_id': patient_id,
                'patient_name': patient_name,
                'facility': facility,
                'registration_date': registration_date.strftime(DATE_FORMAT_OUT) if registration_date else None,
                'encounter_datetime': encounter_datetime.strftime('%Y-%m-%d %H:%M:%S') if encounter_datetime else None,
                'systolic_bp': systolic,
                'diastolic_bp': diastolic,
                'htn_status': str(htn_status) if not pd.isna(htn_status) else None,
                'control_status': str(control_status) if not pd.isna(control_status) else None
            }
            print(json.dumps(log_record, ensure_ascii=False, default=str))
            
            try:
                if is_newly:
                    patient_record = record.copy()
                    patient_record['encounter_datetime'] = registration_date
                    patient_record['systolic_bp'] = None
                    patient_record['diastolic_bp'] = None
                    sql_statement = generate_sql_insert_statement(patient_record, facility, REGION_VALUE)
                    cur.execute(sql_statement)
                    stats['processed_records'] += 1
                else:
                    followup_date = parse_india_date(row.get(COL_HTN_LAST_FOLLOWUP))
                    if (followup_date is not None and 
                        followup_date != registration_date and 
                        exclusion_reason != 'undetermined_exclude'):
                        patient_record = record.copy()
                        patient_record['encounter_datetime'] = registration_date
                        patient_record['systolic_bp'] = None
                        patient_record['diastolic_bp'] = None
                        sql_patient = generate_sql_insert_statement(patient_record, facility, REGION_VALUE)
                        cur.execute(sql_patient)
                    
                    sql_statement = generate_sql_insert_statement(record, facility, REGION_VALUE)
                    cur.execute(sql_statement)
                    stats['processed_records'] += 1
                
            except psycopg2.Error as e:
                print(f"\n--- RECORD FAILURE ---", file=sys.stderr)
                print(f"Error processing row {idx + 2}. Skipping. Details: {e}", file=sys.stderr)
                if 'sql_statement' in locals():
                    print(f"SQL: {sql_statement}", file=sys.stderr)

        print(f"\n--- EXECUTION SUMMARY ---", file=sys.stderr)
        print(f"Total rows in CSV: {stats['total_rows']}", file=sys.stderr)
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
        print("Usage: python ingest_file_india.py <csv_file_path>", file=sys.stderr)
        sys.exit(1)
    else:
        ingest_and_execute(sys.argv[1])
