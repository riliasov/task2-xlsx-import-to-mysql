import pandas as pd
import mysql.connector
import logging
import re
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s'
)

# Configurations
EXCEL_FILE = 'user.xlsx'
MYSQL_CONFIG = {
    'host': os.getenv('MYSQL_HOST'),
    'user': os.getenv('MYSQL_USER'),
    'password': os.getenv('MYSQL_PASSWORD'),
    'database': os.getenv('MYSQL_DATABASE')
}
TABLE_NAME = 'user'

# Mapping from Excel (Russian) to MySQL (English) columns
COLUMN_MAP = {
    'ФИО': 'full_name',
    'телефон': 'phone',
    'страна проживания': 'country',
    'район проживания': 'region',
    'email': 'email',
    'возраст': 'age'
}

# MySQL table schema (add Errors column)
TABLE_SCHEMA = """
CREATE TABLE user (
    id INT AUTO_INCREMENT PRIMARY KEY,
    full_name VARCHAR(255),
    phone VARCHAR(50),
    country VARCHAR(100),
    region VARCHAR(100),
    email VARCHAR(255),
    age INT,
    Errors TEXT
)
"""

def remove_double_spaces(s):
    if isinstance(s, str):
        # Remove leading/trailing spaces and replace multiple spaces with a single space
        return re.sub(r'\s+', ' ', s).strip()
    return s

def validate_email(email):
    if pd.isna(email):
        return None, "Email is empty"
    email_clean = remove_double_spaces(str(email).strip())
    pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
    if re.match(pattern, email_clean):
        return email_clean, ""
    else:
        # Set email to None if invalid, and note the error
        return None, "Invalid email"

def validate_phone(phone):
    if pd.isna(phone):
        return None, "Phone is empty"
    phone_raw = remove_double_spaces(str(phone).strip())
    # Remove all characters except digits and plus
    phone_digits = re.sub(r'\D', '', phone_raw)
    country_codes = {
        'RU': {'codes': ['7'], 'length': 11},
        'UA': {'codes': ['380'], 'length': 12},
        'BY': {'codes': ['375'], 'length': 12},
        'KZ': {'codes': ['7'], 'length': 11},
        'UZ': {'codes': ['998'], 'length': 12},
        'AM': {'codes': ['374'], 'length': 11},
        'AZ': {'codes': ['994'], 'length': 12},
        'GE': {'codes': ['995'], 'length': 12},
        'MD': {'codes': ['373'], 'length': 11},
        'KG': {'codes': ['996'], 'length': 12},
        'EE': {'codes': ['372'], 'length': 11},
        'LV': {'codes': ['371'], 'length': 11},
        'LT': {'codes': ['370'], 'length': 11},
        'PL': {'codes': ['48'], 'length': 11},
        'DE': {'codes': ['49'], 'length': 12},
        'FR': {'codes': ['33'], 'length': 11},
        'IT': {'codes': ['39'], 'length': 11},
        'ES': {'codes': ['34'], 'length': 11},
        'GB': {'codes': ['44'], 'length': 12},
        'AE': {'codes': ['971'], 'length': 12},
    }

    error = ""
    phone_clean = None
    detected = False

    # Handle Russian local formats
    if phone_digits.startswith('8') and len(phone_digits) == 11:
        phone_clean = '+7' + phone_digits[1:]
        code = '7'
        detected = True
    elif phone_digits.startswith('7') and len(phone_digits) == 11:
        phone_clean = '+7' + phone_digits[1:]
        code = '7'
        detected = True
    elif phone_digits.startswith('9') and len(phone_digits) == 10:
        phone_clean = '+7' + phone_digits
        code = '7'
        detected = True
    elif phone_raw.startswith('+'):
        phone_clean = '+' + phone_digits
    else:
        phone_clean = '+' + phone_digits if phone_digits else phone_digits

    # Now validate by country code
    for country, info in country_codes.items():
        for code in info['codes']:
            if phone_clean and phone_clean.startswith('+' + code):
                if len(phone_clean) == len(code) + 1 + (info['length'] - len(code)):
                    detected = True
                else:
                    error = f"Invalid phone length for {country} (expected {info['length']} digits after country code)"
                break
        if detected:
            break

    if not detected:
        error = "Unknown or unsupported country code"

    # Basic validation: must have at least 10 digits (international)
    if not phone_digits or len(phone_digits) < 10:
        error = "Invalid phone format (too short for international)"

    if error:
        return None, f"Invalid phone: '{phone_raw}' - {error}"
    return phone_clean, ""

def validate_age(age):
    if pd.isna(age):
        return None, "Age is empty"
    try:
        age_val = float(age)
        age_int = int(round(age_val))
        if 18 <= age_int <= 120:
            return age_int, ""
        else:
            # Set invalid ages to None and return an error message
            return None, f"Age {age_int} is out of valid range (18-120)"
    except (ValueError, TypeError):
        return None, "Invalid age format (must be a number)"
    
def extract_data(excel_file):
    logging.info(f"Extracting data from {excel_file}")
    df = pd.read_excel(excel_file)
    df = df.rename(columns=COLUMN_MAP)
    # Strip spaces and remove double spaces from all string columns
    for col in ['full_name', 'phone', 'country', 'region', 'email']:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: remove_double_spaces(x) if isinstance(x, str) else x)
    # Prepare Errors column
    errors = []
    emails = []
    phones = []
    for idx, row in df.iterrows():
        row_errors = []
        # Check for Nulls in required columns
        for col in ['full_name', 'country', 'region', 'age']:
            if pd.isna(row.get(col)):
                row_errors.append(f"{col} is empty")
        # Validate email
        email, email_err = validate_email(row.get('email'))
        emails.append(email)
        if email_err:
            row_errors.append(email_err)
        # Validate phone
        phone, phone_err = validate_phone(row.get('phone'))
        phones.append(phone)
        if phone_err:
            row_errors.append(phone_err)
        # Validate age and set to None if invalid
        age, age_err = validate_age(row.get('age'))
        if age_err:
            row_errors.append(age_err)
        df.at[idx, 'age'] = age
        errors.append("; ".join(row_errors) if row_errors else "")
    df['email'] = emails
    df['phone'] = phones
    df['Errors'] = errors
    logging.info("Data extracted, columns renamed, spaces stripped, double spaces removed, validation and error collection applied")
    return df[list(COLUMN_MAP.values()) + ['Errors']]

def recreate_db_and_table(mysql_config, table_name, table_schema):
    logging.info("Recreating database and table")
    # Connect to MySQL server (without specifying database)
    config_no_db = mysql_config.copy()
    db_name = config_no_db.pop('database')
    conn = mysql.connector.connect(**config_no_db)
    cursor = conn.cursor()
    cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")
    logging.info(f"Dropped database {db_name} if it existed")
    cursor.execute(f"CREATE DATABASE {db_name}")
    logging.info(f"Created database {db_name}")
    conn.commit()
    cursor.close()
    conn.close()

    # Connect to the new database and create table
    conn = mysql.connector.connect(**mysql_config)
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    logging.info(f"Dropped table {table_name} if it existed")
    cursor.execute(table_schema)
    logging.info(f"Created table {table_name}")
    conn.commit()
    cursor.close()
    conn.close()

def load_to_mysql(df, mysql_config, table_name):
    import numpy as np
    logging.info(f"Loading data into MySQL table {table_name}")
    conn = mysql.connector.connect(**mysql_config)
    cursor = conn.cursor()
    cols = ','.join(df.columns)
    placeholders = ','.join(['%s'] * len(df.columns))
    insert_sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
    for row in df.itertuples(index=False, name=None):
        row = tuple(None if (isinstance(x, float) and np.isnan(x)) else x for x in row)
        cursor.execute(insert_sql, row)
    conn.commit()
    logging.info(f"Inserted {len(df)} rows into {table_name}")
    cursor.close()
    conn.close()

if __name__ == "__main__":
    logging.info("ETL process started")
    recreate_db_and_table(MYSQL_CONFIG, TABLE_NAME, TABLE_SCHEMA)
    df = extract_data(EXCEL_FILE)
    load_to_mysql(df, MYSQL_CONFIG, TABLE_NAME)
    logging.info("ETL process completed successfully")