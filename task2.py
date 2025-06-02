import pandas as pd
import mysql.connector
import logging
import re
import os
from typing import Tuple, Optional, Dict, Any
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)

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

# MySQL table schema
TABLE_SCHEMA = """
CREATE TABLE user (
    id INT AUTO_INCREMENT PRIMARY KEY,
    full_name VARCHAR(255),
    phone VARCHAR(50),
    country VARCHAR(100),
    region VARCHAR(100),
    email VARCHAR(255),
    age INT,
    errors TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

# Country codes configuration
COUNTRY_CODES = {
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

class ETLProcessor:
    """ETL processor for user data from Excel to MySQL."""
    
    def __init__(self, excel_file: str, mysql_config: Dict[str, str], table_name: str):
        self.excel_file = excel_file
        self.mysql_config = mysql_config
        self.table_name = table_name
        self._validate_config()
    
    def _validate_config(self) -> None:
        """Validate MySQL configuration."""
        required_keys = ['host', 'user', 'password', 'database']
        missing_keys = [key for key in required_keys if not self.mysql_config.get(key)]
        if missing_keys:
            raise ValueError(f"Missing MySQL configuration: {', '.join(missing_keys)}")
        
        if not os.path.exists(self.excel_file):
            raise FileNotFoundError(f"Excel file not found: {self.excel_file}")
    
    @staticmethod
    def remove_double_spaces(text: Any) -> str:
        """Remove double spaces and trim whitespace."""
        if isinstance(text, str):
            return re.sub(r'\s+', ' ', text).strip()
        return str(text) if text is not None else ""
    
    @staticmethod
    def validate_email(email: Any) -> Tuple[Optional[str], str]:
        """Validate email format."""
        if pd.isna(email) or not email:
            return None, "Email is empty"
        
        email_clean = ETLProcessor.remove_double_spaces(str(email))
        pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
        
        if re.match(pattern, email_clean):
            return email_clean, ""
        return None, f"Invalid email format: {email_clean}"
    
    @staticmethod
    def validate_phone(phone: Any) -> Tuple[Optional[str], str]:
        """Validate and format phone number."""
        if pd.isna(phone) or not phone:
            return None, "Phone is empty"
        
        phone_raw = ETLProcessor.remove_double_spaces(str(phone))
        phone_digits = re.sub(r'\D', '', phone_raw)
        
        if len(phone_digits) < 10:
            return None, f"Phone too short: {phone_raw}"
        
        # Handle Russian local formats
        phone_clean = ETLProcessor._format_russian_phone(phone_digits, phone_raw)
        
        # Validate against country codes
        validation_error = ETLProcessor._validate_country_code(phone_clean)
        if validation_error:
            return None, f"Invalid phone '{phone_raw}': {validation_error}"
        
        return phone_clean, ""
    
    @staticmethod
    def _format_russian_phone(phone_digits: str, phone_raw: str) -> str:
        """Format Russian phone numbers to international format."""
        if phone_digits.startswith('8') and len(phone_digits) == 11:
            return '+7' + phone_digits[1:]
        elif phone_digits.startswith('7') and len(phone_digits) == 11:
            return '+7' + phone_digits[1:]
        elif phone_digits.startswith('9') and len(phone_digits) == 10:
            return '+7' + phone_digits
        elif phone_raw.startswith('+'):
            return '+' + phone_digits
        else:
            return '+' + phone_digits if phone_digits else phone_digits
    
    @staticmethod
    def _validate_country_code(phone_clean: str) -> str:
        """Validate phone number against country codes."""
        if not phone_clean or not phone_clean.startswith('+'):
            return "Missing country code"
        
        for country, info in COUNTRY_CODES.items():
            for code in info['codes']:
                if phone_clean.startswith('+' + code):
                    expected_length = len(code) + 1 + (info['length'] - len(code))
                    if len(phone_clean) == expected_length:
                        return ""  # Valid
                    else:
                        return f"Invalid length for {country}"
        
        return "Unknown country code"
    
    @staticmethod
    def validate_age(age: Any) -> Tuple[Optional[int], str]:
        """Validate age value."""
        if pd.isna(age) or age == "":
            return None, "Age is empty"
        
        try:
            age_val = float(age)
            age_int = int(round(age_val))
            if 18 <= age_int <= 120:
                return age_int, ""
            else:
                return None, f"Age {age_int} out of range (18-120)"
        except (ValueError, TypeError):
            return None, f"Invalid age format: {age}"
    
    def extract_data(self) -> pd.DataFrame:
        """Extract and validate data from Excel file."""
        logger.info(f"Extracting data from {self.excel_file}")
        
        try:
            df = pd.read_excel(self.excel_file)
            logger.info(f"Loaded {len(df)} rows from Excel")
        except Exception as e:
            logger.error(f"Failed to read Excel file: {e}")
            raise
        
        # Rename columns
        df = df.rename(columns=COLUMN_MAP)
        
        # Clean string columns
        string_columns = ['full_name', 'phone', 'country', 'region', 'email']
        for col in string_columns:
            if col in df.columns:
                df[col] = df[col].apply(self.remove_double_spaces)
        
        # Validate and collect errors
        df = self._validate_dataframe(df)
        
        logger.info("Data extraction and validation completed")
        return df[list(COLUMN_MAP.values()) + ['errors']]
    
    def _validate_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate all rows in dataframe and collect errors."""
        errors = []
        emails = []
        phones = []
        ages = []
        
        for idx, row in df.iterrows():
            row_errors = []
            
            # Check required fields
            required_fields = ['full_name', 'country', 'region']
            for field in required_fields:
                if pd.isna(row.get(field)) or not str(row.get(field)).strip():
                    row_errors.append(f"{field} is empty")
            
            # Validate email
            email, email_err = self.validate_email(row.get('email'))
            emails.append(email)
            if email_err:
                row_errors.append(email_err)
            
            # Validate phone
            phone, phone_err = self.validate_phone(row.get('phone'))
            phones.append(phone)
            if phone_err:
                row_errors.append(phone_err)
            
            # Validate age
            age, age_err = self.validate_age(row.get('age'))
            ages.append(age)
            if age_err:
                row_errors.append(age_err)
            
            errors.append("; ".join(row_errors) if row_errors else "")
        
        # Update dataframe with validated data
        df['email'] = emails
        df['phone'] = phones
        df['age'] = ages
        df['errors'] = errors
        
        return df
    
    def recreate_database_and_table(self) -> None:
        """Recreate database and table."""
        logger.info("Recreating database and table")
        
        try:
            # Connect without database to recreate it
            config_no_db = self.mysql_config.copy()
            db_name = config_no_db.pop('database')
            
            with mysql.connector.connect(**config_no_db) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")
                    cursor.execute(f"CREATE DATABASE {db_name}")
                    conn.commit()
                    logger.info(f"Database {db_name} recreated")
            
            # Create table in new database
            with mysql.connector.connect(**self.mysql_config) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"DROP TABLE IF EXISTS {self.table_name}")
                    cursor.execute(TABLE_SCHEMA)
                    conn.commit()
                    logger.info(f"Table {self.table_name} created")
                    
        except mysql.connector.Error as e:
            logger.error(f"Database error: {e}")
            raise
    
    def load_to_mysql(self, df: pd.DataFrame) -> None:
        """Load dataframe to MySQL table."""
        logger.info(f"Loading {len(df)} rows to MySQL table {self.table_name}")
        
        try:
            with mysql.connector.connect(**self.mysql_config) as conn:
                with conn.cursor() as cursor:
                    cols = ','.join(df.columns)
                    placeholders = ','.join(['%s'] * len(df.columns))
                    insert_sql = f"INSERT INTO {self.table_name} ({cols}) VALUES ({placeholders})"
                    
                    # Convert DataFrame to list of tuples, handling NaN values
                    data = []
                    for row in df.itertuples(index=False, name=None):
                        clean_row = tuple(
                            None if pd.isna(x) else x for x in row
                        )
                        data.append(clean_row)
                    
                    cursor.executemany(insert_sql, data)
                    conn.commit()
                    logger.info(f"Successfully inserted {len(data)} rows")
                    
        except mysql.connector.Error as e:
            logger.error(f"Failed to load data to MySQL: {e}")
            raise
    
    def run_etl(self) -> None:
        """Run the complete ETL process."""
        logger.info("Starting ETL process")
        
        try:
            self.recreate_database_and_table()
            df = self.extract_data()
            self.load_to_mysql(df)
            logger.info("ETL process completed successfully")
            
            # Log summary statistics
            error_count = len(df[df['errors'] != ''])
            logger.info(f"Processed {len(df)} records, {error_count} with validation errors")
            
        except Exception as e:
            logger.error(f"ETL process failed: {e}")
            raise


def main():
    """Main entry point."""
    try:
        etl = ETLProcessor(EXCEL_FILE, MYSQL_CONFIG, TABLE_NAME)
        etl.run_etl()
    except Exception as e:
        logger.error(f"Application failed: {e}")
        return 1
    return 0


if __name__ == "__main__":
    exit(main())
