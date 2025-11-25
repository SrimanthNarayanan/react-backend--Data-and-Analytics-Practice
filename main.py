from flask import Flask, request, send_file, jsonify, session, send_from_directory
import mysql.connector
import psycopg2
import pyodbc
import snowflake.connector
import oracledb
from flask_cors import CORS
from sqlalchemy import create_engine
import pandas as pd
import os
import docx2txt
import PyPDF2
from bs4 import BeautifulSoup
import requests
import json
import bcrypt
from io import BytesIO
import time
from datetime import datetime
import re
import base64
import mimetypes
from reportlab.lib.pagesizes import letter, landscape
from reportlab.platypus import BaseDocTemplate, Frame, PageTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from ydata_profiling import ProfileReport
import uuid
# --- ADDED FROM main1.py (though not used in catalog, good to have if you expand) ---
from cryptography.fernet import Fernet
from werkzeug.middleware.proxy_fix import ProxyFix

encryption_key_str = os.environ.get('ENCRYPTION_KEY')
if not encryption_key_str:
    print("WARNING: 'ENCRYPTION_KEY' not found in .env. Generating a temporary key.")
    key = Fernet.generate_key()
else:
    key = encryption_key_str.encode()
cipher = Fernet(key)

app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)

app.config.update(
    SESSION_COOKIE_SECURE=True, 
    SESSION_COOKIE_SAMESITE='None',
    SESSION_COOKIE_HTTPONLY=True,
)

app.secret_key = os.environ.get('SECRET_KEY')
if not app.secret_key:
    if app.debug:
        print("Warning: SECRET_KEY not set, using default. DO NOT use in production.")
        app.secret_key = '$2b$12$0Jk75xSGIWeEgIwuPp1tKu.DwVPwLF/bbv1p8ZIugc/iUlk4S7jFa'
    else:
        raise ValueError("SECRET_KEY environment variable is not set. App cannot run.")

CORS(app, supports_credentials=True, origins=["http://localhost:3000", "https://Praveen-R-22.github.io","http://localhost:5000"])

REPORTS_DIR = "reports"
os.makedirs(REPORTS_DIR, exist_ok=True)

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY") 
if not GEMINI_API_KEY:
    print("Warning: GEMINI_API_KEY environment variable not set.")
# --- Use f-string safely as key is loaded from env ---
GEMINI_API_URL = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={GEMINI_API_KEY}"

# --- Snowflake App Backend Connection Details ---
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "ANALYTICSBOT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "LANDING")

# --- DATABASE CONNECTION REFACTOR ---
# Removed global 'conn'. We will create connections on-demand.

def get_db_conn():
    """
    Creates and returns a new Snowflake connection for the app's backend.
    """
    try:
        if not all([SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT]):
            raise ValueError("Missing Snowflake environment variables (USER, PASSWORD, ACCOUNT) for app backend")
            
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            # --- UPDATED: Connect without specifying DB/Schema initially for init ---
            # warehouse=SNOWFLAKE_WAREHOUSE,
            # database=SNOWFLAKE_DATABASE,
            # schema=SNOWFLAKE_SCHEMA
        )
        # --- MOVED: Set warehouse/db/schema after creation in init_db ---
        return conn
    except Exception as e:
        print(f"FATAL: Could not connect to Snowflake for app backend: {e}")
        return None

def init_db_snowflake():
    """
    Checks and creates necessary database, schema, and tables on app startup.
    """
    try:
        with get_db_conn() as conn: # Creates a fresh connection
            if not conn:
                print("Skipping DB init, no connection.")
                return
            
            with conn.cursor() as cur:
                
                # --- NEW: Create Database and Schema if they don't exist ---
                cur.execute(f"CREATE DATABASE IF NOT EXISTS {SNOWFLAKE_DATABASE}")
                cur.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_SCHEMA}")
                cur.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")
                cur.execute(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")

                print(f"Ensured Database '{SNOWFLAKE_DATABASE}' and Schema '{SNOWFLAKE_SCHEMA}' exist.")

                # Use .execute() safely with f-strings for non-user-input
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_SCHEMA}.chat_sessions (
                        id INT AUTOINCREMENT START 1 INCREMENT 1 NOT NULL,
                        user_id STRING NOT NULL,
                        created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
                        CONSTRAINT chat_sessions_pkey PRIMARY KEY (id),
                        CONSTRAINT chat_sessions_user_id_key UNIQUE (user_id)
                    );
                """)
                
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_SCHEMA}.messages (
                        id INT AUTOINCREMENT START 1 INCREMENT 1 NOT NULL,
                        session_id INT NULL,
                        sender VARCHAR(10) NOT NULL,
                        text STRING NOT NULL,
                        created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
                        CONSTRAINT messages_pkey PRIMARY KEY (id),
                        CONSTRAINT messages_session_id_fkey FOREIGN KEY (session_id) REFERENCES {SNOWFLAKE_SCHEMA}.chat_sessions(id) ON DELETE CASCADE
                    );
                """)

                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_SCHEMA}.qa_pairs (
                        id INT AUTOINCREMENT START 1 INCREMENT 1 NOT NULL,
                        session_id INT NULL,
                        question STRING NOT NULL,
                        answer STRING NULL,
                        is_helpful BOOLEAN NULL,
                        created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
                        CONSTRAINT qa_pairs_pkey PRIMARY KEY (id),
                        CONSTRAINT qa_pairs_session_id_fkey FOREIGN KEY (session_id) REFERENCES {SNOWFLAKE_SCHEMA}.chat_sessions(id) ON DELETE CASCADE
                    );
                """)
                
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_SCHEMA}.users (
                        id INT AUTOINCREMENT START 1 INCREMENT 1 NOT NULL,
                        username VARCHAR(50) NOT NULL,
                        email VARCHAR(100) NOT NULL,
                        password_hash VARCHAR(255) NOT NULL,
                        role VARCHAR(20) DEFAULT 'user' NOT NULL,
                        created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
                        CONSTRAINT users_email_key UNIQUE (email),
                        CONSTRAINT users_pkey PRIMARY KEY (id),
                        CONSTRAINT users_username_key UNIQUE (username)
                    );
                """)

                # --- NEW: Logic to create default admin user ---
                cur.execute(f"SELECT COUNT(*) FROM {SNOWFLAKE_SCHEMA}.users")
                user_count = cur.fetchone()[0]
                
                if user_count == 0:
                    print("No users found. Creating default 'admin' user...")
                    try:
                        hashed_pw = bcrypt.hashpw(b'admin', bcrypt.gensalt()).decode('utf-8')
                        cur.execute(
                            f"""
                            INSERT INTO {SNOWFLAKE_SCHEMA}.users (username, password_hash, email, role) 
                            VALUES (%s, %s, %s, %s)
                            """,
                            ('admin', hashed_pw, 'admin@example.com', 'admin')
                        )
                        conn.commit()
                        print("Default 'admin' user created successfully.")
                    except Exception as admin_e:
                        print(f"Error creating default admin user: {admin_e}")
                        conn.rollback()
                # --- END NEW ADMIN LOGIC ---

                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_SCHEMA}.user_activity_log
                    (
                        log_id INT AUTOINCREMENT START 1 INCREMENT 1,
                        username VARCHAR(50) NOT NULL,
                        login_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                        logout_time TIMESTAMP_NTZ NULL,
                        duration_minutes INT NULL,
                        tables_generated_for STRING NULL,
                        updated_comments BOOLEAN DEFAULT FALSE,
                        downloaded_pdf BOOLEAN DEFAULT FALSE,
                        downloaded_excel BOOLEAN DEFAULT FALSE,
                        columns_updated_count INT DEFAULT 0,
                        avg_llm_response_time_ms FLOAT NULL,
                        CONSTRAINT activity_log_pkey PRIMARY KEY (log_id),
                        CONSTRAINT activity_log_user_fkey FOREIGN KEY (username) 
                            REFERENCES {SNOWFLAKE_SCHEMA}.users(username) ON DELETE SET NULL
                    );
                """)
                
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_SCHEMA}.saved_connections (
                        id INT AUTOINCREMENT START 1 INCREMENT 1 NOT NULL,
                        username VARCHAR(50) NOT NULL,
                        nickname VARCHAR(100) NOT NULL,
                        db_type VARCHAR(50) NOT NULL,
                        db_host VARCHAR(255) NOT NULL,
                        db_port VARCHAR(10),
                        db_name VARCHAR(100) NOT NULL,
                        db_user VARCHAR(100) NOT NULL,
                        created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
                        
                        CONSTRAINT saved_connections_pkey PRIMARY KEY (id),
                        CONSTRAINT saved_connections_user_fkey FOREIGN KEY (username) 
                            REFERENCES {SNOWFLAKE_SCHEMA}.users(username) ON DELETE CASCADE,
                        CONSTRAINT saved_connections_user_nickname_key UNIQUE (username, nickname)
                    );
                """)

                # --- NEW: Add Semantic Catalog DDLs (Snowflake dialect) ---
                # --- THIS DDL IS NOW CORRECT and matches the INSERT statement ---
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_SCHEMA}.semantic_catalogs (
                        id INTEGER AUTOINCREMENT START 1 INCREMENT 1,
                        username VARCHAR(50) NOT NULL,
                        catalog_name VARCHAR(100) NOT NULL,
                        db_type VARCHAR(50) NOT NULL,
                        db_host VARCHAR(200) NOT NULL,
                        db_port VARCHAR(200) NOT NULL,
                        db_username VARCHAR(100) NOT NULL,
                        db_password_hash VARCHAR(255) NOT NULL,
                        db_dbname VARCHAR(100) NOT NULL,
                        db_schema VARCHAR(200) NOT NULL,
                        created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
                        CONSTRAINT catalog_pkey PRIMARY KEY (id)
                    );
                """)
                
                # --- MIGRATION (ALTER TABLE) BLOCKS REMOVED AS REQUESTED ---

                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_SCHEMA}.semantic_catalogs_detail (
                        id INTEGER AUTOINCREMENT START 1 INCREMENT 1,
                        catalog_id INTEGER,
                        schema_name VARCHAR(200), -- Added schema_name from main1.py logic
                        table_name VARCHAR(200) NOT NULL,
                        column_name VARCHAR(200) NOT NULL,
                        data_type VARCHAR(200), -- Added data_type from main1.py logic
                        description STRING,
                        created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
                        CONSTRAINT catalogdetail_pkey PRIMARY KEY (id),
                        CONSTRAINT catalogdetail_catalog_fkey FOREIGN KEY (catalog_id)
                            REFERENCES {SNOWFLAKE_SCHEMA}.semantic_catalogs(id) ON DELETE CASCADE
                    );
                """)

                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_SCHEMA}.user_catalogs (
                        user_id INT NOT NULL,
                        catalog_id INT NOT NULL,
                        assigned_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
                        CONSTRAINT user_catalogs_pkey PRIMARY KEY (user_id, catalog_id)
                    );
                """)
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_SCHEMA}.user_preferences (
                        username VARCHAR(50) NOT NULL,
                        preferences_data VARIANT,
                        updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
                        CONSTRAINT user_preferences_pkey PRIMARY KEY (username),
                        CONSTRAINT user_preferences_user_fkey FOREIGN KEY (username) 
                            REFERENCES {SNOWFLAKE_SCHEMA}.users(username) ON DELETE CASCADE
                    );
                """)                
                # --- END NEW DDLs ---
                
            print("Snowflake database, schema, and tables checked/initialized.")
    except Exception as e:
        print(f"FATAL: Could not initialize database: {e}")
        # Depending on severity, you might want to exit:
        # sys.exit(1)

# Initialize DB on startup
init_db_snowflake()

def get_db_conn_configured():
    """
    Creates and returns a Snowflake connection already configured
    with the app's DB and Schema.
    """
    try:
        if not all([SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT]):
            raise ValueError("Missing Snowflake environment variables (USER, PASSWORD, ACCOUNT) for app backend")
            
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        return conn
    except Exception as e:
        print(f"FATAL: Could not connect to Snowflake for app backend: {e}")
        return None

def get_or_create_session_id(user_id):
    with get_db_conn_configured() as conn: # Use new connection
        if not conn:
            raise Exception("Database connection is not available")
        
        with conn.cursor() as cur:
            # --- SQL INJECTION FIX: Use parameterized query ---
            cur.execute(f"SELECT id FROM {SNOWFLAKE_SCHEMA}.chat_sessions WHERE user_id = %s", (user_id,))
            row = cur.fetchone()
            if row:
                return row[0]
            
            # --- SQL INJECTION FIX: Use parameterized query ---
            cur.execute(f"INSERT INTO {SNOWFLAKE_SCHEMA}.chat_sessions (user_id) VALUES (%s)", (user_id,))
            conn.commit()
            
            # --- SQL INJECTION FIX: Use parameterized query ---
            cur.execute(f"SELECT id FROM {SNOWFLAKE_SCHEMA}.chat_sessions WHERE user_id = %s", (user_id,))
            session_id = cur.fetchone()[0]
            return session_id

def call_gemini_api(prompt_text, is_json_output=False):
    if not GEMINI_API_KEY:
        return "Error: GEMINI_API_KEY is not set on the server."

    headers = {'Content-Type': 'application/json'}
    payload = {
        "contents": [{
            "parts": [{"text": prompt_text}]
        }]
    }

    if is_json_output:
        payload["generationConfig"] = {
            "responseMimeType": "application/json",
            "responseSchema": {
                "type": "OBJECT",
                "properties": {
                    "tablename": {"type": "STRING"},
                    "columns": {
                        "type": "ARRAY",
                        "items": {
                            "type": "OBJECT",
                            "properties": {
                                "id": {"type": "NUMBER"},
                                "column": {"type": "STRING"},
                                "column_description": {"type": "STRING"}
                            },
                            "required": ["id", "column", "column_description"]
                        }
                    }
                },
                "required": ["tablename", "columns"]
            }
        }

    try:
        response = requests.post(GEMINI_API_URL, headers=headers, data=json.dumps(payload))
        response.raise_for_status()
        result = response.json()
        
        text_content = result.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")
        return text_content
    except requests.exceptions.RequestException as e:
        print(f"Error calling Gemini API: {e}")
        if e.response:
            print(f"Gemini API Response: {e.response.text}")
        return f"Error: Could not connect to Gemini API. {e}"
    except (KeyError, IndexError, json.JSONDecodeError) as e:
        print(f"Error parsing Gemini response: {e}")
        print(f"Raw Gemini Response: {result}")
        return "Error: Received an invalid response from the AI model."

def call_gemini_vision_api(image_bytes, mime_type, prompt_text):
    if not GEMINI_API_KEY:
        return "Error: GEMINI_API_KEY is not set on the server."
        
    headers = {'Content-Type': 'application/json'}
    encoded_image = base64.b64encode(image_bytes).decode('utf-8')
    
    payload = {
        "contents": [
            {
                "parts": [
                    {"text": prompt_text},
                    {
                        "inlineData": {
                            "mimeType": mime_type,
                            "data": encoded_image
                        }
                    }
                ]
            }
        ],
        "generationConfig": {
            "temperature": 0.2,
            "topK": 32,
            "topP": 1,
            "maxOutputTokens": 4096,
        }
    }

    try:
        response = requests.post(GEMINI_API_URL, headers=headers, data=json.dumps(payload))
        response.raise_for_status()
        result = response.json()
        
        text_content = result.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")
        return text_content
    except requests.exceptions.RequestException as e:
        print(f"Error calling Gemini Vision API: {e}")
        if e.response:
            print(f"Gemini API Response: {e.response.text}")
        return f"Error: Could not connect to Gemini API. {e}"
    except (KeyError, IndexError, json.JSONDecodeError) as e:
        print(f"Error parsing Gemini response: {e}")
        print(f"Raw Gemini Response: {result}")
        return "Error: Received an invalid response from the AI model."

@app.route('/connect_database', methods=['POST'])
def connect_database():
    data = request.json
    db_type = data.get('db_type')
    host = data.get('host')
    port = data.get('port')
    user = data.get('user')
    password = data.get('password')
    dbname = data.get('dbname')

    try:
        if db_type == "MySQL":
            conn_db = mysql.connector.connect(
                host=host, port=port, user=user, password=password, database=dbname
            )
            cursor = conn_db.cursor()
            cursor.execute("SELECT VERSION();")
        elif db_type == "PostgreSQL":
            conn_db = psycopg2.connect(
                host=host, port=port, user=user, password=password, dbname=dbname
            )
            cursor = conn_db.cursor()
            cursor.execute("SELECT version();")
        elif db_type == "Oracle":
            dsn = f"{host}:{port}/{dbname}" if port else f"{host}/{dbname}"
            conn_db = oracledb.connect(user=user, password=password, dsn=dsn)
            cursor = conn_db.cursor()
            cursor.execute("SELECT * FROM v$version WHERE ROWNUM = 1")
        elif db_type == "SQL Server":
            conn_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={host};DATABASE={dbname};UID={user};PWD={password}'
            if port:
                conn_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={host},{port};DATABASE={dbname};UID={user};PWD={password}'
            conn_db = pyodbc.connect(conn_string)
            cursor = conn_db.cursor()
            cursor.execute("SELECT @@VERSION;")
        elif db_type == "Snowflake":
            conn_db = snowflake.connector.connect(
                user=user, password=password, account=host, database=dbname
            )
            cursor = conn_db.cursor()
            cursor.execute("SELECT CURRENT_VERSION();")
        else:
            return jsonify({"success": False, "error": "Unsupported DB type"}), 400

        version = cursor.fetchone()[0]
        cursor.close()
        conn_db.close()

        connection_info = {
            "db_type": db_type,
            "host": host,
            "port": port,
            "user": user,
            "dbname": dbname,
            "version": version,
        }
        return jsonify({"success": True, "connection_info": connection_info})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/list_schemas', methods=['POST'])
def list_schemas():
    data = request.json
    db_type = data['db_type']
    host = data['host']
    port = data['port']
    user = data['user']
    password = data['password']
    dbname = data['dbname']
    
    try:
        conn_db = None
        if db_type == "MySQL":
            conn_db = mysql.connector.connect(host=host, port=port, user=user, password=password, database=dbname)
            df = pd.read_sql("SELECT schema_name FROM information_schema.schemata", conn_db)
            schemas = df["schema_name"].tolist()
        elif db_type == "PostgreSQL":
            db_connection_str = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}'
            engine = create_engine(db_connection_str)
            df = pd.read_sql(r"SELECT schema_name FROM information_schema.schemata", engine)
            schemas = df["schema_name"].tolist()
        elif db_type == "Oracle":
            dsn = f"{host}:{port}/{dbname}" if port else f"{host}/{dbname}"
            conn_db = oracledb.connect(user=user, password=password, dsn=dsn)
            df = pd.read_sql("SELECT username AS schema_name FROM all_users", conn_db)
            schemas = df["schema_name"].tolist()
        elif db_type == "SQL Server":
            conn_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={host};DATABASE={dbname};UID={user};PWD={password}'
            if port:
                conn_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={host},{port};DATABASE={dbname};UID={user};PWD={password}'
            conn_db = pyodbc.connect(conn_string)
            df = pd.read_sql("SELECT name AS schema_name FROM sys.schemas", conn_db)
            schemas = df["schema_name"].tolist()
        elif db_type == "Snowflake":
            conn_db = snowflake.connector.connect(user=user, password=password, account=host, database=dbname)
            df = pd.read_sql(f"SHOW SCHEMAS IN DATABASE {dbname}", conn_db)
            schemas = df["name"].tolist()
        else:
            schemas = ["default"]
            
        return jsonify({"success": True, "schemas": schemas})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if conn_db and db_type not in ["PostgreSQL"]:
            conn_db.close()


@app.route('/list_tables', methods=['POST'])
def list_tables():
    data = request.json
    db_type = data['connectionInfo']['db_type']
    host = data['connectionInfo']['host']
    port = data['connectionInfo']['port']
    user = data['connectionInfo']['user']
    password = data['connectionInfo']['password']
    dbname = data['connectionInfo']['dbname']
    schema = data['schema']
    
    try:
        conn_db = None
        # --- SQL INJECTION FIX: Validate schema/dbname ---
        # A simple regex to allow for quoted or unquoted identifiers
        # This is NOT perfect but blocks basic attacks.
        if not re.match(r'^[A-Za-z0-9_"$]+$', schema) or \
           not re.match(r'^[A-Za-z0-9_"$]+$', dbname):
           return jsonify({"error": "Invalid schema or database name"}), 400

        if db_type == "MySQL":
            conn_db = mysql.connector.connect(host=host, port=port, user=user, password=password, database=dbname)
            # --- SQL INJECTION FIX: Use parameterized query ---
            df = pd.read_sql("SELECT table_name FROM information_schema.tables WHERE table_schema = %s", conn_db, params=(schema,))
            tables = df.iloc[:, 0].tolist()
        elif db_type == "PostgreSQL":
            db_connection_str = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}'
            engine = create_engine(db_connection_str)
            # --- SQL INJECTION FIX: Use parameterized query ---
            query = "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = %s"
            df = pd.read_sql(query, engine, params=(schema,))
            tables = df["tablename"].tolist()
        elif db_type == "Oracle":
            dsn = f"{host}:{port}/{dbname}" if port else f"{host}/{dbname}"
            conn_db = oracledb.connect(user=user, password=password, dsn=dsn)
            # --- SQL INJECTION FIX: Use parameterized query (bind variable) ---
            query = "SELECT table_name FROM all_tables WHERE owner = :schema_name"
            df = pd.read_sql(query, conn_db, params={"schema_name": schema.upper()})
            tables = df["table_name"].tolist()
        elif db_type == "SQL Server":
            conn_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={host};DATABASE={dbname};UID={user};PWD={password}'
            if port:
                conn_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={host},{port};DATABASE={dbname};UID={user};PWD={password}'
            conn_db = pyodbc.connect(conn_string)
            # --- SQL INJECTION FIX: Use parameterized query ---
            query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'"
            df = pd.read_sql(query, conn_db, params=[schema])
            tables = df["TABLE_NAME"].tolist()
        elif db_type == "Snowflake":
            conn_db = snowflake.connector.connect(user=user, password=password, account=host, database=dbname)
            # Identifiers in Snowflake are tricky. We rely on the simple regex check above.
            query = f'SHOW TABLES IN SCHEMA "{dbname}"."{schema}"'
            df = pd.read_sql(query, conn_db)
            tables = df["name"].tolist()
        else:
            tables = []
        return jsonify({"success": True, "tables": tables}) 
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if conn_db and db_type not in ["PostgreSQL"]:
            conn_db.close()


@app.route('/preview_table', methods=['POST'])
def preview_table():
    data = request.json
    conn_info = data.get('connectionInfo', {})
    db_type = conn_info.get('db_type')
    host = conn_info.get('host')
    port = conn_info.get('port')
    user = conn_info.get('user')
    password = conn_info.get('password')
    dbname = conn_info.get('dbname')
    schema = data.get('schema')
    table = data.get('table')

    try:
        df = None
        conn_db = None
        
        # --- SQL INJECTION FIX: Validate identifiers ---
        if not re.match(r'^[A-Za-z0-9_"$]+$', schema) or \
           not re.match(r'^[A-Za-z0-9_"$]+$', table) or \
           not re.match(r'^[A-Za-z0-9_"$]+$', dbname):
           return jsonify({"error": "Invalid schema, table, or database name"}), 400

        if db_type == "MySQL":
            conn_db = mysql.connector.connect(
                host=host, port=int(port), user=user, password=password, database=dbname
            )
            query = f"SELECT * FROM `{schema}`.`{table}` LIMIT 10"
            df = pd.read_sql(query, conn_db)
        elif db_type == "PostgreSQL":
            db_connection_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
            engine = create_engine(db_connection_str)
            query = f'SELECT * FROM "{schema}"."{table}" LIMIT 10'
            df = pd.read_sql(query, engine)
        elif db_type == "Oracle":
            dsn = f"{host}:{port}/{dbname}" if port else f"{host}/{dbname}"
            conn_db = oracledb.connect(user=user, password=password, dsn=dsn)
            query = f'SELECT * FROM "{schema.upper()}"."{table.upper()}" WHERE ROWNUM <= 10'
            df = pd.read_sql(query, conn_db)
        elif db_type == "SQL Server":
            conn_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={host};DATABASE={dbname};UID={user};PWD={password}'
            if port:
                conn_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={host},{port};DATABASE={dbname};UID={user};PWD={password}'
            conn_db = pyodbc.connect(conn_string)
            query = f'SELECT TOP 10 * FROM "{schema}"."{table}"'
            df = pd.read_sql(query, conn_db)
        elif db_type == "Snowflake":
            conn_db = snowflake.connector.connect(
                user=user, password=password, account=host, database=dbname
            )
            query = f'SELECT * FROM "{dbname}"."{schema}"."{table}" LIMIT 10'
            df = pd.read_sql(query, conn_db)
        else:
            return jsonify({"success": False, "error": "Unsupported database type"}), 400

        df = df.astype(str)
        records = df.to_dict(orient="records")
        columns = [{"field": col, "headerName": col.capitalize(), "width": 150} for col in df.columns]

        return jsonify({"success": True, "rows": records, "columns": columns})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn_db and db_type not in ["PostgreSQL"]:
            conn_db.close()


@app.route('/tablecomments', methods=['POST'])
def tablecomments():
    data = request.json
    conn_info = data.get('connectionInfo', {})
    db_type = conn_info.get('db_type')
    host = conn_info.get('host')
    port = conn_info.get('port')
    user = conn_info.get('user')
    password = conn_info.get('password')
    dbname = conn_info.get('dbname')
    schema = data.get('schema')
    table = data.get('table')

    try:
        df = None
        conn_db = None
        
        # --- SQL INJECTION FIX: Validate identifiers ---
        if not re.match(r'^[A-Za-z0-9_"$]+$', schema) or \
           not re.match(r'^[A-Za-z0-9_"$]+$', table) or \
           not re.match(r'^[A-Za-z0-9_"$]+$', dbname):
           return jsonify({"error": "Invalid schema, table, or database name"}), 400

        if db_type == "MySQL":
            conn_db = mysql.connector.connect(
                host=host, port=int(port), user=user, password=password, database=dbname
            )
            # --- SQL INJECTION FIX: Use parameterized query ---
            query = """
                SELECT column_name, column_comment as comment
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
            """
            df = pd.read_sql(query, conn_db, params=(schema, table))
        elif db_type == "PostgreSQL":
            db_connection_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
            engine = create_engine(db_connection_str)
            # --- SQL INJECTION FIX: Use parameterized query ---
            query = """
                SELECT a.column_name as column_name, pgd.description as comment 
                FROM information_schema.columns a 
                LEFT JOIN pg_catalog.pg_statio_all_tables b ON a.table_schema=b.schemaname AND a.table_name=b.relname
                LEFT JOIN pg_catalog.pg_description pgd ON b.relid = pgd.objoid AND a.ordinal_position=pgd.objsubid
                WHERE a.table_schema= %s AND a.table_name= %s;
            """
            df = pd.read_sql(query, engine, params=(schema, table))
        elif db_type == "Oracle":
            dsn = f"{host}:{port}/{dbname}" if port else f"{host}/{dbname}"
            conn_db = oracledb.connect(user=user, password=password, dsn=dsn)
            # --- SQL INJECTION FIX: Use parameterized query (bind variables) ---
            query = """
                SELECT column_name, comments as comment
                FROM all_col_comments
                WHERE owner = :schema_name AND table_name = :table_name
            """
            df = pd.read_sql(query, conn_db, params={"schema_name": schema.upper(), "table_name": table.upper()})
        elif db_type == "SQL Server":
            conn_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={host};DATABASE={dbname};UID={user};PWD={password}'
            if port:
                conn_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={host},{port};DATABASE={dbname};UID={user};PWD={password}'
            conn_db = pyodbc.connect(conn_string)
            # --- SQL INJECTION FIX: Use parameterized query ---
            query = """
                SELECT c.name AS column_name, p.value AS comment
                FROM sys.tables t
                INNER JOIN sys.columns c ON t.object_id = c.object_id
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                LEFT JOIN sys.extended_properties p ON p.major_id = t.object_id AND p.minor_id = c.column_id AND p.name = 'MS_Description'
                WHERE s.name = ? AND t.name = ?
            """
            df = pd.read_sql(query, conn_db, params=[schema, table])
        elif db_type == "Snowflake":
            conn_db = snowflake.connector.connect(
                user=user, password=password, account=host, database=dbname
            )
            # --- SQL INJECTION FIX: Use parameterized query ---
            query = f"""
                SELECT column_name, comment
                FROM {dbname}.information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """
            df = pd.read_sql(query, conn_db, params=(schema.upper(), table.upper()))
        else:
            return jsonify({"success": False, "error": "Unsupported database type"}), 400

        df = df.astype(str).where(pd.notnull(df), None)
        records = df.to_dict(orient="records")
        columns = [{"field": col, "headerName": col.capitalize(), "width": 150} for col in df.columns]

        return jsonify({"success": True, "rows": records, "columns": columns})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn_db and db_type not in ["PostgreSQL"]:
            conn_db.close()


def extract_text_from_doc(uploaded_doc):
    filename = uploaded_doc.filename
    ext = filename.split(".")[-1].lower()
    doc_text = ""
    print(f"Processing file: {filename} (ext: {ext})")

    if ext == "txt":
        doc_text = uploaded_doc.read().decode("utf-8")
    elif ext == "docx":
        doc_text = docx2txt.process(uploaded_doc)
        print(doc_text)
    elif ext == "pdf":
        try:
            reader = PyPDF2.PdfReader(uploaded_doc)
            doc_text = "\n".join([p.extract_text() or "" for p in reader.pages])
        except Exception as e:
            print(f"Error reading PDF, falling back to image analysis: {e}")
            uploaded_doc.seek(0)
            image_bytes = uploaded_doc.read()
            mime_type = "application/pdf"
            analysis_prompt = "Analyze this PDF, which may be a scanned image of an ERD. Extract all tables, columns, and relationships."
            doc_text = call_gemini_vision_api(image_bytes, mime_type, analysis_prompt)
            
    elif ext == "html":
        soup = BeautifulSoup(uploaded_doc, features="html.parser")
        for script in soup(["script", "style"]):
            script.extract()
        doc_text = soup.get_text()
    
    elif ext in ["png", "jpg", "jpeg"]:
        print(f"Image file detected. Analyzing with Gemini Vision...")
        try:
            image_bytes = uploaded_doc.read()
            mime_type, _ = mimetypes.guess_type(filename)
            if not mime_type:
                mime_type = f'image/{ext}'
            
            analysis_prompt = """
            You are an expert database administrator and data analyst.
            Your task is to analyze the attached image, which is an Entity-Relationship Diagram (ERD).
            
            Please provide a detailed, structured report in Markdown format that includes:
            1.  A brief summary of the database schema.
            2.  A list of all tables (entities) you can identify.
            3.  For each table, list its columns (attributes) and any data types or keys (PK, FK) shown.
            4.  Describe any relationships (cardinality: one-to-one, one-to-many, many-to-many) you can see between the tables.
            
            If the image is not an ERD, please describe what you see instead.
            """
            
            doc_text = call_gemini_vision_api(image_bytes, mime_type, analysis_prompt)
            
            if doc_text.startswith("Error:"):
                print(f"Gemini Vision API failed: {doc_text}")
            else:
                print("Gemini Vision analysis successful.")

        except Exception as e:
            print(f"Error processing image {filename}: {e}")
            doc_text = f"Error: Could not process image file {filename}."
    
    else:
        print(f"Unsupported file type for analysis: {ext}")
        doc_text = f"(Unsupported file type: {ext}. No text extracted.)"

    return doc_text

@app.route('/upload_doc', methods=['POST'])
def upload_doc():
    if 'uploaded_doc' not in request.files:
        return jsonify({"success": False, "error": "No file part"}), 400

    uploaded_file = request.files['uploaded_doc']
    print(uploaded_file)
    if uploaded_file.filename == '':
        return jsonify({"success": False, "error": "No selected file"}), 400

    try:
        text = extract_text_from_doc(uploaded_file)
        print(text)
        return jsonify({"success": True, "text": text})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/generate_description", methods=["POST"])
def generate_description():
    log_id = session.get('activity_log_id')
    
    data = request.get_json()
    table_name = data.get("tablename")
    col_defs_with_type = data.get("columns") # Expecting [{"name": "COL1", "type": "VARCHAR"}, ...]
    document_context = data.get("document_context")
    extra_prompt = data.get("user_prompt")
    
    tables_worked_on = []
    if data.get("mode") == "Bulk Tables":
        tables_worked_on = data.get("selectedTables", [])
    elif table_name:
        tables_worked_on = [table_name]
        
    if log_id and tables_worked_on:
        try:
            with get_db_conn_configured() as conn: # Use new connection
                tables_str = ", ".join(tables_worked_on)
                with conn.cursor() as cur:
                    # --- SQL INJECTION FIX: Use parameterized query ---
                    cur.execute(
                        f"""
                        UPDATE {SNOWFLAKE_SCHEMA}.user_activity_log
                        SET tables_generated_for = %s
                        WHERE log_id = %s
                        """,
                        (tables_str, log_id)
                    )
                    conn.commit()
        except Exception as e:
            print(f"Error logging /generate_description: {e}")

    # Prepare columns string from the new format
    columns_str = "\n".join([f"- {col['name']} ({col['type']})" for col in col_defs_with_type])
    
    prompt = f"""
        You are a database documentation generator. Your task is to write a detailed, single-sentence description for each column in the given table.
        Base your descriptions on the column name, data type, and the provided business context.
        Business Context:
        {document_context}
        Table Name: {table_name}
        Columns:
        {columns_str}
        User Instructions:
        {extra_prompt}
        Return your response *only* in the following strict JSON format. Do not include any other text, markdown, or explanations.
        The 'id' should be a simple incrementing number starting from 1.
        The 'column_description' must be a single, concise sentence and must not contain double quotes.
        JSON Format Example:
        {{
          "tablename": "SAMPLE_TABLE",
          "columns": [
            {{
              "id": 1,
              "column": "USER_ID",
              "column_description": "A unique identifier for the user."
            }},
            {{
              "id": 2,
              "column": "USERNAME",
              "column_description": "The user's chosen login name."
            }}
          ]
        }}
        Now, generate the JSON for the table '{table_name}'.
    """
    
    start_time = time.time()
    generated_json_str = call_gemini_api(prompt, is_json_output=True)
    end_time = time.time()
    llm_response_time_ms = (end_time - start_time) * 1000

    if log_id:
         try:
            with get_db_conn_configured() as conn: # Use new connection
                with conn.cursor() as cur:
                    # --- SQL INJECTION FIX: Use parameterized query ---
                    cur.execute(
                        f"""
                        UPDATE {SNOWFLAKE_SCHEMA}.user_activity_log
                        SET avg_llm_response_time_ms = %s
                        WHERE log_id = %s
                        """,
                        (llm_response_time_ms, log_id)
                    )
                    conn.commit()
         except Exception as e:
            print(f"Error logging LLM time: {e}")
    
    if generated_json_str.startswith("Error:"):
        return jsonify({"success": False, "error": generated_json_str}), 500

    print(f"Gemini API Raw JSON Response: {generated_json_str}")
    
    try:
        description_data = json.loads(generated_json_str)
        
        # --- NEW: Add 'type' back into the response ---
        # Create a lookup map for column types
        type_map = {col['name']: col['type'] for col in col_defs_with_type}
        
        # Add the type to each column in the response
        if 'columns' in description_data:
            for col in description_data['columns']:
                col_name = col.get('column')
                if col_name in type_map:
                    col['type'] = type_map[col_name]
                else:
                    col['type'] = 'unknown' # Fallback
        
        return jsonify({"success": True, "description": description_data})
    except json.JSONDecodeError as e:
        print(f"Failed to decode JSON from Gemini: {e}")
        print(f"Received string: {generated_json_str}")
        return jsonify({"success": False, "error": "AI model returned invalid JSON.", "raw_response": generated_json_str}), 500


def build_pdf_header(canvas, doc):
    canvas.saveState()
    page_num = canvas.getPageNumber()
    if page_num > 1:
        canvas.setFont('Helvetica', 9)
        canvas.drawRightString(letter[0] - inch, letter[1] - 0.75 * inch, f"Page {page_num - 1}")
    canvas.restoreState()

def generate_pdf_report(desc_results):
    buffer = BytesIO()
    doc = BaseDocTemplate(buffer, pagesize=landscape(letter))
    
    frame = Frame(doc.leftMargin, doc.bottomMargin, doc.width, doc.height, id='normal')
    template = PageTemplate(id='main_template', frames=frame, onPage=build_pdf_header)
    doc.addPageTemplates([template])

    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name='TitlePage', 
                             fontSize=24, 
                             alignment=TA_CENTER, 
                             spaceAfter=0.5*inch, 
                             fontName="Helvetica-Bold"))
    styles.add(ParagraphStyle(name='TableHeader', 
                             fontSize=16, 
                             alignment=TA_LEFT, 
                             spaceAfter=14, 
                             fontName="Helvetica-Bold",
                             textColor=colors.HexColor("#003554")))
    styles.add(ParagraphStyle(name='ColHeader',
                             fontSize=10,
                             fontName="Helvetica-Bold",
                             alignment=TA_LEFT,
                             textColor=colors.white))
    styles.add(ParagraphStyle(name='ColBody',
                             fontSize=9,
                             fontName="Helvetica",
                             alignment=TA_LEFT))
    
    elements = []

    elements.append(Spacer(1, 3*inch))
    elements.append(Paragraph("Metadata Documentation Report", styles['TitlePage']))
    elements.append(Paragraph(f"Generated on: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}", styles['Normal']))
    elements.append(PageBreak())

    for table_name, table_data_df in desc_results.items():
        elements.append(Paragraph(f"Table: {table_name}", styles['TableHeader']))
        
        col_header_style = styles['ColHeader']
        body_style = styles['ColBody']

        # Use the DataFrame passed in
        df = table_data_df
        
        # Ensure 'Type' column exists, if not, add it as empty
        if 'Type' not in df.columns:
            df['Type'] = ''
            
        # Reorder for PDF
        df_pdf = df[['Column', 'Type', 'Description']]

        data = [
            [Paragraph(c, col_header_style) for c in df_pdf.columns]
        ]
        
        for _, row in df_pdf.iterrows():
            data.append([
                Paragraph(str(row['Column']), body_style),
                Paragraph(str(row['Type']), body_style),
                Paragraph(str(row['Description']), body_style)
            ])
        
        col_widths = [1.5*inch, 1*inch, 6.5*inch]
        tbl = Table(data, colWidths=col_widths, repeatRows=1)
        
        tbl.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#009CDE")),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('BOX', (0, 0), (-1, -1), 1, colors.black),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('PADDING', (0, 0), (-1, -1), 6),
        ]))
        
        elements.append(tbl)
        elements.append(PageBreak())

    doc.build(elements)
    pdf = buffer.getvalue()
    buffer.close()
    return pdf

def get_activity_log_id():
    """Get or create activity log ID for the current session"""
    print(f"🔍 [DEBUG] get_activity_log_id() called")
    print(f"🔍 [DEBUG] Session ID: {session.get('_id')}")
    print(f"🔍 [DEBUG] All session data: {dict(session)}")
    
    log_id = session.get('activity_log_id')
    username = session.get('user')
    
    print(f"🔍 [DEBUG] Current log_id from session: {log_id}")
    print(f"🔍 [DEBUG] Current username from session: {username}")
    
    if not log_id and username:
        print(f"🔍 [DEBUG] Creating new activity log entry for user: {username}")
        try:
            with get_db_conn_configured() as conn:
                with conn.cursor() as cur:
                    # Create a new activity log entry
                    cur.execute(
                        """
                        INSERT INTO LANDING.ANALYTICSBOT.user_activity_log (username, login_time)
                        VALUES (%s, %s)
                        """,
                        (username, datetime.now())
                    )
                    conn.commit()
                    print(f"✅ [DEBUG] Created new activity log entry")
                    
                    # Get the new log_id
                    cur.execute(
                        """
                        SELECT MAX(log_id) FROM LANDING.ANALYTICSBOT.user_activity_log 
                        WHERE username = %s
                        """,
                        (username,)
                    )
                    log_id_result = cur.fetchone()
                    log_id = log_id_result[0] if log_id_result else None
                    print(f"🔍 [DEBUG] Retrieved new log_id: {log_id}")
                    
                    if log_id:
                        session['activity_log_id'] = log_id
                        session.modified = True  # Important: mark session as modified
                        print(f"✅ [DEBUG] Set session activity_log_id: {log_id}")
                    else:
                        print(f"❌ [DEBUG] Failed to retrieve log_id after insert")
        
        except Exception as e:
            print(f"❌ [DEBUG] Error creating activity log: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    print(f"🔍 [DEBUG] Returning log_id: {log_id}")
    return log_id

@app.route('/generate_pdf', methods=['POST', 'OPTIONS']) 
def generate_pdf():
    if request.method == 'OPTIONS':
        return '', 200
        
    print(f"🔍 [DEBUG] PDF generation request received")
    
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'No data provided'}), 400
            
        print(f"🔍 [DEBUG] Request data keys: {list(data.keys())}")
        
        # Get username from request body
        username = data.get('username')
        print(f"🔍 [DEBUG] Username from request: {username}")
        
        # Remove username from data before processing tables
        data_to_process = {k: v for k, v in data.items() if k != 'username'}
        
        print(f"🔍 [DEBUG] Tables to process: {list(data_to_process.keys())}")
        
        # Log to database if username is provided
        if username:
            try:
                with get_db_conn_configured() as conn:
                    with conn.cursor() as cur:
                        # Get the latest log_id for this user
                        cur.execute(
                            """
                            SELECT MAX(log_id) 
                            FROM LANDING.ANALYTICSBOT.user_activity_log 
                            WHERE username = %s
                            """,
                            (username,)
                        )
                        result = cur.fetchone()
                        log_id = result[0] if result else None
                        
                        if log_id:
                            # Update the existing record - ONLY set downloaded_pdf
                            cur.execute(
                                """
                                UPDATE LANDING.ANALYTICSBOT.user_activity_log 
                                SET downloaded_pdf = TRUE
                                WHERE log_id = %s
                                """,
                                (log_id,)  # Only one parameter now
                            )
                            conn.commit()
                            print(f"✅ [DEBUG] PDF download logged for user: {username}, log_id: {log_id}")
                        else:
                            # If no existing log_id found, create a new entry with only necessary fields
                            cur.execute(
                                """
                                INSERT INTO LANDING.ANALYTICSBOT.user_activity_log 
                                (username, login_time, downloaded_pdf)
                                VALUES (%s, %s, %s)
                                """,
                                (username, datetime.now(), True)  # Only 3 parameters for 3 placeholders
                            )
                            conn.commit()
                            print(f"✅ [DEBUG] Created new activity log and logged PDF download for user: {username}")
                            
            except Exception as e:
                print(f"❌ [DEBUG] Error logging PDF download: {e}")
                import traceback
                traceback.print_exc()
                # Continue with PDF generation even if logging fails
        else:
            print(f"⚠️ [DEBUG] No username provided, skipping database logging")
        
        # Process the PDF data
        desc_results = {}
        for table_name, table_data in data_to_process.items():
            if table_data:  # Only process if there's data
                desc_results[table_name] = pd.DataFrame(table_data)
        
        if not desc_results:
            return jsonify({'error': 'No valid table data found'}), 400
        
        print(f"🔍 [DEBUG] Generating PDF for {len(desc_results)} tables")
        pdf = generate_pdf_report(desc_results)
        print("✅ PDF report generated successfully")
        
        return send_file(
            BytesIO(pdf), 
            mimetype='application/pdf', 
            as_attachment=True, 
            download_name="metadata_documentation.pdf"
        )
        
    except Exception as e:
        print(f"❌ Error generating PDF: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route("/api/chat/save", methods=["POST"])
def save_chat():
    data = request.get_json()
    user_id = data.get("userId")
    messages = data.get("messages")
    qa_history = data.get("qaHistory")

    if not user_id:
        return jsonify({"error": "Missing userId"}), 400
    
    messages = messages or []
    qa_history = qa_history or []
        
    try:
        with get_db_conn_configured() as conn: # Use new connection
            session_id = get_or_create_session_id(user_id) # This function now handles its own connection

            with conn.cursor() as cur:
                # --- SQL INJECTION FIX: Use parameterized query ---
                # --- LOGIC CHANGED from main1.py: main.py deletes, main1.py appends. Sticking with main.py's logic.
                cur.execute(f"DELETE FROM {SNOWFLAKE_SCHEMA}.messages WHERE session_id = %s", (session_id,))
                cur.execute(f"DELETE FROM {SNOWFLAKE_SCHEMA}.qa_pairs WHERE session_id = %s", (session_id,))

                for msg in messages:
                    # Filter out interactive messages
                    if msg.get("type") in ["welcome_actions", "suggestion_actions"]:
                        continue
                    
                    cur.execute(
                        f'INSERT INTO {SNOWFLAKE_SCHEMA}.messages (session_id, sender, text) VALUES (%s, %s, %s)',
                        (session_id, msg.get("sender"), msg.get("text"))
                    )

                for qa in qa_history:
                    cur.execute(
                        f"INSERT INTO {SNOWFLAKE_SCHEMA}.qa_pairs (session_id, question, answer, is_helpful) VALUES (%s, %s, %s, %s)",
                        (session_id, qa.get("question"), qa.get("answer"), qa.get("is_helpful"))
                    )
                conn.commit()
        
        return jsonify({"status": "success"})
    except Exception as e:
        print("Error saving chat:", e)
        return jsonify({"error": "Database error"}), 500


@app.route("/api/chat/history", methods=["GET"])
def get_chat_history():
    user_id = request.args.get("userId")
    if not user_id:
        return jsonify({"error": "Missing userId parameter"}), 400
        
    try:
        with get_db_conn_configured() as conn: # Use new connection
            with conn.cursor(snowflake.connector.DictCursor) as cur:
                # --- SQL INJECTION FIX: Use parameterized query ---
                cur.execute(f"SELECT id FROM {SNOWFLAKE_SCHEMA}.chat_sessions WHERE user_id = %s", (user_id,))
                row = cur.fetchone()
                if not row:
                    return jsonify({"messages": [], "qaHistory": []})

                session_id = row["ID"]

                # --- SQL INJECTION FIX: Use parameterized query ---
                # --- LOGIC CHANGED from main1.py: main.py gets all, main1.py limits to 10. Sticking with main.py's logic.
                cur.execute(
                    f'SELECT sender, text FROM {SNOWFLAKE_SCHEMA}.messages WHERE session_id = %s ORDER BY created_at ASC',
                    (session_id,),
                )
                messages = [{"sender": r["SENDER"], "text": r["TEXT"]} for r in cur.fetchall()]
                
                # --- SQL INJECTION FIX: Use parameterized query ---
                cur.execute(
                    f"SELECT question, answer, is_helpful FROM {SNOWFLAKE_SCHEMA}.qa_pairs WHERE session_id = %s ORDER BY created_at ASC",
                    (session_id,),
                )
                qa_history = [{"question": r["QUESTION"], "answer": r["ANSWER"], "is_helpful": r["IS_HELPFUL"]} for r in cur.fetchall()]
        
        if not messages and not qa_history:
             return jsonify({"messages": [], "qaHistory": []})

        return jsonify({"messages": messages, "qaHistory": qa_history})
    except Exception as e:
        print("Error fetching chat history:", e)
        return jsonify({"error": "Database error"}), 500


@app.route('/login', methods=['POST'])
def login():
    data = request.json
    username = data['username']
    password = data['password']
    
    try:
        log_id = None
        with get_db_conn_configured() as conn:
            with conn.cursor() as cur:
                cur.execute(f'SELECT password_hash, role FROM {SNOWFLAKE_SCHEMA}.users WHERE username = %s', (username,))
                result = cur.fetchone()
                
                if result and bcrypt.checkpw(password.encode(), result[0].encode()):
                    user_role = result[1]

                    try:
                        # For Snowflake, we need to get the ID differently
                        cur.execute(
                            f"""
                            INSERT INTO {SNOWFLAKE_SCHEMA}.user_activity_log (username, login_time)
                            VALUES (%s, %s)
                            """,
                            (username, datetime.now())
                        )
                        conn.commit()
                        
                        # SNOWFLAKE FIX: Get the last inserted ID using a query
                        cur.execute(
                            f"""
                            SELECT MAX(log_id) FROM {SNOWFLAKE_SCHEMA}.user_activity_log 
                            WHERE username = %s
                            """,
                            (username,)
                        )
                        log_id_result = cur.fetchone()
                        log_id = log_id_result[0] if log_id_result else None
                        
                        print(f"DEBUG: Captured log_id: {log_id} for user: {username}")
                             
                    except Exception as log_e:
                        print(f"Error creating activity log entry: {log_e}")
                        conn.rollback()
                    
                    session['user'] = username
                    session['role'] = user_role
                    if log_id:
                        session['activity_log_id'] = log_id
                    
                    return jsonify({'success': True,'username': username, 'role': user_role}), 200
                else:
                    return jsonify({'success': False, 'error': 'Invalid credentials'}), 401
    except Exception as e:
        print(f"Login error: {e}")
        return jsonify({'success': False, 'error': 'Database error during login'}), 500



@app.route('/logout', methods=['POST'])
def logout():
    log_id = session.get('activity_log_id')
    username = session.get('user')
    print(f"🔍 DEBUG LOGOUT: Starting logout process")
    print(f"🔍 DEBUG LOGOUT: log_id from session: {log_id}")
    print(f"🔍 DEBUG LOGOUT: username from session: {username}")
    
    if log_id and username:
        try:
            with get_db_conn_configured() as conn:
                with conn.cursor() as cur:
                    # First, get the login time for this session
                    print(f"🔍 DEBUG LOGOUT: Querying login_time for log_id: {log_id}")
                    cur.execute(
                        f"SELECT login_time FROM {SNOWFLAKE_SCHEMA}.user_activity_log WHERE log_id = %s AND username = %s", 
                        (log_id, username)
                    )
                    login_time_result = cur.fetchone()
                    print(f"🔍 DEBUG LOGOUT: login_time_result: {login_time_result}")
                    
                    duration = None
                    if login_time_result and login_time_result[0]:
                        login_time = login_time_result[0]
                        logout_time = datetime.now()
                        
                        print(f"🔍 DEBUG LOGOUT: login_time: {login_time}")
                        print(f"🔍 DEBUG LOGOUT: logout_time: {logout_time}")
                        
                        # Calculate duration in minutes
                        duration = (logout_time - login_time).total_seconds() / 60
                        duration = round(duration, 2)
                        print(f"🔍 DEBUG LOGOUT: calculated duration: {duration} minutes")
                    
                        # Update the logout time and duration
                        print(f"🔍 DEBUG LOGOUT: Executing UPDATE query...")
                        cur.execute(
                            f"""
                            UPDATE {SNOWFLAKE_SCHEMA}.user_activity_log
                            SET logout_time = %s, duration_minutes = %s
                            WHERE log_id = %s AND username = %s
                            """,
                            (logout_time, duration, log_id, username)
                        )
                        rows_updated = cur.rowcount
                        conn.commit()
                        print(f"🔍 DEBUG LOGOUT: UPDATE completed. Rows affected: {rows_updated}")
                        
                        # Verify the update worked
                        cur.execute(
                            f"SELECT logout_time, duration_minutes FROM {SNOWFLAKE_SCHEMA}.user_activity_log WHERE log_id = %s",
                            (log_id,)
                        )
                        verify_result = cur.fetchone()
                        print(f"🔍 DEBUG LOGOUT: Verification query result: {verify_result}")
                        
                    else:
                        print(f"❌ DEBUG LOGOUT: Could not find login time for log_id: {log_id}")
                    
        except Exception as e:
            print(f"❌ DEBUG LOGOUT: Error updating logout time: {e}")
            import traceback
            traceback.print_exc()
    else:
        print(f"❌ DEBUG LOGOUT: Missing log_id or username. log_id: {log_id}, username: {username}")
            
    session.clear()
    print(f"🔍 DEBUG LOGOUT: Session cleared, returning response")
    return jsonify({"success": True, "message": "Logged out"}), 200


@app.route('/api/user/dashboard_kpis', methods=['GET'])
def get_dashboard_kpis():
    username = session.get('user')
    print(f"DEBUG: Getting KPIs for user: {username}")
    
    if not username:
        return jsonify({"error": "Not authenticated"}), 401
        
    try:
        with get_db_conn_configured() as conn:
            with conn.cursor(snowflake.connector.DictCursor) as cur:
                # Enhanced KPI query with COALESCE to handle NULLs
                cur.execute(
                    f"""
                    SELECT
                        COUNT(*) AS total_logins,
                        COALESCE(SUM(duration_minutes), 0) AS total_duration_minutes,
                        COALESCE(SUM(CASE WHEN updated_comments = TRUE THEN 1 ELSE 0 END), 0) AS sessions_with_updates,
                        COALESCE(SUM(columns_updated_count), 0) AS total_columns_updated,
                        COALESCE(SUM(CASE WHEN downloaded_pdf = TRUE THEN 1 ELSE 0 END), 0) AS total_pdf_downloads,
                        COALESCE(SUM(CASE WHEN downloaded_excel = TRUE THEN 1 ELSE 0 END), 0) AS total_excel_downloads,
                        MAX(TO_VARCHAR(login_time, 'YYYY-MM-DD')) AS last_login_time
                    FROM {SNOWFLAKE_SCHEMA}.user_activity_log
                    WHERE username = %s;
                    """,
                    (username,)
                )
                kpis = cur.fetchone()
                
                print(f"DEBUG: Raw KPI data: {kpis}")
                
                if kpis:
                    # Convert to integers (they should already be numbers due to COALESCE)
                    kpis['TOTAL_LOGINS'] = int(kpis['TOTAL_LOGINS'])
                    kpis['TOTAL_DURATION_MINUTES'] = int(kpis['TOTAL_DURATION_MINUTES'])
                    kpis['SESSIONS_WITH_UPDATES'] = int(kpis['SESSIONS_WITH_UPDATES'])
                    kpis['TOTAL_COLUMNS_UPDATED'] = int(kpis['TOTAL_COLUMNS_UPDATED'])
                    kpis['TOTAL_PDF_DOWNLOADS'] = int(kpis['TOTAL_PDF_DOWNLOADS'])
                    kpis['TOTAL_EXCEL_DOWNLOADS'] = int(kpis['TOTAL_EXCEL_DOWNLOADS'])
                    
                    # Format last login time
                    if kpis['LAST_LOGIN_TIME']:
                        kpis['LAST_LOGIN_TIME'] = kpis['LAST_LOGIN_TIME']
                    else:
                        kpis['LAST_LOGIN_TIME'] = None
                
                print(f"DEBUG: Processed KPI data: {kpis}")
                return jsonify({"success": True, "kpis": kpis})
    except Exception as e:
        print(f"Error fetching KPIs: {e}")
        return jsonify({"error": "Database error fetching KPIs"}), 500


@app.route('/api/users/list', methods=['GET'])
def get_users():
    try:
        with get_db_conn_configured() as conn: # Use new connection
            with conn.cursor(snowflake.connector.DictCursor) as cur:
                cur.execute(f'SELECT id, username, email, role FROM {SNOWFLAKE_SCHEMA}.users ORDER BY id ASC')
                # Convert to dict to allow modification
                users = [dict(r) for r in cur.fetchall()]
                
                # --- NEW: Fetch assigned catalogs for each user ---
                for user in users:
                    cur.execute(f"SELECT catalog_id FROM {SNOWFLAKE_SCHEMA}.user_catalogs WHERE user_id = %s", (user['ID'],))
                    user['assigned_catalogs'] = [row['CATALOG_ID'] for row in cur.fetchall()]
                
        return jsonify({"success": True, "users": users})
    except Exception as e:
        print(f"Error fetching users: {e}")
        return jsonify({"success": False, "error": "Database error"}), 500

@app.route('/api/users/create', methods=['POST'])
def create_user():
    data = request.json
    username = data.get('username')
    password = data.get('password')
    email = data.get('email')
    role = data.get('role', 'user')
    # --- NEW: Capture assigned catalogs ---
    assigned_catalogs = data.get('assigned_catalogs', [])

    if not all([username, password, email]):
        return jsonify({"success": False, "error": "Missing required fields"}), 400
        
    try:
        hashed_pw = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())
        
        with get_db_conn_configured() as conn: # Use new connection
            with conn.cursor() as cur:
                # Insert User
                cur.execute(
                    f'INSERT INTO {SNOWFLAKE_SCHEMA}.users (username, password_hash, email, role) VALUES (%s, %s, %s, %s)',
                    (username, hashed_pw.decode('utf-8'), email, role)
                )
                
                # --- NEW: Handle Catalog Assignments ---
                if assigned_catalogs:
                    # Fetch the ID of the newly created user
                    cur.execute(f"SELECT id FROM {SNOWFLAKE_SCHEMA}.users WHERE username = %s", (username,))
                    user_id = cur.fetchone()[0]
                    
                    for cat_id in assigned_catalogs:
                        cur.execute(f"INSERT INTO {SNOWFLAKE_SCHEMA}.user_catalogs (user_id, catalog_id) VALUES (%s, %s)", (user_id, cat_id))
                # ---------------------------------------
                conn.commit()
        
        return jsonify({"success": True, "message": "User created successfully"}), 201
    except snowflake.connector.errors.IntegrityError as e:
        return jsonify({"success": False, "error": "Username or email already exists"}), 409
    except Exception as e:
        print(f"Error creating user: {e}")
        return jsonify({"success": False, "error": "Database error"}), 500


@app.route('/api/users/update_password', methods=['POST'])
def update_password():
    data = request.json
    username = data.get('username')
    new_password = data.get('newPassword')

    if not all([username, new_password]):
        return jsonify({"success": False, "error": "Missing username or new password"}), 400
        
    try:
        hashed_pw = bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt())
        
        with get_db_conn_configured() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f'UPDATE {SNOWFLAKE_SCHEMA}.users SET password_hash = %s, updated_at = CURRENT_TIMESTAMP WHERE username = %s',
                    (hashed_pw.decode('utf-8'), username)
                )
                conn.commit()
        
        return jsonify({"success": True, "message": "Password updated successfully"}), 200
    except Exception as e:
        print(f"Error updating password: {e}")
        return jsonify({"success": False, "error": "Database error"}), 500

@app.route('/get_columns', methods=['POST'])
def get_columns():
    data = request.json
    db_type = data['db_type']
    host = data['host']
    port = data['port']
    user = data['user']
    password = data['password']
    dbname = data['dbname']
    schema = data['schema']
    table = data['table']
    
    try:
        conn_db = None
        
        # --- SQL INJECTION FIX: Validate identifiers ---
        if not re.match(r'^[A-Za-z0-9_"$]+$', schema) or \
           not re.match(r'^[A-Za-z0-9_"$]+$', table) or \
           not re.match(r'^[A-Za-z0-9_"$]+$', dbname):
           return jsonify({"error": "Invalid schema, table, or database name"}), 400

        if db_type == "MySQL":
            conn_db = mysql.connector.connect(host=host, port=port, user=user, password=password, database=dbname)
            query = f"SHOW COLUMNS FROM `{schema}`.`{table}`" # SHOW COLUMNS is harder to parameterize
            df = pd.read_sql(query, conn_db)
            columns = [{"name": r["Field"], "type": r["Type"]} for _, r in df.iterrows()]
        elif db_type == "PostgreSQL":
            db_connection_str = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}'
            engine = create_engine(db_connection_str)
            # --- SQL INJECTION FIX: Use parameterized query ---
            query = """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = %s AND table_schema = %s
                ORDER BY ordinal_position
            """
            df = pd.read_sql(query, engine, params=(table, schema))
            columns = [{"name": r["column_name"], "type": r["data_type"]} for _, r in df.iterrows()]
        elif db_type == "Oracle":
            dsn = f"{host}:{port}/{dbname}" if port else f"{host}/{dbname}"
            conn_db = oracledb.connect(user=user, password=password, dsn=dsn)
            # --- SQL INJECTION FIX: Use parameterized query ---
            query = """
                SELECT column_name, data_type
                FROM all_tab_columns
                WHERE table_name = :table_name AND owner = :schema_name
                ORDER BY column_id
            """
            df = pd.read_sql(query, conn_db, params={"table_name": table.upper(), "schema_name": schema.upper()})
            columns = [{"name": r["column_name"], "type": r["data_type"]} for _, r in df.iterrows()]
        elif db_type == "SQL Server":
            conn_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={host};DATABASE={dbname};UID={user};PWD={password}'
            if port:
                conn_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={host},{port};DATABASE={dbname};UID={user};PWD={password}'
            conn_db = pyodbc.connect(conn_string)
            # --- SQL INJECTION FIX: Use parameterized query ---
            query = """
                SELECT column_name, data_type
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE table_name = ? AND table_schema = ?
                ORDER BY ordinal_position
            """
            df = pd.read_sql(query, conn_db, params=[table, schema])
            columns = [{"name": r["column_name"], "type": r["data_type"]} for _, r in df.iterrows()]
        elif db_type == "Snowflake":
            conn_db = snowflake.connector.connect(
                user=user, password=password, account=host, database=dbname
            )
            # --- SQL INJECTION FIX: Use parameterized query ---
            query = f"""
                SELECT column_name, data_type
                FROM {dbname}.information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """
            df = pd.read_sql(query, conn_db, params=(schema.upper(), table.upper()))
            columns = [{"name": r["COLUMN_NAME"], "type": r["DATA_TYPE"]} for _, r in df.iterrows()]
        else:
            columns = []
        return jsonify(columns)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if conn_db and db_type not in ["PostgreSQL"]:
            conn_db.close()


# --- NEW: Ported from main1.py and adapted for Snowflake ---
def get_schema_for_catalog(catalogid):
    """
    Fetches the schema (tables and columns) from the app's *own* semantic
    catalog tables to be used as context for the AI.
    """
    try:
        with get_db_conn_configured() as conn: # Use new connection
            with conn.cursor(snowflake.connector.DictCursor) as cur:
                # --- SQL INJECTION FIX: Use parameterized query ---
                cur.execute(
                    f"""
                    SELECT schema_name, table_name, column_name, data_type, description 
                    FROM {SNOWFLAKE_SCHEMA}.semantic_catalogs_detail
                    WHERE catalog_id = %s
                    ORDER BY schema_name, table_name, column_name
                    """,
                    (catalogid,)
                )
                rows = cur.fetchall()
        
        if not rows:
            return "Error: Semantic catalog not found or is empty."
            
        # Format the schema for the AI prompt
        schema_str = "DATABASE SCHEMA (from Semantic Catalog):\n"
        current_table = ""
        for row in rows:
            table_key = f"{row['SCHEMA_NAME']}.{row['TABLE_NAME']}"
            if table_key != current_table:
                schema_str += f"\nTable: {table_key}\n"
                current_table = table_key
            schema_str += f"  - {row['COLUMN_NAME']} ({row['DATA_TYPE']}) - {row['DESCRIPTION']}\n"
        
        return schema_str

    except Exception as e:
        print(f"Error getting schema for AI from catalog: {e}")
        return f"Error getting schema from catalog: {str(e)}"

# --- UPDATED: get_schema_for_ai (to filter system schemas) ---
def get_schema_for_ai(connection_info):
    try:
        db_type = connection_info.get('db_type')
        host = connection_info.get('host')
        port = connection_info.get('port')
        user = connection_info.get('user')
        password = connection_info.get('password')
        dbname = connection_info.get('dbname')
        
        conn_db = None
        query = ""
        params = ()

        # --- SQL INJECTION FIX: Validate identifiers ---
        if not re.match(r'^[A-Za-z0-9_"$]+$', dbname):
           return "Error: Invalid database name"

        if db_type == "MySQL":
            conn_db = mysql.connector.connect(host=host, port=int(port), user=user, password=password, database=dbname)
            query = "SELECT table_schema, table_name, column_name, data_type FROM information_schema.columns WHERE table_schema = %s"
            params = (dbname,)
        elif db_type == "PostgreSQL":
            db_connection_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
            engine = create_engine(db_connection_str)
            conn_db = engine.connect()
            query = "SELECT table_schema, table_name, column_name, data_type FROM information_schema.columns WHERE table_schema NOT IN ('pg_catalog', 'information_schema')"
            # No params needed, already filtered
        elif db_type == "SQL Server":
            conn_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={host};DATABASE={dbname};UID={user};PWD={password}'
            if port:
                conn_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={host},{port};DATABASE={dbname};UID={user};PWD={password}'
            conn_db = pyodbc.connect(conn_string)
            query = f"SELECT table_schema, table_name, column_name, data_type FROM {dbname}.information_schema.columns"
            # No params needed, filtered by dbname connection
        elif db_type == "Snowflake":
            conn_db = snowflake.connector.connect(user=user, password=password, account=host, database=dbname)
            # --- UPDATED QUERY ---
            query = f"SELECT table_schema, table_name, column_name, data_type FROM {dbname}.information_schema.columns WHERE table_schema NOT IN ('INFORMATION_SCHEMA')"
            # No params needed, filtered by dbname connection
        elif db_type == "Oracle":
            dsn = f"{host}:{port}/{dbname}" if port else f"{host}/{dbname}"
            conn_db = oracledb.connect(user=user, password=password, dsn=dsn)
            query = "SELECT owner as table_schema, table_name, column_name, data_type FROM all_tab_columns WHERE owner = (SELECT GLOBAL_NAME FROM GLOBAL_NAME)"
            # No params needed
        
        if not conn_db or not query:
            return "Error: Unsupported database type for schema fetching."

        if params:
            df = pd.read_sql(query, conn_db, params=params)
        else:
            df = pd.read_sql(query, conn_db)
            
        if hasattr(conn_db, 'close'):
            conn_db.close()
        
        schema_str = "DATABASE SCHEMA:\n"
        current_table = ""
        for _, row in df.iterrows():
            # Use column indices for safety, as pd may rename columns
            table_schema = row[0]
            table_name = row[1]
            column_name = row[2]
            data_type = row[3]
            
            table_key = f"{table_schema}.{table_name}"
            if table_key != current_table:
                schema_str += f"\nTable: {table_key}\n"
                current_table = table_key
            schema_str += f"  - {column_name} ({data_type})\n"
        
        return schema_str

    except Exception as e:
        print(f"Error getting schema for AI: {e}")
        return f"Error getting schema: {str(e)}"

# --- Saved Connection Endpoints (Now use get_db_conn_configured) ---
@app.route('/api/connections/list', methods=['GET'])
def get_saved_connections():
    username = session.get('user')
    if not username:
        return jsonify({"error": "Not authenticated"}), 401
    
    try:
        with get_db_conn_configured() as conn: # Use new connection
            with conn.cursor(snowflake.connector.DictCursor) as cur:
                # --- SQL INJECTION FIX: Use parameterized query ---
                cur.execute(
                    f"""
                    SELECT id, nickname, db_type, db_host, db_port, db_name, db_user
                    FROM {SNOWFLAKE_SCHEMA}.saved_connections
                    WHERE username = %s
                    ORDER BY nickname ASC;
                    """,
                    (username,)
                )
                connections = [
                    {
                        "id": r["ID"],
                        "nickname": r["NICKNAME"],
                        "connectionInfo": {
                            "db_type": r["DB_TYPE"],
                            "host": r["DB_HOST"],
                            "port": r["DB_PORT"],
                            "dbname": r["DB_NAME"],
                            "user": r["DB_USER"],
                            "password": ""
                        }
                    }
                    for r in cur.fetchall()
                ]
        return jsonify({"success": True, "connections": connections})
    except Exception as e:
        print(f"Error fetching saved connections: {e}")
        return jsonify({"success": False, "error": "Database error"}), 500

@app.route('/api/connections/save', methods=['POST'])
def save_connection():
    username = session.get('user')
    if not username:
        return jsonify({"error": "Not authenticated"}), 401
        
    data = request.json
    nickname = data.get('nickname')
    conn_info = data.get('connectionInfo', {})
    
    if not nickname or not conn_info.get('db_type') or not conn_info.get('host') or not conn_info.get('dbname') or not conn_info.get('user'):
        return jsonify({"success": False, "error": "Missing required fields (nickname, type, host, dbname, user)"}), 400

    try:
        with get_db_conn_configured() as conn: # Use new connection
            with conn.cursor() as cur:
                # --- SQL INJECTION FIX: Use parameterized query ---
                cur.execute(
                    f"""
                    MERGE INTO {SNOWFLAKE_SCHEMA}.saved_connections AS T
                    USING (
                        SELECT %s AS username, %s AS nickname, %s AS db_type, 
                               %s AS db_host, %s AS db_port, %s AS db_name, %s AS db_user
                    ) AS S
                    ON T.username = S.username AND T.nickname = S.nickname
                    WHEN MATCHED THEN
                        UPDATE SET
                            db_type = S.db_type,
                            db_host = S.db_host,
                            db_port = S.db_port,
                            db_name = S.db_name,
                            db_user = S.db_user
                    WHEN NOT MATCHED THEN
                        INSERT (username, nickname, db_type, db_host, db_port, db_name, db_user)
                        VALUES (S.username, S.nickname, S.db_type, S.db_host, S.db_port, S.db_name, S.db_user);
                    """,
                    (
                        username,
                        nickname,
                        conn_info['db_type'],
                        conn_info['host'],
                        conn_info.get('port'),
                        conn_info['dbname'],
                        conn_info['user']
                    )
                )
                conn.commit()
        
        return jsonify({"success": True, "message": f"Connection '{nickname}' saved."})
    except snowflake.connector.errors.IntegrityError as e:
        return jsonify({"success": False, "error": "This nickname is already in use."}), 409
    except Exception as e:
        print(f"Error saving connection: {e}")
        return jsonify({"success": False, "error": "Database error"}), 500

@app.route('/api/connections/delete', methods=['POST'])
def delete_connection():
    username = session.get('user')
    if not username:
        return jsonify({"error": "Not authenticated"}), 401
        
    data = request.json
    nickname = data.get('nickname')
    
    if not nickname:
        return jsonify({"success": False, "error": "Missing nickname"}), 400

    try:
        with get_db_conn_configured() as conn: # Use new connection
            with conn.cursor() as cur:
                # --- SQL INJECTION FIX: Use parameterized query ---
                cur.execute(
                    f"""
                    DELETE FROM {SNOWFLAKE_SCHEMA}.saved_connections
                    WHERE username = %s AND nickname = %s
                    """,
                    (username, nickname)
                )
                conn.commit()
                
                if cur.rowcount > 0:
                    return jsonify({"success": True, "message": f"Connection '{nickname}' deleted."})
                else:
                    return jsonify({"success": False, "error": "Connection not found or permission denied."}), 404
                    
    except Exception as e:
        print(f"Error deleting connection: {e}")
        return jsonify({"success": False, "error": "Database error"}), 500

# --- NEW: Semantic Catalog Endpoints (Ported from main1.py) ---
@app.route('/api/semantic_catalogs', methods=['GET'])
def get_catalog():
    if 'user' not in session:
        return jsonify({"error": "Not authenticated"}), 401
    
    current_username = session.get('user') # Get the logged-in username

    try:
        with get_db_conn_configured() as conn:
            with conn.cursor(snowflake.connector.DictCursor) as cur:
                # --- MODIFIED QUERY: Filter by Owner OR Assigned User ---
                query = f"""
                    SELECT 
                        a.id,
                        a.catalog_name AS semantic_catalog_name,
                        COUNT(DISTINCT b.table_name) AS nooftables,
                        LISTAGG(DISTINCT b.table_name, ', ') WITHIN GROUP (ORDER BY b.table_name) AS table_name
                    FROM 
                        {SNOWFLAKE_SCHEMA}.semantic_catalogs a
                    LEFT JOIN 
                        {SNOWFLAKE_SCHEMA}.semantic_catalogs_detail b ON a.id = b.catalog_id
                    WHERE 
                        -- 1. User is the Owner
                        a.username = %s
                        OR 
                        -- 2. User is explicitly assigned (join with user_catalogs and users)
                        a.id IN (
                            SELECT uc.catalog_id 
                            FROM {SNOWFLAKE_SCHEMA}.user_catalogs uc
                            JOIN {SNOWFLAKE_SCHEMA}.users u ON uc.user_id = u.id
                            WHERE u.username = %s
                        )
                    GROUP BY 
                        a.id, a.catalog_name
                """
                
                # Pass current_username twice: once for the owner check, once for the assignment check
                cur.execute(query, (current_username, current_username))
                
                catalogs = [
                    {
                        "id": r["ID"],
                        "semantic_catalog_name": r["SEMANTIC_CATALOG_NAME"],
                        "NoofTables": r["NOOFTABLES"],
                        "table_name": r["TABLE_NAME"]
                    }
                    for r in cur.fetchall()
                ]
        return jsonify({"success": True, "catalog": catalogs})
    except Exception as e:
        print(f"Error fetching semantic catalogs: {e}")
        return jsonify({"success": False, "error": "Database error"}), 500

@app.route("/api/semantic_catalogs/<int:catalog_id>/details", methods=["GET"])
def catalog_details(catalog_id):
    if 'user' not in session:
        return jsonify({"error": "Not authenticated"}), 401

    group_by_table = request.args.get("group_by_table", "false").lower() in ("1", "true", "yes")
    
    try:
        with get_db_conn_configured() as conn:
            with conn.cursor(snowflake.connector.DictCursor) as cur:
                # 1) fetch catalog metadata (connection info) from semantic_catalogs
                cur.execute(f"""
                    SELECT id, username, catalog_name, db_type, db_host, db_port, db_username, db_dbname, db_schema, created_at, updated_at 
                    FROM {SNOWFLAKE_SCHEMA}.semantic_catalogs
                    WHERE id = %s
                    """,
                    (catalog_id,)
                )
                catalog_row = cur.fetchone()
                if not catalog_row:
                    return jsonify({"success": False, "error": "Catalog not found"}), 404
                
                # Build a sanitized catalog object (NO password hash)
                catalog_info = {
                    "id": catalog_row.get("ID"),
                    "username": catalog_row.get("USERNAME"),
                    "catalog_name": catalog_row.get("CATALOG_NAME"),
                    "db_type": catalog_row.get("DB_TYPE"),
                    "db_host": catalog_row.get("DB_HOST"),
                    "db_port": catalog_row.get("DB_PORT"),
                    "db_username": catalog_row.get("DB_USERNAME"),
                    "db_dbname" : catalog_row.get("DB_DBNAME"),
                    "db_schema": catalog_row.get("DB_SCHEMA"), # Added
                    "created_at": catalog_row.get("CREATED_AT"),
                    "updated_at": catalog_row.get("UPDATED_AT"),
                }

                # This is the connection info for the *target* DB, not the app DB
                connection_info = {
                    'db_type': catalog_info["db_type"],
                    'host': catalog_info["db_host"],
                    'port': catalog_info["db_port"],
                    'user': catalog_info["db_username"],
                    'password': "", # Always empty for security
                    'dbname': catalog_info["db_dbname"]
                }
                
                # 2) fetch details from semantic_catalogs_detail (the metadata table)
                cur.execute(f"""
                    SELECT catalog_id, schema_name, table_name, column_name, data_type, description
                    FROM {SNOWFLAKE_SCHEMA}.semantic_catalogs_detail
                    WHERE catalog_id = %s
                    ORDER BY table_name NULLS LAST, column_name NULLS LAST
                    """,
                    (catalog_id,)
                )
                rows = cur.fetchall()  # List of dicts

                # Return grouped or flat as requested
                if group_by_table:
                    grouped = {}
                    for r in rows:
                        tbl = r.get("TABLE_NAME") or "unknown_table"
                        grouped.setdefault(tbl, []).append({
                            "column": r.get("COLUMN_NAME"),
                            "data_type": r.get("DATA_TYPE"),
                            "description": r.get("DESCRIPTION")
                        })
                    return jsonify({
                        "success": True,
                        "catalog": catalog_info,
                        "connection_info" : connection_info, # Added
                        "catalog_id": catalog_id,
                        "details_by_table": grouped
                    })

                # flat list
                flat_details = [
                    {
                        "catalog_id": r.get("CATALOG_ID"),
                        "schema_name": r.get("SCHEMA_NAME"),
                        "table_name": r.get("TABLE_NAME"),
                        "column_name": r.get("COLUMN_NAME"),
                        "data_type": r.get("DATA_TYPE"),
                        "description": r.get("DESCRIPTION")
                    } for r in rows
                ]

                return jsonify({
                    "success": True,
                    "connection_info" : connection_info,
                    "catalog": catalog_info,
                    "catalog_id": catalog_id,
                    "details": flat_details
                })

    except Exception as e:
        print(f"Failed to fetch catalog details for id={catalog_id}: {e}")
        return jsonify({"success": False, "error": "Failed to fetch catalog details"}), 500

@app.route('/api/semantic_catalogs/list_all', methods=['GET'])
def list_all_catalogs():
    if 'user' not in session:
        return jsonify({"error": "Not authenticated"}), 401

    try:
        with get_db_conn_configured() as conn:
            with conn.cursor(snowflake.connector.DictCursor) as cur:
                # Fetch IDs and Names of all catalogs
                cur.execute(f"SELECT id, catalog_name FROM {SNOWFLAKE_SCHEMA}.semantic_catalogs ORDER BY catalog_name")
                
                # Format specifically for the UserManagement dropdown
                # The frontend expects keys: "id" and "name"
                catalogs = [
                    {"id": r["ID"], "name": r["CATALOG_NAME"]} 
                    for r in cur.fetchall()
                ]
        return jsonify({"success": True, "catalogs": catalogs})
    except Exception as e:
        print(f"Error fetching all catalogs: {e}")
        return jsonify({"success": False, "error": "Database error"}), 500

@app.route('/api/semantic_catalogs/create_or_merge', methods=['POST'])
def create_or_merge_sc():
    if 'user' not in session:
        return jsonify({"error": "Not authenticated"}), 401

    data = request.json or {}
    conn_info = data.get('connectionInfo', {})
    
    # --- ENCRYPTION LOGIC ---
    try:
        password = conn_info.get('password', '')
        # Encrypt password if present
        password_hash = cipher.encrypt(password.encode()).decode() if password else ""
    except Exception as e:
        print(f"Could not encrypt password: {e}")
        password_hash = "" 
    # ------------------------

    db_type = conn_info.get('db_type')
    host = conn_info.get('host')
    port = conn_info.get('port')
    user = conn_info.get('user')
    dbname = conn_info.get('dbname')
    schema = data.get('schema')
    
    appusername = session.get('user')
    comments = data.get('comments', []) 

    scdetail = data.get('semanticdata', {})
    new_sc_name = scdetail.get('newCatalogName')
    old_sc_id = scdetail.get('existingCatalogId')

    try:
        if not all([db_type, host, user, dbname, schema]):
            return jsonify({"success": False, "error": "Missing required connection parameters"}), 400
        
        with get_db_conn_configured() as conn:
            with conn.cursor() as cursor:
                
                catalog_id = None
                catalog_name_to_return = ""
                
                if new_sc_name:
                    catalog_name_to_return = new_sc_name
                    cursor.execute(
                        f"""
                        INSERT INTO {SNOWFLAKE_SCHEMA}.semantic_catalogs 
                        (username, catalog_name, db_type, db_host, db_port, db_username, db_password_hash, db_dbname, db_schema)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (appusername or 'unknown_user', new_sc_name, db_type, host, str(port or ''), user, password_hash, dbname, schema)
                    )
                    # Snowflake specific: get last ID
                    cursor.execute(f"SELECT MAX(id) FROM {SNOWFLAKE_SCHEMA}.semantic_catalogs WHERE catalog_name = %s", (new_sc_name,))
                    catalog_id = cursor.fetchone()[0]
                else:
                    catalog_id = old_sc_id
                    cursor.execute(f"SELECT catalog_name FROM {SNOWFLAKE_SCHEMA}.semantic_catalogs WHERE id = %s", (catalog_id,))
                    res = cursor.fetchone()
                    catalog_name_to_return = res[0] if res else "Unknown"

                if not catalog_id: raise Exception("Failed to create or find catalog ID")

                # Prepare MERGE data
                cursor.execute(f"CREATE TEMPORARY TABLE {SNOWFLAKE_SCHEMA}.merge_temp (catalog_id INT, schema_name VARCHAR, table_name VARCHAR, column_name VARCHAR, data_type VARCHAR, description VARCHAR)")
                
                comments_data = [(catalog_id, schema, c.get('table'), c.get('column'), c.get('type'), c.get('description')) for c in comments if c.get('table')]
                
                if comments_data:
                    cursor.executemany(f"INSERT INTO {SNOWFLAKE_SCHEMA}.merge_temp VALUES (%s, %s, %s, %s, %s, %s)", comments_data)
                    
                    cursor.execute(f"""
                        MERGE INTO {SNOWFLAKE_SCHEMA}.semantic_catalogs_detail AS T
                        USING {SNOWFLAKE_SCHEMA}.merge_temp AS S
                        ON  T.catalog_id = S.catalog_id AND T.table_name = S.table_name AND T.column_name = S.column_name
                        WHEN MATCHED THEN UPDATE SET T.data_type = S.data_type, T.description = S.description, T.updated_at = CURRENT_TIMESTAMP()
                        WHEN NOT MATCHED THEN INSERT (catalog_id, schema_name, table_name, column_name, data_type, description)
                        VALUES (S.catalog_id, S.schema_name, S.table_name, S.column_name, S.data_type, S.description)
                    """)

                conn.commit()
        
        return jsonify({"success": True, "catalog_id": catalog_id, "catalog_name": catalog_name_to_return, "message": "Merged successfully."})

    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

# --- END NEW Semantic Catalog Endpoints ---

@app.route("/api/chat/generate_suggestions", methods=["POST"])
def generate_suggestions():
    connection_info = request.get_json().get("connectionInfo")

    if not connection_info:
        return jsonify({"success": False, "error": "No connection info provided"}), 400

    try:
        schema_context = get_schema_for_ai(connection_info)
        if schema_context.startswith("Error"):
             return jsonify({"success": False, "error": schema_context})
        
        db_type = connection_info.get('db_type', 'SQL')
        
        prompt = f"""
You are an expert {db_type} data analyst. Your task is to generate 5 insightful, actionable business questions a user might ask, based *only* on the provided database schema.
- The questions should be complex enough to require data analysis (e.g., "What are the top 5 selling products this quarter?" not "Show me the products table").
- If the schema looks like it's for sales, ask about sales trends, top customers, or product performance.
- If the schema looks like it's for HR, ask about employee turnover, department costs, or salary distributions.
- Tailor the questions to the table and column names.
- Return ONLY a JSON object with a single key "suggestions", which is an array of strings. Do not include markdown or any other text.

SCHEMA:
{schema_context}

Example Response:
{{
  "suggestions": [
    "What is the total revenue per product category for the last 6 months?",
    "Who are the top 10 customers by total spending?",
    "What is the average order value by region?",
    "Show the monthly sales trend for the current year.",
    "Which employees have the highest sales performance?"
  ]
}}
"""
        
        response_str = call_gemini_api(prompt, is_json_output=False)
        
        # Clean the response
        json_match = re.search(r'```json\s*(\{.*?\})\s*```', response_str, re.DOTALL)
        if json_match:
            response_str = json_match.group(1)
        else:
            response_str = response_str.strip()


        try:
            suggestions_json = json.loads(response_str)
            suggestions = suggestions_json.get("suggestions", [])
            return jsonify({"success": True, "suggestions": suggestions})
        except json.JSONDecodeError as e:
            print(f"Failed to decode suggestions JSON from Gemini: {e}")
            print(f"Received string: {response_str}")
            return jsonify({"success": False, "error": "AI model returned invalid JSON for suggestions."})

    except Exception as e:
        print(f"Error in /api/chat/generate_suggestions: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

# --- NEW: Ported from main1.py ---
@app.route("/api/chat/generate_from_metadata", methods=["POST"])
def metadata_suggestions():
    connection_info = request.get_json().get("connectionInfo", {})
    metadata_context = request.get_json().get("metadata")

    if not metadata_context:
        return jsonify({"success": False, "error": "No metadata provided"}), 400

    try:
        db_type = connection_info.get('db_type', 'SQL')
        
        prompt = f"""
You are an expert {db_type} data analyst. Your task is to generate 5 insightful, actionable business questions a user might ask, based *only* on the provided database schema metadata.
- The questions should be simple for data analysis (e.g., "What are the top 5 selling products this quarter?" not "Show me the products table").
- If the schema looks like it's for sales, ask about sales trends, top customers, or product performance.
- Tailor the questions to the table and column names and their descriptions.
- Return ONLY a JSON object with a single key "suggestions", which is an array of strings. Do not include markdown or any other text.

SCHEMA METADATA:
{metadata_context}

Example Response:
{{
  "suggestions": [
    "What is the total revenue per product category for the last 6 months?",
    "Who are the top 10 customers by total spending?",
    "What is the average order value by region?",
    "Show the monthly sales trend for the current year.",
    "Which employees have the highest sales performance?"
  ]
}}
"""
        
        response_str = call_gemini_api(prompt, is_json_output=False) # Use Gemini
        
        # Clean the response
        json_match = re.search(r'```json\s*(\{.*?\})\s*```', response_str, re.DOTALL)
        if json_match:
            response_str = json_match.group(1)
        else:
            response_str = response_str.strip()

        try:
            suggestions_json = json.loads(response_str)
            suggestions = suggestions_json.get("suggestions", [])
            return jsonify({"success": True, "suggestions": suggestions})
        except json.JSONDecodeError as e:
            print(f"Failed to decode suggestions JSON from Gemini: {e}")
            print(f"Received string: {response_str}")
            return jsonify({"success": False, "error": "AI model returned invalid JSON for suggestions."})

    except Exception as e:
        print(f"Error in /api/chat/generate_from_metadata: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

# --- UPDATED: intelligent_query (to accept catalogid) ---
@app.route("/api/chat/intelligent_query", methods=["POST"])
def intelligent_query():
    data = request.get_json()
    prompt = data.get("prompt")
    connection_info = data.get("connectionInfo")
    catalogid = data.get("catalogid") # --- NEW ---

    # --- Handle General (No DB) Questions ---
    if not connection_info:
        try:
            system_prompt = (
                "You are MetadataGenbot, an AI assistant specializing in data warehousing, "
                "database management, and Snowflake. The user is not currently connected to a database, "
                "so answer their questions generally."
            )
            full_prompt = f"{system_prompt}\n\nUser: {prompt}\nAssistant:"
            ai_reply = call_gemini_api(full_prompt, is_json_output=False)
            if ai_reply.startswith("Error:"):
                return jsonify({"error": ai_reply}), 500
            
            return jsonify({
                "reply": ai_reply.strip(),
                "dashboard_data": None,
                "raw_data": None
            })
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    # --- Handle DB-Connected Questions ---
    try:
        db_type = connection_info.get('db_type', 'Snowflake')
        
        # --- Step 1: Get Schema (from Catalog or Live) ---
        schema_context = ""
        if catalogid:
            print(f"Getting schema from Semantic Catalog ID: {catalogid}")
            schema_context = get_schema_for_catalog(catalogid)
        else:
            print("Getting schema from live database connection...")
            schema_context = get_schema_for_ai(connection_info)

        if schema_context.startswith("Error"):
             return jsonify({"error": schema_context}), 500
        
        # --- Step 2: Generate SQL ---
        prompt_step2 = f"""
You are an expert {db_type} data analyst. Your sole task is to convert the user's question into a single, runnable {db_type} SQL query.
- You are connected to a database with this schema:
{schema_context}
- If the question can be answered with SQL, return ONLY the SQL query.
- If the question is a greeting, a general question (e.g., "what is SQL?"), or cannot be answered with the provided schema, return ONLY the text "NO_SQL".
- Do not add any explanation, markdown, or any text other than the query or "NO_SQL".

User Question: "{prompt}"
"""
        
        sql_query = call_gemini_api(prompt_step2).strip()
        
        if sql_query.startswith("Error:"):
            return jsonify({"error": sql_query}), 500
        
        # Clean SQL query
        if sql_query.upper().startswith("```SQL"):
            sql_query = sql_query[6:-3].strip()
        elif sql_query.upper().startswith("```"):
            sql_query = sql_query[3:-3].strip()

        # --- Handle NO_SQL (General Question) ---
        if sql_query.upper() == "NO_SQL":
            prompt_step_general = f"""
You are MetadataGenbot, an AI data assistant. The user is connected to a {db_type} database.
The user asked: "{prompt}".
You determined this is a general question, not a data query.
Provide a helpful, direct answer.
"""
            final_reply = call_gemini_api(prompt_step_general)
            return jsonify({
                "reply": final_reply.strip(),
                "dashboard_data": None,
                "raw_data": None
            })

        # --- Step 3: Run SQL Query ---
        df = None
        conn_db = None
        try:
            # --- *** NEW SECURITY FIX *** ---
            # Deny-list destructive keywords. This is a crucial safeguard
            # when executing AI-generated code.
            destructive_keywords = [
                'DROP', 'DELETE', 'INSERT', 'UPDATE', 
                'TRUNCATE', 'ALTER', 'CREATE', 'GRANT', 'REVOKE'
            ]
            # Check for keywords as whole words
            sql_upper_tokens = set(re.findall(r"[\w']+", sql_query.upper()))
            
            for keyword in destructive_keywords:
                if keyword in sql_upper_tokens:
                    print(f"Blocked potentially destructive query: {sql_query}")
                    return jsonify({
                        "reply": f"Sorry, I cannot execute queries that might modify the database (e.g., {keyword}). I can only perform read operations (e.g., SELECT).",
                        "dashboard_data": None,
                        "raw_data": None
                    })
            # --- *** END NEW SECURITY FIX *** ---

            db_type = connection_info.get('db_type')
            host = connection_info.get('host')
            port = connection_info.get('port')
            user = connection_info.get('user')
            password = connection_info.get('password')
            dbname = connection_info.get('dbname')
            
            # --- NEW: Decrypt password if from catalog ---
            if catalogid:
                try:
                    with get_db_conn_configured() as conn:
                        with conn.cursor() as cur:
                            cur.execute(f"SELECT db_password_hash FROM {SNOWFLAKE_SCHEMA}.semantic_catalogs WHERE id = %s", (catalogid,))
                            result = cur.fetchone()
                            if result and result[0]:
                                password = cipher.decrypt(result[0].encode()).decode()
                                print("Decrypted password from catalog for query execution.")
                            else:
                                raise Exception("Password hash not found in catalog, cannot execute query.")
                except Exception as e:
                    print(f"Could not decrypt password from catalog: {e}")
                    return jsonify({"error": "Could not decrypt password from catalog to run query."}), 500

            if db_type == "MySQL":
                conn_db = mysql.connector.connect(host=host, port=int(port), user=user, password=password, database=dbname)
            elif db_type == "PostgreSQL":
                db_connection_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
                engine = create_engine(db_connection_str)
                conn_db = engine.connect() 
            elif db_type == "Oracle":
                dsn = f"{host}:{port}/{dbname}" if port else f"{host}/{dbname}"
                conn_db = oracledb.connect(user=user, password=password, dsn=dsn)
            elif db_type == "SQL Server":
                conn_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={host};DATABASE={dbname};UID={user};PWD={password}'
                if port:
                    conn_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={host},{port};DATABASE={dbname};UID={user};PWD={password}'
                conn_db = pyodbc.connect(conn_string)
            elif db_type == "Snowflake":
                conn_db = snowflake.connector.connect(
                    user=user, password=password, account=host, database=dbname
                )
            
            if conn_db:
                # The query has passed the denylist check.
                # A read-only database user is still the best practice.
                df = pd.read_sql(sql_query, conn_db)
            else:
                raise Exception(f"Unsupported database type '{db_type}'")

        except Exception as e:
            # SQL execution error
            print(f"Bad SQL from Gemini: {sql_query}")
            print(f"Error: {e}")
            return jsonify({
                "reply": (
                    f"Sorry, I encountered an error executing the query.\n\n"
                    f"**Generated Query:**\n```sql\n{sql_query}\n```\n\n"
                    f"**Error:**\n`{str(e)}`"
                ),
                "dashboard_data": None,
                "raw_data": None
            })
        finally:
            if conn_db and hasattr(conn_db, 'close'):
                conn_db.close()
        
        if df is None or df.empty:
            return jsonify({
                "reply": f"The query ran successfully but returned no data.\n\n**Generated Query:**\n```sql\n{sql_query}\n```",
                "dashboard_data": None,
                "raw_data": "[]"
            })

        try:
            # Convert only non-numeric/non-string columns to string
            df_copy = df.copy()
            for col in df_copy.columns:
                # Check if col is not numeric, string, or boolean
                if not (pd.api.types.is_numeric_dtype(df_copy[col]) or 
                        pd.api.types.is_string_dtype(df_copy[col]) or 
                        pd.api.types.is_bool_dtype(df_copy[col])):
                    # Convert other types (like datetime, timedelta) to string
                    df_copy[col] = df_copy[col].astype(str)
                    
            query_results_json = df_copy.to_json(orient="records")
        except Exception as e:
            print(f"Error serializing DataFrame: {e}")
            return jsonify({"error": "Failed to serialize query results for analysis."}), 500
            
        if len(query_results_json) > 100000:
             query_results_json = df.head(100).to_json(orient="records")
             print("Warning: Query result too large, truncating for AI analysis.")

        prompt_step4 = f"""
You are MetadataGenbot, an expert AI data analyst.
A user asked: "{prompt}"
You generated a SQL query which returned this data (as a JSON string):
{query_results_json}

Your task is to provide a complete analysis. Return ONLY a JSON object with the following structure:
1.  "chat_reply": A natural-language answer to the user's question, based *only* on the data. Do not mention the JSON or charts. This will be shown in the chat window.
2.  "dashboard_summary": A 2-3 sentence summary of the key insights for a dashboard.
3.  "key_metrics": An array of 2-4 key metrics (label, value). Infer data types (e.g., format currency, dates).
4.  "chart_specs": An array of 1-2 chart specifications.
    - Choose the *best* chart(s) (bar, line, pie, scatter) to answer the user's question.
    - For bar/line/scatter: {{"type": "bar", "title": "...", "keys": {{"x": "col_name_for_x_axis", "y": "col_name_for_y_axis"}} }}
    - For pie: {{"type": "pie", "title": "...", "keys": {{"name": "col_name_for_labels", "value": "col_name_for_values"}} }}
    - Ensure the "col_name_..." values *exactly* match the column names in the JSON data.

Example Response:
{{
  "chat_reply": "Based on the data, total sales from January to March 2024 were $1,500,000, with an average sale of $120.50. The 'Electronics' category was the top performer.",
  "dashboard_summary": "The analysis covers Q1 2024, showing strong performance in Electronics and a steady monthly average.",
  "key_metrics": [
    {{"label": "Time Period", "value": "Jan 2024 - Mar 2024"}},
    {{"label": "Total Sales", "value": "$1,500,000"}},
    {{"label": "Average Sale", "value": "$120.50"}}
  ],
  "chart_specs": [
    {{"type": "bar", "title": "Sales by Month", "keys": {{"x": "MONTH", "y": "TOTAL_SALES"}} }}
  ]
}}
"""
        
        try:
            analysis_response_str = call_gemini_api(prompt_step4)
            
            json_match = re.search(r'```json\s*(\{.*?\})\s*```', analysis_response_str, re.DOTALL)
            if json_match:
                analysis_response_str = json_match.group(1)
            else:
                analysis_response_str = analysis_response_str.strip()

            analysis_json = json.loads(analysis_response_str)
            
            # --- ADDING SQL Query to chat_reply ---
            chat_reply = analysis_json.get("chat_reply", "I found data but couldn't generate a chat reply.")
            chat_reply += f"\n\n**Generated Query:**\n```sql\n{sql_query}\n```"
            
            # --- ADDING Table Data for ChatWithTable ---
            table_data = None
            if df.shape[0] < 200: # Only send small-ish dataframes to the table component
                table_data = json.loads(query_results_json) # Use the same JSON as the AI

            return jsonify({
                "reply": chat_reply,
                "dashboard_data": {
                    "summary": analysis_json.get("dashboard_summary"),
                    "key_metrics": analysis_json.get("key_metrics"),
                    "chart_specs": analysis_json.get("chart_specs")
                },
                "raw_data": query_results_json,
                "table_data": table_data # NEW
            })

        except json.JSONDecodeError as e:
            print(f"Failed to decode analysis JSON from Gemini: {e}")
            print(f"Received string: {analysis_response_str}")
            # --- Also add table_data to fallback ---
            table_data = None
            if df.shape[0] < 200:
                table_data = json.loads(query_results_json)
                
            return jsonify({
                "reply": "I found the following data:\n\n" + df.head(10).to_string() + f"\n\n**Generated Query:**\n```sql\n{sql_query}\n```",
                "dashboard_data": None,
                "raw_data": query_results_json,
                "table_data": table_data # NEW
            })
        except Exception as e:
            print(f"Error in final analysis step: {e}")
            return jsonify({"error": str(e)}), 500

    except Exception as e:
        print(f"Error in /api/chat/intelligent_query: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/generate_excel', methods=['POST'])
def generate_excel():
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'No data provided'}), 400
            
        print(f"🔍 [DEBUG] Excel generation request received")
        print(f"🔍 [DEBUG] Request data keys: {list(data.keys())}")
        
        # Get username from request body
        username = data.get('username')
        print(f"🔍 [DEBUG] Username from request: {username}")
        
        # Remove username from data before processing tables
        data_to_process = {k: v for k, v in data.items() if k != 'username'}
        
        print(f"🔍 [DEBUG] Tables to process: {list(data_to_process.keys())}")
        
        # Log to database if username is provided
        if username:
            try:
                with get_db_conn_configured() as conn:
                    with conn.cursor() as cur:
                        # Get the latest log_id for this user
                        cur.execute(
                            """
                            SELECT MAX(log_id) 
                            FROM LANDING.ANALYTICSBOT.user_activity_log 
                            WHERE username = %s
                            """,
                            (username,)
                        )
                        result = cur.fetchone()
                        log_id = result[0] if result else None
                        
                        if log_id:
                            # Update the existing record
                            cur.execute(
                                """
                                UPDATE LANDING.ANALYTICSBOT.user_activity_log 
                                SET downloaded_excel = TRUE
                                WHERE log_id = %s
                                """,
                                (log_id,)  # Only one parameter
                            )
                            conn.commit()
                            print(f"✅ [DEBUG] Excel download logged for user: {username}, log_id: {log_id}")
                        else:
                            # If no existing log_id found, create a new entry
                            cur.execute(
                                """
                                INSERT INTO LANDING.ANALYTICSBOT.user_activity_log 
                                (username, login_time, downloaded_excel)
                                VALUES (%s, %s, %s)
                                """,
                                (username, datetime.now(), True)
                            )
                            conn.commit()
                            print(f"✅ [DEBUG] Created new activity log and logged Excel download for user: {username}")
                            
            except Exception as e:
                print(f"❌ [DEBUG] Error logging Excel download: {e}")
                import traceback
                traceback.print_exc()
                # Continue with Excel generation even if logging fails
        else:
            print(f"⚠️ [DEBUG] No username provided, skipping database logging")
        
        # Process the Excel data
        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            for table_name, table_data in data_to_process.items():
                if table_data:  # Only process if there's data
                    safe_sheet_name = re.sub(r'[^A-Za-z0-9_ ]', '_', table_name)
                    if len(safe_sheet_name) > 31:
                        safe_sheet_name = safe_sheet_name[:31]
                        
                    df = pd.DataFrame(table_data)
                    
                    if 'Column' in df.columns and 'Description' in df.columns:
                        if 'Type' in df.columns:
                            df = df[['Column', 'Type', 'Description']]
                        else:
                            df = df[['Column', 'Description']]
                    
                    df.to_excel(writer, sheet_name=safe_sheet_name, index=False)
                    
                    worksheet = writer.sheets[safe_sheet_name]
                    
                    for idx, col in enumerate(df):
                        series = df[col]
                        max_len = max((
                            series.astype(str).map(len).max(),
                            len(str(series.name))
                        )) + 2
                        max_len = min(max_len, 70) 
                        worksheet.set_column(idx, idx, max_len)

        output.seek(0)
        
        print("✅ Excel report generated successfully")
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name="metadata_documentation.xlsx"
        )
        
    except Exception as e:
        print(f"❌ Error generating Excel: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/update_column_comments', methods=['POST'])
def update_column_comments():
    data = request.json
    conn_info = data.get('connectionInfo', {})
    db_type = conn_info.get('db_type')
    host = conn_info.get('host')
    port = conn_info.get('port')
    user = conn_info.get('user')
    password = conn_info.get('password')
    dbname = conn_info.get('dbname')
    schema = data.get('schema')
    table = data.get('table')
    comments = data.get('comments')

    if not all([conn_info, db_type, schema, table, comments]):
        return jsonify({"success": False, "error": "Missing required fields"}), 400

    # Get username from request body
    username = data.get('username')  # Get username from connection info
    
    # Log the update activity if we have a username
    if username and comments:
        try:
            with get_db_conn_configured() as conn:
                with conn.cursor() as cur:
                    # Get the latest log_id for this user
                    cur.execute(
                        """
                        SELECT MAX(log_id) 
                        FROM LANDING.ANALYTICSBOT.user_activity_log 
                        WHERE username = %s
                        """,
                        (username,)
                    )
                    result = cur.fetchone()
                    log_id = result[0] if result else None
                    
                    if log_id:
                        # Update the existing record
                        cur.execute(
                            """
                            UPDATE LANDING.ANALYTICSBOT.user_activity_log
                            SET updated_comments = TRUE,
                                columns_updated_count = columns_updated_count + %s
                            WHERE log_id = %s
                            """,
                            (len(comments), log_id)
                        )
                        conn.commit()
                        print(f"✅ Comments update logged for user: {username}, log_id: {log_id}, {len(comments)} columns")
                    else:
                        # If no existing log_id found, create a new entry
                        cur.execute(
                            """
                            INSERT INTO LANDING.ANALYTICSBOT.user_activity_log 
                            (username, login_time, updated_comments, columns_updated_count)
                            VALUES (%s, %s, %s, %s)
                            """,
                            (username, datetime.now(), True, len(comments))
                        )
                        conn.commit()
                        print(f"✅ Created new activity log and logged comments update for user: {username}, {len(comments)} columns")
        except Exception as e:
            print(f"❌ Error logging /update_column_comments: {e}")
            # Continue with the main operation even if logging fails

    # Rest of your existing update logic...
    conn_db = None
    cursor = None
    total_success = 0
    
    try:
        # --- SQL INJECTION FIX: Validate identifiers ---
        if not re.match(r'^[A-Za-z0-9_"$]+$', schema) or \
           not re.match(r'^[A-Za-z0-9_"$]+$', table) or \
           not re.match(r'^[A-Za-z0-9_"$]+$', dbname):
           return jsonify({"error": "Invalid schema, table, or database name"}), 400
           
        if db_type == "PostgreSQL":
            conn_db = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=dbname)
        elif db_type == "Oracle":
            dsn = f"{host}:{port}/{dbname}" if port else f"{host}/{dbname}"
            conn_db = oracledb.connect(user=user, password=password, dsn=dsn)
        elif db_type == "Snowflake":
            conn_db = snowflake.connector.connect(user=user, password=password, account=host, database=dbname)
        elif db_type == "MySQL":
            return jsonify({"success": False, "error": "Updating comments is not supported for MySQL from this interface."}), 400
        elif db_type == "SQL Server":
            return jsonify({"success": False, "error": "Updating comments is not supported for SQL Server from this interface."}), 400
        else:
            return jsonify({"success": False, "error": f"Unsupported DB type for comment updates: {db_type}"}), 400

        cursor = conn_db.cursor()
        
        for item in comments:
            col_name = item.get('column')
            col_desc = item.get('description')
            
            if not col_name or col_desc is None:
                continue

            # --- SQL INJECTION FIX: Validate col_name ---
            if not re.match(r'^[A-Za-z0-9_"$]+$', col_name):
                print(f"Skipping invalid column name: {col_name}")
                continue

            sql = ""
            params = (col_desc,) # Parameter is always the description
            
            if db_type == "PostgreSQL":
                sql = f'COMMENT ON COLUMN "{schema}"."{table}"."{col_name}" IS %s;'
            elif db_type == "Oracle":
                sql = f'COMMENT ON COLUMN "{schema.upper()}"."{table.upper()}"."{col_name.upper()}" IS :1'
            elif db_type == "Snowflake":
                sql = f'COMMENT ON COLUMN "{dbname}"."{schema}"."{table}"."{col_name}" IS %s'
            
            if sql:
                print(f"Executing: {sql}")
                cursor.execute(sql, params) # Pass params separately
                total_success += 1
        
        if db_type not in ["Snowflake"]:
             conn_db.commit()
        else:
             conn_db.commit() # Snowflake also needs commit

        return jsonify({"success": True, "message": f"Successfully updated {total_success} column comments."})

    except Exception as e:
        if conn_db:
            conn_db.rollback()
        print(f"Error updating comments: {e}")
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if conn_db:
            conn_db.close()

@app.route('/api/generate_profiling_report', methods=['POST'])
def generate_profiling_report():
    if 'data_file' not in request.files:
        return jsonify({"success": False, "error": "No file part"}), 400

    file = request.files['data_file']
    if file.filename == '':
        return jsonify({"success": False, "error": "No selected file"}), 400

    try:
        filename = file.filename
        ext = filename.split('.')[-1].lower()
        df = None

        if ext == 'csv':
            df = pd.read_csv(file)
        elif ext in ['xls', 'xlsx']:
            df = pd.read_excel(file)
        elif ext == 'json':
            df = pd.read_json(file)
        elif ext == 'parquet':
            df = pd.read_parquet(file)
        elif ext == 'tsv':
            df = pd.read_csv(file, sep='\t')
        else:
            return jsonify({"success": False, "error": f"Unsupported file type: .{ext}. Please upload CSV, Excel, TSV, JSON, or Parquet."}), 400

        profile = ProfileReport(df, title=f"Data Profile for {filename}", explorative=True)
        
        report_filename = f"report_{uuid.uuid4()}.html"
        save_path = os.path.join(REPORTS_DIR, report_filename)
        
        profile.to_file(save_path)
        
        return jsonify({"success": True, "report_filename": report_filename})

    except Exception as e:
        print(f"Error generating profiling report: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/user/preferences', methods=['GET'])
def get_user_preferences():
    username = session.get('user')
    if not username:
        return jsonify({"error": "Not authenticated"}), 401
    
    try:
        with get_db_conn_configured() as conn:
            with conn.cursor(snowflake.connector.DictCursor) as cur:
                # Retrieve the JSON data directly
                cur.execute(
                    f"SELECT preferences_data FROM {SNOWFLAKE_SCHEMA}.user_preferences WHERE username = %s", 
                    (username,)
                )
                row = cur.fetchone()
                
                if row and row['PREFERENCES_DATA']:
                    # Snowflake connector usually returns VARIANT as a Python dict/list string, 
                    # or a native object depending on version. We ensure it's returned as JSON.
                    prefs = row['PREFERENCES_DATA']
                    
                    # If it comes back as a JSON string, parse it. If it's already a dict, use it.
                    if isinstance(prefs, str):
                        try:
                            prefs = json.loads(prefs)
                        except:
                            prefs = {} 
                            
                    return jsonify({"success": True, "preferences": prefs})
                else:
                    # Return empty structure if no preferences saved yet
                    return jsonify({"success": True, "preferences": { 
                        "pinned_items": [], 
                        "pinned_dashboards": [] 
                    }})

    except Exception as e:
        print(f"Error fetching preferences: {e}")
        return jsonify({"success": False, "error": "Database error fetching preferences"}), 500


@app.route('/api/user/preferences', methods=['POST'])
def save_user_preferences():
    username = session.get('user')
    if not username:
        return jsonify({"error": "Not authenticated"}), 401
        
    data = request.json # Expecting { pinned_items: [...], pinned_dashboards: [...] }
    
    try:
        # Prepare data for Snowflake VARIANT column
        json_data = json.dumps(data)

        with get_db_conn_configured() as conn:
            with conn.cursor() as cur:
                # Use MERGE to Insert (if new) or Update (if exists)
                cur.execute(f"""
                    MERGE INTO {SNOWFLAKE_SCHEMA}.user_preferences AS target
                    USING (SELECT %s AS username, PARSE_JSON(%s) AS preferences_data) AS source
                    ON target.username = source.username
                    WHEN MATCHED THEN
                        UPDATE SET preferences_data = source.preferences_data, updated_at = CURRENT_TIMESTAMP
                    WHEN NOT MATCHED THEN
                        INSERT (username, preferences_data) VALUES (source.username, source.preferences_data)
                """, (username, json_data))
                conn.commit()
                
        return jsonify({"success": True, "message": "Preferences saved successfully"})
    except Exception as e:
        print(f"Error saving preferences: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/generate_db_profiling_report', methods=['POST'])
def generate_db_profiling_report():
    data = request.json
    conn_info = data.get('connectionInfo', {})
    db_type = conn_info.get('db_type')
    host = conn_info.get('host')
    port = conn_info.get('port')
    user = conn_info.get('user')
    password = conn_info.get('password')
    dbname = conn_info.get('dbname')
    schema = data.get('schema')
    table = data.get('table')
    
    rowCount = data.get('rowCount', '1000')
    is_full_table = (rowCount == 'full')
    row_limit = 1000

    if not is_full_table:
        try:
            row_limit = int(rowCount)
            if row_limit <= 0:
                row_limit = 1000
        except (ValueError, TypeError):
            row_limit = 1000
    
    if not all([conn_info, schema, table]):
         return jsonify({"success": False, "error": "Missing connection info, schema, or table."}), 400

    try:
        df = None
        conn_db = None
        query = ""
        
        # --- SQL INJECTION FIX: Validate identifiers ---
        if not re.match(r'^[A-Za-z0-9_"$]+$', schema) or \
           not re.match(r'^[A-Za-z0-9_"$]+$', table) or \
           not re.match(r'^[A-Za-z0-9_"$]+$', dbname):
           return jsonify({"error": "Invalid schema, table, or database name"}), 400
        
        if db_type == "MySQL":
            conn_db = mysql.connector.connect(
                host=host, port=int(port), user=user, password=password, database=dbname
            )
            query = f"SELECT * FROM `{schema}`.`{table}`"
            if not is_full_table:
                query += f" LIMIT {row_limit}"
            df = pd.read_sql(query, conn_db)
            
        elif db_type == "PostgreSQL":
            db_connection_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
            engine = create_engine(db_connection_str)
            query = f'SELECT * FROM "{schema}"."{table}"'
            if not is_full_table:
                query += f" LIMIT {row_limit}"
            df = pd.read_sql(query, engine)
            
        elif db_type == "Oracle":
            dsn = f"{host}:{port}/{dbname}" if port else f"{host}/{dbname}"
            conn_db = oracledb.connect(user=user, password=password, dsn=dsn)
            query = f'SELECT * FROM "{schema.upper()}"."{table.upper()}"'
            if not is_full_table:
                query += f" WHERE ROWNUM <= {row_limit}"
            df = pd.read_sql(query, conn_db)
            
        elif db_type == "SQL Server":
            conn_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={host};DATABASE={dbname};UID={user};PWD={password}'
            if port:
                conn_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={host},{port};DATABASE={dbname};UID={user};PWD={password}'
            conn_db = pyodbc.connect(conn_string)
            top_clause = "" if is_full_table else f"TOP {row_limit}"
            query = f'SELECT {top_clause} * FROM "{schema}"."{table}"'
            df = pd.read_sql(query, conn_db)
            
        elif db_type == "Snowflake":
            conn_db = snowflake.connector.connect(
                user=user, password=password, account=host, database=dbname
            )
            query = f'SELECT * FROM "{dbname}"."{schema}"."{table}"'
            if not is_full_table:
                query += f" LIMIT {row_limit}"
            df = pd.read_sql(query, conn_db)
        else:
            return jsonify({"success": False, "error": "Unsupported database type"}), 400

        profile_title = f"Data Profile for {schema}.{table}"
        if not is_full_table:
            profile_title += f" (First {row_limit} Rows)"
            
        profile = ProfileReport(df, title=profile_title, explorative=True)
        
        report_filename = f"report_{uuid.uuid4()}.html"
        save_path = os.path.join(REPORTS_DIR, report_filename)
        
        profile.to_file(save_path)
        
        return jsonify({"success": True, "report_filename": report_filename})

    except Exception as e:
        print(f"Error generating DB profiling report: {e}")
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn_db and db_type not in ["PostgreSQL"]:
            if hasattr(conn_db, 'close'):
                conn_db.close()


@app.route('/reports/<path:filename>')
def get_report(filename):
    return send_from_directory(REPORTS_DIR, filename)


if __name__ == '__main__':
    app.run(debug=True)

