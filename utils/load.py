import pandas as pd
import os
from sqlalchemy import create_engine, Column, Integer, String, Float, MetaData, Table
from google.oauth2 import service_account
from googleapiclient.discovery import build

def load_to_csv(input_file: str, output_file: str) -> bool:
    """
    Load data to a CSV file.
    
    Args:
        input_file: Path to input CSV file
        output_file: Path to output CSV file
        
    Returns:
        True if loading was successful, False otherwise
    """
    try:
        print(f"Membaca data dari {input_file}")
        df = pd.read_csv(input_file)
        
        print(f"Menyimpan data ke {output_file}")
        df.to_csv(output_file, index=False)
        
        row_count = len(df)
        print(f"Data berhasil dimuat. File output memiliki {row_count} baris.")
        
        return True
        
    except FileNotFoundError as e:
        print(f"File input tidak ditemukan: {str(e)}")
        return False
    except Exception as e:
        print(f"Terjadi kesalahan selama proses load data ke CSV: {str(e)}")
        return False

def load_to_postgresql(input_file: str, table_name: str) -> bool:
    """
    Load data to PostgreSQL database.
    
    Args:
        input_file: Path to input CSV file
        table_name: Name of the table in PostgreSQL
        
    Returns:
        True if loading was successful, False otherwise
    """
    try:
        print(f"Membaca data dari {input_file}")
        df = pd.read_csv(input_file)
        
        # Konfigurasi database
        username = "developer" 
        password = "superpassword"
        host = "localhost"
        port = "5432"     
        database = "fashionsdb"
        
        # Membuat connection string
        conn_string = f"postgresql://{username}:{password}@{host}:{port}/{database}"
        
        # Membuat engine SQLAlchemy
        engine = create_engine(conn_string)
        
        print(f"Menyimpan data ke tabel {table_name} di PostgreSQL")
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        
        row_count = len(df)
        print(f"Data berhasil dimuat ke PostgreSQL. {row_count} baris dimasukkan ke dalam tabel {table_name}.")
        
        return True
        
    except FileNotFoundError as e:
        print(f"File input tidak ditemukan: {str(e)}")
        return False
    except Exception as e:
        print(f"Terjadi kesalahan selama proses load data ke PostgreSQL: {str(e)}")
        return False

def load_to_google_sheets(input_file: str, spreadsheet_id: str, sheet_name: str) -> bool:
    """
    Load data to Google Sheets.
    
    Args:
        input_file: Path to input CSV file
        spreadsheet_id: ID of the Google Spreadsheet
        sheet_name: Name of the sheet in the Google Spreadsheet
        
    Returns:
        True if loading was successful, False otherwise
    """
    try:
        print(f"Membaca data dari {input_file}")
        df = pd.read_csv(input_file)
        
        # Konfigurasi Google Sheets API
        credentials_file = "google-sheets-api.json" 
        
        # Scopes yang dibutuhkan untuk Google Sheets
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
        
        # Autentikasi Google Sheets API
        credentials = service_account.Credentials.from_service_account_file(
            credentials_file, scopes=SCOPES)
        
        # Membangun service Google Sheets
        service = build('sheets', 'v4', credentials=credentials)
        
        # Mempersiapkan data untuk dimasukkan ke Google Sheets
        values = [df.columns.tolist()] + df.values.tolist()
        
        # Memasukkan data ke Google Sheets
        body = {
            'values': values
        }
        
        print(f"Menyimpan data ke Google Sheets dengan ID: {spreadsheet_id}, sheet: {sheet_name}")
        result = service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range='Sheet1!A1:G868',
            valueInputOption="RAW",
            body=body
        ).execute()
        
        row_count = len(df)
        print(f"Data berhasil dimuat ke Google Sheets. {row_count} baris dimasukkan ke dalam sheet {sheet_name}.")
        
        return True
        
    except FileNotFoundError as e:
        print(f"File input tidak ditemukan: {str(e)}")
        return False
    except Exception as e:
        print(f"Terjadi kesalahan selama proses load data ke Google Sheets: {str(e)}")
        return False

if __name__ == "__main__":
    input_file = "transformed.csv"
    output_csv = "products.csv"
    
    # Load to CSV
    load_to_csv(input_file, output_csv)
    
    # Load to PostgreSQL
    load_to_postgresql(input_file, "fashion_products")
    
    # Load to Google Sheets
    spreadsheet_id = "1qkzwYBMQDRx0AFTONigI_vDn2ZUdWgZYl_CoBGktSxg"
    load_to_google_sheets(input_file, spreadsheet_id, "Fashion")
