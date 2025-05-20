from utils.extract import scrape_data
from utils.transform import transform_data
from utils.load import load_to_csv, load_to_postgresql, load_to_google_sheets
import pandas as pd
import os
from datetime import datetime

def main():
    BASE_URL = 'https://fashion-studio.dicoding.dev/'
    scrapped_data_file = 'scrapped_data.csv'
    transformed_file = 'transformed.csv'
    final_products_file = 'products.csv'
    
    try:
        print("="*50)
        print("PROSES SCRAPPING DIMULAI")
        print("="*50)
        
        all_fashion_data = scrape_data(BASE_URL)
        
        if not all_fashion_data:
            print("Tidak ada data yang berhasil discraping")
            return
        
        df = pd.DataFrame(all_fashion_data)
        df.to_csv(scrapped_data_file, index=False)
        print(f"\nData hasil scrapped berhasil di simpan: {scrapped_data_file}")
        
        print("\n" + "="*50)
        print("PROSES TRANSFORMASI DATA DIMULAI")
        print("="*50)
        
        success = transform_data(scrapped_data_file, transformed_file)
        
        if success:
            transformed_df = pd.read_csv(transformed_file)
            
            print("\nSampel data yang sudah ditransformasi:")
            print(transformed_df.head())
            
            print("\n" + "="*50)
            print("PROSES LOADING DATA DIMULAI")
            print("="*50)
            
            # 1. Load ke CSV
            csv_success = load_to_csv(transformed_file, final_products_file)
            if csv_success:
                print("\nData berhasil dimuat ke CSV")
            else:
                print("\nGagal memuat data ke CSV")
            
            # 2. Load ke PostgreSQL
            pg_success = load_to_postgresql(transformed_file, "fashion_products")
            if pg_success:
                print("\nData berhasil dimuat ke PostgreSQL")
            else:
                print("\nGagal memuat data ke PostgreSQL")
            
            # 3. Load ke Google Sheets
            spreadsheet_id = "1qkzwYBMQDRx0AFTONigI_vDn2ZUdWgZYl_CoBGktSxg"
            sheet_name = "Sheet1"
            
            gs_success = load_to_google_sheets(transformed_file, spreadsheet_id, sheet_name)
            if gs_success:
               print("\nData berhasil dimuat ke Google Sheets")
            else:
               print("\nGagal memuat data ke Google Sheets")
            
        else:
            print("Gagal mentransformasikan data")
    
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == '__main__':
    main()