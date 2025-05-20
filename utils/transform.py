import pandas as pd
import re
from typing import Optional

def clean_price(price: str) -> Optional[float]:
    """
    Convert price from USD to IDR (Rupiah) with exchange rate of Rp16,000.
    
    Args:
        price: Price value as string, e.g. '$100.00'
        
    Returns:
        Converted price in IDR as float or None if invalid
    """
    try:
        if pd.isna(price) or price == 'Price Unavailable':
            return None
            
        if isinstance(price, str) and '$' in price:
            usd_price = float(price.replace('$', '').strip())
            idr_price = usd_price * 16000
            return idr_price
        else:
            return None
    except Exception:
        return None

def clean_rating(rating: str) -> Optional[float]:
    """
    Convert rating to float format.
    
    Args:
        rating: Rating value as string, e.g. '4.8' or '4.8 / 5'
        
    Returns:
        Rating as float or None if invalid
    """
    try:
        if pd.isna(rating) or rating == 'Invalid Rating' or 'Invalid Rating' in rating or 'Not Rated' in rating or rating == 'Price Unavailable':
            return None
            
        rating_str = rating.strip()
        
        if '/' in rating_str:
            rating_str = rating_str.split('/')[0].strip()
        
        return float(rating_str)
    except Exception:
        return None

def clean_colors(colors: str) -> Optional[int]:
    """
    Extract number of colors from the string.
    
    Args:
        colors: Colors as string, e.g. '3 Colors'
        
    Returns:
        Number of colors as int or None if invalid
    """
    try:
        if pd.isna(colors):
            return None
            
        match = re.search(r'(\d+)\s*Colors?', colors)
        if match:
            return int(match.group(1))
        else:
            return None
    except Exception:
        return None

def clean_size(size: str) -> Optional[str]:
    """
    Clean size string by removing 'Size: ' prefix.
    
    Args:
        size: Size as string, e.g. 'Size: M' or 'M'
        
    Returns:
        Cleaned size as string or None if invalid
    """
    try:
        if pd.isna(size):
            return None
            
        size_str = size.strip()
        
        if size_str.startswith('Size:'):
            size_str = size_str.replace('Size:', '').strip()
            
        return size_str
    except Exception:
        return None

def clean_gender(gender: str) -> Optional[str]:
    """
    Clean gender string by removing 'Gender: ' prefix.
    
    Args:
        gender: Gender as string, e.g. 'Gender: Men' or 'Men'
        
    Returns:
        Cleaned gender as string or None if invalid
    """
    try:
        if pd.isna(gender):
            return None
            
        gender_str = gender.strip()
        
        if gender_str.startswith('Gender:'):
            gender_str = gender_str.replace('Gender:', '').strip()
            
        return gender_str
    except Exception:
        return None

def transform_data(input_file: str, output_file: str) -> bool:
    """
    Args:
        input_file: Path to input CSV file
        output_file: Path to output CSV file
        
    Returns:
        True if transformation was successful, False otherwise
    """
    try:
        print(f"Membaca data dari {input_file}")
        df = pd.read_csv(input_file)
        
        print("Mengubah nilai harga menjadi Rupiah")
        df['Price'] = df['Price'].apply(clean_price)
        
        print("Mengubah nilai rating menjadi format desimal")
        df['Rating'] = df['Rating'].apply(clean_rating)
        
        print("Mengubah jumlah warna menjadi angka")
        df['Color'] = df['Color'].apply(clean_colors)
        
        print("Mengubah tipe kolom Color menjadi integer")
        df['Color'] = df['Color'].astype(pd.Int64Dtype())
        
        print("Membersihkan nilai ukuran")
        df['Size'] = df['Size'].apply(clean_size)
        
        print("Membersihkan nilai gender")
        df['Gender'] = df['Gender'].apply(clean_gender)
        
        print("Menghapus baris dengan judul tidak valid")
        df = df[~df['Title'].isin(['Unknown Product', '']) & ~df['Title'].isna()]
        
        print("Menghapus baris dengan nilai kosong di kolom penting")
        required_columns = ['Title', 'Price', 'Rating', 'Color', 'Size', 'Gender']
        df = df.dropna(subset=required_columns)
        
        print("Menghapus data produk yang duplikat")
        df = df.drop_duplicates()
        
        if 'Timestamp' in df.columns:
            print("Mengubah tipe kolom Timestamp menjadi datetime")
            df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce')
            df = df.sort_values('Timestamp', ascending=False)
        
        print(f"Menyimpan data yang telah ditransformasi ke {output_file}")
        df.to_csv(output_file, index=False)
        
        row_count = len(df)
        print(f"Transformasi data selesai. File output memiliki {row_count} baris.")
        
        return True

    except FileNotFoundError as e:
        print(f"File input tidak ditemukan: {str(e)}")
        return False
    except Exception as e:
        print(f"Terjadi kesalahan selama proses transformasi data: {str(e)}")
        return False

if __name__ == "__main__":
    transform_data("scrapped_data.csv", "transformed.csv")