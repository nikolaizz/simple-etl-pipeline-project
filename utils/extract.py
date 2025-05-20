import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36"
    )
}

def fetching_content(url):
    """Mengambil konten HTML dari URL https://fashion-studio.dicoding.dev/ """
    session = requests.Session()
    response = session.get(url, headers=HEADERS)
    
    try:
        response.raise_for_status()
        return response.content
    
    except requests.exceptions.RequestException as e:
        print(f"Terjadi kesalahan saat melakukan requests terhadap {url}")
        return None

def extract_fashion_data(collection):
    """Mengambil data berupa Title, Price, Rating, Color, Size, Gender"""
    try:
        current_time = datetime.now()
        timestamp = current_time.strftime('%Y-%m-%d %H:%M:%S.') + f'{int(current_time.microsecond / 1000):03d}'
        
        title = collection.find('h3', class_='product-title').text

        price_element = collection.find('span', class_='price')
        price = price_element.text if price_element else 'Price Unavailable'

        rating_tag = collection.find_all('p')[0]
        rating_text = rating_tag.text.replace('Rating:', '')
        rating = rating_text.replace("⭐", "").split("/")[0]
        
        color_tag = collection.find_all('p')[1]
        color = color_tag.text
        
        size_tag = collection.find_all('p')[2]
        size = size_tag.text.replace("Size:", "")
        
        gender_tag = collection.find_all('p')[3]
        gender = gender_tag.text.replace("Gender:", "")

        return {
            'Title': title,
            'Price': price,
            'Rating': rating,
            'Color': color,
            'Size': size,
            'Gender': gender,
            'Timestamp': timestamp
        }
    except Exception as e:
        print(f"Error saat mengekstrak data: {str(e)}")
        return None

def scrape_data(base_url, start_page=1, delay=1):
    """Fungsi utama untuk mengambil keseluruhan data, mulai dari requests hingga menyimpannya dalam variabel data."""
    data = []
    page_number = start_page
    max_pages = 50

    while page_number <= max_pages:
        url = f"{base_url}" if page_number == 1 else f"{base_url}page{page_number}"
        print(f'Scraping halaman: {url}')

        content = fetching_content(url)
        if not content:
            print(f"Gagal mengambil konten untuk halaman {page_number}")
            break

        soup = BeautifulSoup(content, 'html.parser')
        articles_element = soup.find_all('div', class_='collection-card')

        if not articles_element:
            print(f"Tidak ada produk yang ditemukan pada halaman {page_number}")
            break

        for collection in articles_element:
            try:
                fashion = extract_fashion_data(collection)
                if fashion:
                    data.append(fashion)
            except Exception as e:
                print(f"Error saat mengekstrak data produk: {str(e)}")
                continue

        print(f"Selesai scrapping produk dari halaman {page_number}")
        page_number += 1
        time.sleep(delay)

    if not data:
        print("Peringatan: Tidak ada data yang berhasil di-scrape")
        
    return data
