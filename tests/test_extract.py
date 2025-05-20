import pytest
import sys
import os
from unittest.mock import patch, MagicMock, call
from bs4 import BeautifulSoup
from datetime import datetime
import requests

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.extract import fetching_content, extract_fashion_data, scrape_data

class TestFetchingContent:
    @patch('utils.extract.requests.Session')
    def test_fetching_content_success(self, mock_session):
        mock_response = MagicMock()
        mock_response.content = b'<html><body>Test content</body></html>'
        mock_response.raise_for_status.return_value = None
        
        mock_session_instance = MagicMock()
        mock_session_instance.get.return_value = mock_response
        mock_session.return_value = mock_session_instance
        
        url = 'https://fashion-studio.dicoding.dev/'
        result = fetching_content(url)
        
        assert result == b'<html><body>Test content</body></html>'
        mock_session_instance.get.assert_called_once()
        mock_response.raise_for_status.assert_called_once()
    
    @patch('utils.extract.requests.Session')
    def test_fetching_content_failure(self, mock_session):
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = Exception("Terjadi kesalahan pada permintaan")
        
        mock_session_instance = MagicMock()
        mock_session_instance.get.return_value = mock_response
        mock_session.return_value = mock_session_instance
        
        url = 'https://fashion-studio.dicoding.dev/'
        with pytest.raises(Exception):
            fetching_content(url)

    @patch('utils.extract.requests.Session')
    @patch('builtins.print')
    def test_fetching_content_request_exception(self, mock_print, mock_session):
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.exceptions.RequestException("Connection Error")
        
        mock_session_instance = MagicMock()
        mock_session_instance.get.return_value = mock_response
        mock_session.return_value = mock_session_instance
        
        url = 'https://fashion-studio.dicoding.dev/'
        result = fetching_content(url)
        
        assert result is None
        mock_print.assert_called_once_with(f"Terjadi kesalahan saat melakukan requests terhadap {url}")

    @pytest.mark.skip(reason="Flawed test; skipping get_request_exception in class")
    @patch('utils.extract.requests.Session')
    @patch('builtins.print')
    def test_fetching_content_get_request_exception(self, mock_print, mock_session):
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.exceptions.RequestException("Network failure")
        
        mock_session_instance = MagicMock()
        mock_session_instance.get.return_value = mock_response
        mock_session.return_value = mock_session_instance
        
        url = 'https://fashion-studio.dicoding.dev/'
        with pytest.raises(requests.exceptions.RequestException):
            fetching_content(url)

class TestExtractFashionData:
    def test_extract_fashion_data_success(self):
        # Buat contoh HTML
        html = '''
        <div class="collection-card">
            <h3 class="product-title">Test Product</h3>
            <span class="price">$129.99</span>
            <p>Rating:4.5/5⭐</p>
            <p>2 Colors</p>
            <p>Size: M</p>
            <p>Gender: Women</p>
        </div>
        '''
        
        # Parse HTML
        soup = BeautifulSoup(html, 'html.parser')
        collection = soup.find('div', class_='collection-card')
        
        # Jalankan fungsi yang akan diuji
        with patch('utils.extract.datetime') as mock_datetime:
            mock_now = MagicMock()
            mock_now.strftime.return_value = '2023-04-01 12:00:00.'
            mock_now.microsecond = 500000
            mock_datetime.now.return_value = mock_now
            
            result = extract_fashion_data(collection)
        
        # Verifikasi hasil
        assert result['Title'] == 'Test Product'
        assert result['Price'] == '$129.99'
        assert result['Rating'] == '4.5'
        assert result['Color'] == '2 Colors'
        assert result['Size'] == ' M'
        assert result['Gender'] == ' Women'
        assert '2023-04-01 12:00:00.500' in result['Timestamp']
    
    def test_extract_fashion_data_error(self):
        # Buat HTML yang tidak lengkap untuk memicu error
        html = '''
        <div class="collection-card">
            <h3 class="product-title">Test Product</h3>
            <!-- Elemen lainnya tidak ada -->
        </div>
        '''
        
        # Parse HTML
        soup = BeautifulSoup(html, 'html.parser')
        collection = soup.find('div', class_='collection-card')
        
        # Jalankan fungsi yang akan diuji
        result = extract_fashion_data(collection)
        
        # Verifikasi bahwa fungsi mengembalikan None saat terjadi error
        assert result is None

    def test_extract_fashion_data_missing_price(self):
        # HTML tanpa element price
        html = '''
        <div class="collection-card">
            <h3 class="product-title">Test Product</h3>
            <p>Rating:4.5/5⭐</p>
            <p>2 Colors</p>
            <p>Size: M</p>
            <p>Gender: Women</p>
        </div>
        '''
        
        soup = BeautifulSoup(html, 'html.parser')
        collection = soup.find('div', class_='collection-card')
        
        with patch('utils.extract.datetime') as mock_datetime:
            mock_now = MagicMock()
            mock_now.strftime.return_value = '2023-04-01 12:00:00.'
            mock_now.microsecond = 500000
            mock_datetime.now.return_value = mock_now
            
            result = extract_fashion_data(collection)
        
        assert result['Title'] == 'Test Product'
        assert result['Price'] == 'Price Unavailable'
        assert result['Rating'] == '4.5'

    @patch('builtins.print')
    def test_extract_fashion_data_exception(self, mock_print):
        # Simulasi exception saat ekstraksi
        collection = MagicMock()
        collection.find.side_effect = Exception("Test exception")
        
        result = extract_fashion_data(collection)
        
        assert result is None
        mock_print.assert_called_once()

class TestScrapeData:
    @patch('utils.extract.fetching_content')
    @patch('utils.extract.extract_fashion_data')
    @patch('utils.extract.time.sleep')
    def test_scrape_data_success(self, mock_sleep, mock_extract, mock_fetch):
        # Konfigurasi mock: HTML berisi dua item produk
        html_content = '''
        <div class="collection-card">Item 1</div>
        <div class="collection-card">Item 2</div>
        '''
        mock_fetch.return_value = html_content
        
        mock_extract.side_effect = [
            {'Title': 'Item 1', 'Price': '$100', 'Rating': '4.5', 'Color': '3 Colors', 'Size': 'M', 'Gender': 'Men', 'Timestamp': '2023-04-01 12:00:00.000'},
            {'Title': 'Item 2', 'Price': '$150', 'Rating': '4.8', 'Color': '2 Colors', 'Size': 'L', 'Gender': 'Women', 'Timestamp': '2023-04-01 12:01:00.000'}
        ]
        
        # Jalankan fungsi yang akan diuji
        base_url = 'https://fashion-studio.dicoding.dev/'
        result = scrape_data(base_url, start_page=1, delay=0)
        
        # Verifikasi hasil
        assert len(result) == 2
        assert result[0]['Title'] == 'Item 1'
        assert result[1]['Title'] == 'Item 2'
        
    @patch('utils.extract.fetching_content')
    def test_scrape_data_no_content(self, mock_fetch):
        # Konfigurasi mock: tidak ada konten yang diambil
        mock_fetch.return_value = None
        
        # Jalankan fungsi yang akan diuji
        base_url = 'https://fashion-studio.dicoding.dev/'
        result = scrape_data(base_url, start_page=1, delay=0)
        
        # Verifikasi hasil
        assert result == []
        
    @patch('utils.extract.fetching_content')
    def test_scrape_data_no_products(self, mock_fetch):
        # Konfigurasi mock: HTML tidak mengandung produk
        mock_fetch.return_value = '<html><body>Tidak ada produk</body></html>'
        
        # Jalankan fungsi yang akan diuji
        base_url = 'https://fashion-studio.dicoding.dev/'
        result = scrape_data(base_url, start_page=1, delay=0)
        
        # Verifikasi hasil
        assert result == []

    @patch('utils.extract.fetching_content')
    @patch('utils.extract.extract_fashion_data')
    @patch('utils.extract.time.sleep')
    @patch('builtins.print')
    def test_scrape_data_multiple_pages(self, mock_print, mock_sleep, mock_extract, mock_fetch):
        # Test scraping banyak halaman
        html_content_page1 = '''
        <div class="collection-card">Item 1</div>
        <div class="collection-card">Item 2</div>
        '''
        html_content_page2 = '''
        <div class="collection-card">Item 3</div>
        <div class="collection-card">Item 4</div>
        '''
        
        mock_fetch.side_effect = [html_content_page1, html_content_page2, None]
        
        mock_extract.side_effect = [
            {'Title': 'Item 1', 'Price': '$100'},
            {'Title': 'Item 2', 'Price': '$150'},
            {'Title': 'Item 3', 'Price': '$200'},
            {'Title': 'Item 4', 'Price': '$250'},
        ]
        
        base_url = 'https://fashion-studio.dicoding.dev/'
        result = scrape_data(base_url, start_page=1, delay=0)
        
        assert len(result) == 4
        assert mock_fetch.call_count == 3 
        assert mock_extract.call_count == 4 

    @patch('utils.extract.fetching_content')
    @patch('utils.extract.extract_fashion_data')
    @patch('utils.extract.time.sleep')
    @patch('builtins.print')
    def test_scrape_data_extraction_exception(self, mock_print, mock_sleep, mock_extract, mock_fetch):
        html_content = '''
        <div class="collection-card">Item 1</div>
        <div class="collection-card">Item 2</div>
        '''
        mock_fetch.return_value = html_content
        
        mock_extract.side_effect = [
            {'Title': 'Item 1', 'Price': '$100'},
            Exception("Test exception")
        ]
        
        base_url = 'https://fashion-studio.dicoding.dev/'
        result = scrape_data(base_url, start_page=1, delay=0)
        
        assert len(result) == 1
        assert result[0]['Title'] == 'Item 1'
        
    @patch('utils.extract.fetching_content')
    @patch('utils.extract.extract_fashion_data')
    @patch('utils.extract.time.sleep')
    @patch('builtins.print')
    def test_scrape_data_empty_result(self, mock_print, mock_sleep, mock_extract, mock_fetch):
        html_content = '''
        <div class="collection-card">Item 1</div>
        <div class="collection-card">Item 2</div>
        '''
        mock_fetch.return_value = html_content
        
        mock_extract.return_value = None
        
        base_url = 'https://fashion-studio.dicoding.dev/'
        result = scrape_data(base_url, start_page=1, delay=0)
        
        assert result == []
        assert mock_print.call_count >= 1

    @pytest.mark.skip(reason="Flawed test; skipping empty_bytes in class")
    @patch('utils.extract.fetching_content')
    @patch('utils.extract.extract_fashion_data')
    @patch('utils.extract.time.sleep')
    @patch('builtins.print')
    def test_scrape_data_empty_bytes(self, mock_print, mock_sleep, mock_extract, mock_fetch):
        # Simulasi fetching_content mengembalikan empty bytes
        mock_fetch.return_value = b""
        # Hilangkan delay
        mock_sleep.return_value = None
        result = scrape_data("https://fashion-studio.dicoding.dev/", start_page=1, delay=0)
        assert result == []
        mock_print.assert_called_once_with("Gagal mengambil konten untuk halaman 1")
        mock_print.assert_called_once_with("Peringatan: Tidak ada data yang berhasil di-scrape")

    @pytest.mark.skip(reason="Flawed test; skipping start_page_exceeds_max in class")
    @patch('utils.extract.fetching_content')
    @patch('utils.extract.extract_fashion_data')
    @patch('utils.extract.time.sleep')
    @patch('builtins.print')
    def test_scrape_data_start_page_exceeds_max(self, mock_print, mock_sleep, mock_extract, mock_fetch):
        # Pastikan fetching_content tidak dipanggil
        mock_fetch.side_effect = (_ for _ in ()).throw(AssertionError("fetching_content should not be called"))
        result = scrape_data("https://fashion-studio.dicoding.dev/", start_page=100, delay=0)
        assert result == []
        mock_print.assert_called_once_with("Peringatan: Tidak ada data yang berhasil di-scrape")

# Tambahkan module-level tests setelah kelas-kelas test dan sebelum entrypoint

def test_fetching_content_get_request_exception_module(monkeypatch):
    from utils.extract import fetching_content
    # Simulasi error pada session.get
    import requests
    class DummySession:
        def get(self, url, headers=None):
            raise requests.exceptions.RequestException("Network failure")
    monkeypatch.setattr("utils.extract.requests.Session", lambda: DummySession())
    url = "https://fashion-studio.dicoding.dev/"
    with pytest.raises(requests.exceptions.RequestException):
        fetching_content(url)

# Tambahkan test untuk empty bytes content pada scrape_data

def test_scrape_data_empty_bytes_module(monkeypatch, capsys):
    from utils.extract import scrape_data
    # Simulasi fetching_content mengembalikan empty bytes
    monkeypatch.setattr("utils.extract.fetching_content", lambda url: b"")
    # Hilangkan delay
    monkeypatch.setattr("utils.extract.time.sleep", lambda x: None)
    result = scrape_data("https://fashion-studio.dicoding.dev/", start_page=1, delay=0)
    assert result == []
    captured = capsys.readouterr().out
    assert "Gagal mengambil konten untuk halaman 1" in captured
    assert "Peringatan: Tidak ada data yang berhasil di-scrape" in captured

# Tambahkan test untuk start_page melebihi max_pages

def test_scrape_data_start_page_exceeds_max_module(monkeypatch, capsys):
    from utils.extract import scrape_data
    # Pastikan fetching_content tidak dipanggil
    monkeypatch.setattr("utils.extract.fetching_content", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("fetching_content should not be called")))
    # Hilangkan delay
    monkeypatch.setattr("utils.extract.time.sleep", lambda x: None)
    result = scrape_data("https://fashion-studio.dicoding.dev/", start_page=100, delay=0)
    assert result == []
    captured = capsys.readouterr().out
    assert "Peringatan: Tidak ada data yang berhasil di-scrape" in captured

# Tambahkan entrypoint agar test dapat dijalankan langsung
if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main([__file__]))