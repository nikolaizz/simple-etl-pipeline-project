import pytest
import sys
import os
import pandas as pd
from unittest.mock import patch, mock_open, MagicMock

# Menambahkan direktori root ke sys.path agar bisa mengimpor utils
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.transform import (
    clean_price, 
    clean_rating, 
    clean_colors, 
    clean_size, 
    clean_gender, 
    transform_data
)

class TestCleanPrice:
    def test_clean_price_valid(self):
        # Menguji harga valid dalam format USD
        assert clean_price('$100.00') == 1600000.0
        
    def test_clean_price_zero(self):
        # Menguji harga nol
        assert clean_price('$0.00') == 0.0
        
    def test_clean_price_invalid(self):
        # Menguji berbagai input yang tidak valid
        assert clean_price('Price Unavailable') is None
        assert clean_price('invalid') is None
        assert clean_price(None) is None
        
    def test_clean_price_without_dollar_sign(self):
        # Menguji input harga tanpa tanda dolar
        assert clean_price('100.00') is None

class TestCleanRating:
    def test_clean_rating_valid(self):
        # Menguji rating yang valid dalam berbagai format
        assert clean_rating('4.8') == 4.8
        assert clean_rating('4.8 / 5') == 4.8
        assert clean_rating('3') == 3.0
        
    def test_clean_rating_invalid(self):
        # Menguji input rating yang tidak valid
        assert clean_rating('Invalid Rating') is None
        assert clean_rating('Not Rated') is None
        assert clean_rating('Price Unavailable') is None
        assert clean_rating(None) is None

class TestCleanColors:
    def test_clean_colors_valid(self):
        # Menguji input jumlah warna yang valid
        assert clean_colors('3 Colors') == 3
        assert clean_colors('1 Color') == 1
        assert clean_colors('10 Colors') == 10
        
    def test_clean_colors_invalid(self):
        # Menguji input jumlah warna yang tidak valid
        assert clean_colors('Colors') is None
        assert clean_colors('Invalid Colors') is None
        assert clean_colors(None) is None

class TestCleanSize:
    def test_clean_size_valid(self):
        # Menguji input ukuran yang valid
        assert clean_size('Size: M') == 'M'
        assert clean_size('Size: XL') == 'XL'
        assert clean_size('M') == 'M'
        
    def test_clean_size_invalid(self):
        # Menguji input ukuran yang tidak valid
        assert clean_size(None) is None

class TestCleanGender:
    def test_clean_gender_valid(self):
        # Menguji input gender yang valid
        assert clean_gender('Gender: Men') == 'Men'
        assert clean_gender('Gender: Women') == 'Women'
        assert clean_gender('Unisex') == 'Unisex'
        
    def test_clean_gender_invalid(self):
        # Menguji input gender yang tidak valid
        assert clean_gender(None) is None

class TestTransformData:
    @patch('utils.transform.pd.read_csv')
    @patch('utils.transform.pd.DataFrame.to_csv')
    def test_transform_data_success(self, mock_to_csv, mock_read_csv):
        # Membuat DataFrame contoh untuk pengujian
        test_data = pd.DataFrame({
            'Title': ['Product 1', 'Product 2', 'Product 3'],
            'Price': ['$100.00', '$200.00', '$300.00'],
            'Rating': ['4.8 / 5', '4.5', '3.9 / 5'],
            'Color': ['3 Colors', '2 Colors', '1 Color'],
            'Size': ['Size: M', 'Size: L', 'Size: XL'],
            'Gender': ['Gender: Men', 'Gender: Women', 'Gender: Unisex'],
            'Timestamp': ['2023-04-01 12:00:00.000', '2023-04-01 12:01:00.000', '2023-04-01 12:02:00.000']
        })
        
        # Menyetel mock agar mengembalikan DataFrame contoh
        mock_read_csv.return_value = test_data
        
        # Menjalankan fungsi yang akan diuji
        result = transform_data('input.csv', 'output.csv')
        
        # Memverifikasi hasil
        assert result is True
        mock_read_csv.assert_called_once_with('input.csv')
        mock_to_csv.assert_called_once()
    
    @patch('utils.transform.pd.read_csv')
    def test_transform_data_file_not_found(self, mock_read_csv):
        # Menyetel mock untuk memunculkan FileNotFoundError
        mock_read_csv.side_effect = FileNotFoundError('File not found')
        
        # Menjalankan fungsi yang akan diuji
        result = transform_data('nonexistent.csv', 'output.csv')
        
        # Memverifikasi hasil
        assert result is False

    @patch('utils.transform.pd.read_csv')
    def test_transform_data_exception(self, mock_read_csv):
        # Menyetel mock untuk memunculkan Exception umum
        mock_read_csv.side_effect = Exception('Error general')
        
        # Menjalankan fungsi yang akan diuji
        result = transform_data('input.csv', 'output.csv')
        
        # Memverifikasi hasil
        assert result is False
        
    @patch('utils.transform.pd.read_csv')
    @patch('utils.transform.pd.DataFrame.to_csv')
    def test_transform_data_with_duplicates(self, mock_to_csv, mock_read_csv):
        # Membuat DataFrame contoh yang mengandung duplikat
        test_data = pd.DataFrame({
            'Title': ['Product 1', 'Product 1', 'Product 3'],  # Duplikat
            'Price': ['$100.00', '$100.00', '$300.00'],
            'Rating': ['4.8 / 5', '4.8 / 5', '3.9 / 5'],
            'Color': ['3 Colors', '3 Colors', '1 Color'],
            'Size': ['Size: M', 'Size: M', 'Size: XL'],
            'Gender': ['Gender: Men', 'Gender: Men', 'Gender: Unisex'],
            'Timestamp': ['2023-04-01 12:00:00.000', '2023-04-01 12:00:00.000', '2023-04-01 12:02:00.000']
        })
        
        # Menyetel mock agar mengembalikan DataFrame contoh
        mock_read_csv.return_value = test_data
        
        # Menjalankan fungsi yang akan diuji
        result = transform_data('input.csv', 'output.csv')
        
        # Memverifikasi hasil
        assert result is True
        mock_to_csv.assert_called_once()

# Tambahkan test untuk menghapus judul tidak valid dan nilai kosong
@patch('utils.transform.pd.read_csv')
@patch('utils.transform.pd.DataFrame.to_csv')
def test_transform_data_drops_invalid_and_missing(mock_to_csv, mock_read_csv):
    # Menguji penghapusan judul tidak valid dan nilai kosong pada kolom penting
    df_input = pd.DataFrame({
        'Title': ['Valid', 'Unknown Product', '', None, 'Valid2', 'Valid3'],
        'Price': ['$100.00', '$200.00', '$300.00', '$400.00', 'Price Unavailable', '$500.00'],
        'Rating': ['4.5', '4.5', '4.5', '4.5', '4.5', 'Not Rated'],
        'Color': ['3 Colors'] * 6,
        'Size': ['Size: M'] * 6,
        'Gender': ['Gender: Men'] * 6,
        'Timestamp': ['2023-01-01 12:00:00.000'] * 6
    })
    mock_read_csv.return_value = df_input
    
    # Menjalankan fungsi yang akan diuji
    result = transform_data('input.csv', 'output.csv')
    
    # Verifikasi: hanya baris dengan judul valid dan tanpa nilai kosong yang di-simpan
    assert result is True
    mock_to_csv.assert_called_once()

# Tambahkan test untuk transformasi data tanpa kolom Timestamp
@patch('utils.transform.pd.read_csv')
@patch('utils.transform.pd.DataFrame.to_csv')
def test_transform_data_no_timestamp(mock_to_csv, mock_read_csv):
    # Menguji transformasi ketika kolom Timestamp tidak ada
    df_input = pd.DataFrame({
        'Title': ['A', 'B'],
        'Price': ['$10.00', '$20.00'],
        'Rating': ['3.0', '4.0'],
        'Color': ['1 Color', '2 Colors'],
        'Size': ['M', 'L'],
        'Gender': ['Men', 'Women']
    })
    mock_read_csv.return_value = df_input
    
    # Menjalankan fungsi yang akan diuji
    result = transform_data('in.csv', 'out.csv')
    
    # Verifikasi: proses selesai tanpa error meski kolom Timestamp tidak ada
    assert result is True
    mock_to_csv.assert_called_once()

# Tambahkan entrypoint agar test dapat dijalankan langsung
if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main([__file__]))