import pytest
import sys
import os
import pandas as pd
from unittest.mock import patch, MagicMock
from sqlalchemy.engine import Engine

# Menambahkan direktori root ke sys.path agar bisa mengimpor utils
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.load import load_to_csv, load_to_postgresql, load_to_google_sheets

class TestLoadToCsv:
    @patch('utils.load.pd.read_csv')
    @patch('utils.load.pd.DataFrame.to_csv')
    def test_load_to_csv_success(self, mock_to_csv, mock_read_csv):
        # Membuat DataFrame uji
        test_data = pd.DataFrame({
            'Title': ['Product 1', 'Product 2', 'Product 3'],
            'Price': [1600000.0, 3200000.0, 800000.0],
            'Rating': [4.8, 4.5, 3.9],
            'Color': [3, 2, 1],
            'Size': ['M', 'L', 'XL'],
            'Gender': ['Men', 'Women', 'Unisex'],
            'Timestamp': ['2023-04-01 12:00:00.000', '2023-04-01 12:01:00.000', '2023-04-01 12:02:00.000']
        })

        # Mengatur mock untuk mengembalikan DataFrame uji
        mock_read_csv.return_value = test_data

        # Menjalankan fungsi yang diuji
        result = load_to_csv('input.csv', 'output.csv')

        # Memverifikasi hasil
        assert result is True
        mock_read_csv.assert_called_once_with('input.csv')
        mock_to_csv.assert_called_once_with('output.csv', index=False)

    @patch('utils.load.pd.read_csv')
    def test_load_to_csv_file_not_found(self, mock_read_csv):
        # Mengatur mock untuk menimbulkan FileNotFoundError
        mock_read_csv.side_effect = FileNotFoundError('File not found')

        # Menjalankan fungsi yang diuji
        result = load_to_csv('nonexistent.csv', 'output.csv')

        # Memverifikasi hasil
        assert result is False

    @patch('utils.load.pd.read_csv')
    def test_load_to_csv_exception(self, mock_read_csv):
        # Mengatur mock untuk menimbulkan Exception umum
        mock_read_csv.side_effect = Exception('Error general')

        # Menjalankan fungsi yang diuji
        result = load_to_csv('input.csv', 'output.csv')

        # Memverifikasi hasil
        assert result is False

    @patch('utils.load.pd.read_csv')
    @patch('utils.load.pd.DataFrame.to_csv')
    def test_load_to_csv_to_csv_exception(self, mock_to_csv, mock_read_csv):
        # Menguji penanganan exception pada df.to_csv
        test_data = pd.DataFrame({'A': [1, 2, 3]})
        mock_read_csv.return_value = test_data
        mock_to_csv.side_effect = Exception('to_csv failure')
        result = load_to_csv('input.csv', 'output.csv')
        assert result is False

class TestLoadToPostgresql:
    @patch('utils.load.pd.read_csv')
    @patch('utils.load.create_engine')
    @patch('utils.load.pd.DataFrame.to_sql')
    def test_load_to_postgresql_success(self, mock_to_sql, mock_create_engine, mock_read_csv):
        # Membuat DataFrame uji
        test_data = pd.DataFrame({
            'Title': ['Product 1', 'Product 2', 'Product 3'],
            'Price': [1600000.0, 3200000.0, 800000.0],
            'Rating': [4.8, 4.5, 3.9],
            'Color': [3, 2, 1],
            'Size': ['M', 'L', 'XL'],
            'Gender': ['Men', 'Women', 'Unisex'],
            'Timestamp': ['2023-04-01 12:00:00.000', '2023-04-01 12:01:00.000', '2023-04-01 12:02:00.000']
        })

        # Mengatur mock
        mock_read_csv.return_value = test_data
        mock_engine = MagicMock(spec=Engine)
        mock_create_engine.return_value = mock_engine

        # Menjalankan fungsi yang diuji
        result = load_to_postgresql('input.csv', 'fashion_table')

        # Memverifikasi hasil
        assert result is True
        mock_read_csv.assert_called_once_with('input.csv')
        mock_create_engine.assert_called_once()
        mock_to_sql.assert_called_once_with('fashion_table', mock_engine, if_exists='replace', index=False)

    @patch('utils.load.pd.read_csv')
    def test_load_to_postgresql_file_not_found(self, mock_read_csv):
        # Mengatur mock untuk menimbulkan FileNotFoundError
        mock_read_csv.side_effect = FileNotFoundError('File not found')

        # Menjalankan fungsi yang diuji
        result = load_to_postgresql('nonexistent.csv', 'fashion_table')

        # Memverifikasi hasil
        assert result is False

    @patch('utils.load.pd.read_csv')
    def test_load_to_postgresql_exception(self, mock_read_csv):
        # Mengatur mock untuk menimbulkan Exception umum
        mock_read_csv.side_effect = Exception('Error general')

        # Menjalankan fungsi yang diuji
        result = load_to_postgresql('input.csv', 'fashion_table')

        # Memverifikasi hasil
        assert result is False

    @patch('utils.load.pd.read_csv')
    @patch('utils.load.create_engine')
    @patch('utils.load.pd.DataFrame.to_sql')
    def test_load_to_postgresql_to_sql_exception(self, mock_to_sql, mock_create_engine, mock_read_csv):
        # Menguji penanganan exception pada df.to_sql
        test_data = pd.DataFrame({'A': [1, 2, 3]})
        mock_read_csv.return_value = test_data
        mock_engine = MagicMock(spec=Engine)
        mock_create_engine.return_value = mock_engine
        mock_to_sql.side_effect = Exception('to_sql failure')
        result = load_to_postgresql('input.csv', 'fashion_table')
        assert result is False

class TestLoadToGoogleSheets:
    @patch('utils.load.pd.read_csv')
    @patch('utils.load.service_account.Credentials.from_service_account_file')
    @patch('utils.load.build')
    def test_load_to_google_sheets_success(self, mock_build, mock_credentials, mock_read_csv):
        # Membuat DataFrame uji
        test_data = pd.DataFrame({
            'Title': ['Product 1', 'Product 2', 'Product 3'],
            'Price': [1600000.0, 3200000.0, 800000.0],
            'Rating': [4.8, 4.5, 3.9],
            'Color': [3, 2, 1],
            'Size': ['M', 'L', 'XL'],
            'Gender': ['Men', 'Women', 'Unisex'],
            'Timestamp': ['2023-04-01 12:00:00.000', '2023-04-01 12:01:00.000', '2023-04-01 12:02:00.000']
        })

        # Mengatur mock DataFrame
        mock_read_csv.return_value = test_data

        # Mengatur mock kredensial
        mock_credentials_instance = MagicMock()
        mock_credentials.return_value = mock_credentials_instance

        # Mengatur mock layanan Google Sheets
        mock_service = MagicMock()
        mock_sheets_service = MagicMock()
        mock_values = MagicMock()
        mock_update = MagicMock()

        mock_service.spreadsheets.return_value = mock_sheets_service
        mock_sheets_service.values.return_value = mock_values
        mock_values.update.return_value = mock_update
        mock_update.execute.return_value = {'updatedCells': 24}

        mock_build.return_value = mock_service

        # Menjalankan fungsi yang diuji
        result = load_to_google_sheets('input.csv', 'test_spreadsheet_id', 'Sheet1')

        # Memverifikasi hasil
        assert result is True
        mock_read_csv.assert_called_once_with('input.csv')
        mock_credentials.assert_called_once()
        mock_build.assert_called_once_with('sheets', 'v4', credentials=mock_credentials_instance)
        mock_values.update.assert_called_once()

    @patch('utils.load.pd.read_csv')
    def test_load_to_google_sheets_file_not_found(self, mock_read_csv):
        # Mengatur mock untuk menimbulkan FileNotFoundError
        mock_read_csv.side_effect = FileNotFoundError('File not found')

        # Menjalankan fungsi yang diuji
        result = load_to_google_sheets('nonexistent.csv', 'test_spreadsheet_id', 'Sheet1')

        # Memverifikasi hasil
        assert result is False

    @patch('utils.load.pd.read_csv')
    def test_load_to_google_sheets_exception(self, mock_read_csv):
        # Mengatur mock untuk menimbulkan Exception umum
        mock_read_csv.side_effect = Exception('Error general')

        # Menjalankan fungsi yang diuji
        result = load_to_google_sheets('input.csv', 'test_spreadsheet_id', 'Sheet1')

        # Memverifikasi hasil
        assert result is False

    @patch('utils.load.pd.read_csv')
    @patch('utils.load.service_account.Credentials.from_service_account_file')
    def test_load_to_google_sheets_credentials_file_not_found(self, mock_cred, mock_read_csv):
        # Menguji FileNotFoundError pada saat memuat credentials
        test_data = pd.DataFrame({'A': [1, 2, 3]})
        mock_read_csv.return_value = test_data
        mock_cred.side_effect = FileNotFoundError('credential file not found')
        result = load_to_google_sheets('input.csv', 'test_spreadsheet_id', 'Sheet1')
        assert result is False

# Tambahkan entrypoint agar test dapat dijalankan langsung
if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main([__file__]))
