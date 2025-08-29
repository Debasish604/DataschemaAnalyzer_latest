import pandas as pd
import logging
from .file_parser import BaseParser

class CSVParser(BaseParser):
    """Parser for CSV files"""
    
    def parse(self, file_path):
        """Parse CSV file and return pandas DataFrame"""
        try:
            # Try different encodings and separators
            encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
            separators = [',', ';', '\t', '|']
            
            for encoding in encodings:
                for sep in separators:
                    try:
                        df = pd.read_csv(file_path, encoding=encoding, sep=sep)
                        
                        # Check if we got meaningful data (more than 1 column or reasonable number of rows)
                        if len(df.columns) > 1 or len(df) > 0:
                            logging.info(f"Successfully parsed CSV with encoding={encoding}, separator='{sep}'")
                            return self._clean_dataframe(df)
                    
                    except Exception:
                        continue
            
            # If all attempts fail, try with default settings
            df = pd.read_csv(file_path)
            return self._clean_dataframe(df)
            
        except Exception as e:
            logging.error(f"Error parsing CSV file {file_path}: {str(e)}")
            raise Exception(f"Failed to parse CSV file: {str(e)}")
    
    def _clean_dataframe(self, df):
        """Clean and standardize the DataFrame"""
        # Remove completely empty rows and columns
        df = df.dropna(how='all').dropna(axis=1, how='all')
        
        # Strip whitespace from string columns
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].astype(str).str.strip()
        
        # Replace 'nan' strings with actual NaN
        df = df.replace(['nan', 'NaN', 'NULL', 'null', ''], pd.NA)
        
        return df
