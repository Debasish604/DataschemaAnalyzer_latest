import pandas as pd
import logging
from .file_parser import BaseParser

class ExcelParser(BaseParser):
    """Parser for Excel files (.xls and .xlsx)"""
    
    def parse(self, file_path):
        """Parse Excel file and return pandas DataFrame"""
        try:
            # Read Excel file - handle multiple sheets
            excel_file = pd.ExcelFile(file_path)
            
            # If multiple sheets, combine them or take the first substantial one
            all_sheets = {}
            
            for sheet_name in excel_file.sheet_names:
                try:
                    df = pd.read_excel(file_path, sheet_name=sheet_name)
                    
                    # Skip empty or very small sheets
                    if not df.empty and len(df) > 0:
                        all_sheets[sheet_name] = self._clean_dataframe(df)
                        
                except Exception as e:
                    logging.warning(f"Could not read sheet '{sheet_name}': {str(e)}")
                    continue
            
            if not all_sheets:
                # Try reading without specifying sheet
                df = pd.read_excel(file_path)
                return self._clean_dataframe(df)
            
            # Return the largest sheet by row count
            largest_sheet = max(all_sheets.items(), key=lambda x: len(x[1]))
            logging.info(f"Using sheet '{largest_sheet[0]}' with {len(largest_sheet[1])} rows")
            
            return largest_sheet[1]
            
        except Exception as e:
            logging.error(f"Error parsing Excel file {file_path}: {str(e)}")
            raise Exception(f"Failed to parse Excel file: {str(e)}")
    
    def _clean_dataframe(self, df):
        """Clean and standardize the DataFrame"""
        # Remove completely empty rows and columns
        df = df.dropna(how='all').dropna(axis=1, how='all')
        
        # Handle unnamed columns (common in Excel files)
        df.columns = [f'Column_{i}' if str(col).startswith('Unnamed:') else str(col) 
                     for i, col in enumerate(df.columns)]
        
        # Strip whitespace from string columns
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].astype(str).str.strip()
        
        # Replace common null representations
        df = df.replace(['nan', 'NaN', 'NULL', 'null', '', 'N/A', 'n/a'], pd.NA)
        
        return df
