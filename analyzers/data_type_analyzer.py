import pandas as pd
import numpy as np
import re
from datetime import datetime
import logging

class DataTypeAnalyzer:
    """Analyzer for identifying and classifying data types in DataFrame columns"""
    
    def __init__(self):
        self.date_patterns = [
            r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
            r'\d{2}/\d{2}/\d{4}',  # MM/DD/YYYY
            r'\d{2}-\d{2}-\d{4}',  # MM-DD-YYYY
            r'\d{4}/\d{2}/\d{2}',  # YYYY/MM/DD
            r'\d{2}\.\d{2}\.\d{4}', # DD.MM.YYYY
        ]
        
        self.id_patterns = [
            r'^[A-Z]{2,3}\d{3,}$',  # Prefix + numbers (e.g., ABC123)
            r'^\d{6,}$',            # Long numeric IDs
            r'^[A-Za-z0-9]{8,}$',   # Alphanumeric IDs
        ]
        
        self.name_indicators = [
            'name', 'first', 'last', 'full', 'fname', 'lname', 'firstname', 'lastname',
            'title', 'label', 'description', 'city', 'country', 'state', 'product',
            'company', 'organization', 'department'
        ]
    
    def analyze(self, df):
        """Analyze data types for all columns in DataFrame"""
        results = {}
        
        for column in df.columns:
            results[column] = self._analyze_column(df[column], column)
        
        # [Debug] Summary of analysis
        print("\n=== Data Type Analysis Summary ===")
        for col, info in results.items():
            print(f"\nColumn: {col}")
            print(f"  Pandas dtype   : {info['pandas_dtype']}")
            print(f"  Inferred type  : {info['inferred_type']} (confidence {info['confidence']:.2f})")
            print(f"  Nulls          : {info['null_count']}")
            print(f"  Unique values  : {info['unique_count']}")
            if info['characteristics']:
                print(f"  Characteristics: {', '.join(info['characteristics'])}")
            print(f"  Sample values  : {info['sample_values']}")

        
        return results
    
    
    def _analyze_column(self, series, column_name):
        """Analyze a single column and determine its data type characteristics"""
        analysis = {
            'column_name': column_name,
            'pandas_dtype': str(series.dtype),
            'inferred_type': 'unknown',
            'confidence': 0.0,
            'characteristics': [],
            'sample_values': self._get_sample_values(series),
            'null_count': series.isnull().sum(),
            'unique_count': series.nunique(),
            'total_count': len(series)
        }
        
        
        # Skip analysis if column is mostly null
        if analysis['null_count'] / analysis['total_count'] > 0.9:
            print(f"Column '{column_name}' is mostly null.")
            analysis['inferred_type'] = 'mostly_null'
            analysis['confidence'] = 0.9
            return analysis
        
        # Get non-null values for analysis
        non_null_series = series.dropna()
        
        if len(non_null_series) == 0:
            print(f"Column '{column_name}' is all null.")
            analysis['inferred_type'] = 'all_null'
            analysis['confidence'] = 1.0
            return analysis
        
        # Analyze based on various criteria
        type_scores = {}
        
        # Check for numeric types
        numeric_scores = self._check_numeric_types(non_null_series)
        type_scores.update(numeric_scores)
        
        # Check for date types
        date_scores = self._check_date_types(non_null_series)
        type_scores.update(date_scores)
        
        # Check for categorical/text types
        text_scores = self._check_text_types(non_null_series, column_name)
        type_scores.update(text_scores)
        
        # Check for ID types
        id_scores = self._check_id_types(non_null_series, column_name)
        type_scores.update(id_scores)
        
        # Determine best type based on scores
        if type_scores:
            best_type = max(type_scores, key=type_scores.get)
            analysis['inferred_type'] = best_type
            analysis['confidence'] = type_scores[best_type]
        else:
            print(f"No type could be inferred for '{column_name}'.")
        
        # Add characteristics
        analysis['characteristics'] = self._get_characteristics(non_null_series, analysis['inferred_type'])
        # print(f"Final analysis for '{column_name}': {analysis}") # debug for final analysis
        
        return analysis
    
    def _get_sample_values(self, series, n=5):
        """Get sample values from the series"""
        non_null = series.dropna()
        if len(non_null) == 0:
            return []
        
        sample_size = min(n, len(non_null))
        samples = non_null.head(sample_size).tolist()
        return samples
    
    def _check_numeric_types(self, series):
        """Check for numeric data types"""
        scores = {}
        
        try:
            numeric_series = pd.to_numeric(series, errors='coerce')
            non_null_numeric = numeric_series.dropna()
            
            if len(non_null_numeric) > 0:
                conversion_rate = len(non_null_numeric) / len(series)
                
                if conversion_rate > 0.8:
                    if all(x == int(x) for x in non_null_numeric if pd.notna(x)):
                        scores['integer'] = conversion_rate
                        
                        if all(x >= 0 for x in non_null_numeric):
                            scores['positive_integer'] = conversion_rate * 0.9
                        
                        if series.nunique() == len(series) and len(series) > 1:
                            scores['sequential_id'] = conversion_rate * 0.8
                    
                    else:
                        scores['float'] = conversion_rate
                        
                        if all(0 <= x <= 100 for x in non_null_numeric):
                            scores['percentage'] = conversion_rate * 0.8
                        
                        if all(x >= 0 for x in non_null_numeric) and any(x > 10 for x in non_null_numeric):
                            scores['monetary'] = conversion_rate * 0.7
        
        except Exception as e:
            print(f"Exception in _check_numeric_types: {e}")
        
        return scores
    
    def _check_date_types(self, series):
        """Check for date/time data types"""
        scores = {}
        
        str_series = series.astype(str)
        
        for pattern in self.date_patterns:
            matches = str_series.str.match(pattern).sum()
            if matches > 0:
                match_rate = matches / len(str_series)
                print(f"Pattern '{pattern}' match rate: {match_rate}")
                if match_rate > 0.5:
                    scores['date'] = match_rate
        
        try:
            parsed_dates = pd.to_datetime(series, errors='coerce')
            valid_dates = parsed_dates.dropna()
            
            if len(valid_dates) > 0:
                date_rate = len(valid_dates) / len(series)
                if date_rate > 0.5:
                    scores['datetime'] = date_rate
                    
                    date_range = valid_dates.max() - valid_dates.min()
                    
                    if date_range.days < 365:
                        scores['recent_date'] = date_rate * 0.9
                    elif date_range.days > 10000:
                        scores['historical_date'] = date_rate * 0.8
                    
                    current_year = datetime.now().year
                    birth_years = valid_dates.dt.year
                    if all(1900 <= year <= current_year - 10 for year in birth_years):
                        scores['birth_date'] = date_rate * 0.8
        
        except Exception as e:
            print(f"Exception in _check_date_types: {e}")
        
        return scores
    
    def _check_text_types(self, series, column_name):
        """Check for text/categorical data types"""
        scores = {}
        
        str_series = series.astype(str)
        uniqueness_ratio = series.nunique() / len(series)
        
        if uniqueness_ratio < 0.1:
            scores['categorical'] = 0.9
        elif uniqueness_ratio < 0.5:
            scores['limited_categorical'] = 0.7
        
        col_lower = column_name.lower()
        for name_indicator in self.name_indicators:
            if name_indicator in col_lower:
                scores['descriptive_name'] = 0.8
                break
        
        avg_length = str_series.str.len().mean()

        if avg_length < 3:
            scores['code'] = 0.6
        elif avg_length > 50:
            scores['long_text'] = 0.8
        elif 3 <= avg_length <= 20:
            scores['short_text'] = 0.7
        
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        email_matches = str_series.str.match(email_pattern).sum()
        if email_matches > 0:
            email_rate = email_matches / len(str_series)
            if email_rate > 0.8:
                scores['email'] = email_rate
        
        url_pattern = r'^https?://'
        url_matches = str_series.str.match(url_pattern).sum()
        if url_matches > 0:
            url_rate = url_matches / len(str_series)
            if url_rate > 0.8:
                scores['url'] = url_rate
        
        return scores
    
    def _check_id_types(self, series, column_name):
        """Check for ID/key data types"""
        scores = {}
        
        col_lower = column_name.lower()
        id_indicators = ['id', 'key', 'pk', 'primary', 'foreign', 'fk', 'code', 'ref']
        
        for indicator in id_indicators:
            if indicator in col_lower:
                scores['identifier'] = 0.8
                break
        
        uniqueness_ratio = series.nunique() / len(series)
        
        if uniqueness_ratio > 0.95:
            scores['unique_identifier'] = 0.9
        elif uniqueness_ratio > 0.8:
            scores['mostly_unique_identifier'] = 0.7
        
        str_series = series.astype(str)
        for pattern in self.id_patterns:
            matches = str_series.str.match(pattern).sum()
            if matches > 0:
                match_rate = matches / len(str_series)
                if match_rate > 0.7:
                    scores['formatted_id'] = match_rate
        
        return scores
    
    def _get_characteristics(self, series, inferred_type):
        """Get additional characteristics based on inferred type"""
        characteristics = []
        
        if series.nunique() == len(series):
            characteristics.append('unique_values')
        
        if series.isnull().sum() == 0:
            characteristics.append('no_missing_values')
        
        if inferred_type in ['integer', 'float', 'positive_integer']:
            numeric_series = pd.to_numeric(series, errors='coerce')
            characteristics.append(f'range: {numeric_series.min():.2f} to {numeric_series.max():.2f}')
            characteristics.append(f'mean: {numeric_series.mean():.2f}')
        
        elif inferred_type in ['date', 'datetime']:
            try:
                date_series = pd.to_datetime(series, errors='coerce')
                characteristics.append(f'date_range: {date_series.min()} to {date_series.max()}')
            except Exception as e:
                print(f"Exception in _get_characteristics (date): {e}")
        
        elif inferred_type in ['categorical', 'limited_categorical']:
            value_counts = series.value_counts()
            characteristics.append(f'most_common: {value_counts.index[0]} ({value_counts.iloc[0]} occurrences)')
        
        return characteristics
